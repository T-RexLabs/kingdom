package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
)

// The first agent update occurs after terminalization has selected the live
// attempts. Commit a real preparation heartbeat on another WAL connection at
// that boundary, invalidating the cancellation transaction's read snapshot.
type cancelPreparationWALConnector struct {
	dsn         string
	base        driver.Driver
	beforeWrite func()
	armed       atomic.Bool
	reads       atomic.Int32
	writes      atomic.Int32
}

func (c *cancelPreparationWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &cancelPreparationWALConn{Conn: conn, connector: c}, nil
}

func (c *cancelPreparationWALConnector) Driver() driver.Driver { return c.base }

type cancelPreparationWALConn struct {
	driver.Conn
	connector *cancelPreparationWALConnector
}

func (c *cancelPreparationWALConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *cancelPreparationWALConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.connector.armed.Load() && strings.Contains(query, "FROM node_runs nr") && strings.Contains(query, "LEFT JOIN attempt_executions ae") {
		c.connector.reads.Add(1)
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *cancelPreparationWALConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.connector.armed.Load() && strings.HasPrefix(strings.TrimSpace(query), "UPDATE agents") {
		if c.connector.writes.Add(1) == 1 {
			c.connector.beforeWrite()
		}
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestServerCancelQueuedPreparationsRetriesConcurrentWALWriter(t *testing.T) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", filepath.Join(t.TempDir(), "cancel.db"))
	otherDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	migrations, err := dbmigrations.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range migrations {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatal(err)
		}
		mustExecSQL(t, otherDB, string(raw))
	}
	seedRoles(t, otherDB)
	connector := &cancelPreparationWALConnector{dsn: dsn, base: &sqlite.Driver{}}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{DB: db})
	decreeID := seedDecree(t, handler)
	roles := []string{"burgher-delegate", "clergy-delegate", "noble-delegate", "peasant-delegate"}
	nodes := make([]map[string]any, 0, len(roles))
	for index, role := range roles {
		nodes = append(nodes, map[string]any{"node_id": fmt.Sprintf("advisor_%d", index), "required_role": role, "node_class": workflow.NodeClassAdvisorConsultation})
	}
	published := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost, path: "/api/v1/workflows/publish", idempotencyKey: "cancel-preparation-workflow",
		body: map[string]any{"workflow_id": workflow.DefaultWorkflowID, "version": "cancel-preparation-v1", "definition_json": map[string]any{
			"version": "v1", "workflow_id": workflow.DefaultWorkflowID, "nodes": nodes, "edges": []map[string]any{},
		}},
	})
	if published.Code != http.StatusCreated {
		t.Fatalf("publish advisor workflow: %d %s", published.Code, published.Body.String())
	}
	created := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost, path: "/api/v1/runs", idempotencyKey: "cancel-preparation-run",
		body: map[string]any{"decree_id": decreeID, "publish_mode": "direct", "model_selection": defaultTestRunModelSelectionBody()},
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create advisor run: %d %s", created.Code, created.Body.String())
	}
	var run gen.RunSummary
	if err := json.Unmarshal(created.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	setRunStatus(t, db, run.RunId, "running")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	leaseUntil := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	for index, role := range roles {
		nodeID, nodeRunID, agentID := fmt.Sprintf("advisor_%d", index), fmt.Sprintf("nr_cancel_prep_%d", index), fmt.Sprintf("agent_cancel_prep_%d", index)
		mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, spawn_depth, created_at, updated_at) VALUES (?, ?, 'busy', 0, ?, ?)`, agentID, role, now, now)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES (?, ?, ?, 1, 1, 'ready', ?, ?, 1)`, nodeRunID, run.RunId, nodeID, role, agentID)
		mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, created_at, updated_at) VALUES (?, ?, ?, 'queued', 'provider_backed', 'server_response', ?, ?, 1, '', ?, ?)`, run.RunId+":"+nodeID+":1", run.RunId, nodeRunID, fmt.Sprintf("preparation_%d", index), leaseUntil, now, now)
	}
	var writerErr error
	connector.beforeWrite = func() {
		_, writerErr = otherDB.Exec(`UPDATE attempt_executions SET lease_expires_at = ? WHERE attempt_id = ?`, time.Now().UTC().Add(2*time.Minute).Format(time.RFC3339Nano), run.RunId+":advisor_0:1")
	}
	connector.armed.Store(true)
	spec := requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + run.RunId + "/cancel", body: map[string]any{"reason": "cancel active preparation"}, idempotencyKey: "cancel-four-preparations"}
	res := performJSONRequest(t, handler, spec)
	if writerErr != nil || connector.reads.Load() == 0 || connector.writes.Load() == 0 {
		t.Fatalf("WAL interleaving not exercised: reads=%d writes=%d writer=%v", connector.reads.Load(), connector.writes.Load(), writerErr)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("cancel must retry its invalidated WAL snapshot: status=%d body=%s", res.Code, res.Body.String())
	}
	if connector.reads.Load() < 2 || connector.writes.Load() < 5 {
		t.Fatalf("expected a fresh cancellation transaction after WAL conflict: reads=%d writes=%d", connector.reads.Load(), connector.writes.Load())
	}
	var summary gen.RunSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Status != gen.RunSummaryStatusCancelled {
		t.Fatalf("cancel response status=%q", summary.Status)
	}
	replayed := performJSONRequest(t, handler, spec)
	if replayed.Code != http.StatusOK || replayed.Body.String() != res.Body.String() {
		t.Fatalf("idempotent cancel replay changed response: status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	for label, query := range map[string]string{
		"cancelled nodes":                `SELECT COUNT(*) FROM node_runs WHERE run_id = ? AND node_status = 'cancelled'`,
		"fenced attempts":                `SELECT COUNT(*) FROM attempt_executions WHERE run_id = ? AND state = 'committed_cancelled' AND lease_owner = '' AND lease_expires_at IS NULL AND session_ref = ''`,
		"pending stops without sessions": `SELECT COUNT(*) FROM attempt_cancel_outbox WHERE run_id = ? AND status = 'pending' AND session_id = ''`,
		"single terminal events":         `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_type = 'attempt.cancelled'`,
	} {
		var count int
		if err := db.QueryRow(query, run.RunId).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 4 {
			t.Fatalf("%s=%d, want four", label, count)
		}
	}
	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&status); err != nil || status != "cancelled" {
		t.Fatalf("durable run status=%q err=%v", status, err)
	}
}
