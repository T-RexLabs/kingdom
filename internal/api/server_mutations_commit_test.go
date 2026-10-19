package api

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"
	"kingdom/internal/testutil/providerledger"

	"modernc.org/sqlite"
)

func TestServerCreateRunReconcilesCommitOutcomeAfterError(t *testing.T) {
	tests := []struct {
		name              string
		durable           bool
		cancelAfterCommit bool
		wantStatus        int
		wantRunCount      int
	}{
		{
			name:              "durable row succeeds after request cancellation",
			durable:           true,
			cancelAfterCommit: true,
			wantStatus:        http.StatusCreated,
			wantRunCount:      1,
		},
		{
			name:         "absent row preserves commit failure",
			wantStatus:   http.StatusInternalServerError,
			wantRunCount: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			workOrderID := seedWorkOrder(t, server)
			seedWorkflow(t, server, workflow.DefaultWorkflowID)

			ctx := context.Background()
			var cancel context.CancelFunc
			if test.cancelAfterCommit {
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			connector.failNextCommit(&runCreationCommitTestFault{
				durable: test.durable,
				afterCommit: func() {
					if cancel != nil {
						cancel()
					}
				},
			})

			response := invokeCreateRunHandler(t, server, ctx, map[string]any{
				"work_order_id":   workOrderID,
				"publish_mode":    "direct",
				"model_selection": defaultTestRunModelSelectionBody(),
			})
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d, want %d; body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.cancelAfterCommit && ctx.Err() != context.Canceled {
				t.Fatalf("request context error=%v, want cancellation from simulated commit", ctx.Err())
			}

			var runCount int
			if err := db.QueryRow(`SELECT COUNT(*) FROM workflow_runs WHERE work_order_id = ?`, workOrderID).Scan(&runCount); err != nil {
				t.Fatalf("count created runs: %v", err)
			}
			if runCount != test.wantRunCount {
				t.Fatalf("created run count=%d, want %d", runCount, test.wantRunCount)
			}

			if test.durable {
				var run gen.RunSummary
				if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
					t.Fatalf("decode created run response: %v", err)
				}
				if run.RunId == "" || run.WorkOrderId != workOrderID || run.Status != "pending" {
					t.Fatalf("unexpected created run response: %#v", run)
				}
				var storedWorkOrderID string
				if err := db.QueryRow(`SELECT work_order_id FROM workflow_runs WHERE run_id = ?`, run.RunId).Scan(&storedWorkOrderID); err != nil {
					t.Fatalf("read durable run row: %v", err)
				}
				if storedWorkOrderID != workOrderID {
					t.Fatalf("durable run work_order_id=%q, want %q", storedWorkOrderID, workOrderID)
				}
			} else {
				assertErrorCode(t, response.Body.Bytes(), "internal_error")
			}
		})
	}
}

func TestServerCreateRunKeepsReservationPendingWhenCommitOutcomeIsUnknown(t *testing.T) {
	server, db, connector := newAPIServerForCommitOutcomeTest(t)
	workOrderID := seedWorkOrder(t, server)
	seedWorkflow(t, server, workflow.DefaultWorkflowID)

	catalog := &runCreationReconciliationFaultCatalog{connector: connector}
	server.providerModelCatalog = catalog
	key := testIdempotencyKey(t, "ambiguous-run-create", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{
			method:         http.MethodPost,
			path:           "/api/v1/runs",
			idempotencyKey: key,
			body: map[string]any{
				"work_order_id":   workOrderID,
				"publish_mode":    "direct",
				"model_selection": defaultTestRunModelSelectionBody(),
			},
		})
	}

	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("first request status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_run", "control_plane", true)
	firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
	runID, ok := firstError.Error.Details["run_id"].(string)
	if !ok || strings.TrimSpace(runID) == "" {
		t.Fatalf("expected safe run_id in pending error details, got %#v", firstError.Error.Details)
	}
	var durableRunID string
	if err := db.QueryRow(`SELECT run_id FROM workflow_runs WHERE work_order_id = ?`, workOrderID).Scan(&durableRunID); err != nil {
		t.Fatalf("read durable run ID: %v", err)
	}
	if runID != durableRunID {
		t.Fatalf("pending error run_id=%q, durable run_id=%q", runID, durableRunID)
	}
	assertPendingCreateRunReservation(t, db, key)
	assertCreateRunCount(t, db, workOrderID, 1)

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCreateRunReservation(t, db, key)
	assertCreateRunCount(t, db, workOrderID, 1)
	if got := catalog.listCalls.Load(); got != 1 {
		t.Fatalf("create-run handler model lookup calls=%d, want 1; same-key retry must not reexecute", got)
	}
}

func assertPendingCreateRunReservation(t *testing.T, db *sql.DB, key string) {
	t.Helper()

	var status string
	var responseStatus int
	var responseBody string
	if err := db.QueryRow(`SELECT status, response_status, response_body_json FROM idempotency_keys WHERE client_key = ?`, key).Scan(&status, &responseStatus, &responseBody); err != nil {
		t.Fatalf("read create-run idempotency reservation: %v", err)
	}
	if status != state.IdempotencyStatusPending || responseStatus != 0 || responseBody != "" {
		t.Fatalf("create-run reservation status=%q response_status=%d response_body=%q, want pending without a finalized response", status, responseStatus, responseBody)
	}
}

func assertCreateRunCount(t *testing.T, db *sql.DB, workOrderID string, want int) {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM workflow_runs WHERE work_order_id = ?`, workOrderID).Scan(&count); err != nil {
		t.Fatalf("count create-run rows: %v", err)
	}
	if count != want {
		t.Fatalf("run count for work order %q=%d, want %d", workOrderID, count, want)
	}
}

func invokeCreateRunHandler(t *testing.T, server *Server, ctx context.Context, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal create-run body: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/v1/runs", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build create-run request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleCreateRun(response, req)
	return response
}

func newAPIServerForCommitOutcomeTest(t *testing.T) (*Server, *sql.DB, *runCreationCommitTestConnector) {
	return newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{})
}

func newAPIServerForCommitOutcomeTestWithConfig(t *testing.T, cfg ServerConfig) (*Server, *sql.DB, *runCreationCommitTestConnector) {
	t.Helper()

	connector := &runCreationCommitTestConnector{base: &sqlite.Driver{}, dsn: ":memory:"}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(providerledger.Register(db))

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys pragma: %v", err)
	}
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	seedRoles(t, db)

	cfg.DB = db
	handler, _ := newAPIServerForTestWithConfig(t, cfg)
	return handler.(*Server), db, connector
}

type runCreationCommitInjectedError struct{}

func (runCreationCommitInjectedError) Error() string {
	return "injected ambiguous commit error"
}

type runCreationCommitTestFault struct {
	durable            bool
	commitWithoutError bool
	afterCommit        func()
}

type runCreationCommitTestConnector struct {
	base                           driver.Driver
	dsn                            string
	nextFault                      atomic.Pointer[runCreationCommitTestFault]
	nextCommitArm                  atomic.Pointer[runCreationCommitTestCommitArm]
	nextQueryFailure               atomic.Pointer[runCreationCommitTestQueryFailure]
	nextReconciliationQueryFailure atomic.Bool
}

type runCreationCommitTestCommitArm struct {
	queryFragment string
	fault         *runCreationCommitTestFault
}

type runCreationCommitTestQueryFailure struct {
	queryFragment string
	remaining     int
	skip          int
}

func (c *runCreationCommitTestConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.open(c.dsn)
}

func (c *runCreationCommitTestConnector) Driver() driver.Driver {
	return c
}

func (c *runCreationCommitTestConnector) Open(name string) (driver.Conn, error) {
	return c.open(name)
}

func (c *runCreationCommitTestConnector) open(name string) (driver.Conn, error) {
	conn, err := c.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &runCreationCommitTestConn{Conn: conn, connector: c}, nil
}

func (c *runCreationCommitTestConnector) failNextCommit(fault *runCreationCommitTestFault) {
	c.nextFault.Store(fault)
}

func (c *runCreationCommitTestConnector) failNextCommitAfterExec(queryFragment string, fault *runCreationCommitTestFault) {
	c.nextCommitArm.Store(&runCreationCommitTestCommitArm{queryFragment: normalizeCommitTestQuery(queryFragment), fault: fault})
}

func (c *runCreationCommitTestConnector) commitNextAfterExec(queryFragment string, afterCommit func()) {
	c.failNextCommitAfterExec(queryFragment, &runCreationCommitTestFault{durable: true, commitWithoutError: true, afterCommit: afterCommit})
}

func (c *runCreationCommitTestConnector) failNextReconciliationQuery() {
	c.nextReconciliationQueryFailure.Store(true)
}

func (c *runCreationCommitTestConnector) failNextQuery(queryFragment string) {
	c.failNextQueries(queryFragment, 1)
}

func (c *runCreationCommitTestConnector) failNextQueries(queryFragment string, count int) {
	if count <= 0 {
		return
	}
	c.nextQueryFailure.Store(&runCreationCommitTestQueryFailure{queryFragment: normalizeCommitTestQuery(queryFragment), remaining: count})
}

func (c *runCreationCommitTestConnector) failQueryAfter(queryFragment string, successfulMatches int) {
	if successfulMatches < 0 {
		successfulMatches = 0
	}
	c.nextQueryFailure.Store(&runCreationCommitTestQueryFailure{
		queryFragment: normalizeCommitTestQuery(queryFragment),
		remaining:     1,
		skip:          successfulMatches,
	})
}

type runCreationCommitTestConn struct {
	driver.Conn
	connector *runCreationCommitTestConnector
}

func (c *runCreationCommitTestConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return conn.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

func (c *runCreationCommitTestConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &runCreationCommitTestTx{Tx: tx, connector: c.connector}, nil
}

func (c *runCreationCommitTestConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if conn, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = conn.BeginTx(ctx, opts)
	} else {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if opts != (driver.TxOptions{}) {
			return nil, errors.New("transaction options are unsupported by the test driver")
		}
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &runCreationCommitTestTx{Tx: tx, connector: c.connector}, nil
}

func (c *runCreationCommitTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if conn, ok := c.Conn.(driver.ExecerContext); ok {
		result, err := conn.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}
		if arm := c.connector.nextCommitArm.Load(); arm != nil && strings.Contains(normalizeCommitTestQuery(query), arm.queryFragment) && c.connector.nextCommitArm.CompareAndSwap(arm, nil) {
			c.connector.failNextCommit(arm.fault)
		}
		return result, nil
	}
	return nil, driver.ErrSkip
}

func (c *runCreationCommitTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if isRunCreationReconciliationQuery(query) && c.connector.nextReconciliationQueryFailure.Swap(false) {
		return nil, errors.New("injected run reconciliation lookup failure")
	}
	for failure := c.connector.nextQueryFailure.Load(); failure != nil && strings.Contains(normalizeCommitTestQuery(query), failure.queryFragment); failure = c.connector.nextQueryFailure.Load() {
		if failure.skip > 0 {
			next := &runCreationCommitTestQueryFailure{queryFragment: failure.queryFragment, remaining: failure.remaining, skip: failure.skip - 1}
			if c.connector.nextQueryFailure.CompareAndSwap(failure, next) {
				break
			}
			continue
		}
		var next *runCreationCommitTestQueryFailure
		if failure.remaining > 1 {
			next = &runCreationCommitTestQueryFailure{queryFragment: failure.queryFragment, remaining: failure.remaining - 1, skip: failure.skip}
		}
		if c.connector.nextQueryFailure.CompareAndSwap(failure, next) {
			return nil, errors.New("injected commit outcome readback failure")
		}
	}
	if conn, ok := c.Conn.(driver.QueryerContext); ok {
		return conn.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func normalizeCommitTestQuery(query string) string {
	return strings.ToLower(strings.Join(strings.Fields(query), " "))
}

func isRunCreationReconciliationQuery(query string) bool {
	const reconciliationQuery = "SELECT 1 FROM workflow_runs WHERE run_id = ? AND work_order_id = ? LIMIT 1"
	return strings.EqualFold(strings.Join(strings.Fields(query), " "), reconciliationQuery)
}

type runCreationReconciliationFaultCatalog struct {
	connector *runCreationCommitTestConnector
	listCalls atomic.Int32
}

func (c *runCreationReconciliationFaultCatalog) List(_ context.Context, _ bool) ([]gen.ProviderModel, error) {
	c.listCalls.Add(1)
	c.connector.failNextCommit(&runCreationCommitTestFault{durable: true})
	c.connector.failNextReconciliationQuery()
	return defaultTestProviderModels(), nil
}

func (c *runCreationReconciliationFaultCatalog) Snapshot(_ context.Context, _ bool) (ProviderModelCatalogSnapshot, error) {
	return ProviderModelCatalogSnapshot{Items: defaultTestProviderModels(), Status: ProviderModelCatalogStatusFresh}, nil
}

type runCreationCommitTestTx struct {
	driver.Tx
	connector *runCreationCommitTestConnector
}

func (tx *runCreationCommitTestTx) Commit() error {
	fault := tx.connector.nextFault.Swap(nil)
	if fault == nil {
		return tx.Tx.Commit()
	}
	if !fault.durable {
		return errors.Join(runCreationCommitInjectedError{}, tx.Tx.Rollback())
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if fault.afterCommit != nil {
		fault.afterCommit()
	}
	if fault.commitWithoutError {
		return nil
	}
	return runCreationCommitInjectedError{}
}
