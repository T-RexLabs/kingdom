package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"modernc.org/sqlite"
	"path/filepath"
	"strings"
	"testing"
)

type cancellationContentionConnector struct {
	dsn         string
	beforeWrite func()
}

func (c *cancellationContentionConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *cancellationContentionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &cancellationContentionConn{Conn: conn, c: c}, nil
}

type cancellationContentionConn struct {
	driver.Conn
	c *cancellationContentionConnector
}

func (c *cancellationContentionConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "UPDATE provider_requests SET admission_closed_at=") && c.c.beforeWrite != nil {
		f := c.c.beforeWrite
		c.c.beforeWrite = nil
		f()
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func TestQueueAttemptCancelRetriesConcurrentWALWriter(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "cancel.db")
	connector := &cancellationContentionConnector{dsn: dsn}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	defer db.Close()
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	s, req := requestActivationFixture(t, db)
	other, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := s.ActivateProviderInvocation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	connector.beforeWrite = func() {
		if _, err := other.Exec(`UPDATE attempt_executions SET updated_at='concurrent write' WHERE attempt_id=?`, req.AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	err = s.QueueAttemptCancel(context.Background(), QueueAttemptCancelRequest{CancelOutboxID: "cancel-contention", AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, RequestedAt: req.OpenedAt})
	if err != nil {
		t.Fatalf("cancellation must retry invalidated WAL snapshot: %v", err)
	}
}
