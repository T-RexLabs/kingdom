package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"modernc.org/sqlite"
)

// Capture the actual production query so the plan assertion cannot silently
// drift onto a simplified SQL copy that the store no longer executes.
type outboxQueryConnector struct {
	query string
	args  []any
}

func (c *outboxQueryConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *outboxQueryConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(":memory:")
	if err != nil {
		return nil, err
	}
	return &outboxQueryConn{Conn: conn, capture: c}, nil
}

type outboxQueryConn struct {
	driver.Conn
	capture *outboxQueryConnector
}

func (c *outboxQueryConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "SELECT") && strings.Contains(query, "FROM attempt_outbox ao") {
		c.capture.query = query
		c.capture.args = nil
		for _, arg := range args {
			c.capture.args = append(c.capture.args, arg.Value)
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestPendingOutboxPagesUseBoundedIndexSeeks(t *testing.T) {
	capture := &outboxQueryConnector{}
	db := sql.OpenDB(capture)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := New(db)
	ctx := context.Background()
	for _, tc := range []struct {
		name           string
		after, through OutboxCursor
		wantLower      bool
		wantUpper      bool
	}{
		{name: "both", after: OutboxCursor{"2026-09-30T12:00:00Z", "outbox_0128"}, through: OutboxCursor{"2026-09-30T12:00:00Z", "outbox_0257"}, wantLower: true, wantUpper: true},
		{name: "lower", after: OutboxCursor{"2026-09-30T12:00:00Z", "outbox_0128"}, wantLower: true},
		{name: "upper", through: OutboxCursor{"2026-09-30T12:00:00Z", "outbox_0257"}, wantUpper: true},
		{name: "unbounded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.ListPendingOutboxAfter(ctx, tc.after, tc.through, 64); err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+capture.query, capture.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan strings.Builder
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				plan.WriteString(detail + "\n")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			t.Log(plan.String())
			if strings.Contains(plan.String(), "TEMP B-TREE") || !strings.Contains(plan.String(), "idx_attempt_outbox_status_created") {
				t.Fatalf("outbox page must stream in index order without sorting: %s", plan.String())
			}
			if tc.wantLower && !strings.Contains(plan.String(), "(created_at,outbox_id)>(?,?)") {
				t.Fatalf("outbox page must seek past its cursor instead of rescanning the pending prefix: %s", plan.String())
			}
			if tc.wantUpper && !strings.Contains(plan.String(), "(created_at,outbox_id)<(?,?)") {
				t.Fatalf("outbox page must stop at its captured upper boundary: %s", plan.String())
			}
		})
	}
}

func TestPendingOutboxPagesPreserveEqualTimestampBounds(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	store := New(db)
	ctx := context.Background()
	const stamp = "2026-09-30T12:00:00Z"
	const attemptID = "run_1:decree_intake:1"
	if err := store.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	queue := func(index int) {
		t.Helper()
		if err := store.QueueOutbox(ctx, QueueOutboxRequest{OutboxID: fmt.Sprintf("outbox_%04d", index), AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalSeq: index, TerminalState: "committed_success", PayloadJSON: `{}`, CreatedAt: stamp}); err != nil {
			t.Fatal(err)
		}
	}
	const original = 257
	for i := 1; i <= original; i++ {
		queue(i)
	}
	through, err := store.PendingOutboxHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	queue(original + 1)
	var after OutboxCursor
	visited := 0
	for {
		page, err := store.ListPendingOutboxAfter(ctx, after, through, 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page {
			visited++
			if want := fmt.Sprintf("outbox_%04d", visited); record.OutboxID != want || record.CreatedAt != stamp {
				t.Fatalf("equal-timestamp page skipped, duplicated or exceeded its boundary: got=%s want=%s", record.OutboxID, want)
			}
			after = OutboxCursor{record.CreatedAt, record.OutboxID}
		}
		if len(page) < 64 {
			break
		}
	}
	if visited != original || after != through {
		t.Fatalf("finite page cycle ended at %v after %d records, want %v/%d", after, visited, through, original)
	}
	remaining, err := store.ListPendingOutboxAfter(ctx, after, OutboxCursor{}, 64)
	if err != nil || len(remaining) != 1 || remaining[0].OutboxID != "outbox_0258" {
		t.Fatalf("later record must remain available beyond prior upper boundary: records=%v err=%v", remaining, err)
	}
}
