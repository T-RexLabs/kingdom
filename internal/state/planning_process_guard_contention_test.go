package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// Commit through another real connection after acknowledgement reads its WAL
// snapshot, but before its insert. This produces SQLITE_BUSY_SNAPSHOT without
// adding production hooks or depending on goroutine scheduling.
type planningGuardWALConnector struct {
	dsn         string
	beforeWrite func()
	afterWrite  func()
	begins      int
	insertIDs   []string
	insertErrs  []error
}

func (c *planningGuardWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &planningGuardWALConn{Conn: conn, connector: c}, nil
}

func (c *planningGuardWALConnector) Driver() driver.Driver { return &sqlite.Driver{} }

type planningGuardWALConn struct {
	driver.Conn
	connector *planningGuardWALConnector
}

func (c *planningGuardWALConn) Begin() (driver.Tx, error) {
	c.connector.begins++
	return c.Conn.Begin()
}

func (c *planningGuardWALConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(query, "INSERT INTO provider_process_guards") {
		return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	}
	c.connector.insertIDs = append(c.connector.insertIDs, args[0].Value.(string))
	before, after := c.connector.beforeWrite, c.connector.afterWrite
	c.connector.beforeWrite, c.connector.afterWrite = nil, nil
	if before != nil {
		before()
	}
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	c.connector.insertErrs = append(c.connector.insertErrs, err)
	if after != nil {
		after()
	}
	return result, err
}

func TestPlanningProcessGuardAcknowledgementRetriesConcurrentWALWriter(t *testing.T) {
	for _, sameGuard := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_guard_%t", sameGuard), func(t *testing.T) {
			store, other, connector, ownership := newPlanningGuardWALFixture(t)
			concurrent := ownership
			if !sameGuard {
				concurrent.GuardID = "guard_concurrent"
				concurrent.CommandOrdinal++
				concurrent.Capability = "independent-capability"
			}
			connector.beforeWrite = func() {
				if err := other.AcknowledgePlanningProcessGuard(context.Background(), concurrent); err != nil {
					t.Fatalf("acknowledge concurrent guard: %v", err)
				}
			}
			if err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership); err != nil {
				t.Fatalf("acknowledgement must recover its invalidated WAL snapshot: %v", err)
			}
			assertPlanningGuardSnapshotConflict(t, connector)
			if connector.begins != 2 || len(connector.insertIDs) != 2 || connector.insertIDs[0] != ownership.GuardID || connector.insertIDs[1] != ownership.GuardID {
				t.Fatalf("retry must use a new transaction and the same guard: begins=%d inserts=%v", connector.begins, connector.insertIDs)
			}
			var count int
			if err := other.db.QueryRow(`SELECT COUNT(*) FROM provider_process_guards`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 2
			if sameGuard {
				want = 1
			}
			if count != want {
				t.Fatalf("unexpected guard count after retry: got=%d want=%d", count, want)
			}
			if err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership); err != nil {
				t.Fatalf("exact replay after retry: %v", err)
			}
		})
	}
}

func TestPlanningProcessGuardAcknowledgementRechecksAuthorityAfterWALConflict(t *testing.T) {
	cases := []struct {
		name, query, want string
	}{
		{"lease_owner", `UPDATE planning_operations SET lease_owner = 'new-owner'`, "lost authority"},
		{"lease_version", `UPDATE planning_operations SET lease_version = lease_version + 1`, "lost authority"},
		{"active_invocation", `UPDATE planning_operations SET active_provider_invocation_id = NULL`, "lost authority"},
		{"operation_status", `UPDATE planning_operations SET status = 'succeeded'`, "lost authority"},
		{"expired_lease", `UPDATE planning_operations SET lease_expires_at = ?`, "lease expired"},
		{"settled_success", `UPDATE planning_provider_invocations SET invocation_state = 'settled_success'`, "invocation is terminal"},
		{"settled_failed", `UPDATE planning_provider_invocations SET invocation_state = 'settled_failed'`, "invocation is terminal"},
		{"settled_cancelled", `UPDATE planning_provider_invocations SET invocation_state = 'settled_cancelled'`, "invocation is terminal"},
		{"superseded", `UPDATE planning_provider_invocations SET invocation_state = 'superseded'`, "invocation is terminal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, other, connector, ownership := newPlanningGuardWALFixture(t)
			connector.beforeWrite = func() {
				var args []any
				if tc.name == "expired_lease" {
					// The original acknowledgement time is before this expiry;
					// only checking fresh wall time detects the lost lease.
					args = []any{time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)}
				}
				if _, err := other.db.Exec(tc.query, args...); err != nil {
					t.Fatalf("change planning authority: %v", err)
				}
			}
			err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership)
			assertPlanningGuardSnapshotConflict(t, connector)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("retry must reject changed authority (%s): %v", tc.want, err)
			}
			if connector.begins != 2 || len(connector.insertIDs) != 1 {
				t.Fatalf("retry must recheck authority before inserting: begins=%d inserts=%v", connector.begins, connector.insertIDs)
			}
			assertPlanningGuardNotAcknowledged(t, other, ownership.GuardID)
		})
	}
}

func TestPlanningProcessGuardAcknowledgementRejectsConflictingConcurrentReplay(t *testing.T) {
	store, other, connector, ownership := newPlanningGuardWALFixture(t)
	concurrent := ownership
	concurrent.Capability = "different-capability"
	connector.beforeWrite = func() {
		if err := other.AcknowledgePlanningProcessGuard(context.Background(), concurrent); err != nil {
			t.Fatal(err)
		}
	}
	err := store.AcknowledgePlanningProcessGuard(context.Background(), ownership)
	assertPlanningGuardSnapshotConflict(t, connector)
	if err == nil || !strings.Contains(err.Error(), "acknowledgement conflict") || connector.begins != 2 {
		t.Fatalf("retry must preserve exact guard conflict detection: err=%v begins=%d", err, connector.begins)
	}
	var capabilityHash string
	if err := other.db.QueryRow(`SELECT quiescence_capability_hash FROM provider_process_guards WHERE guard_id = ?`, ownership.GuardID).Scan(&capabilityHash); err != nil {
		t.Fatal(err)
	}
	if capabilityHash != sha256HexPlanningGuard(concurrent.Capability) {
		t.Fatal("retry replaced the concurrently persisted guard capability")
	}
}

func TestPlanningProcessGuardAcknowledgementCancellationStopsWALRetry(t *testing.T) {
	store, other, connector, ownership := newPlanningGuardWALFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.beforeWrite = func() {
		if _, err := other.db.Exec(`UPDATE planning_sessions SET title = 'Concurrent title'`); err != nil {
			t.Fatal(err)
		}
	}
	connector.afterWrite = cancel
	err := store.AcknowledgePlanningProcessGuard(ctx, ownership)
	assertPlanningGuardSnapshotConflict(t, connector)
	if !errors.Is(err, context.Canceled) || connector.begins != 1 || len(connector.insertIDs) != 1 {
		t.Fatalf("cancellation must stop before retry: err=%v begins=%d inserts=%v", err, connector.begins, connector.insertIDs)
	}
	assertPlanningGuardNotAcknowledged(t, other, ownership.GuardID)
}

func newPlanningGuardWALFixture(t *testing.T) (*PlanningStore, *PlanningStore, *planningGuardWALConnector, PlanningProcessGuardOwnership) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", filepath.Join(t.TempDir(), "planning.db"))
	otherDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	other, operation := newPlanningProviderSettlementTestStoreWithDB(t, otherDB)
	now := time.Now().UTC()
	if _, err := otherDB.Exec(`UPDATE planning_operations SET lease_expires_at = ? WHERE operation_id = ?`, now.Add(5*time.Minute).Format(time.RFC3339Nano), operation.OperationID); err != nil {
		t.Fatal(err)
	}
	connector := &planningGuardWALConnector{dsn: dsn}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	stamp := now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	return NewPlanningStore(db), other, connector, PlanningProcessGuardOwnership{
		GuardID: "guard_wal", PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID,
		CommandOrdinal: 1, Platform: "unix_process_group", GuardianPID: 101, GuardianIdentityToken: "guardian",
		ChildPID: 102, ChildIdentityToken: "child", TreeID: "102", TreeIdentityToken: "tree",
		Capability: "capability-wal", LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion,
		StartedAt: stamp, AcknowledgedAt: stamp,
	}
}

func assertPlanningGuardSnapshotConflict(t *testing.T, connector *planningGuardWALConnector) {
	t.Helper()
	var coded interface{ Code() int }
	if len(connector.insertErrs) == 0 || !errors.As(connector.insertErrs[0], &coded) || coded.Code() != 517 {
		t.Fatalf("test must produce real SQLITE_BUSY_SNAPSHOT (517): errors=%v", connector.insertErrs)
	}
}

func assertPlanningGuardNotAcknowledged(t *testing.T, store *PlanningStore, guardID string) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("guard %q persisted after acknowledgement rejection", guardID)
	}
}
