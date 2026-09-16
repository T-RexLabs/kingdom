package sqlitex

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

type codedError struct {
	code int
	text string
}

func (e codedError) Error() string { return e.text }
func (e codedError) Code() int     { return e.code }

type scriptedDriver struct {
	mu        sync.Mutex
	beginErr  []error
	execErr   []error
	commitErr []error
	commits   int
}

func (d *scriptedDriver) Open(string) (driver.Conn, error) { return &scriptedConn{driver: d}, nil }

type scriptedConn struct{ driver *scriptedDriver }

func (c *scriptedConn) Prepare(string) (driver.Stmt, error) {
	return scriptedStmt{driver: c.driver}, nil
}
func (c *scriptedConn) Close() error { return nil }
func (c *scriptedConn) Begin() (driver.Tx, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	if len(c.driver.beginErr) > 0 {
		err := c.driver.beginErr[0]
		c.driver.beginErr = c.driver.beginErr[1:]
		return nil, err
	}
	return scriptedTx{driver: c.driver}, nil
}

type scriptedStmt struct{ driver *scriptedDriver }

func (s scriptedStmt) Close() error  { return nil }
func (s scriptedStmt) NumInput() int { return -1 }
func (s scriptedStmt) Exec([]driver.Value) (driver.Result, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if len(s.driver.execErr) > 0 {
		err := s.driver.execErr[0]
		s.driver.execErr = s.driver.execErr[1:]
		return nil, err
	}
	return driver.RowsAffected(1), nil
}
func (scriptedStmt) Query([]driver.Value) (driver.Rows, error) { return nil, driver.ErrSkip }

type scriptedTx struct{ driver *scriptedDriver }

func (t scriptedTx) Commit() error {
	t.driver.mu.Lock()
	defer t.driver.mu.Unlock()
	if len(t.driver.commitErr) > 0 {
		err := t.driver.commitErr[0]
		t.driver.commitErr = t.driver.commitErr[1:]
		return err
	}
	t.driver.commits++
	return nil
}
func (scriptedTx) Rollback() error { return nil }

func openScriptedDB(t *testing.T, d *scriptedDriver) *sql.DB {
	t.Helper()
	return sql.OpenDB(dsnConnector{driver: d})
}

type dsnConnector struct{ driver driver.Driver }

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) { return c.driver.Open("") }
func (c dsnConnector) Driver() driver.Driver                        { return c.driver }

func TestTransactionRetriesBusyAtEveryPhaseWithoutDuplicateCommit(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		driver *scriptedDriver
	}{
		{name: "begin", driver: &scriptedDriver{beginErr: []error{codedError{code: 5, text: "busy"}}}},
		{name: "body", driver: &scriptedDriver{execErr: []error{codedError{code: 5, text: "busy"}}}},
		{name: "commit", driver: &scriptedDriver{commitErr: []error{codedError{code: 5, text: "busy"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openScriptedDB(t, test.driver)
			defer db.Close()
			bodyCalls := 0
			var observed Stats
			stats, err := Transaction(context.Background(), db, func(ctx context.Context, tx *sql.Tx) error {
				bodyCalls++
				_, err := tx.ExecContext(ctx, "write")
				return err
			}, Options{BaseDelay: time.Nanosecond, MaxDelay: time.Nanosecond, Random: func() float64 { return 0 }, Observe: func(stats Stats) { observed = stats }})
			if err != nil {
				t.Fatalf("transaction: %v", err)
			}
			if stats.Attempts != 2 || stats.ContentionAttempts != 1 {
				t.Fatalf("unexpected stats: %+v", stats)
			}
			if observed.Attempts != stats.Attempts || observed.ContentionAttempts != stats.ContentionAttempts || observed.TerminalError != nil {
				t.Fatalf("unexpected observed stats: %+v", observed)
			}
			if test.driver.commits != 1 {
				t.Fatalf("expected exactly one committed effect, got %d", test.driver.commits)
			}
			if test.name == "begin" && bodyCalls != 1 || test.name != "begin" && bodyCalls != 2 {
				t.Fatalf("unexpected body calls: %d", bodyCalls)
			}
		})
	}
}

func TestTransactionReturnsNonBusyErrorImmediately(t *testing.T) {
	t.Parallel()
	db := openScriptedDB(t, &scriptedDriver{execErr: []error{errors.New("not busy")}})
	defer db.Close()
	var observed Stats
	stats, err := Transaction(context.Background(), db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "write")
		return err
	}, Options{Sleep: func(context.Context, time.Duration) error { t.Fatal("must not sleep"); return nil }, Observe: func(stats Stats) { observed = stats }})
	if err == nil || stats.Attempts != 1 || stats.ContentionAttempts != 0 {
		t.Fatalf("expected immediate non-busy failure, stats=%+v err=%v", stats, err)
	}
	if !errors.Is(observed.TerminalError, err) {
		t.Fatalf("expected terminal error in observation, got %+v", observed)
	}
}

func TestTransactionUsesBoundedJitteredDelays(t *testing.T) {
	t.Parallel()
	db := openScriptedDB(t, &scriptedDriver{beginErr: []error{
		codedError{code: 5, text: "busy"},
		codedError{code: 5, text: "busy"},
	}})
	defer db.Close()
	var delays []time.Duration
	stats, err := Transaction(context.Background(), db, func(context.Context, *sql.Tx) error { return nil }, Options{
		BaseDelay: 10 * time.Millisecond,
		MaxDelay:  15 * time.Millisecond,
		Random:    func() float64 { return 0 },
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	if stats.Attempts != 3 || len(delays) != 2 || delays[0] != 5*time.Millisecond || delays[1] != 7500*time.Microsecond {
		t.Fatalf("unexpected bounded jitter delays=%v stats=%+v", delays, stats)
	}
}

func TestTransactionCancellationInterruptsContentionDelay(t *testing.T) {
	t.Parallel()
	db := openScriptedDB(t, &scriptedDriver{beginErr: []error{codedError{code: 5, text: "busy"}}})
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats, err := Transaction(ctx, db, func(context.Context, *sql.Tx) error { return nil }, Options{
		Sleep: func(ctx context.Context, _ time.Duration) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if !errors.Is(err, context.Canceled) || stats.Attempts != 1 {
		t.Fatalf("expected cancellation before replay, stats=%+v err=%v", stats, err)
	}
}

func TestIsBusyPrefersTypedCodeOverMessage(t *testing.T) {
	t.Parallel()
	if IsBusy(codedError{code: 19, text: "database is locked"}) {
		t.Fatal("typed non-busy code must not fall back to its message")
	}
	if !IsBusy(codedError{code: 517, text: "busy snapshot"}) {
		t.Fatal("expected extended SQLITE_BUSY code")
	}
	if !IsBusy(codedError{code: 262, text: "locked shared cache"}) {
		t.Fatal("expected extended SQLITE_LOCKED code")
	}
}
