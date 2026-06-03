package state

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type fakeRollbacker struct {
	err error
}

func (f fakeRollbacker) Rollback() error {
	return f.err
}

func TestRollbackPlanningTx(t *testing.T) {
	t.Parallel()

	primary := errors.New("primary failure")
	rollback := errors.New("rollback failure")

	t.Run("preserves primary when rollback succeeds", func(t *testing.T) {
		t.Parallel()

		err := rollbackPlanningTx(fakeRollbacker{}, "create planning reply bundle", primary)
		if !errors.Is(err, primary) {
			t.Fatalf("expected primary failure to survive, got %v", err)
		}
	})

	t.Run("joins rollback failure with primary cause", func(t *testing.T) {
		t.Parallel()

		err := rollbackPlanningTx(fakeRollbacker{err: rollback}, "create planning reply bundle", primary)
		if !errors.Is(err, primary) {
			t.Fatalf("expected joined error to include primary cause, got %v", err)
		}
		if !errors.Is(err, rollback) {
			t.Fatalf("expected joined error to include rollback failure, got %v", err)
		}
		if !strings.Contains(err.Error(), "create planning reply bundle rollback") {
			t.Fatalf("expected rollback context in error text, got %q", err.Error())
		}
	})

	t.Run("returns rollback failure when no primary cause exists", func(t *testing.T) {
		t.Parallel()

		err := rollbackPlanningTx(fakeRollbacker{err: rollback}, "claim planning operation", nil)
		if !errors.Is(err, rollback) {
			t.Fatalf("expected rollback failure, got %v", err)
		}
		if !strings.Contains(err.Error(), "claim planning operation rollback") {
			t.Fatalf("expected rollback context in error text, got %q", err.Error())
		}
	})

	t.Run("ignores err tx done", func(t *testing.T) {
		t.Parallel()

		err := rollbackPlanningTx(fakeRollbacker{err: sql.ErrTxDone}, "claim planning operation", primary)
		if !errors.Is(err, primary) {
			t.Fatalf("expected sql.ErrTxDone to be ignored, got %v", err)
		}
	})
}
