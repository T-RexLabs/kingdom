package state

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
)

func TestOptimisticConcurrencyWorkflowRunOneSuccessOneConflict(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)

	store := NewOptimisticConcurrencyStore(db)

	timestamps := []string{"2026-03-01T12:01:00Z", "2026-03-01T12:02:00Z"}
	errCh := make(chan error, len(timestamps))

	var wg sync.WaitGroup
	for _, ts := range timestamps {
		wg.Add(1)
		go func(updatedAt string) {
			defer wg.Done()
			errCh <- store.UpdateWorkflowRun(context.Background(), WorkflowRunUpdateCAS{
				RunID:              "run_1",
				ExpectedRowVersion: 1,
				RunStatus:          "running",
				PlanState:          "draft",
				UpdatedAt:          updatedAt,
			})
		}(ts)
	}

	wg.Wait()
	close(errCh)

	successes := 0
	conflicts := 0
	for err := range errCh {
		if err == nil {
			successes++
			continue
		}

		var conflictErr *RowVersionConflictError
		if errors.As(err, &conflictErr) {
			conflicts++
			continue
		}

		t.Fatalf("unexpected update error: %v", err)
	}

	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one success and one conflict, got success=%d conflict=%d", successes, conflicts)
	}

	if got := queryRunRowVersion(t, db, "run_1"); got != 2 {
		t.Fatalf("expected row_version=2 after one accepted write, got %d", got)
	}

	updatedAt := queryRunUpdatedAt(t, db, "run_1")
	if updatedAt != timestamps[0] && updatedAt != timestamps[1] {
		t.Fatalf("expected updated_at to equal one accepted write timestamp, got %q", updatedAt)
	}
}

func TestOptimisticConcurrencyNodeRunUpdatePaths(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		db := openMigratedTestDB(t)
		seedMinimumGraph(t, db)

		store := NewOptimisticConcurrencyStore(db)
		err := store.UpdateNodeRun(context.Background(), NodeRunUpdateCAS{
			NodeRunID:          "nr_1",
			RunID:              "run_1",
			ExpectedRowVersion: 1,
			NodeStatus:         "succeeded",
		})
		if err != nil {
			t.Fatalf("update node run: %v", err)
		}

		if got := queryNodeStatus(t, db, "nr_1"); got != "succeeded" {
			t.Fatalf("unexpected node status: %q", got)
		}
		if got := queryNodeRowVersion(t, db, "nr_1"); got != 2 {
			t.Fatalf("unexpected row version: %d", got)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		db := openMigratedTestDB(t)
		seedMinimumGraph(t, db)

		store := NewOptimisticConcurrencyStore(db)
		err := store.UpdateNodeRun(context.Background(), NodeRunUpdateCAS{
			NodeRunID:          "nr_1",
			RunID:              "run_1",
			ExpectedRowVersion: 99,
			NodeStatus:         "failed",
		})
		if err == nil {
			t.Fatal("expected row-version conflict")
		}

		var conflict *RowVersionConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("expected RowVersionConflictError, got %T", err)
		}
		if conflict.ActualVersion != 1 {
			t.Fatalf("unexpected actual version: %d", conflict.ActualVersion)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		db := openMigratedTestDB(t)
		seedMinimumGraph(t, db)

		store := NewOptimisticConcurrencyStore(db)
		err := store.UpdateNodeRun(context.Background(), NodeRunUpdateCAS{
			NodeRunID:          "missing",
			RunID:              "run_1",
			ExpectedRowVersion: 1,
			NodeStatus:         "failed",
		})
		if err == nil {
			t.Fatal("expected not found error")
		}

		var notFound *EntityNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("expected EntityNotFoundError, got %T", err)
		}
	})
}

func TestRowVersionConflictErrorMessage(t *testing.T) {
	t.Parallel()

	err := (&RowVersionConflictError{
		EntityType:      "node",
		EntityID:        "nr_1",
		ExpectedVersion: 1,
		ActualVersion:   2,
	}).Error()

	if err != `row version conflict for node "nr_1": expected=1 actual=2` {
		t.Fatalf("unexpected message: %q", err)
	}
}

func queryRunUpdatedAt(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	var updatedAt string
	if err := db.QueryRow(`SELECT updated_at FROM workflow_runs WHERE run_id = ?`, runID).Scan(&updatedAt); err != nil {
		t.Fatalf("query workflow_runs.updated_at: %v", err)
	}

	return updatedAt
}
