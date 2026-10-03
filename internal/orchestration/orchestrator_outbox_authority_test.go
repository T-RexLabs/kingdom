package orchestration

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestAdmittedOutboxWorkerSkipsTerminalAuthority(t *testing.T) {
	for _, status := range []string{"published", "failed"} {
		t.Run(status, func(t *testing.T) {
			loop, result := completionTimestampFixture(t)
			ctx := t.Context()
			records, err := loop.executionStore.ListPendingOutbox(ctx, 1)
			if err != nil || len(records) != 1 {
				t.Fatalf("load admission record: records=%d err=%v", len(records), err)
			}
			rec := records[0]
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			loop.maintenance.draining = true
			if !loop.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "outbox", attemptID: result.attemptID, runID: result.run.RunID}, func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				_, err := loop.projectExecutionOutboxRecord(ctx, rec)
				return err
			}) {
				t.Fatal("outbox worker was not admitted")
			}
			<-entered
			if status == "published" {
				err = loop.executionStore.MarkOutboxPublished(ctx, rec.OutboxID, "2026-09-30T12:01:00Z")
			} else {
				err = loop.executionStore.MarkOutboxFailed(ctx, rec.OutboxID, "invalid original payload", "2026-09-30T12:01:00Z")
			}
			if err != nil {
				t.Fatal(err)
			}
			eventsBefore := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`)
			releaseOnce.Do(func() { close(release) })
			if err := loop.waitForMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			loop.execMu.Lock()
			drainErr := loop.maintenance.drainError
			loop.execMu.Unlock()
			if drainErr != nil {
				t.Fatalf("stale admission poisoned shutdown: %v", drainErr)
			}
			if after := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`); after != eventsBefore {
				t.Fatalf("terminal authority was projected again: events before=%d after=%d", eventsBefore, after)
			}
			var persistedStatus string
			var attempts int
			if err := loop.db.QueryRow(`SELECT status,publish_attempts FROM attempt_outbox WHERE outbox_id=?`, rec.OutboxID).Scan(&persistedStatus, &attempts); err != nil || persistedStatus != status || attempts != 1 {
				t.Fatalf("terminal authority changed: status=%s attempts=%d err=%v", persistedStatus, attempts, err)
			}
		})
	}
}

func TestOutboxProjectionUsesCurrentPendingPayload(t *testing.T) {
	loop, _ := completionTimestampFixture(t)
	ctx := t.Context()
	records, err := loop.executionStore.ListPendingOutbox(ctx, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("load admission record: records=%d err=%v", len(records), err)
	}
	rec := records[0]
	if _, err := loop.db.Exec(`UPDATE attempt_outbox SET payload_json=? WHERE outbox_id=?`, "{invalid", rec.OutboxID); err != nil {
		t.Fatal(err)
	}
	if published, err := loop.projectExecutionOutboxRecord(ctx, rec); err != nil || published {
		t.Fatalf("updated malformed payload was not dead-lettered: published=%t err=%v", published, err)
	}
	var status string
	if err := loop.db.QueryRow(`SELECT status FROM attempt_outbox WHERE outbox_id=?`, rec.OutboxID).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("stale valid payload replaced current authority: status=%s err=%v", status, err)
	}
}

func TestOutboxProjectionRejectsMissingAuthorityBeforeEffects(t *testing.T) {
	loop, _ := completionTimestampFixture(t)
	ctx := t.Context()
	records, err := loop.executionStore.ListPendingOutbox(ctx, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("load admission record: records=%d err=%v", len(records), err)
	}
	rec := records[0]
	mustExec(t, loop.db, `DELETE FROM attempt_outbox`)
	eventsBefore := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`)
	if published, err := loop.projectExecutionOutboxRecord(ctx, rec); err == nil || published {
		t.Fatalf("missing authority was accepted: published=%t err=%v", published, err)
	}
	if after := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`); after != eventsBefore {
		t.Fatalf("missing authority emitted projection events: before=%d after=%d", eventsBefore, after)
	}
}

func TestOutboxProjectionPreservesPublicationFailure(t *testing.T) {
	loop, _ := completionTimestampFixture(t)
	ctx := t.Context()
	records, err := loop.executionStore.ListPendingOutbox(ctx, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("load admission record: records=%d err=%v", len(records), err)
	}
	mustExec(t, loop.db, `CREATE TRIGGER reject_publication BEFORE UPDATE OF status ON attempt_outbox WHEN NEW.status='published' BEGIN SELECT RAISE(ABORT,'publication unavailable'); END`)
	if published, err := loop.projectExecutionOutboxRecord(ctx, records[0]); err == nil || published || errors.Is(err, context.Canceled) {
		t.Fatalf("publication failure was hidden: published=%t err=%v", published, err)
	}
	var status string
	if err := loop.db.QueryRow(`SELECT status FROM attempt_outbox WHERE outbox_id=?`, records[0].OutboxID).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("publication failure lost retry authority: status=%s err=%v", status, err)
	}
}

func TestOutboxProjectionPreservesAuthorityReadFailure(t *testing.T) {
	loop, _ := completionTimestampFixture(t)
	records, err := loop.executionStore.ListPendingOutbox(t.Context(), 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("load admission record: records=%d err=%v", len(records), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	eventsBefore := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`)
	if published, err := loop.projectExecutionOutboxRecord(ctx, records[0]); published || !errors.Is(err, context.Canceled) {
		t.Fatalf("authority read failure was hidden: published=%t err=%v", published, err)
	}
	if after := countTestRows(t, loop.db, `SELECT COUNT(*) FROM events`); after != eventsBefore {
		t.Fatalf("failed authority read emitted projection events: before=%d after=%d", eventsBefore, after)
	}
}
