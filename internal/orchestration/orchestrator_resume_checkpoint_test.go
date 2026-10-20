package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func TestCancellationPreservesPartialSnapshotBeforeTrustedRestore(t *testing.T) {
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", "2026-03-01T12:01:00Z"); err != nil {
		t.Fatal(err)
	}
	partial := "saved partial implementation\n"
	if err := os.WriteFile(filepath.Join(target.ExecutionRoot, "README.md"), []byte(partial), 0o644); err != nil {
		t.Fatal(err)
	}
	h.loop.clearExecutionInflight(invocation.AttemptID)
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	var snapshot string
	if err := h.db.QueryRow(`SELECT COALESCE(result_snapshot_id,'') FROM attempt_executions WHERE attempt_id=?`, invocation.AttemptID).Scan(&snapshot); err != nil || snapshot == "" {
		t.Fatalf("partial work discarded before trusted restoration: snapshot=%q err=%v", snapshot, err)
	}
	root := filepath.Join(t.TempDir(), "saved-partial")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := h.fsStore.MaterializeSnapshot(ctx, snapshot, root); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(root, "README.md")); err != nil || string(raw) != partial {
		t.Fatalf("snapshot does not contain partial progress: %q %v", raw, err)
	}
	if raw, err := os.ReadFile(filepath.Join(target.ExecutionRoot, "README.md")); err != nil || string(raw) != "trusted baseline\n" {
		t.Fatalf("cancellation did not restore trusted files: %q %v", raw, err)
	}
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	var after string
	if err := h.db.QueryRow(`SELECT result_snapshot_id FROM attempt_executions WHERE attempt_id=?`, invocation.AttemptID).Scan(&after); err != nil || after != snapshot {
		t.Fatalf("recovery replaced partial snapshot with restored baseline: before=%q after=%q err=%v", snapshot, after, err)
	}
	target, err = h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil || target.TrustedSnapshotID != h.baselineSnapshotID || target.PublishedSnapshotID != h.baselineSnapshotID {
		t.Fatalf("partial progress was promoted: target=%#v err=%v", target, err)
	}
}

func TestCancellationCannotRestoreWhileCheckpointEffectsRemainOpen(t *testing.T) {
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", "2026-03-01T12:01:00Z"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(target.ExecutionRoot, "README.md")
	if err := os.WriteFile(path, []byte("still owned partial files\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.loop.provider = checkpointTestProvider{Provider: h.loop.provider, capture: func(context.Context, providerexecution.RequestIdentity) (opencode.SessionCheckpoint, error) {
		return opencode.SessionCheckpoint{}, providerrequest.ErrResourcesOpen
	}}
	h.loop.clearExecutionInflight(invocation.AttemptID)
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); !errors.Is(err, providerrequest.ErrResourcesOpen) {
		t.Fatalf("open-effect blocker was lost: %v", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "still owned partial files\n" {
		t.Fatalf("workspace restored despite unclosed effects: %q %v", raw, err)
	}
	if _, err := state.LoadAttemptResumeCheckpoint(ctx, h.db, invocation.AttemptID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unclosed effects became an unavailable checkpoint: %v", err)
	}
}

func TestCancellationCannotPairSavedSnapshotWithChangedWorkspace(t *testing.T) {
	for _, restored := range []bool{false, true} {
		t.Run(fmt.Sprintf("already_restored=%t", restored), func(t *testing.T) {
			h, invocation, _ := staleCancellationFixture(t)
			ctx := context.Background()
			target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", "2026-03-01T12:01:00Z"); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(target.ExecutionRoot, "README.md")
			if err := os.WriteFile(path, []byte("original partial files\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			saved, err := h.loop.captureAttemptResultSnapshot(ctx, asyncExecutionResult{attemptID: invocation.AttemptID, run: runSnapshot{RunID: invocation.RunID}, node: nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, workspacePath: target.ExecutionRoot, baseSnapshotID: h.baselineSnapshotID}, "2026-03-01T12:01:01Z")
			if err != nil {
				t.Fatal(err)
			}
			if restored {
				if err := h.loop.restoreExecutionTarget(ctx, invocation.RunID, target.ExecutionRoot, h.baselineSnapshotID, "2026-03-01T12:01:02Z"); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte("different files after snapshot\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			h.loop.provider = checkpointTestProvider{Provider: h.loop.provider, capture: func(context.Context, providerexecution.RequestIdentity) (opencode.SessionCheckpoint, error) {
				return opencode.SessionCheckpoint{MessageID: "last-message", MessagesSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, nil
			}}
			h.loop.clearExecutionInflight(invocation.AttemptID)
			if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			checkpoint, err := state.LoadAttemptResumeCheckpoint(ctx, h.db, invocation.AttemptID)
			if err != nil || checkpoint.UnavailableReason == "" {
				t.Fatalf("mismatched progress became resumable: checkpoint=%#v err=%v", checkpoint, err)
			}
			var resultSnapshot string
			if err := h.db.QueryRow(`SELECT result_snapshot_id FROM attempt_executions WHERE attempt_id=?`, invocation.AttemptID).Scan(&resultSnapshot); err != nil || resultSnapshot != saved.SnapshotID {
				t.Fatalf("saved diagnostics changed: snapshot=%q err=%v", resultSnapshot, err)
			}
		})
	}
}

type checkpointTestProvider struct {
	opencode.Provider
	capture func(context.Context, providerexecution.RequestIdentity) (opencode.SessionCheckpoint, error)
}

func (p checkpointTestProvider) CaptureSessionCheckpoint(ctx context.Context, identity providerexecution.RequestIdentity) (opencode.SessionCheckpoint, error) {
	return p.capture(ctx, identity)
}

func TestCancellationCheckpointIsPublishedBeforeRestoreAndRetriedAfterCrash(t *testing.T) {
	for _, captureFailure := range []bool{false, true} {
		name := "ready checkpoint"
		if captureFailure {
			name = "conversation unavailable"
		}
		t.Run(name, func(t *testing.T) {
			h, invocation, _ := staleCancellationFixture(t)
			ctx := context.Background()
			target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", "2026-03-01T12:01:00Z"); err != nil {
				t.Fatal(err)
			}
			workspaceFile := filepath.Join(target.ExecutionRoot, "README.md")
			if err := os.WriteFile(workspaceFile, []byte("partial before interruption\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			captures := 0
			h.loop.provider = checkpointTestProvider{Provider: h.loop.provider, capture: func(_ context.Context, identity providerexecution.RequestIdentity) (opencode.SessionCheckpoint, error) {
				captures++
				if identity.SessionID != invocation.ProviderSessionID {
					t.Fatalf("wrong checkpoint owner: %#v", identity)
				}
				if raw, err := os.ReadFile(workspaceFile); err != nil || string(raw) != "partial before interruption\n" {
					t.Fatalf("conversation captured after restoration: %q %v", raw, err)
				}
				if captureFailure {
					return opencode.SessionCheckpoint{}, errors.New("session is unavailable")
				}
				return opencode.SessionCheckpoint{MessageID: "last-message", MessagesSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, nil
			}}
			h.loop.clearExecutionInflight(invocation.AttemptID)
			mustExec(t, h.db, `CREATE TRIGGER interrupt_checkpoint_publication BEFORE INSERT ON attempt_resume_checkpoints BEGIN SELECT RAISE(ABORT,'simulated crash before checkpoint publication'); END`)
			if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			if raw, err := os.ReadFile(workspaceFile); err != nil || string(raw) != "partial before interruption\n" {
				t.Fatalf("workspace restored despite failed checkpoint publication: %q %v", raw, err)
			}
			if _, err := state.LoadAttemptResumeCheckpoint(ctx, h.db, invocation.AttemptID); err == nil {
				t.Fatal("incomplete checkpoint claimed saved progress")
			}
			mustExec(t, h.db, `DROP TRIGGER interrupt_checkpoint_publication`)
			if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			checkpoint, err := state.LoadAttemptResumeCheckpoint(ctx, h.db, invocation.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			if captureFailure {
				if checkpoint.UnavailableReason == "" || checkpoint.WorkspaceSnapshotID != "" {
					t.Fatalf("failed conversation claimed resumability: %#v", checkpoint)
				}
			} else if checkpoint.WorkspaceSnapshotID == "" || checkpoint.ConversationBoundary.ContentID == "" || checkpoint.BaseSnapshotID != h.baselineSnapshotID {
				t.Fatalf("checkpoint lost seed, context, or original trust: %#v", checkpoint)
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")
			if raw, err := os.ReadFile(workspaceFile); err != nil || string(raw) != "trusted baseline\n" {
				t.Fatalf("cancellation did not finish restoration: %q %v", raw, err)
			}
			if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			if captures != 2 {
				t.Fatalf("published checkpoint was recaptured: calls=%d", captures)
			}
		})
	}
}

// A cancellation can race a provider turn that settled successfully. Success
// is not interrupted work: it has no checkpoint and must not block the handoff.
func TestCancellationOfSettledSuccessSkipsCheckpointAndRestores(t *testing.T) {
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	mustExec(t, h.db, `UPDATE attempt_executions SET state='committed_success', failure_code='' WHERE attempt_id='`+invocation.AttemptID+`'`)
	h.loop.clearExecutionInflight(invocation.AttemptID)
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	var handoffState string
	if err := h.db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE attempt_id=?`, invocation.AttemptID).Scan(&handoffState); err != nil {
		t.Fatal(err)
	}
	if handoffState == "workspace_restore_failed" {
		t.Fatal("settled success blocked cancellation workspace restoration")
	}
	if _, err := state.LoadAttemptResumeCheckpoint(ctx, h.db, invocation.AttemptID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("settled success produced a resume checkpoint: %v", err)
	}
}
