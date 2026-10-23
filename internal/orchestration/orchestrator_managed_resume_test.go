package orchestration

import (
	"context"
	"encoding/json"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
	"strings"
	"testing"
)

func TestManagedResumeRejectionRetainsCheckpointAndWorkspace(t *testing.T) {
	h, attempt, _ := resumePreparationFixture(t)
	ctx := context.Background()
	before, e := state.LoadAttemptResumeInput(ctx, h.db, attempt.NodeRunID)
	if e != nil {
		t.Fatal(e)
	}
	target, e := h.fsStore.GetExecutionTarget(ctx, "run_terminal_handoff")
	if e != nil {
		t.Fatal(e)
	}
	h.loop.provider = &opencode.ManagedService{}
	if _, e = h.loop.loadDispatchResume(ctx, attempt.NodeRunID); e == nil || !strings.Contains(e.Error(), "Rerun") {
		t.Fatalf("managed resume accepted: %v", e)
	}
	after, e := state.LoadAttemptResumeInput(ctx, h.db, attempt.NodeRunID)
	if e != nil {
		t.Fatal(e)
	}
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("checkpoint history changed")
	}
	changes, e := h.fsStore.CountChangesAgainstSnapshot(ctx, target.ExecutionRoot, h.baselineSnapshotID)
	if e != nil || changes != 0 {
		t.Fatalf("workspace seeded before rejection: %d %v", changes, e)
	}
	var count int
	if e = h.db.QueryRow(`SELECT count(*) FROM provider_invocations WHERE attempt_id='run_terminal_handoff:implementation:2'`).Scan(&count); e != nil || count != 0 {
		t.Fatal("rejection created preparation debt", count, e)
	}
}
