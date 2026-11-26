package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// seedResumableRun fails the run's kickoff step after its provider request
// closed, with a saved checkpoint, exactly as completion would record it.
func seedResumableRun(t *testing.T, handler http.Handler, db *sql.DB) (string, string) {
	t.Helper()
	ctx := context.Background()
	runID := seedRunViaAPI(t, handler)
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	setRunStatus(t, db, runID, "running")
	nodeRunID := seedKickoffNodeRun(t, db, runID, "running", stamp)
	attemptID := runID + ":kickoff:1"
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: runID, NodeRunID: nodeRunID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker", LeaseUntil: now.Add(time.Hour).Format(time.RFC3339Nano), ClaimedAt: stamp})
	if err != nil || !acquired {
		t.Fatalf("claim: %v %v", acquired, err)
	}
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "ses_resume", ProfileID: "profile", RequestID: "request"}
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: "inv_resume", AttemptID: attemptID, RunID: runID, NodeRunID: nodeRunID, LogicalSessionID: "conversation", ProviderSessionID: identity.SessionID, CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: stamp, CreatedAt: stamp, OpenedAt: stamp, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, RequestIdentity: identity}); err != nil {
		t.Fatal(err)
	}
	binding, found, err := providerrequest.ForOwner(ctx, db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: "inv_resume"})
	if err != nil || !found {
		t.Fatalf("binding: %v %v", found, err)
	}
	mustExecSQL(t, db, `INSERT INTO filesystem_snapshots(snapshot_id,run_id,snapshot_kind,created_at) VALUES ('resume-base',?,'baseline',?),('resume-partial',?,'attempt_result',?)`, runID, stamp, runID, stamp)
	mustExecSQL(t, db, `UPDATE attempt_executions SET state='committed_failure',committed_at=?,lease_owner='',lease_expires_at=NULL,base_snapshot_id='resume-base',result_snapshot_id='resume-partial' WHERE attempt_id=?`, stamp, attemptID)
	mustExecSQL(t, db, `UPDATE provider_requests SET admission_closed_at=?`, stamp)
	if _, err := state.PersistAttemptResumeCheckpoint(ctx, db, state.AttemptResumeCheckpointRequest{
		SourceAttemptID: attemptID, BindingID: binding.ID, WorkspaceSnapshotID: "resume-partial", CapturedAt: stamp,
		Boundary: state.AttemptResumeBoundary{InvocationID: "inv_resume", SessionID: identity.SessionID, MessageID: "msg_last", MessagesSHA256: strings.Repeat("a", 64)},
	}); err != nil {
		t.Fatal(err)
	}
	mustExecSQL(t, db, `UPDATE node_runs SET node_status='failed' WHERE node_run_id=?`, nodeRunID)
	setRunStatus(t, db, runID, "failed")
	return runID, attemptID
}

func seedKickoffNodeRun(t *testing.T, db *sql.DB, runID, status, stamp string) string {
	t.Helper()
	nodeRunID := "nr_kickoff_" + runID
	agentID := "agent_kickoff_" + runID
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'plan-author', 'idle', NULL, 0, ?, ?)`, agentID, stamp, stamp)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'kickoff', 1, 1, ?, 'plan-author', ?, 1, ?, ?)`, nodeRunID, runID, status, agentID, stamp, stamp)
	return nodeRunID
}

func getResumePlan(t *testing.T, handler http.Handler, runID string) gen.RunResumePlan {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/resume"})
	if res.Code != http.StatusOK {
		t.Fatalf("resume plan: %d %s", res.Code, res.Body.String())
	}
	var plan gen.RunResumePlan
	if err := json.Unmarshal(res.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func resumeAttemptCount(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_attempt_recoveries WHERE run_id=? AND trigger='operator_resume'`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestServerResumeContinuesOnlyTheConfirmedFrontierOnce(t *testing.T) {
	t.Parallel()
	handler, db := newAPIServerForTest(t)
	runID, attemptID := seedResumableRun(t, handler, db)

	plan := getResumePlan(t, handler, runID)
	if !plan.Available || len(plan.Steps) != 1 || plan.Steps[0].NodeId != "kickoff" || plan.Steps[0].SourceAttemptId == nil || *plan.Steps[0].SourceAttemptId != attemptID || plan.Steps[0].CheckpointCapturedAt == nil {
		t.Fatalf("resumable frontier was not offered: %s", mustJSON(t, plan))
	}

	stale := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/resume", idempotencyKey: "idem-resume-stale",
		body: map[string]any{"reason": "continue", "steps": []any{map[string]any{"node_id": "kickoff", "source_attempt_id": runID + ":kickoff:0"}}}})
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "interrupted steps changed") {
		t.Fatalf("stale confirmation was not rejected: %d %s", stale.Code, stale.Body.String())
	}
	if resumeAttemptCount(t, db, runID) != 0 {
		t.Fatal("stale confirmation queued work")
	}

	confirm := requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/resume", idempotencyKey: "idem-resume",
		body: map[string]any{"reason": "continue", "steps": []any{map[string]any{"node_id": "kickoff", "source_attempt_id": attemptID}}}}
	first := performJSONRequest(t, handler, confirm)
	if first.Code != http.StatusAccepted {
		t.Fatalf("resume was not queued: %d %s", first.Code, first.Body.String())
	}
	// A lost response is recovered by repeating the same request and key.
	again := performJSONRequest(t, handler, confirm)
	if again.Code != http.StatusAccepted || again.Body.String() != first.Body.String() {
		t.Fatalf("repeated resume did not replay its outcome: %d %s", again.Code, again.Body.String())
	}
	// A second click with a new key finds the run already running.
	confirm.idempotencyKey = "idem-resume-second-click"
	if second := performJSONRequest(t, handler, confirm); second.Code != http.StatusConflict {
		t.Fatalf("second resume was accepted: %d %s", second.Code, second.Body.String())
	}
	if got := resumeAttemptCount(t, db, runID); got != 1 {
		t.Fatalf("expected exactly one resumed attempt, got %d", got)
	}
	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id=?`, runID).Scan(&status); err != nil || status != "running" {
		t.Fatalf("run did not reopen: %q %v", status, err)
	}
}

func TestServerResumeExplainsUnavailableCheckpointAndOffersNoSilentRerun(t *testing.T) {
	t.Parallel()
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedKickoffNodeRun(t, db, runID, "failed", time.Now().UTC().Format(time.RFC3339Nano))
	setRunStatus(t, db, runID, "failed")

	plan := getResumePlan(t, handler, runID)
	if plan.Available || len(plan.Steps) != 1 || plan.Steps[0].UnavailableReason == nil || *plan.Steps[0].UnavailableReason != "This step stopped before Kingdom started an attempt." {
		t.Fatalf("unavailable resume was not explained: %s", mustJSON(t, plan))
	}
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/resume", idempotencyKey: "idem-resume-unavailable",
		body: map[string]any{"reason": "continue", "steps": []any{map[string]any{"node_id": "kickoff", "source_attempt_id": "none"}}}})
	if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), "resume is not available") {
		t.Fatalf("unavailable resume was accepted: %d %s", res.Code, res.Body.String())
	}
	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id=?`, runID).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("unavailable resume changed the run: %q %v", status, err)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type resumeBackendProvider struct {
	opencode.Provider
	backend providerexecution.Backend
}

func (p resumeBackendProvider) ExecutionBackend() providerexecution.Backend { return p.backend }
func TestServerManagedResumeRetainsCheckpointWithoutEnqueue(t *testing.T) {
	for _, backend := range []providerexecution.Backend{providerexecution.BackendOpenShell, "unknown"} {
		t.Run(string(backend), func(t *testing.T) {
			handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProviderService: resumeBackendProvider{backend: backend}})
			runID, attemptID := seedResumableRun(t, handler, db)
			plan := getResumePlan(t, handler, runID)
			if plan.Available || plan.UnavailableReason == nil || !strings.Contains(*plan.UnavailableReason, "Rerun") || len(plan.Steps) != 1 || plan.Steps[0].CheckpointCapturedAt == nil {
				t.Fatalf("unsafe resume plan: %+v", plan)
			}
			res := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/resume", idempotencyKey: "resume-managed", body: map[string]any{"reason": "continue", "steps": []any{map[string]any{"node_id": "kickoff", "source_attempt_id": attemptID}}}})
			if res.Code != http.StatusConflict || resumeAttemptCount(t, db, runID) != 0 {
				t.Fatalf("resume mutated managed run: %d %s", res.Code, res.Body.String())
			}
			if after := getResumePlan(t, handler, runID); len(after.Steps) != 1 || after.Steps[0].CheckpointCapturedAt == nil {
				t.Fatal("checkpoint lost")
			}
		})
	}
}

type scopedResumeBackend struct {
	resumeBackendProvider
	source providerexecution.Backend
}

func (p scopedResumeBackend) SourceBackend(context.Context, string, string) (providerexecution.Backend, error) {
	return p.source, nil
}
func TestResumeUsesRetainedRunBackendAfterDefaultSwitch(t *testing.T) {
	for _, source := range []providerexecution.Backend{providerexecution.BackendLocal, providerexecution.BackendOpenShell} {
		selected := providerexecution.BackendLocal
		if source == selected {
			selected = providerexecution.BackendOpenShell
		}
		handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProviderService: scopedResumeBackend{resumeBackendProvider: resumeBackendProvider{backend: selected}, source: source}})
		runID, _ := seedResumableRun(t, handler, db)
		plan := getResumePlan(t, handler, runID)
		if plan.Available != (source == providerexecution.BackendLocal) {
			t.Fatalf("default %s overrode source %s: %+v", selected, source, plan)
		}
	}
}
