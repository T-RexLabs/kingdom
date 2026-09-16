package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
)

func TestStoreEnqueueAttemptIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	req := EnqueueAttemptRequest{
		AttemptID:      "run_1:decree_intake:1",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}

	if err := s.EnqueueAttempt(ctx, req); err != nil {
		t.Fatalf("enqueue attempt first call: %v", err)
	}
	if err := s.EnqueueAttempt(ctx, req); err != nil {
		t.Fatalf("enqueue attempt second call: %v", err)
	}
	conflicting := req
	conflicting.CompletionMode = ProviderCompletionModeServerResponse
	if err := s.EnqueueAttempt(ctx, conflicting); err == nil {
		t.Fatal("expected completion-mode change on an existing attempt to be rejected")
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE attempt_id = ?`, req.AttemptID).Scan(&count); err != nil {
		t.Fatalf("query attempt row count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected idempotent enqueue to keep one row, got %d", count)
	}
}

func TestStoreEnqueueAttemptPersistsExecutionMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      "run_1:decree_intake:1",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeRuntimeOwned,
		CompletionMode: ProviderCompletionModeNotApplicable,
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}

	attempt, ok, err := s.GetAttempt(ctx, "run_1:decree_intake:1")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !ok {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ExecutionMode != ExecutionModeRuntimeOwned {
		t.Fatalf("expected execution_mode=%q, got %q", ExecutionModeRuntimeOwned, attempt.ExecutionMode)
	}
	if attempt.CompletionMode != ProviderCompletionModeNotApplicable {
		t.Fatalf("expected completion_mode=%q, got %q", ProviderCompletionModeNotApplicable, attempt.CompletionMode)
	}
	if attempt.SessionRef != "" {
		t.Fatalf("expected empty session_ref for runtime-owned attempt, got %q", attempt.SessionRef)
	}
	if attempt.ProviderSessionID != "" {
		t.Fatalf("expected empty provider_session_id for runtime-owned attempt, got %q", attempt.ProviderSessionID)
	}
}

func TestStoreEnqueueAttemptRequiresCompatibleExplicitCompletionMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	tests := []struct {
		name           string
		executionMode  ExecutionMode
		completionMode ProviderCompletionMode
	}{
		{name: "missing", executionMode: ExecutionModeProviderBacked},
		{name: "provider not applicable", executionMode: ExecutionModeProviderBacked, completionMode: ProviderCompletionModeNotApplicable},
		{name: "runtime server response", executionMode: ExecutionModeRuntimeOwned, completionMode: ProviderCompletionModeServerResponse},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s.EnqueueAttempt(context.Background(), EnqueueAttemptRequest{
				AttemptID:      "run_1:decree_intake:" + strings.NewReplacer(" ", "_", "-", "_").Replace(tc.name),
				RunID:          "run_1",
				NodeRunID:      "nr_1",
				ExecutionMode:  tc.executionMode,
				CompletionMode: tc.completionMode,
				CreatedAt:      "2026-03-01T12:00:01Z",
			})
			if err == nil {
				t.Fatal("expected incompatible completion mode to be rejected")
			}
		})
	}
}

func TestStoreUpdateProviderSessionIDRoundTripsOnAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.UpdateProviderSessionID(ctx, UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_real_123",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}

	attempt, ok, err := s.GetAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !ok {
		t.Fatal("expected attempt to exist")
	}
	if attempt.ProviderSessionID != "ses_real_123" {
		t.Fatalf("expected provider_session_id ses_real_123, got %q", attempt.ProviderSessionID)
	}

	var updatedAt string
	if err := db.QueryRow(`SELECT updated_at FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&updatedAt); err != nil {
		t.Fatalf("query attempt updated_at: %v", err)
	}
	if updatedAt != "2026-03-01T12:00:01Z" {
		t.Fatalf("expected metadata write to preserve execution updated_at, got %q", updatedAt)
	}
}

func TestStoreGetRunExecutionDetailIncludesCanonicalSettlementProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review', 'run_1', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:execution_review_shieldmaiden:1"
	seedCanonicalProviderExecutionOutput(t, db, attemptID, "run_1", "nr_review", "committed_failure", "execution_review_rework", "review requested rework", "VERDICT: REWORK\n\nCanonical reviewer response.", "2026-03-01T12:00:00Z")
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_failure',
		    terminal_observed_at = '2026-03-01T12:05:00Z',
		    committed_at = '2026-03-01T12:05:00Z',
		    failure_code = 'execution_review_rework',
		    failure_text = 'review requested rework',
		    updated_at = '2026-03-01T12:05:00Z'
		WHERE attempt_id = 'run_1:execution_review_shieldmaiden:1'`)
	mustExec(t, db, `UPDATE attempt_outbox SET payload_json = '{"provider_invocation_id":"inv_review_stale","provider_invocation_at":"2026-03-01T12:00:01Z","output_text":"VERDICT: PASS\n\nStale reviewer response."}' WHERE attempt_id = 'run_1:execution_review_shieldmaiden:1'`)

	record, found, err := s.GetRunExecutionDetail(ctx, "run_1", attemptID)
	if err != nil {
		t.Fatalf("get run execution detail: %v", err)
	}
	if !found {
		t.Fatal("expected run execution detail to be found")
	}
	if record.AttemptID != attemptID || record.NodeID != "execution_review_shieldmaiden" {
		t.Fatalf("unexpected execution detail lineage: %#v", record)
	}
	if !record.ProviderInvocationID.Valid || record.ProviderInvocationID.String != "pinv_run_1_execution_review_shieldmaiden_1" {
		t.Fatalf("expected provider invocation id, got %#v", record.ProviderInvocationID)
	}
	if record.QueuedAt != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected queued_at from attempt creation, got %q", record.QueuedAt)
	}
	if !record.ProviderInvocationAt.Valid || record.ProviderInvocationAt.String != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected provider invocation at, got %#v", record.ProviderInvocationAt)
	}
	if !record.ObservedOutputText.Valid || record.ObservedOutputText.String != "VERDICT: REWORK\n\nCanonical reviewer response." {
		t.Fatalf("expected observed output text, got %#v", record.ObservedOutputText)
	}
	if !record.ObservedControlVerdict.Valid || record.ObservedControlVerdict.String != "REWORK" {
		t.Fatalf("expected observed control verdict REWORK, got %#v", record.ObservedControlVerdict)
	}
	if !record.CanonicalSettlementID.Valid || record.CanonicalSettlementID.String != "settlement_run_1_execution_review_shieldmaiden_1" {
		t.Fatalf("expected canonical settlement id, got %#v", record.CanonicalSettlementID)
	}
	if !record.CanonicalSettlementOutcome.Valid || record.CanonicalSettlementOutcome.String != "failed" {
		t.Fatalf("expected canonical settlement outcome failed, got %#v", record.CanonicalSettlementOutcome)
	}
	if !record.CanonicalSettlementAt.Valid || record.CanonicalSettlementAt.String != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected canonical settlement timestamp, got %#v", record.CanonicalSettlementAt)
	}
	if !record.CanonicalStructuredPayloadJSON.Valid || record.CanonicalStructuredPayloadJSON.String != `{"verdict":"REWORK"}` {
		t.Fatalf("expected canonical structured payload, got %#v", record.CanonicalStructuredPayloadJSON)
	}
}

func TestStoreGetRunExecutionDetailReturnsFalseForUnknownAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	record, found, err := s.GetRunExecutionDetail(context.Background(), "run_1", "run_1:missing:1")
	if err != nil {
		t.Fatalf("get run execution detail: %v", err)
	}
	if found {
		t.Fatalf("expected missing attempt to return found=false, got %#v", record)
	}
}

func TestStoreListLatestRunExecutionOutputsUsesLatestCommittedAttemptPerNode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at, display_name) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', 'Reviewer Shieldmaiden')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at, display_name) VALUES ('agent_validator', 'intelligence-qa-scout', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', 'QA Scout')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation', 'wfdef_1', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_1', 'run_1', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_2', 'run_1', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:03:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_1', 'run_1', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validator', 1, '2026-03-01T12:04:00Z', '2026-03-01T12:05:00Z')`)

	s := New(db)
	ctx := context.Background()
	seedOutput := func(attemptID, nodeRunID, state, failureCode, failureText, reasonCode, outputText, createdAt string) {
		t.Helper()
		seedCanonicalProviderExecutionOutput(t, db, attemptID, "run_1", nodeRunID, state, failureCode, failureText, outputText, createdAt)
		if _, err := db.Exec(`UPDATE attempt_executions
			SET state = ?,
			    terminal_observed_at = ?,
			    committed_at = ?,
			    failure_code = ?,
			    failure_text = ?,
		    updated_at = ?
			WHERE attempt_id = ?`, state, createdAt, createdAt, failureCode, failureText, createdAt, attemptID); err != nil {
			t.Fatalf("update attempt %s: %v", attemptID, err)
		}
		payloadJSON := fmt.Sprintf(`{"attempt_id":%q,"output_text":%q,"outcome":{"reason_code":%q}}`, attemptID, "VERDICT: PASS\n\nStale outbox payload.", reasonCode)
		if _, err := db.Exec(`UPDATE attempt_outbox SET payload_json = ? WHERE attempt_id = ?`, payloadJSON, attemptID); err != nil {
			t.Fatalf("update outbox %s: %v", attemptID, err)
		}
	}
	seedOutput("run_1:execution_review_shieldmaiden:1", "nr_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nOld review failure.", "2026-03-01T12:01:00Z")
	seedOutput("run_1:execution_review_shieldmaiden:2", "nr_review_2", "committed_success", "", "", "execution_completed", "VERDICT: PASS\n\nLatest review pass.", "2026-03-01T12:03:00Z")
	seedOutput("run_1:validation_gates_qa_scout:1", "nr_validation_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure.", "2026-03-01T12:05:00Z")

	records, err := s.ListLatestRunExecutionOutputs(ctx, ListRunExecutionOutputsRequest{
		RunID:       "run_1",
		NodeClasses: []string{"execution_review", "validation_gates"},
		States:      []string{"committed_success", "committed_failure"},
	})
	if err != nil {
		t.Fatalf("list latest run execution outputs: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected two latest output records, got %#v", records)
	}
	if records[0].NodeID != "execution_review_shieldmaiden" || records[0].AttemptID != "run_1:execution_review_shieldmaiden:2" {
		t.Fatalf("expected latest review output record, got %#v", records[0])
	}
	if !records[0].OutputText.Valid || records[0].OutputText.String != "VERDICT: PASS\n\nLatest review pass." {
		t.Fatalf("expected latest review output text, got %#v", records[0].OutputText)
	}
	if !records[0].OutcomeReasonCode.Valid || records[0].OutcomeReasonCode.String != "execution_completed" {
		t.Fatalf("expected latest review reason code, got %#v", records[0].OutcomeReasonCode)
	}
	if !records[0].AgentDisplayName.Valid || records[0].AgentDisplayName.String != "Reviewer Shieldmaiden" {
		t.Fatalf("expected latest review agent display name, got %#v", records[0].AgentDisplayName)
	}
	if records[1].NodeID != "validation_gates_qa_scout" || records[1].AttemptID != "run_1:validation_gates_qa_scout:1" {
		t.Fatalf("expected latest validator output record, got %#v", records[1])
	}
	if !records[1].OutputText.Valid || records[1].OutputText.String != "VERDICT: REWORK\n\nLatest validator failure." {
		t.Fatalf("expected latest validator output text, got %#v", records[1].OutputText)
	}
	if !records[1].OutcomeReasonCode.Valid || records[1].OutcomeReasonCode.String != "validation_gate_rework" {
		t.Fatalf("expected latest validator reason code, got %#v", records[1].OutcomeReasonCode)
	}
}

func TestStoreAcquireLeaseHonorsExpiry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	acquired, err := s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_a",
		LeaseUntil:   "2026-03-01T12:01:00Z",
		AcquiredAt:   "2026-03-01T12:00:10Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_a: %v", err)
	}
	if !acquired {
		t.Fatal("expected first lease acquire to succeed")
	}

	acquired, err = s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_b",
		LeaseUntil:   "2026-03-01T12:02:00Z",
		AcquiredAt:   "2026-03-01T12:00:30Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_b before expiry: %v", err)
	}
	if acquired {
		t.Fatal("expected competing lease acquire before expiry to fail")
	}

	acquired, err = s.AcquireLease(ctx, AcquireLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   "worker_b",
		LeaseUntil:   "2026-03-01T12:03:00Z",
		AcquiredAt:   "2026-03-01T12:01:01Z",
		StateOnLease: "running",
	})
	if err != nil {
		t.Fatalf("acquire lease worker_b after expiry: %v", err)
	}
	if !acquired {
		t.Fatal("expected lease steal after expiry to succeed")
	}
}

func TestStoreClaimQueuedProviderAttemptFencesCompetingClaimantsWithoutStarting(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CompletionMode: ProviderCompletionModeServerResponse,
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}

	first, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker-a",
		LeaseUntil: "2026-03-01T12:10:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim queued attempt: acquired=%t err=%v", acquired, err)
	}
	if first.State != "queued" || first.LeaseOwner != "worker-a" || first.LeaseVersion != 1 || first.LeaseExpiresAt != "2026-03-01T12:10:00Z" {
		t.Fatalf("unexpected first claim: %#v", first)
	}
	if first.StartedAt != "" || first.LaunchStartedAt != "" {
		t.Fatalf("queued claim fabricated provider start timestamps: %#v", first)
	}

	if second, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker-b",
		LeaseUntil: "2026-03-01T12:20:00Z",
		ClaimedAt:  "2026-03-01T12:00:02Z",
	}); err != nil || acquired || second.AttemptID != "" {
		t.Fatalf("competing claim should lose without a record: claim=%#v acquired=%t err=%v", second, acquired, err)
	}

	second, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker-b",
		LeaseUntil: "2026-03-01T12:30:00Z",
		ClaimedAt:  "2026-03-01T12:10:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim expired queued attempt: acquired=%t err=%v", acquired, err)
	}
	if second.LeaseOwner != "worker-b" || second.LeaseVersion != 2 || second.StartedAt != "" {
		t.Fatalf("expected monotonically fenced second generation without provider start, got %#v", second)
	}
}

func TestStoreClaimQueuedProviderAttemptLeavesRuntimeOwnedLeaseEmpty(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeRuntimeOwned,
		CompletionMode: ProviderCompletionModeNotApplicable,
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}
	if claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker-a",
		LeaseUntil: "2026-03-01T12:10:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	}); err != nil || acquired || claim.AttemptID != "" {
		t.Fatalf("runtime-owned attempt must not be provider-claimed: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load runtime-owned attempt: found=%t err=%v", found, err)
	}
	if attempt.LeaseOwner != "" || attempt.LeaseExpiresAt != "" || attempt.LeaseVersion != 0 {
		t.Fatalf("runtime-owned attempt gained lease lineage: %#v", attempt)
	}
}

func TestStoreRenewProviderAttemptLeasePreservesGeneration(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CompletionMode: ProviderCompletionModeServerResponse,
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "worker-a",
		LeaseUntil: "2026-03-01T12:01:00Z",
		ClaimedAt:  "2026-03-01T12:00:00Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: acquired=%t err=%v", acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'starting' WHERE attempt_id = 'run_1:decree_intake:1'`)

	renewed, err := s.RenewProviderAttemptLease(ctx, RenewProviderAttemptLeaseRequest{
		AttemptID:    attemptID,
		LeaseOwner:   claim.LeaseOwner,
		LeaseVersion: claim.LeaseVersion,
		LeaseUntil:   "2026-03-01T12:02:00Z",
		RenewedAt:    "2026-03-01T12:00:30Z",
	})
	if err != nil || !renewed {
		t.Fatalf("renew provider attempt lease: renewed=%t err=%v", renewed, err)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load renewed attempt: found=%t err=%v", found, err)
	}
	if attempt.LeaseExpiresAt != "2026-03-01T12:02:00Z" || attempt.LeaseOwner != claim.LeaseOwner || attempt.LeaseVersion != claim.LeaseVersion {
		t.Fatalf("renewal changed lease lineage: %#v", attempt)
	}
}

func TestStoreRenewProviderAttemptLeaseRejectsWrongOrExpiredOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		owner      string
		version    int64
		renewedAt  string
		clearLease bool
	}{
		{name: "wrong owner", owner: "worker-b", version: 1, renewedAt: "2026-03-01T12:00:30Z"},
		{name: "wrong version", owner: "worker-a", version: 2, renewedAt: "2026-03-01T12:00:30Z"},
		{name: "expired lease", owner: "worker-a", version: 1, renewedAt: "2026-03-01T12:01:00Z"},
		{name: "cleared lease", owner: "worker-a", version: 1, renewedAt: "2026-03-01T12:00:30Z", clearLease: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			s := New(db)
			ctx := context.Background()
			const attemptID = "run_1:decree_intake:1"
			if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
				t.Fatalf("enqueue provider attempt: %v", err)
			}
			if _, acquired, err := s.ClaimQueuedProviderAttempt(ctx, ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-a", LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: "2026-03-01T12:00:00Z"}); err != nil || !acquired {
				t.Fatalf("claim provider attempt: acquired=%t err=%v", acquired, err)
			}
			mustExec(t, db, `UPDATE attempt_executions SET state = 'running' WHERE attempt_id = 'run_1:decree_intake:1'`)
			if tt.clearLease {
				mustExec(t, db, `UPDATE attempt_executions SET lease_owner = '', lease_expires_at = NULL WHERE attempt_id = 'run_1:decree_intake:1'`)
			}
			renewed, err := s.RenewProviderAttemptLease(ctx, RenewProviderAttemptLeaseRequest{
				AttemptID:    attemptID,
				LeaseOwner:   tt.owner,
				LeaseVersion: tt.version,
				LeaseUntil:   "2026-03-01T12:02:00Z",
				RenewedAt:    tt.renewedAt,
			})
			if err != nil {
				t.Fatalf("renew rejected lease: %v", err)
			}
			if renewed {
				t.Fatal("expected renewal to lose its fence")
			}
		})
	}
}

func TestStoreCommitRecoveredTerminalRejectsActiveUnsettledProviderInvocation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	s := New(db)
	ctx := context.Background()
	const attemptID = "run_1:recovered_terminal_guard:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  ExecutionModeProviderBacked,
		CompletionMode: ProviderCompletionModeReceiptRequired,
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := s.CreateProviderInvocation(ctx, CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_recovered_terminal_guard",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       ProviderCompletionModeReceiptRequired,
		CompletionCapability: "capability-recovered-terminal-guard",
		CreatedAt:            "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("create invocation: %v", err)
	}
	if _, err := s.MarkProviderInvocationOpen(ctx, MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_recovered_terminal_guard",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_recovered_terminal_guard",
		OpenedAt:             "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("open invocation: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatalf("mark attempt running: %v", err)
	}

	_, err := s.CommitRecoveredTerminal(ctx, CommitRecoveredTerminalRequest{
		AttemptID:          attemptID,
		RunID:              "run_1",
		NodeRunID:          "nr_1",
		TerminalState:      "committed_failure",
		CommittedAt:        "2026-03-01T12:10:00Z",
		TerminalReasonCode: "agent_unresponsive",
		FailureCode:        "agent_unresponsive",
		FailureText:        "generic recovery must not win provider terminal authority",
	})
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Reason != "canonical_settlement_required" {
		t.Fatalf("expected canonical settlement guard, got %v", err)
	}
	attempt, found, err := s.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load guarded attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "running" || attempt.CanonicalSettlementID != "" {
		t.Fatalf("recovered terminal rejection changed provider attempt truth: %#v", attempt)
	}
	if count := countPhase2Table(t, db, "attempt_event_log"); count != 0 {
		t.Fatalf("recovered terminal rejection persisted %d attempt events", count)
	}
}

func TestStoreNextAttemptSeqAndQueuedListing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	queued, err := s.ListQueuedAttempts(ctx, 10)
	if err != nil {
		t.Fatalf("list queued attempts: %v", err)
	}
	if len(queued) != 1 || queued[0].AttemptID != attemptID {
		t.Fatalf("expected one queued attempt %q, got %#v", attemptID, queued)
	}

	seq, err := s.NextAttemptSeq(ctx, attemptID)
	if err != nil {
		t.Fatalf("next attempt seq (empty): %v", err)
	}
	if seq != 1 {
		t.Fatalf("expected next seq 1 for empty event log, got %d", seq)
	}

	if err := s.AppendAttemptEvent(ctx, AppendAttemptEventRequest{
		AttemptEventID: "ael_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            1,
		EventType:      "attempt.started",
		ReasonCode:     "orchestration_progress",
		PayloadJSON:    `{}`,
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("append attempt event: %v", err)
	}

	seq, err = s.NextAttemptSeq(ctx, attemptID)
	if err != nil {
		t.Fatalf("next attempt seq after append: %v", err)
	}
	if seq != 2 {
		t.Fatalf("expected next seq 2 after one event, got %d", seq)
	}
}

func TestStoreListPendingOutboxAndPublish(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"ok"}`,
		CreatedAt:     "2026-03-01T12:00:30Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	pending, err := s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	if len(pending) != 1 || pending[0].OutboxID != "outbox_terminal_1" {
		t.Fatalf("expected pending outbox record, got %#v", pending)
	}

	if err := s.MarkOutboxPublished(ctx, "outbox_terminal_1", "2026-03-01T12:00:40Z"); err != nil {
		t.Fatalf("mark outbox published: %v", err)
	}

	pending, err = s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox after publish: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending outbox records after publish, got %#v", pending)
	}
}

func TestStoreRecordOutboxFailureAndMarkFailed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"ok"}`,
		CreatedAt:     "2026-03-01T12:00:30Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	if err := s.RecordOutboxFailure(ctx, "outbox_terminal_1", "projection failed", "2026-03-01T12:00:35Z"); err != nil {
		t.Fatalf("record outbox failure: %v", err)
	}

	var status string
	var publishAttempts int
	var lastError string
	if err := db.QueryRow(`SELECT status, publish_attempts, last_error FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_1'`).Scan(&status, &publishAttempts, &lastError); err != nil {
		t.Fatalf("query outbox failure bookkeeping: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected outbox to remain pending after retryable failure, got %q", status)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected publish_attempts=1 after retryable failure, got %d", publishAttempts)
	}
	if lastError != "projection failed" {
		t.Fatalf("expected last_error to be recorded, got %q", lastError)
	}

	if err := s.MarkOutboxFailed(ctx, "outbox_terminal_1", "malformed replay payload", "2026-03-01T12:00:36Z"); err != nil {
		t.Fatalf("mark outbox failed: %v", err)
	}
	if err := db.QueryRow(`SELECT status, publish_attempts, last_error FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_1'`).Scan(&status, &publishAttempts, &lastError); err != nil {
		t.Fatalf("query outbox failed status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("expected outbox failed status, got %q", status)
	}
	if publishAttempts != 2 {
		t.Fatalf("expected publish_attempts=2 after dead-lettering, got %d", publishAttempts)
	}
	if lastError != "malformed replay payload" {
		t.Fatalf("expected dead-letter last_error to be recorded, got %q", lastError)
	}

	pending, err := s.ListPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("list pending outbox after dead-lettering: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected failed outbox rows to be omitted from pending list, got %#v", pending)
	}
}

func TestStoreQueueAttemptCancelIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	req := QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_cancelled_by_user",
		ReasonText:     "operator cancelled the run",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}
	if err := s.QueueAttemptCancel(ctx, req); err != nil {
		t.Fatalf("queue attempt cancel first insert: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, req); err != nil {
		t.Fatalf("queue attempt cancel second insert: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query attempt cancel outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one attempt cancel outbox row, got %d", count)
	}
}

func TestStoreListPendingAttemptCancelsPublishAndFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_terminalized",
		ReasonText:     "run moved to failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	pending, err := s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels: %v", err)
	}
	if len(pending) != 1 || pending[0].CancelOutboxID != "cancel_outbox_1" {
		t.Fatalf("expected one pending attempt cancel row, got %#v", pending)
	}
	if pending[0].SessionID != "session_run_1_decree_intake_1" {
		t.Fatalf("expected logical fallback session id, got %q", pending[0].SessionID)
	}
	if pending[0].HandoffState != "cancellation_requested" {
		t.Fatalf("expected new cancellation to request cancellation, got %q", pending[0].HandoffState)
	}

	if err := s.RecordAttemptCancelFailure(ctx, "cancel_outbox_1", "transport timeout", "awaiting_quiescence", "2026-03-01T12:00:03Z"); err != nil {
		t.Fatalf("record attempt cancel failure: %v", err)
	}

	var publishAttempts int
	var lastError string
	var handoffState string
	if err := db.QueryRow(`SELECT publish_attempts, last_error, handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&publishAttempts, &lastError, &handoffState); err != nil {
		t.Fatalf("query attempt cancel failure bookkeeping: %v", err)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected publish_attempts=1 after failure, got %d", publishAttempts)
	}
	if lastError != "transport timeout" {
		t.Fatalf("expected last_error to be recorded, got %q", lastError)
	}
	if handoffState != "awaiting_quiescence" {
		t.Fatalf("expected failed cancellation to await quiescence, got %q", handoffState)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'stop_retry_dispatched' WHERE cancel_outbox_id = 'cancel_outbox_1'`)
	pending, err = s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list dispatched stop retry handoffs: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected dispatched stop retry to wait for an operator action after restart, got %#v", pending)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'awaiting_quiescence' WHERE cancel_outbox_id = 'cancel_outbox_1'`)

	if err := s.MarkAttemptCancelPublished(ctx, "cancel_outbox_1", "2026-03-01T12:00:04Z"); err != nil {
		t.Fatalf("mark attempt cancel published: %v", err)
	}

	pending, err = s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels after publish: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending attempt cancels after publish, got %#v", pending)
	}

	var status string
	if err := db.QueryRow(`SELECT status, handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&status, &handoffState); err != nil {
		t.Fatalf("query attempt cancel status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected published attempt cancel status, got %q", status)
	}
	if handoffState != "released" {
		t.Fatalf("expected published cancellation handoff to be released, got %q", handoffState)
	}
}

func TestStoreListPendingAttemptCancelsPrefersProviderSessionID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.UpdateProviderSessionID(ctx, UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_real_456",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}
	if err := s.QueueAttemptCancel(ctx, QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "session_run_1_decree_intake_1",
		ReasonCode:     "run_terminalized",
		ReasonText:     "run moved to failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	pending, err := s.ListPendingAttemptCancels(ctx, 10)
	if err != nil {
		t.Fatalf("list pending attempt cancels: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one pending cancel, got %#v", pending)
	}
	if pending[0].SessionID != "ses_real_456" {
		t.Fatalf("expected provider session id preference, got %q", pending[0].SessionID)
	}
}

func TestStoreQueueOutboxIsIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	req := QueueOutboxRequest{
		OutboxID:      "outbox_queue_1",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   1,
		TerminalState: "committed_success",
		PayloadJSON:   `{"phase":"exec_done"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}
	if err := s.QueueOutbox(ctx, req); err != nil {
		t.Fatalf("queue outbox first insert: %v", err)
	}
	if err := s.QueueOutbox(ctx, req); err != nil {
		t.Fatalf("queue outbox second insert: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ? AND terminal_seq = 1`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query queued outbox rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one queued outbox row, got %d", count)
	}
}

func TestStoreFinalizeTerminalWritesSettledAttemptStateAndUpsertsLegacyEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_finalize",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"raw"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	if err := s.AppendAttemptEvent(ctx, AppendAttemptEventRequest{
		AttemptEventID: "attempt_event:run_1:decree_intake:1:2",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            2,
		EventType:      "attempt.succeeded",
		ReasonCode:     "execution_completed",
		PayloadJSON:    `{"status":"raw"}`,
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("append legacy terminal event: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:          "outbox_terminal_finalize",
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		TerminalState:     "committed_failure",
		PayloadJSON:       `{"error":"settled failure"}`,
		CommittedAt:       "2026-03-01T12:00:30Z",
		FailureCode:       "provider_empty_output",
		FailureText:       "settled failure",
		ProviderSessionID: "ses_terminal_2",
	})
	if err != nil {
		t.Fatalf("finalize terminal: %v", err)
	}
	if !result.Applied || result.AttemptState != "committed_failure" {
		t.Fatalf("expected finalized terminal result, got %#v", result)
	}

	var state, failureCode, failureText, providerSessionID string
	if err := db.QueryRow(`SELECT state, failure_code, failure_text, provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode, &failureText, &providerSessionID); err != nil {
		t.Fatalf("query finalized attempt: %v", err)
	}
	if state != "committed_failure" {
		t.Fatalf("expected committed_failure finalized state, got %q", state)
	}
	if failureCode != "provider_empty_output" || failureText != "settled failure" {
		t.Fatalf("unexpected finalized failure metadata code=%q text=%q", failureCode, failureText)
	}
	if providerSessionID != "ses_terminal_2" {
		t.Fatalf("expected finalized provider session id ses_terminal_2, got %q", providerSessionID)
	}

	var eventType, reasonCode, payload string
	if err := db.QueryRow(`SELECT event_type, reason_code, payload_json FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&eventType, &reasonCode, &payload); err != nil {
		t.Fatalf("query finalized terminal event: %v", err)
	}
	if eventType != "attempt.failed" || reasonCode != "provider_empty_output" || payload != `{"error":"settled failure"}` {
		t.Fatalf("unexpected finalized terminal event type=%q reason=%q payload=%q", eventType, reasonCode, payload)
	}
}

func TestStoreFinalizeTerminalNoopsWhenAttemptAlreadyTerminalized(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = '2026-03-01T12:00:05Z',
		    committed_at = '2026-03-01T12:00:05Z',
		    failure_code = 'run_terminalized',
		    failure_text = 'run cancelled before completion',
		    updated_at = '2026-03-01T12:00:05Z'
		WHERE attempt_id = 'run_1:decree_intake:1'`)
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_noop",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"status":"late-success"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:          "outbox_terminal_noop",
		AttemptID:         attemptID,
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		TerminalState:     "committed_success",
		PayloadJSON:       `{"status":"late-success"}`,
		CommittedAt:       "2026-03-01T12:00:30Z",
		ProviderSessionID: "ses_late_success",
	})
	if err != nil {
		t.Fatalf("finalize terminal noop: %v", err)
	}
	if result.Applied {
		t.Fatalf("expected finalized terminal noop result, got %#v", result)
	}
	if result.AttemptState != "committed_cancelled" {
		t.Fatalf("expected finalized terminal noop to preserve committed_cancelled, got %#v", result)
	}

	var state, failureCode, providerSessionID string
	if err := db.QueryRow(`SELECT state, failure_code, provider_session_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode, &providerSessionID); err != nil {
		t.Fatalf("query finalized noop attempt: %v", err)
	}
	if state != "committed_cancelled" || failureCode != "run_terminalized" {
		t.Fatalf("expected cancelled attempt state to remain canonical, got state=%q failure_code=%q", state, failureCode)
	}
	if providerSessionID != "" {
		t.Fatalf("expected noop finalization not to rewrite provider session id, got %q", providerSessionID)
	}

	var terminalEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&terminalEventCount); err != nil {
		t.Fatalf("count terminal noop attempt events: %v", err)
	}
	if terminalEventCount != 0 {
		t.Fatalf("expected noop finalization not to rewrite terminal attempt events, got %d", terminalEventCount)
	}

	var outboxTerminalState string
	if err := db.QueryRow(`SELECT terminal_state FROM attempt_outbox WHERE outbox_id = 'outbox_terminal_noop'`).Scan(&outboxTerminalState); err != nil {
		t.Fatalf("query terminal noop outbox state: %v", err)
	}
	if outboxTerminalState != "committed_cancelled" {
		t.Fatalf("expected noop finalization to preserve canonical outbox terminal state, got %q", outboxTerminalState)
	}
}

func TestStoreFinalizeTerminalPreservesSuccessReasonCodeInAttemptEventLog(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	s := New(db)
	ctx := context.Background()
	attemptID := "run_1:decree_intake:1"
	if err := s.EnqueueAttempt(ctx, EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: ProviderCompletionModeReceiptRequired,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := s.QueueOutbox(ctx, QueueOutboxRequest{
		OutboxID:      "outbox_terminal_success_reason",
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		TerminalSeq:   2,
		TerminalState: "committed_success",
		PayloadJSON:   `{"attempt_id":"run_1:decree_intake:1"}`,
		CreatedAt:     "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue outbox: %v", err)
	}

	result, err := s.FinalizeTerminal(ctx, FinalizeTerminalRequest{
		OutboxID:           "outbox_terminal_success_reason",
		AttemptID:          attemptID,
		RunID:              "run_1",
		NodeRunID:          "nr_1",
		TerminalState:      "committed_success",
		PayloadJSON:        `{"status":"ok"}`,
		CommittedAt:        "2026-03-01T12:00:30Z",
		TerminalReasonCode: "execution_review_cap_auto_succeeded",
	})
	if err != nil {
		t.Fatalf("finalize terminal success reason: %v", err)
	}
	if !result.Applied || result.AttemptState != "committed_success" {
		t.Fatalf("expected committed_success finalize result, got %#v", result)
	}

	var state, failureCode string
	if err := db.QueryRow(`SELECT state, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &failureCode); err != nil {
		t.Fatalf("query finalized success attempt: %v", err)
	}
	if state != "committed_success" {
		t.Fatalf("expected committed_success state, got %q", state)
	}
	if failureCode != "" {
		t.Fatalf("expected success finalize to keep failure_code blank, got %q", failureCode)
	}

	var eventType, reasonCode string
	if err := db.QueryRow(`SELECT event_type, reason_code FROM attempt_event_log WHERE attempt_id = ? AND seq = 2`, attemptID).Scan(&eventType, &reasonCode); err != nil {
		t.Fatalf("query finalized success attempt event: %v", err)
	}
	if eventType != "attempt.succeeded" {
		t.Fatalf("expected attempt.succeeded event type, got %q", eventType)
	}
	if reasonCode != "execution_review_cap_auto_succeeded" {
		t.Fatalf("expected success reason override to be preserved, got %q", reasonCode)
	}
}

func seedCanonicalProviderExecutionOutput(t *testing.T, db *sql.DB, attemptID, runID, nodeRunID, stateValue, failureCode, failureText, outputText, createdAt string) {
	t.Helper()
	s := New(db)
	providerInvocationID := fmt.Sprintf("pinv_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	providerSessionID := fmt.Sprintf("ses_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID))
	if err := s.EnqueueAttempt(context.Background(), EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             runID,
		NodeRunID:         nodeRunID,
		ExecutionMode:     ExecutionModeProviderBacked,
		CompletionMode:    ProviderCompletionModeServerResponse,
		SessionRef:        "session_" + strings.NewReplacer(":", "_", "-", "_").Replace(attemptID),
		ProviderSessionID: providerSessionID,
		CreatedAt:         createdAt,
	}); err != nil {
		t.Fatalf("enqueue canonical provider attempt %q: %v", attemptID, err)
	}
	claim, acquired, err := s.ClaimQueuedProviderAttempt(context.Background(), ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "fixture_worker_" + strings.NewReplacer(":", "_", "-", "_").Replace(attemptID),
		LeaseUntil: "2099-12-31T23:59:59Z",
		ClaimedAt:  createdAt,
	})
	if err != nil || !acquired {
		t.Fatalf("claim canonical provider attempt %q: claim=%#v acquired=%t err=%v", attemptID, claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = ? WHERE attempt_id = ?`, createdAt, attemptID); err != nil {
		t.Fatalf("mark canonical provider attempt %q running: %v", attemptID, err)
	}
	if _, err := s.ActivateProviderInvocation(context.Background(), ActivateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     "session_" + strings.NewReplacer(":", "_", "-", "_").Replace(attemptID),
		ProviderSessionID:    providerSessionID,
		CompletionMode:       ProviderCompletionModeServerResponse,
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
		OpenedAt:             createdAt,
	}); err != nil {
		t.Fatalf("activate canonical provider invocation %q: %v", attemptID, err)
	}
	acknowledgeQuiescentProviderProcessGuard(t, s, providerInvocationID, attemptID, runID, nodeRunID, claim.LeaseOwner, claim.LeaseVersion)
	structuredPayloadJSON := canonicalStructuredPayloadForStoreTest(outputText)
	var requiredRole string
	if err := db.QueryRow(`SELECT required_role FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&requiredRole); err != nil {
		t.Fatalf("load canonical provider role %q: %v", attemptID, err)
	}
	settlementOutcome := ProviderCompletionOutcomeSuccess
	switch strings.TrimSpace(stateValue) {
	case "committed_failure":
		settlementOutcome = ProviderCompletionOutcomeFailed
	case "committed_cancelled":
		settlementOutcome = ProviderCompletionOutcomeCancelled
	}
	zero := 0
	observation := providerexecution.InvocationStreamObservation{
		ProviderSessionID:      providerSessionID,
		FinalTurnID:            "turn_" + providerInvocationID,
		FinalStepID:            "step_" + providerInvocationID,
		FinalSequence:          1,
		RawFinishReason:        "stop",
		NormalizedFinishReason: "stop",
		CanonicalTerminalText:  outputText,
		ProcessExited:          true,
		ProcessExitCode:        &zero,
		StdoutDrained:          true,
		StderrDrained:          true,
	}
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), SettleProviderCompletionAndTerminalRequest{
		Settlement: FinalizeProviderCompletionSettlementRequest{
			SettlementID:          fmt.Sprintf("settlement_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID)),
			ProviderInvocationID:  providerInvocationID,
			AttemptID:             attemptID,
			RunID:                 runID,
			NodeRunID:             nodeRunID,
			Outcome:               settlementOutcome,
			FinalText:             outputText,
			StructuredPayloadJSON: structuredPayloadJSON,
			FailureReasonCode:     failureCode,
			FailureReasonText:     failureText,
			SettledAt:             createdAt,
		},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
		TokenUsage:           &ProviderInvocationTokenUsage{RequiredRole: requiredRole},
		Terminal: CommitTerminalRequest{
			OutboxID:           fmt.Sprintf("outbox_fixture_%s", strings.NewReplacer(":", "_", "-", "_").Replace(attemptID)),
			AttemptID:          attemptID,
			RunID:              runID,
			NodeRunID:          nodeRunID,
			TerminalState:      stateValue,
			PayloadJSON:        `{}`,
			TerminalReasonCode: failureCode,
			FailureCode:        failureCode,
			FailureText:        failureText,
			ProviderSessionID:  providerSessionID,
		},
	}); err != nil {
		t.Fatalf("settle canonical provider execution %q: %v", attemptID, err)
	}
}

func canonicalStructuredPayloadForStoreTest(outputText string) string {
	for _, line := range strings.Split(strings.TrimSpace(outputText), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch trimmed {
		case "VERDICT: PASS":
			return `{"verdict":"PASS"}`
		case "VERDICT: REWORK":
			return `{"verdict":"REWORK"}`
		default:
			return `{}`
		}
	}
	return `{}`
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	return db
}

func applyMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()
	_ = fileName

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range entries {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func seedMinimumGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_1', 'wfdef_1', 'decree_intake', 'royal_closure', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, NULL, NULL)`)
}

func TestStoreListRunNodeExecutionsIncludesNodesWithoutPhysicalAttempts(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	records, err := New(db).ListRunNodeExecutions(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("list logical node executions: %v", err)
	}
	if len(records) != 2 || records[0].NodeID != "decree_intake" || records[1].NodeID != "royal_closure" {
		t.Fatalf("unexpected logical execution rows: %#v", records)
	}
	if records[1].Current != nil || records[1].AttemptCount != 0 {
		t.Fatalf("expected node without physical attempt to remain nullable: %#v", records[1])
	}
}

func TestStoreListRunNodeExecutionsOrdersLogicalNodesByCurrentActivity(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	for _, nodeID := range []string{"active_running", "active_starting", "blocked_node", "ready_node", "pending_node"} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_%s', 'wfdef_1', '%s', 'king', 'advisory_consultation', '2026-03-01T12:00:00Z')`, nodeID, nodeID))
	}
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('nr_active_running', 'run_1', 'active_running', 1, 1, 'running', 'king', 'agent_king', 1, '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('nr_active_starting', 'run_1', 'active_starting', 1, 1, 'starting', 'king', 'agent_king', 1, '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('nr_blocked', 'run_1', 'blocked_node', 1, 1, 'blocked', 'king', 'agent_king', 1, '2026-03-01T12:30:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_ready', 'run_1', 'ready_node', 1, 1, 'ready', 'king', 'agent_king', 1)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('attempt_ready', 'run_1', 'nr_ready', 'queued', 'provider_backed', 'server_response', '2026-03-01T12:20:00Z', '2026-03-01T12:20:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, finished_at) VALUES ('nr_closure', 'run_1', 'royal_closure', 1, 1, 'succeeded', 'king', 'agent_king', 1, '2026-03-01T12:50:00Z')`)

	records, err := New(db).ListRunNodeExecutions(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("list logical node executions: %v", err)
	}
	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.NodeID)
	}
	want := []string{"decree_intake", "active_running", "active_starting", "blocked_node", "ready_node", "pending_node", "royal_closure"}
	if !slices.Equal(got, want) {
		t.Fatalf("expected current-activity order %v, got %v", want, got)
	}
}

func mustExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("exec failed for %q: %v", statement, err)
	}
}
