package orchestration

import (
	"context"
	"testing"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func TestExecutionSpecForNodeUsesApprovedCompletionModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		nodeID         string
		nodeClass      string
		completionMode executionstore.ProviderCompletionMode
	}{
		{name: "advisor", nodeID: "architecture_consultation", nodeClass: workflow.NodeClassAdvisorConsultation, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "synthesis", nodeID: "advisory_synthesis", nodeClass: workflow.NodeClassAdvisorySynthesis, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "plan drafting", nodeID: "plan_drafting", nodeClass: workflow.NodeClassPlanDrafting, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "review", nodeID: "execution_review", nodeClass: workflow.NodeClassExecutionReview, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "validation", nodeID: "quality_validation", nodeClass: workflow.NodeClassValidationGates, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "builder", nodeID: "implementation", nodeClass: workflow.NodeClassDelegatedExecution, completionMode: executionstore.ProviderCompletionModeServerResponse},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			spec := executionSpecForNode(test.nodeID, test.nodeClass)
			if spec.Mode != executionstore.ExecutionModeProviderBacked {
				t.Fatalf("execution mode = %q, want provider-backed", spec.Mode)
			}
			if spec.CompletionMode != test.completionMode {
				t.Fatalf("completion mode = %q, want %q", spec.CompletionMode, test.completionMode)
			}

			if spec.CompletionMode != executionstore.ProviderCompletionModeServerResponse {
				t.Fatalf("provider node %q does not use server-response completion: %q", test.name, spec.CompletionMode)
			}
		})
	}
}

func TestCommitProviderExecutionCompletionRejectsNonterminalServerResponse(t *testing.T) {
	t.Parallel()

	const (
		runID              = "run_server_response"
		workOrderID        = "work_order_server_response"
		workflowID         = "wfdef_server_response"
		nodeRunID          = "nr_server_response"
		nodeID             = "architecture_consultation"
		agentID            = "agent_server_response"
		attemptID          = "run_server_response:architecture_consultation:1"
		invocationID       = "invocation_server_response"
		logicalSessionID   = "session_server_response"
		attemptStartedAt   = "2026-03-01T12:00:00Z"
		attemptClaimedAt   = "2026-03-01T12:00:01Z"
		invocationOpenedAt = "2026-03-01T12:00:02Z"
	)

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        runID,
		WorkOrderID:  workOrderID,
		WorkflowID:   workflowID,
		NodeRunID:    nodeRunID,
		NodeID:       nodeID,
		NodeClass:    workflow.NodeClassAdvisorConsultation,
		RequiredRole: "architecture-advisor",
		AgentID:      agentID,
		RoleIDs:      []string{"architecture-advisor"},
	})
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+attemptStartedAt+`' WHERE node_run_id = '`+nodeRunID+`'`)

	ctx := context.Background()
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          runID,
		NodeRunID:      nodeRunID,
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     logicalSessionID,
		CreatedAt:      attemptStartedAt,
	}); err != nil {
		t.Fatalf("enqueue response-first attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "server-response-test-worker",
		LeaseUntil: "2099-03-01T12:10:00Z",
		ClaimedAt:  attemptClaimedAt,
	})
	if err != nil || !acquired {
		t.Fatalf("claim response-first attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '`+attemptStartedAt+`' WHERE attempt_id = '`+attemptID+`'`)

	requests := newRequestFixture(db, nil)
	prepared := requests.prepare(t, attemptID, invocationID)
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		RequestIdentity:      &prepared.Identity,
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     logicalSessionID,
		ProviderSessionID:    prepared.ProviderSessionID,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             invocationOpenedAt,
		CreatedAt:            invocationOpenedAt,
		OpenedAt:             invocationOpenedAt,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	}); err != nil {
		t.Fatalf("activate response-first invocation: %v", err)
	}
	response := requests.run(t, attemptID, invocationID, opencode.ExecuteResponse{
		Accepted:          true,
		OutputText:        "Partial streamed response",
		StreamObservation: opencode.InvocationStreamObservation{PendingToolCount: 1},
	}, nil)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime filesystem store: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		executionStore: store,
		fsStore:        fsStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC)
		},
		sleep: func(time.Duration) {},
	}
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: runID, RunStatus: "running"},
		node:                      nodeDefinition{NodeID: nodeID, NodeClass: workflow.NodeClassAdvisorConsultation, RequiredRole: "architecture-advisor"},
		attempt:                   nodeAttempt{NodeRunID: nodeRunID, NodeID: nodeID, Attempt: 1, RequiredRole: "architecture-advisor", AgentID: agentID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:                 attemptID,
		providerInvocationID:      invocationID,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		outputText:                "Direct adapter text must not authorize a nonterminal response.",
		providerSessionID:         response.ProviderSessionID,
		providerStreamObservation: response.StreamObservation,
		outcome: opencode.ExecutionOutcome{
			State:          "success",
			ReasonCode:     "execution_completed",
			EvidenceSource: "stream",
		},
	}
	if err := loop.persistProviderTerminalHandoff(ctx, &result); err != nil {
		t.Fatalf("persist provider terminal handoff: %v", err)
	}
	if err := loop.commitProviderExecutionCompletion(ctx, result); err != nil {
		t.Fatalf("settle nonterminal response observation: %v", err)
	}

	attempt, found, err := store.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("get settled response-first attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "committed_failure" || attempt.FailureCode != "provider_incomplete_turn" {
		t.Fatalf("nonterminal server response settled as state=%q failure=%q, want committed_failure/provider_incomplete_turn", attempt.State, attempt.FailureCode)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(ctx, invocationID)
	if err != nil || !found {
		t.Fatalf("get response-first settlement: found=%t err=%v", found, err)
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("nonterminal server response settlement=%#v, want failed/provider_incomplete_turn", settlement)
	}

	invocation, found, err := store.GetProviderInvocation(ctx, invocationID)
	if err != nil || !found {
		t.Fatalf("get response-first invocation: found=%t err=%v", found, err)
	}
	if invocation.CompletionMode != executionstore.ProviderCompletionModeServerResponse {
		t.Fatalf("invocation completion mode = %q, want server response", invocation.CompletionMode)
	}
	var primaryEvidenceKind string
	if err := db.QueryRow(`SELECT evidence_kind FROM provider_completion_evidence WHERE evidence_id = ?`, settlement.PrimaryEvidenceID).Scan(&primaryEvidenceKind); err != nil {
		t.Fatalf("load response-first settlement primary evidence: %v", err)
	}
	if primaryEvidenceKind != string(executionstore.ProviderCompletionEvidenceKindStreamObservation) {
		t.Fatalf("response-first settlement primary evidence kind=%q, want stream observation", primaryEvidenceKind)
	}
	var streamEvidenceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_completion_evidence WHERE provider_invocation_id = ? AND evidence_kind = 'stream_observation'`, invocationID).Scan(&streamEvidenceCount); err != nil {
		t.Fatalf("count server-response stream evidence: %v", err)
	}
	if streamEvidenceCount != 1 {
		t.Fatalf("response-first evidence stream=%d, want one stream observation", streamEvidenceCount)
	}
}
