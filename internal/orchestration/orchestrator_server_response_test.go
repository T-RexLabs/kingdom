package orchestration

import (
	"context"
	"testing"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
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
		{name: "advisor", nodeID: "advisor_consultation_noble", nodeClass: workflow.NodeClassAdvisorConsultation, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "synthesis", nodeID: "advisory_synthesis", nodeClass: workflow.NodeClassAdvisorySynthesis, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "plan drafting", nodeID: "plan_drafting", nodeClass: workflow.NodeClassPlanDrafting, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "review", nodeID: "execution_review_shieldmaiden", nodeClass: workflow.NodeClassExecutionReview, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "validation", nodeID: "validation_gates_qa_scout", nodeClass: workflow.NodeClassValidationGates, completionMode: executionstore.ProviderCompletionModeServerResponse},
		{name: "builder", nodeID: "delegated_execution_berserker", nodeClass: workflow.NodeClassDelegatedExecution, completionMode: executionstore.ProviderCompletionModeReceiptRequired},
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

			capability := completionCapabilityForMode(spec.CompletionMode, "test-capability")
			receiptDir := completionReceiptDirForMode(spec.CompletionMode, "/receipts/test")
			protocolVersion := completionProtocolVersionForMode(spec.CompletionMode)
			if test.completionMode == executionstore.ProviderCompletionModeReceiptRequired {
				if capability != "test-capability" || receiptDir != "/receipts/test" || protocolVersion != opencode.CompletionProtocolVersion {
					t.Fatalf("builder receipt configuration capability=%q directory=%q protocol=%q", capability, receiptDir, protocolVersion)
				}
				return
			}
			if capability != "" || receiptDir != "" || protocolVersion != "" {
				t.Fatalf("response-first completion exposed receipt configuration capability=%q directory=%q protocol=%q", capability, receiptDir, protocolVersion)
			}
		})
	}
}

func TestCommitProviderExecutionCompletionRejectsNonterminalServerResponse(t *testing.T) {
	t.Parallel()

	const (
		runID              = "run_server_response"
		decreeID           = "decree_server_response"
		workflowID         = "wfdef_server_response"
		nodeRunID          = "nr_server_response"
		nodeID             = "advisor_consultation_noble"
		agentID            = "agent_server_response"
		attemptID          = "run_server_response:advisor_consultation_noble:1"
		invocationID       = "invocation_server_response"
		logicalSessionID   = "session_server_response"
		attemptStartedAt   = "2026-03-01T12:00:00Z"
		attemptClaimedAt   = "2026-03-01T12:00:01Z"
		invocationOpenedAt = "2026-03-01T12:00:02Z"
		terminalObservedAt = "2026-03-01T12:00:03Z"
	)

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        runID,
		DecreeID:     decreeID,
		WorkflowID:   workflowID,
		NodeRunID:    nodeRunID,
		NodeID:       nodeID,
		NodeClass:    workflow.NodeClassAdvisorConsultation,
		RequiredRole: "noble-delegate",
		AgentID:      agentID,
		RoleIDs:      []string{"noble-delegate"},
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

	observation := testSuccessfulExecuteObservation(opencode.ExecuteRequest{InvocationID: invocationID}, "Partial streamed response")
	observation.RawFinishReason = "tool-calls"
	observation.NormalizedFinishReason = "tool-calls"
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     logicalSessionID,
		ProviderSessionID:    observation.ProviderSessionID,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             invocationOpenedAt,
		CreatedAt:            invocationOpenedAt,
		OpenedAt:             invocationOpenedAt,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	}); err != nil {
		t.Fatalf("activate response-first invocation: %v", err)
	}
	attachQuiescentProviderProcessGuard(t, store, invocationID, attemptID, runID, nodeRunID, claim.LeaseOwner, claim.LeaseVersion, terminalObservedAt)

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		executionStore: store,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC)
		},
		sleep: func(time.Duration) {},
	}
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: runID, RunStatus: "running"},
		node:                      nodeDefinition{NodeID: nodeID, NodeClass: workflow.NodeClassAdvisorConsultation, RequiredRole: "noble-delegate"},
		attempt:                   nodeAttempt{NodeRunID: nodeRunID, NodeID: nodeID, Attempt: 1, RequiredRole: "noble-delegate", AgentID: agentID},
		attemptID:                 attemptID,
		providerInvocationID:      invocationID,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		outputText:                "Direct adapter text must not authorize a nonterminal response.",
		providerStreamObservation: observation,
		outcome: opencode.ExecutionOutcome{
			State:          "success",
			ReasonCode:     "execution_completed",
			EvidenceSource: "stream",
		},
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
	if invocation.CompletionMode != executionstore.ProviderCompletionModeServerResponse || invocation.CompletionCapabilityHash != "" || invocation.CompletionCapabilityIssuedAt != "" || invocation.ReceiptSeenCount != 0 {
		t.Fatalf("response-first invocation unexpectedly used receipt authority: %#v", invocation)
	}
	var primaryEvidenceKind string
	if err := db.QueryRow(`SELECT evidence_kind FROM provider_completion_evidence WHERE evidence_id = ?`, settlement.PrimaryEvidenceID).Scan(&primaryEvidenceKind); err != nil {
		t.Fatalf("load response-first settlement primary evidence: %v", err)
	}
	if primaryEvidenceKind != string(executionstore.ProviderCompletionEvidenceKindStreamObservation) {
		t.Fatalf("response-first settlement primary evidence kind=%q, want stream observation", primaryEvidenceKind)
	}
	var streamEvidenceCount, receiptEvidenceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_completion_evidence WHERE provider_invocation_id = ? AND evidence_kind = 'stream_observation'`, invocationID).Scan(&streamEvidenceCount); err != nil {
		t.Fatalf("count server-response stream evidence: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_completion_evidence WHERE provider_invocation_id = ? AND evidence_kind IN ('completion_receipt', 'completion_receipt_attempt')`, invocationID).Scan(&receiptEvidenceCount); err != nil {
		t.Fatalf("count receipt evidence: %v", err)
	}
	if streamEvidenceCount != 1 || receiptEvidenceCount != 0 {
		t.Fatalf("response-first evidence stream=%d receipts=%d, want one stream observation and no receipt", streamEvidenceCount, receiptEvidenceCount)
	}
}
