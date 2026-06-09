package planning

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
	"kingdom/internal/testutil/planningfixture"

	_ "modernc.org/sqlite"
)

type mockAgent struct{}

var defaultPlanningServiceTestScript = planningfixture.DefaultServiceScript()

func testReceiptMetadata(input AgentContext, finalText string) (ProviderExecutionMetadata, error) {
	metadata := ProviderExecutionMetadata{FinalText: finalText, OutcomeState: "success", CompletionStructuredPayloadJSON: "{}"}
	if input.ProviderInvocation != nil && input.ProviderInvocation.OnProviderSession != nil {
		input.ProviderInvocation.OnProviderSession("provider-session-1")
	}
	if input.ProviderInvocation == nil || input.ProviderInvocation.OnCompletionReceipt == nil {
		return metadata, nil
	}
	accepted, err := input.ProviderInvocation.OnCompletionReceipt(CompletionReceipt{
		ProtocolVersion:       firstNonEmpty(input.ProviderInvocation.CompletionProtocolVersion, "v1"),
		AttemptID:             input.ProviderInvocation.AttemptID,
		InvocationID:          input.ProviderInvocation.InvocationID,
		CompletionCapability:  input.ProviderInvocation.CompletionCapability,
		FinalText:             finalText,
		StructuredPayloadJSON: "{}",
	})
	if err != nil {
		return ProviderExecutionMetadata{}, err
	}
	metadata.CompletionReceiptEvidenceID = accepted.EvidenceID
	metadata.CompletionStructuredPayloadJSON = firstNonEmpty(accepted.StructuredPayloadJSON, "{}")
	return metadata, nil
}

func (m *mockAgent) OpeningTurn(_ context.Context, input AgentContext) (AgentTurn, error) {
	return AgentTurn{
		ContentMarkdown: defaultPlanningServiceTestScript.OpeningContent,
		Question: &StructuredQuestion{
			Prompt: defaultPlanningServiceTestScript.OpeningQuestion.Prompt,
			Type:   defaultPlanningServiceTestScript.OpeningQuestion.Type,
		},
	}, nil
}

func (m *mockAgent) OpeningTurnStream(_ context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	turn, err := m.OpeningTurn(context.Background(), input)
	if err == nil && onChunk != nil {
		onChunk(turn.ContentMarkdown)
	}
	return turn, err
}

func (m *mockAgent) ContinueTurnStream(_ context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	metadata, err := testReceiptMetadata(input, defaultPlanningServiceTestScript.ContinueContent)
	if err != nil {
		return AgentTurn{}, err
	}
	turn := AgentTurn{
		ContentMarkdown: defaultPlanningServiceTestScript.ContinueContent,
		Question: &StructuredQuestion{
			Prompt: defaultPlanningServiceTestScript.ContinueQuestion.Prompt,
			Type:   defaultPlanningServiceTestScript.ContinueQuestion.Type,
		},
		Metadata: metadata,
	}
	if onChunk != nil {
		onChunk(turn.ContentMarkdown)
	}
	return turn, nil
}

func (m *mockAgent) FormalizePlanStream(_ context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error) {
	metadata, err := testReceiptMetadata(input, defaultPlanningServiceTestScript.FormalizedText)
	if err != nil {
		return FormalizedPlan{}, err
	}
	if onChunk != nil {
		onChunk("## Objective\n\n")
	}
	return FormalizedPlan{
		DecreeTitle: input.Title,
		DecreeText:  defaultPlanningServiceTestScript.FormalizedText,
		Metadata:    metadata,
	}, nil
}

func (m *mockAgent) GenerateTitle(_ context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error) {
	metadata, err := testReceiptMetadata(input, defaultPlanningServiceTestScript.GeneratedTitle)
	if err != nil {
		return GeneratedTitle{}, err
	}
	if onChunk != nil {
		onChunk(defaultPlanningServiceTestScript.GeneratedTitle)
	}
	return GeneratedTitle{
		Title:    defaultPlanningServiceTestScript.GeneratedTitle,
		Metadata: metadata,
	}, nil
}

type streamingMockAgent struct {
	chunks   []string
	question *StructuredQuestion
	final    string
	err      error
}

type parallelAutoTitlePlanner struct {
	mockAgent
	release            chan struct{}
	secondReplyStarted chan struct{}
	titleStarted       chan struct{}
	replyCalls         int
	secondReplyOnce    sync.Once
	titleStartedOnce   sync.Once
	mu                 sync.Mutex
}

type missingReceiptAgent struct{ mockAgent }

func (m *missingReceiptAgent) ContinueTurnStream(_ context.Context, _ AgentContext, onChunk func(text string)) (AgentTurn, error) {
	if onChunk != nil {
		onChunk(defaultPlanningServiceTestScript.ContinueContent)
	}
	return AgentTurn{
		ContentMarkdown: defaultPlanningServiceTestScript.ContinueContent,
		Question: &StructuredQuestion{
			Prompt: defaultPlanningServiceTestScript.ContinueQuestion.Prompt,
			Type:   defaultPlanningServiceTestScript.ContinueQuestion.Type,
		},
		Metadata: ProviderExecutionMetadata{FinalText: defaultPlanningServiceTestScript.ContinueContent, OutcomeState: "success"},
	}, nil
}

type invalidReceiptAgent struct{ mockAgent }

func (m *invalidReceiptAgent) ContinueTurnStream(_ context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	if onChunk != nil {
		onChunk(defaultPlanningServiceTestScript.ContinueContent)
	}
	if input.ProviderInvocation != nil && input.ProviderInvocation.OnCompletionReceipt != nil {
		_, err := input.ProviderInvocation.OnCompletionReceipt(CompletionReceipt{
			ProtocolVersion:       firstNonEmpty(input.ProviderInvocation.CompletionProtocolVersion, "v1"),
			AttemptID:             input.ProviderInvocation.AttemptID,
			InvocationID:          input.ProviderInvocation.InvocationID,
			CompletionCapability:  "wrong-capability",
			FinalText:             defaultPlanningServiceTestScript.ContinueContent,
			StructuredPayloadJSON: "{}",
		})
		if err != nil {
			return AgentTurn{}, err
		}
	}
	return AgentTurn{}, nil
}

type erroringAgent struct {
	err error
}

func (m *erroringAgent) ContinueTurnStream(_ context.Context, _ AgentContext, _ func(text string)) (AgentTurn, error) {
	return AgentTurn{}, m.err
}

func (m *erroringAgent) FormalizePlanStream(_ context.Context, _ AgentContext, _ func(text string)) (FormalizedPlan, error) {
	return FormalizedPlan{}, m.err
}

func (m *erroringAgent) GenerateTitle(_ context.Context, _ AgentContext, _ func(text string)) (GeneratedTitle, error) {
	return GeneratedTitle{}, m.err
}

func (m *streamingMockAgent) OpeningTurn(_ context.Context, input AgentContext) (AgentTurn, error) {
	return (&mockAgent{}).OpeningTurn(context.Background(), input)
}

func (m *streamingMockAgent) OpeningTurnStream(_ context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	return (&mockAgent{}).OpeningTurnStream(context.Background(), input, onChunk)
}

func (m *streamingMockAgent) ContinueTurnStream(_ context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	for _, chunk := range m.chunks {
		if onChunk != nil {
			onChunk(chunk)
		}
	}
	if m.err != nil {
		return AgentTurn{}, m.err
	}
	metadata, err := testReceiptMetadata(input, m.final)
	if err != nil {
		return AgentTurn{}, err
	}
	return AgentTurn{
		ContentMarkdown: m.final,
		Question:        m.question,
		Metadata:        metadata,
	}, nil
}

func (m *streamingMockAgent) FormalizePlanStream(_ context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error) {
	return (&mockAgent{}).FormalizePlanStream(context.Background(), input, onChunk)
}

func (m *streamingMockAgent) GenerateTitle(_ context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error) {
	return (&mockAgent{}).GenerateTitle(context.Background(), input, onChunk)
}

func newParallelAutoTitlePlanner() *parallelAutoTitlePlanner {
	return &parallelAutoTitlePlanner{
		release:            make(chan struct{}),
		secondReplyStarted: make(chan struct{}),
		titleStarted:       make(chan struct{}),
	}
}

func (p *parallelAutoTitlePlanner) ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	p.mu.Lock()
	p.replyCalls++
	call := p.replyCalls
	p.mu.Unlock()
	if call < 2 {
		return (&mockAgent{}).ContinueTurnStream(ctx, input, onChunk)
	}
	p.secondReplyOnce.Do(func() { close(p.secondReplyStarted) })
	select {
	case <-p.release:
		metadata, err := testReceiptMetadata(input, "Second reply content")
		if err != nil {
			return AgentTurn{}, err
		}
		return AgentTurn{
			ContentMarkdown: "Second reply content",
			Question: &StructuredQuestion{
				Prompt:  "Choose scope",
				Type:    "single_select",
				Options: []StructuredQuestionOption{{Label: "Lean", Value: "lean"}},
			},
			Metadata: metadata,
		}, nil
	case <-ctx.Done():
		return AgentTurn{}, ctx.Err()
	}
}

func (p *parallelAutoTitlePlanner) GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error) {
	p.titleStartedOnce.Do(func() { close(p.titleStarted) })
	select {
	case <-p.release:
		metadata, err := testReceiptMetadata(input, "Parallel Title")
		if err != nil {
			return GeneratedTitle{}, err
		}
		if onChunk != nil {
			onChunk("Parallel Title")
		}
		return GeneratedTitle{Title: "Parallel Title", Metadata: metadata}, nil
	case <-ctx.Done():
		return GeneratedTitle{}, ctx.Err()
	}
}

func awaitParallelAutoTitleReplyStarted(t *testing.T, planner *parallelAutoTitlePlanner) {
	t.Helper()
	select {
	case <-planner.secondReplyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second reply never started")
	}
}

func awaitParallelAutoTitleStarted(t *testing.T, planner *parallelAutoTitlePlanner) {
	t.Helper()
	select {
	case <-planner.titleStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("auto title never started")
	}
}

func TestServiceCreateSessionPersistsImmediateOpeningQuestion(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:00:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Planner bootstrap", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if session.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected ready conversation state, got %q", session.ConversationState)
	}
	if session.CurrentQuestionMessageID == nil {
		t.Fatal("expected current question message id to be set immediately")
	}

	messages, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one immediate opening message, got %d", len(messages))
	}
	if messages[0].ContentMarkdown != "" {
		t.Fatalf("expected empty content_markdown for opening question (prompt is in question_payload), got %q", messages[0].ContentMarkdown)
	}
	if !strings.Contains(messages[0].QuestionPayloadJSON, openingQuestionPrompt) {
		t.Fatalf("expected stored question payload, got %q", messages[0].QuestionPayloadJSON)
	}

	operations, err := store.ListSessionOperations(context.Background(), session.PlanningSessionID, 10)
	if err != nil {
		t.Fatalf("list session operations: %v", err)
	}
	if len(operations) != 1 || operations[0].Type != state.PlanningOperationTypeBootstrapContext {
		t.Fatalf("expected queued bootstrap operation, got %+v", operations)
	}
	awaitOperationTerminal(t, store, operations[0].OperationID)

	updated, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if !ok {
		t.Fatal("expected session to exist")
	}
	if updated.ContextSnapshotID == nil {
		t.Fatal("expected bootstrap background operation to populate context snapshot")
	}
}

func TestServiceReplyOperationPersistsUserAndAssistantMessages(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	ids := deterministicID()
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, ids)

	session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	bootstrap := latestSessionOperation(t, store, session.PlanningSessionID)
	awaitOperationTerminal(t, store, bootstrap.OperationID)

	reply := "We need a planner for decree composition that captures scope, constraints, acceptance criteria, and verification checkpoints before implementation begins."
	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     reply,
		AnswerPayloadJSON: `{"text":"` + reply + `"}`,
	})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	awaitOperationTerminal(t, store, operation.Operation.OperationID)

	messages, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("expected opening + user + assistant messages, got %d", len(messages))
	}
	if messages[1].Role != state.PlanningMessageRoleUser || messages[2].Role != state.PlanningMessageRoleAssistant {
		t.Fatalf("unexpected persisted reply message order: %+v", messages)
	}

	updated, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if !ok {
		t.Fatal("expected session to exist")
	}
	if updated.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected ready conversation state, got %q", updated.ConversationState)
	}
	if updated.CurrentQuestionMessageID == nil || *updated.CurrentQuestionMessageID != messages[2].PlanningMessageID {
		t.Fatalf("expected latest assistant question to be active, got %+v", updated.CurrentQuestionMessageID)
	}
}

func TestServiceFormalizeOperationCreatesDraftWithoutPlanningMessages(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	ids := deterministicID()
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, ids)

	session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)
	_, err = service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Need clearer scope", AnswerPayloadJSON: `{"text":"Need clearer scope"}`})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	beforeMessages, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages before formalize: %v", err)
	}
	formalizeOperation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
	if err != nil {
		t.Fatalf("create formalize operation: %v", err)
	}
	awaitOperationTerminal(t, store, formalizeOperation.Operation.OperationID)

	afterMessages, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages after formalize: %v", err)
	}
	if len(afterMessages) != len(beforeMessages) {
		t.Fatalf("expected formalize to avoid transcript mutations, before=%d after=%d", len(beforeMessages), len(afterMessages))
	}

	updated, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if !ok || updated.CurrentDraftID == nil {
		t.Fatalf("expected current draft on session, got %+v", updated)
	}
	draft, ok, err := store.GetDraft(context.Background(), *updated.CurrentDraftID)
	if err != nil {
		t.Fatalf("get draft: %v", err)
	}
	if !ok {
		t.Fatal("expected draft to exist")
	}
	if !strings.Contains(draft.DecreeText, "## Objective") {
		t.Fatalf("expected formalized plan scaffold, got %q", draft.DecreeText)
	}
}

func TestServiceReplyOperationRequiresCompletionReceipt(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &missingReceiptAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Need a settled planner response",
		AnswerPayloadJSON: `{"text":"Need a settled planner response"}`,
	})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	terminal := awaitOperationTerminal(t, store, operation.Operation.OperationID)
	if terminal.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected failed reply operation, got %+v", terminal)
	}
	if terminal.CanonicalSettlementID == nil || strings.TrimSpace(*terminal.CanonicalSettlementID) == "" {
		t.Fatalf("expected failed reply operation to record canonical settlement, got %+v", terminal)
	}
	if terminal.Error == nil || !strings.Contains(*terminal.Error, "missing completion receipt evidence id") {
		t.Fatalf("expected missing completion receipt failure, got %+v", terminal.Error)
	}
}

func TestServiceReplyOperationRejectsInvalidCompletionReceipt(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &invalidReceiptAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Need a rejected planner receipt",
		AnswerPayloadJSON: `{"text":"Need a rejected planner receipt"}`,
	})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	terminal := awaitOperationTerminal(t, store, operation.Operation.OperationID)
	if terminal.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected failed reply operation, got %+v", terminal)
	}
	if terminal.Error == nil || strings.TrimSpace(*terminal.Error) == "" {
		t.Fatalf("expected rejected receipt to fail the planner operation, got %+v", terminal.Error)
	}
	if terminal.CanonicalSettlementID == nil || strings.TrimSpace(*terminal.CanonicalSettlementID) == "" {
		t.Fatalf("expected failed reply operation to retain canonical settlement lineage for audit, got %+v", terminal)
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(context.Background(), strings.TrimSpace(*terminal.CanonicalSettlementID))
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider settlement to exist")
	}
	invocation, found, err := store.GetPlanningProviderInvocation(context.Background(), settlement.PlanningProviderInvocationID)
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvalidReason != "completion_capability_mismatch" {
		t.Fatalf("expected completion_capability_mismatch invalid reason, got %+v", invocation)
	}
	if invocation.InvocationState != state.PlanningProviderInvocationStateSettledFailed {
		t.Fatalf("expected planner operation to finish by settling the invalid invocation as failed, got %+v", invocation)
	}
}

func TestServiceReplyOperationClassifiesPlannerExecutionFailuresInCanonicalSettlement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "timeout",
			err:        context.DeadlineExceeded,
			wantReason: "provider_timeout",
		},
		{
			name:       "generic internal error",
			err:        fmt.Errorf("planner adapter exploded"),
			wantReason: "internal_error",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			projectRoot := t.TempDir()
			writeTestFile(t, projectRoot, "README.md", "# Project\n")

			db := stateTestDB(t)
			store := state.NewPlanningStore(db)
			service := NewService(store, &erroringAgent{err: tc.err}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

			session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
			if err != nil {
				t.Fatalf("create session: %v", err)
			}
			awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

			operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
				Type:              state.PlanningOperationTypeReply,
				ReplyMarkdown:     "Need a classified planner failure",
				AnswerPayloadJSON: `{"text":"Need a classified planner failure"}`,
			})
			if err != nil {
				t.Fatalf("create reply operation: %v", err)
			}
			terminal := awaitOperationTerminal(t, store, operation.Operation.OperationID)
			if terminal.Status != state.PlanningOperationStatusFailed {
				t.Fatalf("expected failed reply operation, got %+v", terminal)
			}
			if terminal.CanonicalSettlementID == nil || strings.TrimSpace(*terminal.CanonicalSettlementID) == "" {
				t.Fatalf("expected canonical settlement on failed reply operation, got %+v", terminal)
			}
			settlement, found, err := store.GetPlanningProviderCompletionSettlement(context.Background(), *terminal.CanonicalSettlementID)
			if err != nil {
				t.Fatalf("get planning provider settlement: %v", err)
			}
			if !found {
				t.Fatal("expected planning provider settlement to exist")
			}
			if settlement.Outcome != state.PlanningProviderCompletionOutcomeFailed {
				t.Fatalf("expected failed planning settlement, got %+v", settlement)
			}
			if settlement.FailureReasonCode != tc.wantReason {
				t.Fatalf("expected failure reason %q, got %+v", tc.wantReason, settlement)
			}
		})
	}
}

func TestServiceEditedReplyDefersTranscriptRewriteUntilSuccess(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	ids := deterministicID()
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, ids)

	session, err := service.CreateSession(context.Background(), "Planner flow", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)
	firstReply, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Original user turn",
		AnswerPayloadJSON: `{"text":"Original user turn"}`,
	})
	if err != nil {
		t.Fatalf("create first reply operation: %v", err)
	}
	awaitOperationTerminal(t, store, firstReply.Operation.OperationID)
	beforeEdit, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages before edit: %v", err)
	}
	if len(beforeEdit) != 3 {
		t.Fatalf("expected opening + first user + first assistant, got %d", len(beforeEdit))
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("close first service: %v", err)
	}

	blocking := newBlockingPlanner(false)
	service = NewService(store, blocking, projectRoot, func() string { return "2026-04-21T17:10:01Z" }, ids)
	editTurn := 2
	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Replacement user turn",
		AnswerPayloadJSON: `{"text":"Replacement user turn"}`,
		TruncateFromTurn:  &editTurn,
	})
	if err != nil {
		t.Fatalf("create edited reply operation: %v", err)
	}
	awaitPlannerStarted(t, blocking)
	awaitRunnerRunning(t, service.runner, operation.Operation.OperationID)

	duringEdit, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages during edit: %v", err)
	}
	if len(duringEdit) != len(beforeEdit) {
		t.Fatalf("expected transcript unchanged while edited reply is unsettled, before=%d during=%d", len(beforeEdit), len(duringEdit))
	}
	if duringEdit[1].ContentMarkdown != beforeEdit[1].ContentMarkdown || duringEdit[2].PlanningMessageID != beforeEdit[2].PlanningMessageID {
		t.Fatalf("expected original turn to remain canonical before success, before=%+v during=%+v", beforeEdit, duringEdit)
	}

	close(blocking.release)
	awaitOperationTerminal(t, store, operation.Operation.OperationID)
	afterEdit, err := store.ListMessages(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("list messages after edit: %v", err)
	}
	if len(afterEdit) != 3 {
		t.Fatalf("expected rewritten transcript to stay compact after success, got %d", len(afterEdit))
	}
	if afterEdit[1].ContentMarkdown != "Replacement user turn" {
		t.Fatalf("expected replacement user turn after success, got %+v", afterEdit[1])
	}
	if afterEdit[2].PlanningMessageID == beforeEdit[2].PlanningMessageID {
		t.Fatalf("expected assistant turn to be replaced after success")
	}
}

func TestServiceGenerateTitlePersistsDurableOperationSettlement(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	updated, err := service.GenerateTitle(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("generate title: %v", err)
	}
	if updated.CurrentTitleOperationID == nil || strings.TrimSpace(*updated.CurrentTitleOperationID) == "" {
		t.Fatalf("expected generate_title to enqueue a background title operation, got %+v", updated.CurrentTitleOperationID)
	}
	latest := latestSessionOperation(t, store, session.PlanningSessionID)
	if latest.Type != state.PlanningOperationTypeGenerateTitle {
		t.Fatalf("expected latest operation to be generate_title, got %+v", latest)
	}
	awaitOperationTerminal(t, store, latest.OperationID)
	updated, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get updated session after title completion: ok=%v err=%v", ok, err)
	}
	if updated.TitleState != planningSessionTitleStateGenerated {
		t.Fatalf("expected generated title state after completion, got %+v", updated)
	}
	latest = latestSessionOperation(t, store, session.PlanningSessionID)
	if latest.Type != state.PlanningOperationTypeGenerateTitle || latest.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded generate_title operation, got %+v", latest)
	}
	if latest.CanonicalSettlementID == nil || strings.TrimSpace(*latest.CanonicalSettlementID) == "" {
		t.Fatalf("expected generate_title operation to record canonical settlement, got %+v", latest)
	}
	if latest.ObservedOutputText != updated.Title {
		t.Fatalf("expected generate_title operation to persist the durable observed title, got %+v", latest)
	}

	metrics, err := service.RuntimeMetrics(context.Background())
	if err != nil {
		t.Fatalf("runtime metrics: %v", err)
	}
	if metrics.GeneratedTitle == nil {
		t.Fatal("expected generated_title runtime metrics to be present")
	}
	if metrics.GeneratedTitle.Latency.RecentSampleSize < 1 {
		t.Fatalf("expected generated_title latency samples, got %+v", metrics.GeneratedTitle.Latency)
	}
	if metrics.GeneratedTitle.Latency.Total.Samples < 1 || metrics.GeneratedTitle.Latency.Total.AvgMs < 0 {
		t.Fatalf("expected generated_title total latency metric, got %+v", metrics.GeneratedTitle.Latency.Total)
	}
	if metrics.Dispatch.ForegroundStartRequests < 1 || metrics.Dispatch.ForegroundClaimSuccess < 1 {
		t.Fatalf("expected generated title to contribute foreground dispatch metrics, got %+v", metrics.Dispatch)
	}
}

func TestServiceGenerateTitlePreservesNewerCustomTitle(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	planner := newSlowTitlePlanner()
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)
	if session.TitleState != planningSessionTitleStatePlaceholder {
		t.Fatalf("expected placeholder session title, got %+v", session)
	}

	updated, err := service.GenerateTitle(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("generate title: %v", err)
	}
	if updated.CurrentTitleOperationID == nil || strings.TrimSpace(*updated.CurrentTitleOperationID) == "" {
		t.Fatalf("expected queued title operation id, got %+v", updated.CurrentTitleOperationID)
	}
	titleOperationID := strings.TrimSpace(*updated.CurrentTitleOperationID)

	awaitSlowTitleStarted(t, planner)

	customTitle := "Operator locked title"
	updatedDuringFlight, err := service.UpdateSession(context.Background(), session.PlanningSessionID, UpdateSessionInput{Title: &customTitle})
	if err != nil {
		t.Fatalf("update session title while generate_title is running: %v", err)
	}
	if updatedDuringFlight.Title != customTitle || updatedDuringFlight.TitleState != planningSessionTitleStateCustom {
		t.Fatalf("expected custom title to persist immediately, got %+v", updatedDuringFlight)
	}
	if updatedDuringFlight.CurrentTitleOperationID == nil || strings.TrimSpace(*updatedDuringFlight.CurrentTitleOperationID) == "" {
		t.Fatalf("expected generate_title operation to remain active during manual title edit, got %+v", updatedDuringFlight.CurrentTitleOperationID)
	}

	close(planner.release)
	awaitOperationTerminal(t, store, titleOperationID)

	stored, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get stored session: ok=%v err=%v", ok, err)
	}
	if stored.Title != customTitle || stored.TitleState != planningSessionTitleStateCustom {
		t.Fatalf("expected stored session to preserve custom title, got %+v", stored)
	}
	if stored.CurrentTitleOperationID != nil {
		t.Fatalf("expected generate_title title slot cleared after completion, got %+v", stored.CurrentTitleOperationID)
	}
	if stored.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation state ready after completion, got %q", stored.ConversationState)
	}

	latest := latestSessionOperation(t, store, session.PlanningSessionID)
	if latest.Type != state.PlanningOperationTypeGenerateTitle || latest.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded generate_title operation, got %+v", latest)
	}
}

func TestServiceGenerateTitleRunsAlongsideInteractiveOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	planner := newSlowTitlePlanner()
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "", `{"model":"github-copilot/gpt-5.4"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	_, err = store.CreateReplyBundle(context.Background(), state.CreatePlanningReplyBundleRequest{
		SessionID: session.PlanningSessionID,
		UserMessage: state.AppendPlanningMessageRequest{
			PlanningMessageID:   "planning_message_reply_seed",
			PlanningSessionID:   session.PlanningSessionID,
			TurnIndex:           2,
			Role:                state.PlanningMessageRoleUser,
			ContentMarkdown:     "Keep the planner busy",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{"text":"Keep the planner busy"}`,
			Active:              true,
			CreatedAt:           "2026-04-21T17:10:01Z",
		},
		Operation: state.CreatePlanningOperationRequest{
			OperationID:        "planning_operation_reply_seed",
			PlanningSessionID:  session.PlanningSessionID,
			Type:               state.PlanningOperationTypeReply,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: `{"content_markdown":"Keep the planner busy"}`,
			CreatedAt:          "2026-04-21T17:10:01Z",
		},
		ConversationState: state.PlanningSessionConversationStateProcessing,
		UpdatedAt:         "2026-04-21T17:10:01Z",
	})
	if err != nil {
		t.Fatalf("seed active interactive operation: %v", err)
	}

	updated, err := service.GenerateTitle(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("generate title alongside interactive operation: %v", err)
	}
	if updated.CurrentOperationID == nil || strings.TrimSpace(*updated.CurrentOperationID) != "planning_operation_reply_seed" {
		t.Fatalf("expected interactive slot to remain with reply op, got %+v", updated.CurrentOperationID)
	}
	if updated.CurrentTitleOperationID == nil || strings.TrimSpace(*updated.CurrentTitleOperationID) == "" {
		t.Fatalf("expected background title slot to be populated, got %+v", updated.CurrentTitleOperationID)
	}
	if updated.ConversationState != state.PlanningSessionConversationStateProcessing {
		t.Fatalf("expected interactive conversation state preserved, got %q", updated.ConversationState)
	}
	titleOperationID := strings.TrimSpace(*updated.CurrentTitleOperationID)

	awaitSlowTitleStarted(t, planner)
	close(planner.release)
	awaitOperationTerminal(t, store, titleOperationID)

	stored, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get stored session: ok=%v err=%v", ok, err)
	}
	if stored.CurrentOperationID == nil || strings.TrimSpace(*stored.CurrentOperationID) != "planning_operation_reply_seed" {
		t.Fatalf("expected interactive slot preserved after title completion, got %+v", stored.CurrentOperationID)
	}
	if stored.CurrentTitleOperationID != nil {
		t.Fatalf("expected title slot cleared after completion, got %+v", stored.CurrentTitleOperationID)
	}
	if stored.ConversationState != state.PlanningSessionConversationStateProcessing {
		t.Fatalf("expected conversation_state preserved as processing, got %q", stored.ConversationState)
	}
	if stored.TitleState != planningSessionTitleStateGenerated {
		t.Fatalf("expected generated title to settle, got %+v", stored)
	}
}

func TestServiceSecondReplyAutoGeneratesTitleInParallel(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	planner := newParallelAutoTitlePlanner()
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	firstReply, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "First answer",
		AnswerPayloadJSON: `{"text":"First answer"}`,
	})
	if err != nil {
		t.Fatalf("create first reply: %v", err)
	}
	awaitOperationTerminal(t, store, firstReply.Operation.OperationID)
	updatedSession, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get session after first reply: ok=%v err=%v", ok, err)
	}
	if updatedSession.CurrentTitleOperationID != nil {
		t.Fatalf("expected no auto title after first reply, got %+v", updatedSession.CurrentTitleOperationID)
	}

	secondReply, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Second answer",
		AnswerPayloadJSON: `{"text":"Second answer"}`,
	})
	if err != nil {
		t.Fatalf("create second reply: %v", err)
	}
	awaitParallelAutoTitleReplyStarted(t, planner)
	awaitParallelAutoTitleStarted(t, planner)

	updatedSession, ok, err = store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get updated session: ok=%v err=%v", ok, err)
	}
	if updatedSession.CurrentTitleOperationID == nil || strings.TrimSpace(*updatedSession.CurrentTitleOperationID) == "" {
		t.Fatalf("expected auto title operation to be active after second reply, got %+v", updatedSession.CurrentTitleOperationID)
	}
	titleOperationID := strings.TrimSpace(*updatedSession.CurrentTitleOperationID)
	titleOperation, ok, err := store.GetOperation(context.Background(), titleOperationID)
	if err != nil || !ok {
		t.Fatalf("get auto title operation: ok=%v err=%v", ok, err)
	}
	if titleOperation.StartedAt == nil || strings.TrimSpace(*titleOperation.StartedAt) == "" {
		t.Fatalf("expected auto title operation to start immediately, got %+v", titleOperation)
	}
	close(planner.release)
	awaitOperationTerminal(t, store, secondReply.Operation.OperationID)
	awaitOperationTerminal(t, store, titleOperationID)

	titleOperation, ok, err = store.GetOperation(context.Background(), titleOperationID)
	if err != nil || !ok {
		t.Fatalf("get completed auto title operation: ok=%v err=%v", ok, err)
	}
	if titleOperation.Type != state.PlanningOperationTypeGenerateTitle || titleOperation.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded generate_title operation, got %+v", titleOperation)
	}
	request, err := decodeOperationRequest(titleOperation.RequestPayloadJSON)
	if err != nil {
		t.Fatalf("decode auto title request payload: %v", err)
	}
	if request.TriggerSource != string(titleTriggerSourceAutoSecondUserMessageCreate) {
		t.Fatalf("expected auto title trigger source %q, got %q", titleTriggerSourceAutoSecondUserMessageCreate, request.TriggerSource)
	}
	metrics, err := service.RuntimeMetrics(context.Background())
	if err != nil {
		t.Fatalf("runtime metrics: %v", err)
	}
	if metrics.Dispatch.ForegroundStartRequests < 3 || metrics.Dispatch.ForegroundClaimSuccess < 3 {
		t.Fatalf("expected foreground dispatch metrics to include reply+title starts, got %+v", metrics.Dispatch)
	}
}

func TestServiceOperationEventsResumeAfterLastEventID(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &streamingMockAgent{
		chunks: []string{
			"```structured_question\n{\"prompt\":\"Choose planning depth\",\"type\":\"single_select\",\"options\":[{\"label\":\"Lean\",\"value\":\"lean\"},{\"label\":\"Thorough\",\"value\":\"thorough\"}]}\n```\n\n",
			"A lean plan keeps the next decree focused on outcome and verification.\n",
		},
		question: &StructuredQuestion{
			Prompt:  "Choose planning depth",
			Type:    "single_select",
			Options: []StructuredQuestionOption{{Label: "Lean", Value: "lean"}, {Label: "Thorough", Value: "thorough"}},
		},
		final: "A lean plan keeps the next decree focused on outcome and verification.",
	}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Stream question test", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Test reply", AnswerPayloadJSON: `{"text":"Test reply"}`})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	awaitOperationTerminal(t, store, operation.Operation.OperationID)

	eventsAfterFirst, err := service.ListOperationEventsAfterEventID(context.Background(), operation.Operation.OperationID, operation.Operation.OperationID+":1")
	if err != nil {
		t.Fatalf("list operation events after seq: %v", err)
	}
	if len(eventsAfterFirst) == 0 {
		t.Fatal("expected replayable operation events after seq 1")
	}
	if eventsAfterFirst[0].Seq != 2 {
		t.Fatalf("expected first resumed seq 2, got %d", eventsAfterFirst[0].Seq)
	}
	if eventsAfterFirst[len(eventsAfterFirst)-1].EventType != "done" {
		t.Fatalf("expected terminal done event, got %+v", eventsAfterFirst[len(eventsAfterFirst)-1])
	}
}

func TestServiceReplyOperationEmitsLifecycleEventsAndBatchedTextDeltas(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &streamingMockAgent{
		chunks: []string{"Hello, ", "planner."},
		question: &StructuredQuestion{
			Prompt:  "Choose planning depth",
			Type:    "single_select",
			Options: []StructuredQuestionOption{{Label: "Lean", Value: "lean"}},
		},
		final: "Hello, planner.",
	}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Lifecycle test", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Lean", AnswerPayloadJSON: `{"value":"lean","text":"Lean"}`})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	awaitOperationTerminal(t, store, operation.Operation.OperationID)

	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), operation.Operation.OperationID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected planner events")
	}

	types := make([]string, 0, len(events))
	textDeltas := make([]string, 0)
	for _, event := range events {
		types = append(types, event.EventType)
		if event.EventType == "text_delta" {
			textDeltas = append(textDeltas, event.PayloadJSON)
		}
	}

	for _, expected := range []string{"started", "user_message", "provider_launching", "provider_session_open", "receipt_persisted", "finalizing", "done"} {
		if !containsString(types, expected) {
			t.Fatalf("expected lifecycle event %q in %v", expected, types)
		}
	}
	if len(textDeltas) != 1 {
		t.Fatalf("expected one batched text_delta event, got %d (%v)", len(textDeltas), textDeltas)
	}
	if !strings.Contains(textDeltas[0], "Hello, planner.") {
		t.Fatalf("expected batched text delta to contain full reply, got %s", textDeltas[0])
	}
	if strings.Contains(textDeltas[0], "structured_question") {
		t.Fatalf("expected streamed text delta to be sanitized, got %s", textDeltas[0])
	}
	if !strings.Contains(events[0].PayloadJSON, `"start_source":"foreground"`) {
		t.Fatalf("expected started payload to record foreground source, got %s", events[0].PayloadJSON)
	}
	if !strings.Contains(events[0].PayloadJSON, `"queue_age_ms":`) {
		t.Fatalf("expected started payload to record queue age, got %s", events[0].PayloadJSON)
	}
	metrics, err := service.RuntimeMetrics(context.Background())
	if err != nil {
		t.Fatalf("runtime metrics: %v", err)
	}
	if metrics.Dispatch.ForegroundStartRequests < 1 || metrics.Dispatch.ForegroundClaimSuccess < 1 {
		t.Fatalf("expected foreground dispatch metrics to be populated, got %+v", metrics.Dispatch)
	}
}

func TestServiceRecoverOperationsRestartsQueuedWork(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Recovery test", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	operation, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "planning_operation_recovery",
		PlanningSessionID:  session.PlanningSessionID,
		Type:               state.PlanningOperationTypeFormalize,
		Status:             state.PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"draft_id":"planning_draft_recovery","draft_created_at":"2026-04-21T17:10:00Z"}`,
		CreatedAt:          "2026-04-21T17:10:00Z",
	})
	if err != nil {
		t.Fatalf("seed queued operation: %v", err)
	}
	conversationState := state.PlanningSessionConversationStateProcessing
	if err := store.UpdateSession(context.Background(), state.UpdatePlanningSessionRequest{
		PlanningSessionID:  session.PlanningSessionID,
		ConversationState:  &conversationState,
		CurrentOperationID: refString(operation.OperationID),
		UpdatedAt:          "2026-04-21T17:10:00Z",
	}); err != nil {
		t.Fatalf("mark session processing: %v", err)
	}

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover operations: %v", err)
	}
	awaitOperationTerminal(t, store, operation.OperationID)

	recovered, ok, err := store.GetOperation(context.Background(), operation.OperationID)
	if err != nil {
		t.Fatalf("get recovered operation: %v", err)
	}
	if !ok || recovered.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected recovered operation to succeed, got %+v", recovered)
	}
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), operation.OperationID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list recovered operation events: %v", err)
	}
	if len(events) == 0 || !strings.Contains(events[0].PayloadJSON, `"start_source":"recovery"`) {
		t.Fatalf("expected recovered operation started payload to record recovery source, got %+v", events)
	}
	metrics, err := service.RuntimeMetrics(context.Background())
	if err != nil {
		t.Fatalf("runtime metrics: %v", err)
	}
	if metrics.Dispatch.RecoveryStartRequests < 1 || metrics.Dispatch.RecoveryClaimSuccess < 1 {
		t.Fatalf("expected recovery dispatch metrics to be populated, got %+v", metrics.Dispatch)
	}
}

// TestRefreshContextReturnsAdoptedSnapshot guards the API contract: the
// returned snapshot must match session.context_snapshot_id, even when a
// concurrent /context/refresh adopted a newer snapshot before ours.
// Otherwise the API can return a session pointer that doesn't match the
// snapshot body in the same response.
func TestRefreshContextReturnsAdoptedSnapshot(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	// Use a clock that returns increasing timestamps so each
	// refreshContextSnapshot call (and each created snapshot) gets a later
	// created_at than the previous one. The default deterministicID +
	// fixed clock would make snapshot ids and timestamps too similar to
	// test the race meaningfully.
	tick := 0
	clock := func() string {
		tick++
		return fmt.Sprintf("2026-04-26T10:%02d:00Z", tick)
	}
	service := NewService(store, &mockAgent{}, projectRoot, clock, deterministicID())

	session, err := service.CreateSession(context.Background(), "Refresh adopted", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	// First refresh produces snap_A and adopts it.
	snapA, sessionA, err := service.RefreshContext(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if sessionA.ContextSnapshotID == nil || *sessionA.ContextSnapshotID != snapA.PlanningContextSnapshotID {
		t.Fatalf("first refresh: snapshot/session mismatch: %+v vs %+v", snapA, sessionA.ContextSnapshotID)
	}

	// Second refresh produces snap_B and adopts it.
	snapB, sessionB, err := service.RefreshContext(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if snapB.PlanningContextSnapshotID == snapA.PlanningContextSnapshotID {
		t.Fatalf("second refresh produced same snapshot id, test fixture broken")
	}
	if sessionB.ContextSnapshotID == nil || *sessionB.ContextSnapshotID != snapB.PlanningContextSnapshotID {
		t.Fatalf("second refresh: snapshot/session mismatch: %+v vs %+v", snapB, sessionB.ContextSnapshotID)
	}

	// Now simulate a slow refresh: create a snapshot with an *older*
	// created_at than what's already adopted, then publish through the
	// store directly (bypassing refreshContextSnapshot which always uses
	// "now"). The publish helper rejects the demotion, RefreshContext
	// must observe the same and return the previously-adopted snapshot.
	if _, err := store.CreateContextSnapshot(context.Background(), state.CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_slow",
		PlanningSessionID:         session.PlanningSessionID,
		SummaryMarkdown:           "older",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-26T09:00:00Z",
	}); err != nil {
		t.Fatalf("create slow snapshot: %v", err)
	}
	adopted, err := store.PublishContextSnapshot(context.Background(), session.PlanningSessionID, "snap_slow", "2026-04-26T11:00:00Z")
	if err != nil {
		t.Fatalf("publish slow: %v", err)
	}
	if adopted != snapB.PlanningContextSnapshotID {
		t.Fatalf("expected adopted=%s (newer wins), got %q", snapB.PlanningContextSnapshotID, adopted)
	}
}

// TestRefreshContextReturnsErrSessionNotFoundForUnknownSession pins the
// "no orphan snapshot for a bogus session id" guarantee. Without the
// pre-existence check at the top of RefreshContext, refreshContextSnapshot
// would happily insert a snapshot row keyed to a non-existent session and
// the caller would see a generic ErrPlanningSessionNotFound surfaced
// downstream from PublishContextSnapshot. Sister mutation methods
// (UpdateSession, GenerateTitle) already gate on GetSession +
// ErrSessionNotFound; this asserts RefreshContext now matches.
func TestRefreshContextReturnsErrSessionNotFoundForUnknownSession(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T11:00:00Z" }, deterministicID())
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(closeCtx)
	})

	countSnapshots := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM planning_context_snapshots`).Scan(&n); err != nil {
			t.Fatalf("count snapshots: %v", err)
		}
		return n
	}
	before := countSnapshots()

	_, _, err := service.RefreshContext(context.Background(), "ps_does_not_exist")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}

	if after := countSnapshots(); after != before {
		t.Fatalf("expected snapshot count to be unchanged (%d), got %d — orphan row inserted for missing session", before, after)
	}
}

// TestRecoveryReplaysReceiptPersistedStuckOperationAsSuccess exercises the
// stuck-op recovery path when the planner already persisted a valid receipt.
// Recovery must treat the receipt as canonical success authority and replay
// the normal finalize path rather than overwriting it as failure.
func TestRecoveryReplaysReceiptPersistedStuckOperationAsSuccess(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	// Service nowString is fixed at 11:00 — well beyond the operation's
	// lease_expires_at (09:00) plus StuckOperationAfter (90s), so
	// IsStuckOperation returns true on the first observation.
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T11:00:00Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_stuck_e2e",
		Title:              "stuck",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_stuck_e2e",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_stuck_e2e",
		PlanningSessionID:  "ps_stuck_e2e",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"assistant_message_id":"pm_stuck_e2e_assistant","assistant_created_at":"2026-04-26T08:55:05Z","turn_index":1}`,
		LeaseOwner:         "abandoned-worker",
		LeaseExpiresAt:     "2026-04-26T09:00:00Z", // 2h before service.now → well past StuckOperationAfter
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T08:55:00Z",
		StartedAt:          "2026-04-26T08:55:01Z",
	}); err != nil {
		t.Fatalf("seed running operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_stuck_e2e",
		PlanningOperationID:          "po_stuck_e2e",
		PlanningSessionID:            "ps_stuck_e2e",
		LogicalSessionID:             "planner-ps_stuck_e2e",
		CompletionCapability:         "capability_stuck_e2e",
		IssuedAt:                     "2026-04-26T08:55:02Z",
		CreatedAt:                    "2026-04-26T08:55:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_stuck_e2e",
		PlanningOperationID:          "po_stuck_e2e",
		ProviderSessionID:            "ses_stuck_e2e",
		OpenedAt:                     "2026-04-26T08:55:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_stuck_e2e",
		AttemptID:                    "po_stuck_e2e",
		PlanningProviderInvocationID: "ppinv_stuck_e2e",
		PlanningOperationID:          "po_stuck_e2e",
		PlanningSessionID:            "ps_stuck_e2e",
		ProtocolVersion:              "v1",
		CompletionCapability:         "capability_stuck_e2e",
		FinalText:                    "Planner reply that never finalized.",
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T08:55:04Z",
	})
	if err != nil {
		t.Fatalf("accept completion receipt: %v", err)
	}
	if accepted.Idempotent {
		t.Fatal("expected first completion receipt accept to be non-idempotent")
	}

	// A single recovery pass must finalise the abandoned op. With the new
	// timestamp-derived predicate, no per-observer counter to ramp through.
	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), "po_stuck_e2e")
	if err != nil || !ok {
		t.Fatalf("get operation: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded status, got %q", op.Status)
	}
	if op.Error != nil {
		t.Fatalf("expected no error message, got %q", *op.Error)
	}
	if op.ResultRef == nil || strings.TrimSpace(*op.ResultRef) != "pm_stuck_e2e_assistant" {
		t.Fatalf("expected result_ref to point at replayed assistant message, got %+v", op.ResultRef)
	}
	if op.CanonicalSettlementID == nil || strings.TrimSpace(*op.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id on stuck provider-backed operation, got %+v", op)
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(context.Background(), *op.CanonicalSettlementID)
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider settlement to exist")
	}
	if settlement.Outcome != state.PlanningProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success planning settlement, got %+v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected stuck recovery to reuse persisted receipt evidence %q, got %+v", accepted.Evidence.EvidenceID, settlement)
	}
	if settlement.FinalText != "Planner reply that never finalized." {
		t.Fatalf("expected planning settlement final text to come from receipt, got %+v", settlement)
	}
	messages, err := store.ListMessages(context.Background(), "ps_stuck_e2e")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one replayed assistant message, got %d", len(messages))
	}
	if messages[0].PlanningMessageID != "pm_stuck_e2e_assistant" {
		t.Fatalf("expected replayed assistant message id pm_stuck_e2e_assistant, got %+v", messages[0])
	}
	if messages[0].ContentMarkdown != "Planner reply that never finalized." {
		t.Fatalf("expected replayed assistant content from receipt final_text, got %+v", messages[0])
	}
	invocation, found, err := store.GetPlanningProviderInvocation(context.Background(), "ppinv_stuck_e2e")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != state.PlanningProviderInvocationStateSettledSuccess {
		t.Fatalf("expected stuck recovery to settle invocation as success, got %+v", invocation)
	}
}

func TestRecoveryImmediatelyReplaysRecentReceiptPersistedOperationAsSuccess(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T10:00:31Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_recent_receipt_recovery",
		Title:              "recent",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_recent_receipt_recovery",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T09:55:00Z",
		UpdatedAt:          "2026-04-26T09:55:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_recent_receipt_recovery",
		PlanningSessionID:  "ps_recent_receipt_recovery",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"assistant_message_id":"pm_recent_receipt_assistant","assistant_created_at":"2026-04-26T09:55:05Z","turn_index":1}`,
		LeaseOwner:         "slow-worker",
		LeaseExpiresAt:     "2026-04-26T10:00:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:55:00Z",
		StartedAt:          "2026-04-26T09:55:01Z",
	}); err != nil {
		t.Fatalf("seed running operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_recent_receipt_recovery",
		PlanningOperationID:          "po_recent_receipt_recovery",
		PlanningSessionID:            "ps_recent_receipt_recovery",
		LogicalSessionID:             "planner-ps_recent_receipt_recovery",
		CompletionCapability:         "capability_recent_receipt_recovery",
		IssuedAt:                     "2026-04-26T09:55:02Z",
		CreatedAt:                    "2026-04-26T09:55:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_recent_receipt_recovery",
		PlanningOperationID:          "po_recent_receipt_recovery",
		ProviderSessionID:            "ses_recent_receipt_recovery",
		OpenedAt:                     "2026-04-26T09:55:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_recent_receipt_recovery",
		AttemptID:                    "po_recent_receipt_recovery",
		PlanningProviderInvocationID: "ppinv_recent_receipt_recovery",
		PlanningOperationID:          "po_recent_receipt_recovery",
		PlanningSessionID:            "ps_recent_receipt_recovery",
		ProtocolVersion:              "v1",
		CompletionCapability:         "capability_recent_receipt_recovery",
		FinalText:                    "Planner reply recovered before the stuck threshold.",
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T09:55:04Z",
	})
	if err != nil {
		t.Fatalf("accept completion receipt: %v", err)
	}

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), "po_recent_receipt_recovery")
	if err != nil || !ok {
		t.Fatalf("get operation: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded status, got %+v", op)
	}
	if op.CanonicalSettlementID == nil || strings.TrimSpace(*op.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id on recovered operation, got %+v", op)
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(context.Background(), *op.CanonicalSettlementID)
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider settlement to exist")
	}
	if settlement.Outcome != state.PlanningProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success planning settlement, got %+v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected recent recovery to reuse persisted receipt evidence %q, got %+v", accepted.Evidence.EvidenceID, settlement)
	}
	if settlement.FinalText != "Planner reply recovered before the stuck threshold." {
		t.Fatalf("expected planning settlement final text to come from receipt, got %+v", settlement)
	}
}

func TestRecoveryImmediatelyFailsInvalidHandshakeOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T10:00:31Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_invalid_handshake_recovery",
		Title:              "invalid",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_invalid_handshake_recovery",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T09:55:00Z",
		UpdatedAt:          "2026-04-26T10:00:30Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_invalid_handshake_recovery",
		PlanningSessionID:  "ps_invalid_handshake_recovery",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"assistant_message_id":"pm_invalid_handshake_recovery","assistant_created_at":"2026-04-26T09:55:05Z","turn_index":1}`,
		LeaseOwner:         "slow-worker",
		LeaseExpiresAt:     "2026-04-26T10:00:10Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:55:00Z",
		StartedAt:          "2026-04-26T09:55:01Z",
	}); err != nil {
		t.Fatalf("seed running operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_invalid_handshake_recovery",
		PlanningOperationID:          "po_invalid_handshake_recovery",
		PlanningSessionID:            "ps_invalid_handshake_recovery",
		LogicalSessionID:             "planner-ps_invalid_handshake_recovery",
		CompletionCapability:         "capability_invalid_handshake_recovery",
		IssuedAt:                     "2026-04-26T09:55:02Z",
		CreatedAt:                    "2026-04-26T09:55:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_invalid_handshake_recovery",
		PlanningOperationID:          "po_invalid_handshake_recovery",
		ProviderSessionID:            "ses_invalid_handshake_recovery",
		OpenedAt:                     "2026-04-26T09:55:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if _, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_invalid_handshake_recovery",
		AttemptID:                    "po_invalid_handshake_recovery",
		PlanningProviderInvocationID: "ppinv_invalid_handshake_recovery",
		PlanningOperationID:          "po_invalid_handshake_recovery",
		PlanningSessionID:            "ps_invalid_handshake_recovery",
		ProtocolVersion:              "v1",
		CompletionCapability:         "wrong_capability_invalid_handshake_recovery",
		FinalText:                    "This receipt should be rejected.",
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T09:55:04Z",
	}); err == nil {
		t.Fatal("expected mismatched capability receipt to be rejected")
	}

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), "po_invalid_handshake_recovery")
	if err != nil || !ok {
		t.Fatalf("get operation: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected invalid-handshake operation to fail immediately, got %+v", op)
	}
	if op.Error == nil || !strings.Contains(*op.Error, "invalidated") {
		t.Fatalf("expected invalid-handshake error to be recorded, got %+v", op.Error)
	}
	if op.CanonicalSettlementID == nil || strings.TrimSpace(*op.CanonicalSettlementID) == "" {
		t.Fatalf("expected invalid-handshake recovery to persist canonical settlement, got %+v", op)
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(context.Background(), strings.TrimSpace(*op.CanonicalSettlementID))
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected invalid-handshake recovery settlement to exist")
	}
	if settlement.Outcome != state.PlanningProviderCompletionOutcomeFailed {
		t.Fatalf("expected invalid-handshake recovery settlement to fail, got %+v", settlement)
	}
	invocation, found, err := store.GetPlanningProviderInvocation(context.Background(), "ppinv_invalid_handshake_recovery")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != state.PlanningProviderInvocationStateSettledFailed {
		t.Fatalf("expected invalid-handshake recovery to settle invocation as failed, got %+v", invocation)
	}
}

func TestRecoveryGenerateTitleUsesRecoveryTimeForSessionFreshness(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T10:00:31Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_recovered_title_freshness",
		Title:                   "Untitled",
		TitleState:              "placeholder",
		LifecycleState:          state.PlanningSessionLifecycleStateActive,
		ConversationState:       state.PlanningSessionConversationStateReady,
		CurrentTitleOperationID: "po_recovered_title_freshness",
		ProjectRoot:             projectRoot,
		ModelSelectionJSON:      `{"model":"test-model"}`,
		CreatedAt:               "2026-04-26T09:55:00Z",
		UpdatedAt:               "2026-04-26T10:00:30Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_recovered_title_freshness",
		PlanningSessionID:  "ps_recovered_title_freshness",
		Type:               state.PlanningOperationTypeGenerateTitle,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "title-worker",
		LeaseExpiresAt:     "2026-04-26T10:00:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:55:00Z",
		StartedAt:          "2026-04-26T09:55:01Z",
	}); err != nil {
		t.Fatalf("seed running title operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_recovered_title_freshness",
		PlanningOperationID:          "po_recovered_title_freshness",
		PlanningSessionID:            "ps_recovered_title_freshness",
		LogicalSessionID:             "planner-ps_recovered_title_freshness",
		CompletionCapability:         "capability_recovered_title_freshness",
		IssuedAt:                     "2026-04-26T09:55:02Z",
		CreatedAt:                    "2026-04-26T09:55:02Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_recovered_title_freshness",
		PlanningOperationID:          "po_recovered_title_freshness",
		ProviderSessionID:            "ses_recovered_title_freshness",
		OpenedAt:                     "2026-04-26T09:55:03Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if _, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_recovered_title_freshness",
		AttemptID:                    "po_recovered_title_freshness",
		PlanningProviderInvocationID: "ppinv_recovered_title_freshness",
		PlanningOperationID:          "po_recovered_title_freshness",
		PlanningSessionID:            "ps_recovered_title_freshness",
		ProtocolVersion:              "v1",
		CompletionCapability:         "capability_recovered_title_freshness",
		FinalText:                    "Recovered Title",
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T09:55:04Z",
	}); err != nil {
		t.Fatalf("accept completion receipt: %v", err)
	}

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	session, ok, err := store.GetSession(context.Background(), "ps_recovered_title_freshness")
	if err != nil || !ok {
		t.Fatalf("get session after recovery: ok=%v err=%v", ok, err)
	}
	if session.Title != "Recovered Title" {
		t.Fatalf("expected recovered title to be applied, got %+v", session)
	}
	if session.TitleState != "generated" {
		t.Fatalf("expected recovered title state generated, got %+v", session)
	}
	if session.UpdatedAt != "2026-04-26T10:00:31Z" {
		t.Fatalf("expected title recovery to stamp session freshness with recovery time, got %+v", session)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected title lane cleared after recovered success, got %+v", session.CurrentTitleOperationID)
	}
}

// TestFailOperationStaleLeaseDoesNotBumpFailureMetric verifies that losing a
// lease race during finalization does not increment failureCount. This uses a
// direct UPDATE so the stale-lease path is deterministic on every run.
//
// Bootstrap variant covers the FinalizeBootstrapAbort path; the
// reply variant covers FinalizeOperation. Both routes live inside
// failOperation so a single ordering fix protects both.
func TestFailOperationStaleLeaseDoesNotBumpFailureMetric(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		opID       string
		opType     state.PlanningOperationType
		seedBundle func(t *testing.T, store *state.PlanningStore, sessionID string)
	}{
		{
			name:   "bootstrap",
			opID:   "po_bootstrap_stale",
			opType: state.PlanningOperationTypeBootstrapContext,
		},
		{
			name:   "reply",
			opID:   "po_reply_stale",
			opType: state.PlanningOperationTypeReply,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			projectRoot := t.TempDir()
			writeTestFile(t, projectRoot, "README.md", "# Project\n")

			db := stateTestDB(t)
			store := state.NewPlanningStore(db)
			service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())
			t.Cleanup(func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = service.Close(closeCtx)
			})

			sessionID := "ps_failop_stale_" + tc.name
			currentOpID := tc.opID
			// Reply needs current_operation_id pointing at the op so
			// the slot fence is testable. Bootstrap deliberately leaves
			// it NULL — that's part of the lifecycle distinction.
			currentOp := ""
			if tc.opType == state.PlanningOperationTypeReply {
				currentOp = currentOpID
			}
			if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
				PlanningSessionID:  sessionID,
				Title:              "stale-lease",
				TitleState:         "custom",
				LifecycleState:     state.PlanningSessionLifecycleStateActive,
				ConversationState:  state.PlanningSessionConversationStateProcessing,
				CurrentOperationID: currentOp,
				ProjectRoot:        projectRoot,
				ModelSelectionJSON: `{"model":"test-model"}`,
				CreatedAt:          "2026-04-26T13:00:00Z",
				UpdatedAt:          "2026-04-26T13:00:00Z",
			}); err != nil {
				t.Fatalf("create session: %v", err)
			}
			if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
				OperationID:        tc.opID,
				PlanningSessionID:  sessionID,
				Type:               tc.opType,
				Status:             state.PlanningOperationStatusQueued,
				RequestPayloadJSON: `{}`,
				CreatedAt:          "2026-04-26T13:00:01Z",
			}); err != nil {
				t.Fatalf("create op: %v", err)
			}
			claimed, ok, err := store.ClaimOperation(context.Background(), tc.opID, "worker-a", "2026-04-26T14:00:00Z", "2026-04-26T13:00:02Z")
			if err != nil || !ok {
				t.Fatalf("claim: ok=%v err=%v", ok, err)
			}

			// Simulate a concurrent observer winning the race by bumping
			// lease_version. Worker-a's claim is now stale; their
			// failOperation call below must observe ErrPlanningLeaseConflict
			// from the lease-fenced finalize.
			if _, err := db.ExecContext(context.Background(),
				`UPDATE planning_operations SET lease_owner = 'worker-b', lease_version = lease_version + 1 WHERE operation_id = ?`,
				tc.opID); err != nil {
				t.Fatalf("simulate reclaim: %v", err)
			}

			beforeFailure := service.runner.deps.metrics.failureCount.Load()

			cursor := newSequenceCursor(1)
			result := service.runner.failOperation(context.Background(), claimed, cursor, fmt.Errorf("simulated planner failure"))
			if result != nil {
				t.Fatalf("expected nil from lease-conflict-loser path, got %v", result)
			}

			afterFailure := service.runner.deps.metrics.failureCount.Load()
			if afterFailure != beforeFailure {
				t.Fatalf("failureCount bumped on lease-conflict loser: before=%d after=%d", beforeFailure, afterFailure)
			}

			// Op row stays running with worker-b's lease — winner's
			// finalize hasn't actually happened in this test scenario,
			// but the loser must not have mutated the row.
			op, ok, err := store.GetOperation(context.Background(), tc.opID)
			if err != nil || !ok {
				t.Fatalf("get op: ok=%v err=%v", ok, err)
			}
			if op.Status != state.PlanningOperationStatusRunning {
				t.Fatalf("expected op to remain running after rejected stale-lease finalize, got %q", op.Status)
			}
			if op.LeaseOwner != "worker-b" {
				t.Fatalf("expected lease_owner unchanged at worker-b, got %q", op.LeaseOwner)
			}
		})
	}
}

// TestServiceCloseSurfacesCancelDrainTimeout verifies that a bounded cancel
// write timeout propagates back through runner.Wait and Service.Close. The DB
// row stays running for later recovery, but shutdown must surface the failed
// drain instead of claiming success.
//
// Contention is real: a long-held tx on the shared db blocks the write path,
// so the test exercises the concrete store rather than a fake seam.
func TestServiceCloseSurfacesCancelDrainTimeout(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(true)
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())

	opID := startBlockingReplyForCloseTest(t, service, store, planner)

	// Acquire the connection-pool slot via a long-running tx. Any
	// subsequent DB call (specifically: cancelOperation's
	// FinalizeOperation write inside the bounded writeCtx) blocks
	// waiting on the pool until the held tx releases or the
	// waiter's ctx fires.
	blockerCtx, cancelBlocker := context.WithCancel(context.Background())
	defer cancelBlocker()
	blocker, err := db.BeginTx(blockerCtx, nil)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	// LIFO defer: Rollback runs before any later-registered cleanup.
	// Service.Close (driven below) needs the pool to be free for its
	// own operations, so we release the blocker after capturing the
	// closeErr but before the test ends.
	rollbackOnce := sync.Once{}
	rollback := func() { rollbackOnce.Do(func() { _ = blocker.Rollback() }) }
	defer rollback()

	// Service.Close drives the contract: cancels the runner ctx,
	// the blockingPlanner observes ctx.Done and returns ctx.Err(),
	// executeReplyOperation routes through cancelOperation, the
	// bounded writeCtx contends with the held blocker tx, hits 5s,
	// returns wrapped DeadlineExceeded → runner goroutine wrapper
	// records via cancelDrainErr → runner.Wait returns it → Close
	// joins it into its own returned error.
	//
	// Outer deadline must be generous enough for the inner
	// terminalCancelWriteTimeout to fire BEFORE we time out at the
	// Wait layer (otherwise we'd observe a Wait-side ctx.Err
	// instead of the actual drain error).
	closeCtx, cancelClose := context.WithTimeout(context.Background(), terminalCancelWriteTimeout+5*time.Second)
	defer cancelClose()
	start := time.Now()
	closeErr := service.Close(closeCtx)
	elapsed := time.Since(start)

	// Release the blocker so any test-cleanup path (none required
	// here, but be tidy) doesn't deadlock.
	rollback()

	// Primary assertion: Close surfaced the failure.
	if closeErr == nil {
		t.Fatal("Service.Close returned nil — drain failure was silently dropped")
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("expected closeErr to wrap context.DeadlineExceeded, got: %v", closeErr)
	}
	if !strings.Contains(closeErr.Error(), "cancel drain") {
		t.Fatalf("expected closeErr message to mention 'cancel drain', got: %v", closeErr)
	}
	if !strings.Contains(closeErr.Error(), opID) {
		t.Fatalf("expected closeErr message to mention operation id %q, got: %v", opID, closeErr)
	}

	// Lower bound proves the write path was actually contended; upper bound
	// proves the write timeout fired rather than some unrelated deadline.
	if elapsed < terminalCancelWriteTimeout-500*time.Millisecond {
		t.Fatalf("Close returned before the bound — write path was not actually contended (took %v, bound=%v)", elapsed, terminalCancelWriteTimeout)
	}
	if elapsed > terminalCancelWriteTimeout+1500*time.Millisecond {
		t.Fatalf("Close outlived the bounded deadline: took %v (bound=%v + 1.5s slack)", elapsed, terminalCancelWriteTimeout)
	}

	// DB row remains `running` — recovery handles it on next boot
	// via the 3× TTL stuck-finalise path. The whole point of the
	// surfaced exit code is that the operator KNOWS this happened.
	op, ok, err := store.GetOperation(context.Background(), opID)
	if err != nil || !ok {
		t.Fatalf("get op after Close: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusRunning {
		t.Fatalf("expected op to remain `running` after failed cancel-drain, got %q", op.Status)
	}
}

// TestServiceCloseDoesNotRecordBenignCancelOutcomes verifies that a clean
// cancel and a lease-conflict race do not flow through cancelDrainErr.
//
// Exercises the discriminator directly via shouldRecordCancelDrainFailure.
// A full integration version would require simulating lease races
// during Close, which is rarer than the (already covered)
// concurrent-finalise tests elsewhere in this file. Direct
// exercise of the predicate is the right level of granularity.
func TestServiceCloseDoesNotRecordBenignCancelOutcomes(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(closeCtx)
	})

	// shouldRecordCancelDrainFailure must report false when r.ctx
	// has not been cancelled, regardless of error type — the
	// discriminator's first guard is "is this even a shutdown path?"
	if service.runner.shouldRecordCancelDrainFailure(context.DeadlineExceeded) {
		t.Fatal("recorded a failure outside a shutdown-driven path")
	}

	// Now simulate a shutdown by cancelling the recovery ctx
	// (which is the same ctx the runner observes).
	service.recoveryCancel()

	// Clean-cancel sentinel must be ignored — that's the contract
	// returned by cancelOperation on a successful cancel write.
	if service.runner.shouldRecordCancelDrainFailure(context.Canceled) {
		t.Fatal("recorded context.Canceled as a drain failure — that's cancelOperation's clean-success sentinel")
	}

	// Lease-conflict races are benign: another observer already
	// persisted the terminal state.
	if service.runner.shouldRecordCancelDrainFailure(state.ErrPlanningLeaseConflict) {
		t.Fatal("recorded ErrPlanningLeaseConflict as a drain failure — that's a benign race")
	}

	// And confirm the positive case still records: a real
	// DeadlineExceeded from the bounded write IS a failure worth
	// surfacing.
	if !service.runner.shouldRecordCancelDrainFailure(fmt.Errorf("finalize: %w", context.DeadlineExceeded)) {
		t.Fatal("did NOT record a wrapped DeadlineExceeded — that's the actual drain failure")
	}
}

// TestFailStuckMetricBumpsExactlyOnceUnderConcurrentFinalize pins the
// recovery metric ordering: when two observers race to finalise the
// same stuck operation, only the *winner* of the lease-fenced
// FinalizeOperation increments failureCount. The loser sees
// ErrPlanningLeaseConflict (the op is already terminal — benign) and
// must not bump the counter, otherwise the failure-rate metric
// reports double the actual failure count under any concurrent
// recovery + SSE-observer race.
func TestFailStuckMetricBumpsExactlyOnceUnderConcurrentFinalize(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T11:00:00Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_metric_race",
		Title:              "metric-race",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_metric_race",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_metric_race",
		PlanningSessionID:  "ps_metric_race",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "abandoned-worker",
		LeaseExpiresAt:     "2026-04-26T09:00:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T08:55:00Z",
		StartedAt:          "2026-04-26T08:55:01Z",
	}); err != nil {
		t.Fatalf("seed running operation: %v", err)
	}

	// Two concurrent finalisers. One will win the lease-fenced UPDATE;
	// the other will see ErrPlanningLeaseConflict and exit cleanly.
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			_, err := service.FinaliseIfStuck(context.Background(), "po_metric_race")
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	// Both calls return nil — winner finalised cleanly, loser
	// translated ErrPlanningLeaseConflict to nil internally.
	for err := range results {
		if err != nil {
			t.Fatalf("expected both FinaliseIfStuck calls to return nil, got %v", err)
		}
	}

	// Operation row terminal as expected.
	op, ok, err := store.GetOperation(context.Background(), "po_metric_race")
	if err != nil || !ok {
		t.Fatalf("get operation: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected status=failed, got %q", op.Status)
	}

	// Failure metric incremented exactly once. Before the recovery
	// metric reordering, both observers bumped the counter — this is
	// the contract test that prevents that regression.
	got := service.runner.deps.metrics.failureCount.Load()
	if got != 1 {
		t.Fatalf("expected failureCount=1 (only the winner bumps), got %d", got)
	}
}

// TestRecoveryDoesNotRestartExpiredButNotStuckRunningOperation pins
// the Wait-3×-TTL invariant on the recovery side: an operation whose
// lease has expired but is still inside the StuckOperationAfter grace
// window must NOT be restarted by recovery. Restarting before the
// grace expires races the original worker (whose heartbeat may just
// be slow) and duplicates any LLM/planner side effects it produced.
// The op stays `running` — either the original worker recovers and
// finalises, or the next recovery cycle (past 3× TTL) finalises it
// as failed.
func TestRecoveryDoesNotRestartExpiredButNotStuckRunningOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	// Service clock at 11:00:31 — 31s past lease_expires_at (11:00:00),
	// well inside the 3× TTL = 90s grace window.
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T11:00:31Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_grace",
		Title:              "grace",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_grace",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_grace",
		PlanningSessionID:  "ps_grace",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "slow-worker",
		LeaseExpiresAt:     "2026-04-26T11:00:00Z", // service.now − 31s, < 3× TTL (90s)
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T10:55:00Z",
		StartedAt:          "2026-04-26T10:55:01Z",
	}); err != nil {
		t.Fatalf("seed running op: %v", err)
	}

	// Snapshot pre-recovery state for comparison.
	beforeFailureCount := service.runner.deps.metrics.failureCount.Load()

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Op stays running — neither finalised nor restarted.
	op, ok, err := store.GetOperation(context.Background(), "po_grace")
	if err != nil || !ok {
		t.Fatalf("get op: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusRunning {
		t.Fatalf("expected op to remain running inside grace window, got %q (recovery is restarting too eagerly)", op.Status)
	}

	// runner.running map must not contain the op — Start was NOT
	// called by recovery. Without the queued-only restart change,
	// this map would have an entry.
	service.runner.mu.Lock()
	_, restarted := service.runner.running["po_grace"]
	service.runner.mu.Unlock()
	if restarted {
		t.Fatal("expected runner.Start NOT to be called for non-stuck running op (Wait-3×-TTL grace window)")
	}

	// Failure metric must not have been bumped — we didn't fail the op.
	afterFailureCount := service.runner.deps.metrics.failureCount.Load()
	if afterFailureCount != beforeFailureCount {
		t.Fatalf("expected failureCount unchanged (op is in grace, not failed), got %d → %d", beforeFailureCount, afterFailureCount)
	}
}

// TestRecoveryFailStuckBootstrapWhileForegroundOpActive pins the
// realistic shape: a bootstrap op stuck mid-refresh while the user
// has already submitted a reply that holds the session's interactive
// slot. Recovery must finalise the bootstrap as failed via the
// dedicated FinalizeBootstrapAbort path — without it, the
// FinalizeOperation interactive-slot fence rejects the bootstrap
// finalize (current_operation_id is po_reply_fg, not the bootstrap
// op id) and the bootstrap row stays stuck in `running` forever.
func TestRecoveryFailStuckBootstrapWhileForegroundOpActive(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T11:00:00Z" }, deterministicID())

	// Session has a foreground reply holding the interactive slot.
	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_bs_recovery",
		Title:              "bs-recovery",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_reply_fg",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-26T08:00:00Z",
		UpdatedAt:          "2026-04-26T08:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	// Foreground reply (running, lease still valid) — owns the slot.
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_reply_fg",
		PlanningSessionID:  "ps_bs_recovery",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "fg-worker",
		LeaseExpiresAt:     "2026-04-26T11:30:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T08:00:01Z",
		StartedAt:          "2026-04-26T08:00:02Z",
	}); err != nil {
		t.Fatalf("seed reply op: %v", err)
	}
	// Bootstrap op stuck (lease expired well past 3× TTL).
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_bs_stuck",
		PlanningSessionID:  "ps_bs_recovery",
		Type:               state.PlanningOperationTypeBootstrapContext,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "abandoned-bs-worker",
		LeaseExpiresAt:     "2026-04-26T09:00:00Z", // 2h before service.now → past 3× TTL
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T08:55:00Z",
		StartedAt:          "2026-04-26T08:55:01Z",
	}); err != nil {
		t.Fatalf("seed stuck bootstrap op: %v", err)
	}

	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Bootstrap row finalised as failed.
	op, ok, err := store.GetOperation(context.Background(), "po_bs_stuck")
	if err != nil || !ok {
		t.Fatalf("get bootstrap op: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected stuck bootstrap finalised as failed, got %q (this is the case where FinalizeOperation's interactive fence would have rejected the abort)", op.Status)
	}

	// Foreground reply slot untouched.
	session, ok, err := store.GetSession(context.Background(), "ps_bs_recovery")
	if err != nil || !ok {
		t.Fatalf("get session: ok=%v err=%v", ok, err)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_reply_fg" {
		t.Fatalf("expected session.current_operation_id to remain po_reply_fg, got %+v", session.CurrentOperationID)
	}
	if session.ConversationState != state.PlanningSessionConversationStateProcessing {
		t.Fatalf("expected conversation_state preserved as processing, got %q", session.ConversationState)
	}
}

func TestRecoverOperationsPagesStuckBootstrapOperations(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T11:00:00Z" }, deterministicID())
	t.Cleanup(func() {
		_ = service.Close(context.Background())
	})

	ctx := context.Background()
	for i := 1; i <= state.DefaultPlanningOperationListLimit+1; i++ {
		sessionID := fmt.Sprintf("ps_bootstrap_page_%03d", i)
		operationID := fmt.Sprintf("po_bootstrap_page_%03d", i)
		createdAt := fmt.Sprintf("2026-04-26T08:%02d:00Z", i%60)
		if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
			PlanningSessionID:  sessionID,
			Title:              fmt.Sprintf("bootstrap-%03d", i),
			TitleState:         "custom",
			LifecycleState:     state.PlanningSessionLifecycleStateActive,
			ConversationState:  state.PlanningSessionConversationStateReady,
			ProjectRoot:        projectRoot,
			ModelSelectionJSON: `{"model":"test-model"}`,
			CreatedAt:          createdAt,
			UpdatedAt:          createdAt,
		}); err != nil {
			t.Fatalf("create session %d: %v", i, err)
		}
		if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
			OperationID:        operationID,
			PlanningSessionID:  sessionID,
			Type:               state.PlanningOperationTypeBootstrapContext,
			Status:             state.PlanningOperationStatusRunning,
			RequestPayloadJSON: `{}`,
			LeaseOwner:         fmt.Sprintf("worker-%03d", i),
			LeaseExpiresAt:     "2026-04-26T09:00:00Z",
			LeaseVersion:       1,
			CreatedAt:          createdAt,
			StartedAt:          createdAt,
		}); err != nil {
			t.Fatalf("create operation %d: %v", i, err)
		}
	}

	if err := service.RecoverOperations(ctx); err != nil {
		t.Fatalf("recover operations: %v", err)
	}

	var failedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM planning_operations WHERE status = ?`, state.PlanningOperationStatusFailed).Scan(&failedCount); err != nil {
		t.Fatalf("count failed operations: %v", err)
	}
	if failedCount != state.DefaultPlanningOperationListLimit+1 {
		t.Fatalf("expected all stuck bootstrap operations to fail across pages, got %d", failedCount)
	}
}

// TestFinaliseIfStuckBeforeThreshold guards the no-counter contract: an
// operation whose lease is freshly expired (less than StuckOperationAfter
// past) must NOT be finalised, no matter how many times observers call
// FinaliseIfStuck.
func TestFinaliseIfStuckBeforeThreshold(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T10:00:30Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_fresh",
		Title:              "fresh",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_fresh",
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_fresh",
		PlanningSessionID:  "ps_fresh",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "worker-a",
		LeaseExpiresAt:     "2026-04-26T10:00:10Z", // 20s before service.now → less than threshold
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:59:00Z",
		StartedAt:          "2026-04-26T09:59:01Z",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Many observations below threshold must NOT finalise.
	for i := 0; i < 10; i++ {
		stuck, err := service.FinaliseIfStuck(context.Background(), "po_fresh")
		if err != nil {
			t.Fatalf("observe iteration %d: %v", i, err)
		}
		if stuck {
			t.Fatalf("operation finalised before threshold on iteration %d (observer-count bias)", i)
		}
	}
	op, _, _ := store.GetOperation(context.Background(), "po_fresh")
	if op.Status != state.PlanningOperationStatusRunning {
		t.Fatalf("expected status=running, got %q", op.Status)
	}
}

func TestFinaliseIfStuckTerminalizesInvalidRecoveredGeneratedTitle(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T10:01:00Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_invalid_title_recovery",
		Title:                   "plan_1",
		TitleState:              "placeholder",
		LifecycleState:          state.PlanningSessionLifecycleStateActive,
		ConversationState:       state.PlanningSessionConversationStateReady,
		CurrentTitleOperationID: "po_invalid_title_recovery",
		ProjectRoot:             projectRoot,
		ModelSelectionJSON:      `{"model":"m"}`,
		CreatedAt:               "2026-04-26T10:00:00Z",
		UpdatedAt:               "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_invalid_title_recovery",
		PlanningSessionID:  "ps_invalid_title_recovery",
		Type:               state.PlanningOperationTypeGenerateTitle,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "worker-a",
		LeaseExpiresAt:     "2026-04-26T09:59:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:58:00Z",
		StartedAt:          "2026-04-26T09:58:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_invalid_title_recovery",
		PlanningOperationID:          "po_invalid_title_recovery",
		PlanningSessionID:            "ps_invalid_title_recovery",
		LogicalSessionID:             "planner-title-ps_invalid_title_recovery",
		CompletionCapability:         "planning-capability-invalid-title",
		IssuedAt:                     "2026-04-26T09:58:02Z",
		CreatedAt:                    "2026-04-26T09:58:02Z",
	}); err != nil {
		t.Fatalf("create planning provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_invalid_title_recovery",
		PlanningOperationID:          "po_invalid_title_recovery",
		ProviderSessionID:            "ses_invalid_title_recovery",
		OpenedAt:                     "2026-04-26T09:58:03Z",
	}); err != nil {
		t.Fatalf("mark planning provider invocation open: %v", err)
	}
	if _, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_invalid_title_recovery",
		AttemptID:                    "po_invalid_title_recovery",
		PlanningProviderInvocationID: "ppinv_invalid_title_recovery",
		PlanningOperationID:          "po_invalid_title_recovery",
		PlanningSessionID:            "ps_invalid_title_recovery",
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-invalid-title",
		FinalText:                    `""`,
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T09:58:04Z",
	}); err != nil {
		t.Fatalf("accept planning provider completion receipt: %v", err)
	}

	stuck, err := service.FinaliseIfStuck(context.Background(), "po_invalid_title_recovery")
	if err != nil {
		t.Fatalf("finalise invalid recovered title if stuck: %v", err)
	}
	if !stuck {
		t.Fatal("expected invalid recovered title operation to finalize as stuck")
	}

	op, found, err := store.GetOperation(context.Background(), "po_invalid_title_recovery")
	if err != nil || !found {
		t.Fatalf("get recovered title operation: ok=%v err=%v", found, err)
	}
	if op.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected invalid recovered title op to fail, got %#v", op)
	}
	if op.Error == nil || !strings.Contains(*op.Error, "generated title is empty") {
		t.Fatalf("expected invalid recovered title error to be recorded, got %#v", op)
	}

	session, found, err := store.GetSession(context.Background(), "ps_invalid_title_recovery")
	if err != nil || !found {
		t.Fatalf("get recovered title session: ok=%v err=%v", found, err)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected title lane to clear after invalid recovered title failure, got %#v", session)
	}
}

func TestFinaliseIfStuckImmediatelyTerminalizesInvalidRecoveredGeneratedTitleBeforeThreshold(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, nil, projectRoot, func() string { return "2026-04-26T10:00:30Z" }, deterministicID())

	if _, err := store.CreateSession(context.Background(), state.CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_invalid_title_recent",
		Title:                   "plan_1",
		TitleState:              "placeholder",
		LifecycleState:          state.PlanningSessionLifecycleStateActive,
		ConversationState:       state.PlanningSessionConversationStateReady,
		CurrentTitleOperationID: "po_invalid_title_recent",
		ProjectRoot:             projectRoot,
		ModelSelectionJSON:      `{"model":"m"}`,
		CreatedAt:               "2026-04-26T09:58:00Z",
		UpdatedAt:               "2026-04-26T09:58:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(context.Background(), state.CreatePlanningOperationRequest{
		OperationID:        "po_invalid_title_recent",
		PlanningSessionID:  "ps_invalid_title_recent",
		Type:               state.PlanningOperationTypeGenerateTitle,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "worker-a",
		LeaseExpiresAt:     "2026-04-26T10:00:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T09:58:00Z",
		StartedAt:          "2026-04-26T09:58:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_invalid_title_recent",
		PlanningOperationID:          "po_invalid_title_recent",
		PlanningSessionID:            "ps_invalid_title_recent",
		LogicalSessionID:             "planner-title-ps_invalid_title_recent",
		CompletionCapability:         "planning-capability-invalid-title-recent",
		IssuedAt:                     "2026-04-26T09:58:02Z",
		CreatedAt:                    "2026-04-26T09:58:02Z",
	}); err != nil {
		t.Fatalf("create planning provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(context.Background(), state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_invalid_title_recent",
		PlanningOperationID:          "po_invalid_title_recent",
		ProviderSessionID:            "ses_invalid_title_recent",
		OpenedAt:                     "2026-04-26T09:58:03Z",
	}); err != nil {
		t.Fatalf("mark planning provider invocation open: %v", err)
	}
	if _, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), state.AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "receipt_invalid_title_recent",
		AttemptID:                    "po_invalid_title_recent",
		PlanningProviderInvocationID: "ppinv_invalid_title_recent",
		PlanningOperationID:          "po_invalid_title_recent",
		PlanningSessionID:            "ps_invalid_title_recent",
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-invalid-title-recent",
		FinalText:                    `""`,
		StructuredPayloadJSON:        `{}`,
		ReceivedAt:                   "2026-04-26T09:58:04Z",
	}); err != nil {
		t.Fatalf("accept planning provider completion receipt: %v", err)
	}

	stuck, err := service.FinaliseIfStuck(context.Background(), "po_invalid_title_recent")
	if err != nil {
		t.Fatalf("finalise invalid recovered title before threshold: %v", err)
	}
	if !stuck {
		t.Fatal("expected receipt-backed invalid title to finalize immediately before the stuck threshold")
	}

	op, found, err := store.GetOperation(context.Background(), "po_invalid_title_recent")
	if err != nil || !found {
		t.Fatalf("get recovered title operation: ok=%v err=%v", found, err)
	}
	if op.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("expected invalid recovered title op to fail, got %#v", op)
	}
	if op.Error == nil || !strings.Contains(*op.Error, "generated title is empty") {
		t.Fatalf("expected invalid recovered title error to be recorded, got %#v", op)
	}

	session, found, err := store.GetSession(context.Background(), "ps_invalid_title_recent")
	if err != nil || !found {
		t.Fatalf("get recovered title session: ok=%v err=%v", found, err)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected title lane to clear after invalid recovered title failure, got %#v", session)
	}
}

func TestServiceRejectsDuplicateInteractiveOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-21T17:10:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Duplicate op test", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	firstOp, err := store.CreateFormalizeBundle(context.Background(), state.CreatePlanningFormalizeBundleRequest{
		SessionID: session.PlanningSessionID,
		Operation: state.CreatePlanningOperationRequest{
			OperationID:        "planning_operation_active_1",
			PlanningSessionID:  session.PlanningSessionID,
			Type:               state.PlanningOperationTypeFormalize,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: `{"draft_id":"planning_draft_1","draft_created_at":"2026-04-21T17:10:00Z"}`,
			LeaseOwner:         "",
			LeaseVersion:       0,
			CreatedAt:          "2026-04-21T17:10:00Z",
		},
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "planning_operation_active_1",
		UpdatedAt:          "2026-04-21T17:10:00Z",
	})
	if err != nil {
		t.Fatalf("seed first active interactive operation: %v", err)
	}
	if firstOp.OperationID == "" {
		t.Fatal("expected first operation to be created")
	}

	_, err = service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
	if err == nil || !strings.Contains(err.Error(), "active interactive operation") {
		t.Fatalf("expected duplicate interactive operation rejection, got %v", err)
	}
}

func awaitOperationTerminal(t *testing.T, store *state.PlanningStore, operationID string) state.PlanningOperation {
	t.Helper()
	var terminal state.PlanningOperation
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		operation, ok, err := store.GetOperation(context.Background(), operationID)
		if err != nil {
			return fmt.Errorf("get operation %s: %w", operationID, err)
		}
		if !ok {
			return fmt.Errorf("operation %s not found", operationID)
		}
		if operation.Status != state.PlanningOperationStatusSucceeded && operation.Status != state.PlanningOperationStatusFailed && operation.Status != state.PlanningOperationStatusCancelled {
			return fmt.Errorf("operation %s still %s", operationID, operation.Status)
		}
		terminal = operation
		return nil
	})
	return terminal
}

func latestSessionOperation(t *testing.T, store *state.PlanningStore, sessionID string) state.PlanningOperation {
	t.Helper()
	operations, err := store.ListSessionOperations(context.Background(), sessionID, 10)
	if err != nil {
		t.Fatalf("list session operations: %v", err)
	}
	if len(operations) == 0 {
		t.Fatal("expected session operations")
	}
	return operations[0]
}

func stateTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openPlanningTestDB(t)
	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, name := range entries {
		if strings.HasSuffix(name, ".up.sql") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		applyPlanningMigration(t, db, name)
	}
	return db
}

func openPlanningTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func applyPlanningMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()
	raw, err := dbmigrations.Read(fileName)
	if err != nil {
		t.Fatalf("read migration %s: %v", fileName, err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply migration %s: %v", fileName, err)
	}
}

func writeTestFile(t *testing.T, root, relativePath, content string) {
	t.Helper()
	absPath := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", relativePath, err)
	}
	if err := os.WriteFile(absPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write file %s: %v", relativePath, err)
	}
}

func deterministicID() func(prefix string) string {
	var (
		mu  sync.Mutex
		seq int
	)
	return func(prefix string) string {
		mu.Lock()
		seq++
		current := seq
		mu.Unlock()
		return fmt.Sprintf("%s_test_%d", prefix, current)
	}
}

func countPlanningOperations(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations`).Scan(&n); err != nil {
		t.Fatalf("count planning_operations: %v", err)
	}
	return n
}

func countPlanningSessions(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_sessions`).Scan(&n); err != nil {
		t.Fatalf("count planning_sessions: %v", err)
	}
	return n
}

// slowTitlePlanner blocks GenerateTitle's planner call until release
// is closed. Used to test that Service.Close honours its ctx deadline
// even when an admitted write is parked deep inside a planner call.
type slowTitlePlanner struct {
	mockAgent
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func newSlowTitlePlanner() *slowTitlePlanner {
	return &slowTitlePlanner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// GenerateTitle is the Agent method session_service.GenerateTitle
// invokes. Block on release so the test owns the timing.
func (p *slowTitlePlanner) GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error) {
	p.startedOnce.Do(func() { close(p.started) })
	select {
	case <-p.release:
		if onChunk != nil {
			onChunk("Released Title")
		}
		metadata, err := testReceiptMetadata(input, "Released Title")
		if err != nil {
			return GeneratedTitle{}, err
		}
		return GeneratedTitle{
			Title:    "Released Title",
			Metadata: metadata,
		}, nil
	case <-ctx.Done():
		return GeneratedTitle{}, ctx.Err()
	}
}

func awaitSlowTitleStarted(t *testing.T, p *slowTitlePlanner) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow planner never reached GenerateTitle")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// awaitAllSessionsAdvanced verifies that every session in the DB has
// its bootstrap operation in a non-queued state — proxy for "Start
// was called on it". Without admission control, sessions admitted
// during shutdown would have their bootstrap stuck in queued forever.
func awaitAllSessionsAdvanced(t *testing.T, store *state.PlanningStore, expectedAtLeast int, timeout time.Duration) {
	t.Helper()
	var lastStuck []string
	testutil.Eventually(t, timeout, 10*time.Millisecond, func() error {
		sessions, err := store.ListSessions(context.Background(), 100)
		if err != nil {
			return fmt.Errorf("list sessions: %w", err)
		}
		if len(sessions) < expectedAtLeast {
			return fmt.Errorf("have %d sessions, want at least %d", len(sessions), expectedAtLeast)
		}
		var stuck []string
		for _, s := range sessions {
			ops, err := store.ListSessionOperations(context.Background(), s.PlanningSessionID, 10)
			if err != nil {
				return fmt.Errorf("list session operations for %s: %w", s.PlanningSessionID, err)
			}
			for _, op := range ops {
				if op.Status == state.PlanningOperationStatusQueued {
					stuck = append(stuck, op.OperationID)
				}
			}
		}
		if len(stuck) == 0 {
			return nil
		}
		lastStuck = stuck
		return fmt.Errorf("still queued=%v", lastStuck)
	})
}

func mustOperationSessionID(t *testing.T, store *state.PlanningStore, operationID string) string {
	t.Helper()
	op, ok, err := store.GetOperation(context.Background(), operationID)
	if err != nil || !ok {
		t.Fatalf("get operation %s: ok=%v err=%v", operationID, ok, err)
	}
	return op.PlanningSessionID
}

// blockingPlanner is a planner whose ContinueTurnStream blocks until
// either ctx.Done fires (cooperative — the cancel-drain test) or an
// explicit release channel closes (uncooperative — the deadline test).
// started closes the moment the planner enters ContinueTurnStream so
// tests can synchronise on "the goroutine is actually blocked here"
// rather than the weaker "Start added the operation to its running map".
// observed closes when the planner observed cancellation, so tests can
// assert ctx propagation reached the agent.
type blockingPlanner struct {
	mockAgent
	release      chan struct{}
	observed     chan struct{}
	started      chan struct{}
	respectCtx   bool
	startedOnce  sync.Once
	observedOnce sync.Once
}

func newBlockingPlanner(respectCtx bool) *blockingPlanner {
	return &blockingPlanner{
		release:    make(chan struct{}),
		observed:   make(chan struct{}),
		started:    make(chan struct{}),
		respectCtx: respectCtx,
	}
}

func (p *blockingPlanner) ContinueTurnStream(ctx context.Context, input AgentContext, _ func(text string)) (AgentTurn, error) {
	p.startedOnce.Do(func() { close(p.started) })
	if p.respectCtx {
		select {
		case <-ctx.Done():
			p.observedOnce.Do(func() { close(p.observed) })
			return AgentTurn{}, ctx.Err()
		case <-p.release:
			metadata, err := testReceiptMetadata(input, "released")
			if err != nil {
				return AgentTurn{}, err
			}
			return AgentTurn{ContentMarkdown: "released", Metadata: metadata}, nil
		}
	}
	<-p.release
	metadata, err := testReceiptMetadata(input, "released")
	if err != nil {
		return AgentTurn{}, err
	}
	return AgentTurn{ContentMarkdown: "released", Metadata: metadata}, nil
}

// awaitPlannerStarted waits for the blocking planner to enter its
// ContinueTurnStream call so the caller can confidently assert the
// runner goroutine is actually parked inside the planner — not still in
// the queue or in the DB-claim hop.
func awaitPlannerStarted(t *testing.T, p *blockingPlanner) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking planner never reached ContinueTurnStream")
	}
}

// awaitRunnerRunning polls the runner's internal running map (legitimate
// in-package test reach) until the named operation is observed in flight.
// Bounded by a deadline so a stuck test fails fast rather than hanging.
func awaitRunnerRunning(t *testing.T, runner *operationRunner, operationID string) {
	t.Helper()
	testutil.Eventually(t, 2*time.Second, 5*time.Millisecond, func() error {
		runner.mu.Lock()
		_, exists := runner.running[operationID]
		runner.mu.Unlock()
		if exists {
			return nil
		}
		return fmt.Errorf("operation %s not yet running", operationID)
	})
}

// startBlockingReplyForCloseTest creates a session, awaits bootstrap,
// and kicks off a reply operation that hits the supplied blocking
// planner. Synchronises on the planner's started signal so the caller
// can rely on "the runner goroutine is parked inside the planner" — the
// only signal strong enough to test ctx propagation through the stream
// without races.
func startBlockingReplyForCloseTest(t *testing.T, service *Service, store *state.PlanningStore, planner *blockingPlanner) string {
	t.Helper()
	session, err := service.CreateSession(context.Background(), "Close drain", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	op, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Tell me more.",
		AnswerPayloadJSON: `{"text":"Tell me more."}`,
	})
	if err != nil {
		t.Fatalf("create reply operation: %v", err)
	}
	awaitRunnerRunning(t, service.runner, op.Operation.OperationID)
	awaitPlannerStarted(t, planner)
	return op.Operation.OperationID
}

// TestServiceCloseWaitsForInFlightRunnerOperation pins the cooperative
// shutdown contract: when the planner respects ctx, Service.Close cancels
// the runner-owned context, every in-flight goroutine exits at its next
// chunk boundary, and Wait drains within the parent deadline.
func TestServiceCloseWaitsForInFlightRunnerOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(true)
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	_ = startBlockingReplyForCloseTest(t, service, store, planner)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("expected Close to drain cleanly, got %v", err)
	}

	select {
	case <-planner.observed:
		// Good — the planner saw ctx.Done and returned.
	default:
		t.Fatal("planner did not observe ctx cancellation; runner ctx was not propagated through Generate")
	}
}

// TestServiceCloseFinalizesInFlightOperationAsCancelled pins the
// cancellation-vs-failure semantics: a cooperative shutdown of an
// in-flight operation must land the row in `cancelled` (not `failed`)
// with the session's conversation_state back to `ready` and the
// current_operation_id cleared. This is the contract that keeps
// failure-rate metrics honest and lets the UI render shutdowns as
// neutral, not red.
func TestServiceCloseFinalizesInFlightOperationAsCancelled(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(true)
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	opID := startBlockingReplyForCloseTest(t, service, store, planner)
	sessionID := mustOperationSessionID(t, store, opID)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("expected Close to drain cleanly, got %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), opID)
	if err != nil || !ok {
		t.Fatalf("get operation after close: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusCancelled {
		t.Fatalf("expected status=cancelled (shutdown is not a failure), got %q", op.Status)
	}
	if op.Error == nil || strings.TrimSpace(*op.Error) == "" {
		t.Fatalf("expected cancellation reason in error column, got %+v", op.Error)
	}
	if op.LeaseOwner != "" {
		t.Fatalf("expected lease cleared after cancel, got owner=%q", op.LeaseOwner)
	}

	session, ok, err := store.GetSession(context.Background(), sessionID)
	if err != nil || !ok {
		t.Fatalf("get session after close: ok=%v err=%v", ok, err)
	}
	if session.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected session conversation_state=ready after cancel, got %q", session.ConversationState)
	}
	if session.CurrentOperationID != nil {
		t.Fatalf("expected session current_operation_id cleared, got %q", *session.CurrentOperationID)
	}

	// Terminal event must be the dedicated "cancelled" event type so the
	// SSE consumer can distinguish shutdown from a planner failure.
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), opID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one operation event")
	}
	terminal := events[len(events)-1]
	if terminal.EventType != "cancelled" {
		t.Fatalf("expected terminal event type=cancelled, got %q", terminal.EventType)
	}
}

// TestServiceCloseUncooperativePlannerStillFinalizesAsCancelled pins
// the post-planner shutdown guard: even when an uncooperative planner
// returns *successfully* after Close has timed out, the runner must
// finalize the operation as cancelled rather than write a `done`
// terminal event for an operation the user expected to be cancelled.
// The decoupled-ctx terminal write (context.WithoutCancel) is what
// lets this happen — the cancelled service ctx would otherwise abort
// the persistence.
func TestServiceCloseUncooperativePlannerStillFinalizesAsCancelled(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(false) // ignores ctx
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(planner.release) })
	})

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	opID := startBlockingReplyForCloseTest(t, service, store, planner)
	sessionID := mustOperationSessionID(t, store, opID)

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := service.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded from uncooperative-planner Close, got %v", err)
	}

	// Release the planner so its successful return races with the
	// shutdown. The post-planner guard must catch this and finalize as
	// cancelled, not succeeded.
	releaseOnce.Do(func() { close(planner.release) })
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := service.runner.Wait(drainCtx); err != nil {
		t.Fatalf("post-deadline runner drain failed: %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), opID)
	if err != nil || !ok {
		t.Fatalf("get operation after drain: ok=%v err=%v", ok, err)
	}
	if op.Status == state.PlanningOperationStatusSucceeded {
		t.Fatalf("uncooperative planner returned success after shutdown but we wrote a `done` terminal event — operation should be cancelled, not succeeded")
	}
	if op.Status != state.PlanningOperationStatusCancelled {
		t.Fatalf("expected status=cancelled after post-deadline drain, got %q", op.Status)
	}

	session, ok, err := store.GetSession(context.Background(), sessionID)
	if err != nil || !ok {
		t.Fatalf("get session after drain: ok=%v err=%v", ok, err)
	}
	if session.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected session conversation_state=ready after post-deadline cancel, got %q", session.ConversationState)
	}
}

// TestFailOperationDuringShutdownRoutesToCancelled is a focused unit
// test for the shutdown router added at the top of failOperation.
// Without that router, a pre-planner DB error (or any error path that
// calls failOperation) during shutdown would write a `failed` terminal
// using the cancelled service ctx — which often fails outright,
// leaving the row stuck in `running` until next-startup recovery.
//
// The router instead routes every failOperation call to cancelOperation
// when r.ctx is cancelled. This test cancels the service ctx before
// calling failOperation directly, and asserts the row lands in
// `cancelled` (not `failed`), failureCount stays at 0, and the
// terminal event type is `cancelled`. It exercises the shutdown
// branch without needing to inject a slow refresh or stub the planner
// — both seams that don't exist on the current runtimeDeps.
func TestFailOperationDuringShutdownRoutesToCancelled(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Fail-route", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	// Create a fresh reply operation row in the DB, then claim it
	// directly through the store so we have a known LeaseOwner /
	// LeaseVersion to fence the failOperation call. We do NOT call
	// runner.Start because the goal here is to drive failOperation in
	// isolation, not exercise the planner path.
	opID := "po_failroute"
	if _, err := store.CreateReplyBundle(context.Background(), state.CreatePlanningReplyBundleRequest{
		SessionID: session.PlanningSessionID,
		UserMessage: state.AppendPlanningMessageRequest{
			PlanningMessageID:   "pm_user_failroute",
			PlanningSessionID:   session.PlanningSessionID,
			TurnIndex:           1,
			Role:                state.PlanningMessageRoleUser,
			ContentMarkdown:     "ping",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{}`,
			Active:              true,
			CreatedAt:           "2026-04-26T13:00:01Z",
		},
		Operation: state.CreatePlanningOperationRequest{
			OperationID:        opID,
			PlanningSessionID:  session.PlanningSessionID,
			Type:               state.PlanningOperationTypeReply,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: `{}`,
			CreatedAt:          "2026-04-26T13:00:01Z",
		},
		ConversationState: state.PlanningSessionConversationStateProcessing,
		UpdatedAt:         "2026-04-26T13:00:01Z",
	}); err != nil {
		t.Fatalf("create reply bundle: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(context.Background(), opID, "test-worker", "2026-04-26T14:00:00Z", "2026-04-26T13:00:02Z")
	if err != nil || !ok {
		t.Fatalf("claim operation: ok=%v err=%v", ok, err)
	}

	// Cancel the service-owned context before calling failOperation.
	// This is the precondition the router checks for.
	service.recoveryCancel()
	// Also drain the recovery loop so it doesn't race with our manual
	// finalize. The runner.Wait isn't needed because no goroutine has
	// been started for opID — we're calling failOperation directly.
	<-service.recoveryDone

	cursor := newSequenceCursor(1)
	failErr := service.runner.failOperation(context.Background(), claimed, cursor, fmt.Errorf("simulated pre-planner error"))
	// failOperation returns context.Canceled when it routes to
	// cancelOperation (see cancelOperation's return value contract).
	if !errors.Is(failErr, context.Canceled) {
		t.Fatalf("expected context.Canceled return from cancel routing, got %v", failErr)
	}

	op, ok, err := store.GetOperation(context.Background(), opID)
	if err != nil || !ok {
		t.Fatalf("get operation: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusCancelled {
		t.Fatalf("expected status=cancelled (router should re-route the failure), got %q", op.Status)
	}
	if op.Error == nil || *op.Error == "" {
		t.Fatalf("expected cancellation reason in error column, got %+v", op.Error)
	}

	// Failure metric must NOT have been bumped — cancelled ops feed
	// into a separate cancellation count, not the failure count, so
	// graceful shutdowns don't pollute alerts/SLOs.
	if got := service.runner.deps.metrics.failureCount.Load(); got != 0 {
		t.Fatalf("expected failureCount=0 (shutdown is not a failure), got %d", got)
	}

	// Terminal event type must be `cancelled`, not `error` — distinct
	// SSE event type so the frontend can render neutrally.
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), opID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one terminal event")
	}
	terminal := events[len(events)-1]
	if terminal.EventType != "cancelled" {
		t.Fatalf("expected terminal event type=cancelled, got %q", terminal.EventType)
	}
}

// TestServiceWritesRejectedAfterCloseAndPersistNoRow pins the
// admission gate's rejection contract: every Service-facade write
// returns ErrServiceShuttingDown after Close has flipped the gate,
// AND no DB row gets persisted. Per-write parametric so a future
// write that's added to the service is forced to either join the gate
// or break this test loudly.
func TestServiceWritesRejectedAfterCloseAndPersistNoRow(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())

	// Seed a session so update/delete/etc. have a target. Bootstrap
	// must drain before we Close, otherwise its Start path becomes
	// noisy in this test (irrelevant to the admission contract).
	session, err := service.CreateSession(context.Background(), "Pre-close", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("seed session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	preCloseOps := countPlanningOperations(t, db)
	preCloseSessions := countPlanningSessions(t, db)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Each write method must reject and not touch the DB.
	cases := []struct {
		name string
		call func() error
	}{
		{"CreateSession", func() error {
			_, err := service.CreateSession(context.Background(), "After-close", `{"model":"x"}`)
			return err
		}},
		{"CreateOperation", func() error {
			_, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
				Type:              state.PlanningOperationTypeReply,
				ReplyMarkdown:     "after-close reply",
				AnswerPayloadJSON: `{"text":"after-close reply"}`,
			})
			return err
		}},
		{"UpdateSession", func() error {
			title := "renamed-after-close"
			_, err := service.UpdateSession(context.Background(), session.PlanningSessionID, UpdateSessionInput{Title: &title})
			return err
		}},
		{"GenerateTitle", func() error {
			_, err := service.GenerateTitle(context.Background(), session.PlanningSessionID)
			return err
		}},
		{"RefreshContext", func() error {
			_, _, err := service.RefreshContext(context.Background(), session.PlanningSessionID)
			return err
		}},
		{"ActivateVariant", func() error {
			_, err := service.ActivateVariant(context.Background(), session.PlanningSessionID, "any-message-id")
			return err
		}},
		{"DeleteSession", func() error {
			_, err := service.DeleteSession(context.Background(), session.PlanningSessionID)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if !errors.Is(err, ErrServiceShuttingDown) {
				t.Fatalf("expected ErrServiceShuttingDown, got %v", err)
			}
		})
	}

	// Counts unchanged: every rejected write was a no-op at the
	// store level. Without admission control these would have
	// persisted rows that no runner would ever pick up.
	if got := countPlanningOperations(t, db); got != preCloseOps {
		t.Fatalf("operation rows changed across rejected writes: %d → %d", preCloseOps, got)
	}
	if got := countPlanningSessions(t, db); got != preCloseSessions {
		t.Fatalf("session rows changed across rejected writes: %d → %d", preCloseSessions, got)
	}
}

// TestServiceCloseCancelsInFlightGenerateTitleOperation pins the new title-lane
// shutdown contract: GenerateTitle only enqueues the background operation, so
// Close should not block on write admission, but it still must cancel and drain
// the in-flight title runner cleanly.
func TestServiceCloseCancelsInFlightGenerateTitleOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newSlowTitlePlanner()
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	updated, err := service.GenerateTitle(context.Background(), session.PlanningSessionID)
	if err != nil {
		t.Fatalf("generate title: %v", err)
	}
	if updated.CurrentTitleOperationID == nil || strings.TrimSpace(*updated.CurrentTitleOperationID) == "" {
		t.Fatalf("expected background title operation id, got %+v", updated.CurrentTitleOperationID)
	}
	opID := strings.TrimSpace(*updated.CurrentTitleOperationID)
	awaitSlowTitleStarted(t, planner)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("expected Close to drain cleanly, got %v", err)
	}

	op, ok, err := store.GetOperation(context.Background(), opID)
	if err != nil || !ok {
		t.Fatalf("get operation after close: ok=%v err=%v", ok, err)
	}
	if op.Status != state.PlanningOperationStatusCancelled {
		t.Fatalf("expected title operation to be cancelled on shutdown, got %q", op.Status)
	}

	stored, ok, err := store.GetSession(context.Background(), session.PlanningSessionID)
	if err != nil || !ok {
		t.Fatalf("get session after close: ok=%v err=%v", ok, err)
	}
	if stored.CurrentTitleOperationID != nil {
		t.Fatalf("expected title slot cleared after shutdown cancel, got %+v", stored.CurrentTitleOperationID)
	}
	if stored.CurrentOperationID != nil {
		t.Fatalf("expected interactive slot to remain empty, got %+v", stored.CurrentOperationID)
	}
	if stored.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation_state to remain ready, got %q", stored.ConversationState)
	}
}

// TestServiceCreateOperationRaceWithCloseHasNoOrphans is the
// load-bearing TOCTOU regression test. Without the admission gate,
// a Service.CreateSession admitted *before* Close could still hit
// runner.Start *after* Close had cancelled runner.ctx — leaving the
// row persisted with Start never invoked, invisible to the runner's
// in-process bookkeeping. With admission control, Close's drained
// channel waits for every admitted write to finish persist+Start
// atomically, so every persisted row had Start invoked.
//
// Note on the bootstrap-goroutine state: once Start has been
// invoked, the goroutine may still fail to claim the row (if
// runner.ctx was cancelled by the time it ran the claim query).
// That leaves the row in `queued`, which is correct — recovery on
// the next process start will pick it up. A queued row is NOT an
// orphan; the bug we're guarding against is the row existing
// without runner.Start ever being called for it.
//
// We assert the row count equals the admitted-call count (no rows
// persisted by rejected calls, no admitted calls that failed
// mid-persist). The "Start was called" invariant is enforced by
// construction: with admission control, every successful
// CreateSession completes persist+Start before releaseWrite.
func TestServiceCreateOperationRaceWithCloseHasNoOrphans(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T13:00:00Z" }, deterministicID())

	const concurrency = 16
	results := make(chan error, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, err := service.CreateSession(context.Background(), "race", `{"model":"test-model"}`)
			results <- err
		}()
	}

	// Race Close against the in-flight writes. The exact interleaving
	// is non-deterministic; the invariant is what we assert on.
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	closeErr := service.Close(closeCtx)
	if closeErr != nil && !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("close: %v", closeErr)
	}

	wg.Wait()
	close(results)

	// Count outcomes.
	var admitted, rejected int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrServiceShuttingDown):
			rejected++
		default:
			t.Fatalf("unexpected CreateSession error: %v", err)
		}
	}
	if admitted+rejected != concurrency {
		t.Fatalf("expected admitted+rejected to equal concurrency=%d, got admitted=%d rejected=%d", concurrency, admitted, rejected)
	}

	// Invariant: exactly `admitted` rows persisted. Rejected calls
	// must not leave half-persisted state behind, and admitted calls
	// must each persist exactly one row. This is what proves the
	// admission gate covered persist+Start atomically.
	persistedSessions := countPlanningSessions(t, db)
	if persistedSessions != admitted {
		t.Fatalf("expected %d persisted sessions (matching admitted count), got %d", admitted, persistedSessions)
	}
	persistedOps := countPlanningOperations(t, db)
	if persistedOps != admitted {
		t.Fatalf("expected %d persisted operations (one bootstrap per admitted session), got %d", admitted, persistedOps)
	}

	t.Logf("admitted=%d rejected=%d (every persisted row had Start invoked under the gate)", admitted, rejected)
}

// TestServiceCloseRespectsParentDeadline pins the bounded-shutdown
// contract: when a planner ignores ctx, Close still returns within the
// supplied deadline rather than hanging the process. The test releases
// the planner via t.Cleanup so the runner goroutine drains before the
// test exits — keeping the suite leak-free.
func TestServiceCloseRespectsParentDeadline(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(false)
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(planner.release) })
	})

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	_ = startBlockingReplyForCloseTest(t, service, store, planner)

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := service.Close(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected Close to time out with DeadlineExceeded, got %v", err)
	}

	// Drain the goroutine cleanly: release the planner, then wait for
	// runner.Wait without a deadline so the test exits with no leaked
	// goroutines (-race would otherwise flag it).
	releaseOnce.Do(func() { close(planner.release) })
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := service.runner.Wait(drainCtx); err != nil {
		t.Fatalf("post-deadline runner drain failed: %v", err)
	}
}

// TestServiceForceCloseBreaksInFlightClose pins the hard-stop contract:
// when a graceful Close is already blocked on an uncooperative runner,
// ForceClose aborts the wait immediately instead of sitting on the
// caller's graceful deadline.
func TestServiceForceCloseBreaksInFlightClose(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	planner := newBlockingPlanner(false)
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(planner.release) })
	})

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	_ = startBlockingReplyForCloseTest(t, service, store, planner)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- service.Close(closeCtx)
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before ForceClose: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	forceAt := time.Now()
	service.ForceClose()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("expected ForceClose to break Close without error, got %v", err)
		}
		if elapsed := time.Since(forceAt); elapsed > 250*time.Millisecond {
			t.Fatalf("expected Close to return promptly after ForceClose, elapsed=%v", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not return promptly after ForceClose")
	}

	releaseOnce.Do(func() { close(planner.release) })
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := service.runner.Wait(drainCtx); err != nil {
		t.Fatalf("post-force runner drain failed: %v", err)
	}
}

// TestServiceCloseRejectsLaterStart pins the no-zombies-after-Close
// contract: once Service.Close has cancelled the runner ctx, any
// CreateOperation racing in must be a no-op so it cannot leak a
// goroutine past the drain. The runner enforces this under its own
// mutex by checking ctx.Err() before incrementing the WaitGroup.
func TestServiceCloseRejectsLaterStart(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())

	session, err := service.CreateSession(context.Background(), "Reject after close", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	awaitOperationTerminal(t, store, latestSessionOperation(t, store, session.PlanningSessionID).OperationID)

	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("close service: %v", err)
	}

	// After Close, the runner's service-owned ctx is cancelled. A late
	// Start must short-circuit — no goroutine launched, no entry in the
	// running map, no WaitGroup increment to leak.
	service.runner.Start("po_after_close")

	service.runner.mu.Lock()
	defer service.runner.mu.Unlock()
	if len(service.runner.running) != 0 {
		t.Fatalf("expected runner.running to stay empty after Close-then-Start, got %v", service.runner.running)
	}
}

func refString(value string) *string {
	return &value
}
