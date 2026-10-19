package planning

import (
	"context"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/prompts"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// Agent defines the interface for planner agents that converse with users to explore, research, and shape plans.
type Agent interface {
	ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error)
	FormalizePlanStream(ctx context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error)
	GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error)
}

type ProviderActivity = providerexecution.ActivityObservation

type ProviderInvocationContext struct {
	AttemptID        string
	LogicalSessionID string
	InvocationID     string
	// ReusableProviderSessionID is the settled provider session that this
	// request may continue. Empty means the request starts a new session.
	ReusableProviderSessionID string
	// Activate binds the prepared provider request to this canonical invocation
	// before any work is dispatched, and fails if the binding cannot be made.
	Activate                func(context.Context, opencode.Prepared) error
	OnProviderSessionOpened func(sessionID string)
	OnActivity              func(activity ProviderActivity)

	activated bool
}

type AgentContext struct {
	VisitMessages        state.PlanningMessageSource
	WriteExternalContext func(context.Context, io.Writer) error
	SessionID            string
	Title                string
	ProjectRoot          string
	ContextSummary       string
	VoiceInstruction     string
	GuidanceInstruction  string
	PromptCatalog        prompts.Catalog
	ExternalContext      string
	ExternalSnapshotIDs  []string
	Messages             []state.PlanningMessage
	ModelSelection       PlannerModelSelection
	ForceNewSession      bool
	// CurrentMessageID is the user message this request answers. A continued
	// session already holds the earlier transcript, so only it is sent.
	CurrentMessageID   string
	ProviderInvocation *ProviderInvocationContext
}

type ProviderExecutionMetadata struct {
	FinalText                       string
	ProviderSessionID               string
	CompletionStructuredPayloadJSON string
	ProviderDiagnosticsJSON         string
	OutcomeState                    string
	FailureReasonCode               string
	FailureReasonText               string
	StreamObservation               opencode.InvocationStreamObservation
}

type AgentExecutionError struct {
	Metadata ProviderExecutionMetadata
	Err      error
}

func (e *AgentExecutionError) Error() string {
	if e == nil || e.Err == nil {
		return "planner execution failed"
	}
	return e.Err.Error()
}

func (e *AgentExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func executionError(metadata ProviderExecutionMetadata, err error) error {
	if err == nil {
		return nil
	}
	return &AgentExecutionError{Metadata: metadata, Err: err}
}

// requirePlannerTurnSettled gates canonical persistence on the provider's own
// structural stop signal: the adjudicated terminal outcome must be success and
// the assembled output text must be non-empty. The planner settles server-side
// from this signal.
func requirePlannerTurnSettled(metadata ProviderExecutionMetadata) error {
	if metadata.OutcomeState != "success" {
		return fmt.Errorf("planner provider execution did not settle success")
	}
	if metadata.FinalText == "" && metadata.StreamObservation.CanonicalTerminalTextRef == nil {
		return fmt.Errorf("planner provider execution missing settled final text")
	}
	if err := providerexecution.ValidateServerResponseSuccess(metadata.StreamObservation); err != nil {
		return fmt.Errorf("planner provider execution has invalid terminal stream observation: %w", err)
	}
	if metadata.StreamObservation.CanonicalTerminalTextRef == nil && strings.TrimSpace(metadata.StreamObservation.CanonicalTerminalText) != strings.TrimSpace(metadata.FinalText) {
		return fmt.Errorf("planner provider execution terminal stream output does not match settled final text")
	}
	return nil
}

// AgentTurn owns any parsed text staging until Close, after the caller has
// finalized its canonical message and terminal events.
type AgentTurn struct {
	ContentSource   *state.PlanningTextSource
	closeContent    func() error
	ContentMarkdown string
	Question        *StructuredQuestion
	Metadata        ProviderExecutionMetadata
}

// FormalizedPlan owns its parsed body source until Close after finalization.
type FormalizedPlan struct {
	TextSource     *state.PlanningTextSource
	closeContent   func() error
	WorkOrderTitle string
	WorkOrderText  string
	Metadata       ProviderExecutionMetadata
}

func (t AgentTurn) Close() error {
	if t.closeContent != nil {
		return t.closeContent()
	}
	return nil
}
func (p FormalizedPlan) Close() error {
	if p.closeContent != nil {
		return p.closeContent()
	}
	return nil
}

type GeneratedTitle struct {
	Title    string
	Metadata ProviderExecutionMetadata
}
