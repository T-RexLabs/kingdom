package planning

import (
	"context"
	"fmt"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// Agent defines the interface for planner agents that converse with users to explore, research, and shape plans.
type Agent interface {
	ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error)
	FormalizePlanStream(ctx context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error)
	GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error)
}

type ProviderActivity struct {
	Kind           string
	ProviderPartID string
	OccurredAt     string
	ToolName       string
	ToolStatus     string
	ToolTitle      string
	ToolInput      string
	Label          string
	Summary        string
	RawText        string
}

type ProviderInvocationContext struct {
	AttemptID                string
	LogicalSessionID         string
	ProviderSessionID        string
	InvocationID             string
	OnProviderSession        func(sessionID string)
	OnProviderSessionOpened  func(sessionID string)
	OnActivity               func(activity ProviderActivity)
	OnProcessGuardOwnership  func(context.Context, opencode.ProcessGuardOwnership) error
	OnProcessGuardQuiescence func(context.Context, opencode.ProcessGuardQuiescence) error
}

type AgentContext struct {
	SessionID           string
	Title               string
	ProjectRoot         string
	ContextSummary      string
	ExternalContext     string
	ExternalSnapshotIDs []string
	Messages            []state.PlanningMessage
	ModelSelection      PlannerModelSelection
	ForceNewSession     bool
	ProviderInvocation  *ProviderInvocationContext
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
	if metadata.FinalText == "" {
		return fmt.Errorf("planner provider execution missing settled final text")
	}
	return nil
}

type AgentTurn struct {
	ContentMarkdown string
	Question        *StructuredQuestion
	Metadata        ProviderExecutionMetadata
}

type FormalizedPlan struct {
	DecreeTitle string
	DecreeText  string
	Metadata    ProviderExecutionMetadata
}

type GeneratedTitle struct {
	Title    string
	Metadata ProviderExecutionMetadata
}
