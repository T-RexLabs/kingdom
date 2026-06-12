package planning

import (
	"context"
	"fmt"

	"kingdom/internal/state"
)

// Agent defines the interface for planner agents that converse with users to explore, research, and shape plans.
type Agent interface {
	ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error)
	FormalizePlanStream(ctx context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error)
	GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error)
}

type CompletionReceipt struct {
	ProtocolVersion       string
	AttemptID             string
	InvocationID          string
	CompletionCapability  string
	FinalText             string
	StructuredPayloadJSON string
}

type CompletionReceiptAcceptance struct {
	EvidenceID            string
	FinalText             string
	StructuredPayloadJSON string
	Idempotent            bool
	Rejected              bool
	InvalidatesInvocation bool
	RejectedReason        string
}

type ProviderActivity struct {
	Kind       string
	OccurredAt string
	ToolName   string
	ToolStatus string
	ToolTitle  string
	ToolInput  string
	Label      string
	Summary    string
	RawText    string
}

type ProviderInvocationContext struct {
	AttemptID                 string
	LogicalSessionID          string
	ProviderSessionID         string
	InvocationID              string
	InvocationIssuedAt        string
	CompletionProtocolVersion string
	CompletionCapability      string
	CompletionReceiptDir      string
	OnProviderSession         func(sessionID string)
	OnActivity                func(activity ProviderActivity)
	OnCompletionReceipt       func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error)
}

type AgentContext struct {
	SessionID          string
	Title              string
	ProjectRoot        string
	ContextSummary     string
	Messages           []state.PlanningMessage
	ModelSelection     PlannerModelSelection
	ForceNewSession    bool
	ProviderInvocation *ProviderInvocationContext
}

type ProviderExecutionMetadata struct {
	FinalText                       string
	ProviderSessionID               string
	CompletionReceiptEvidenceID     string
	CompletionStructuredPayloadJSON string
	OutcomeState                    string
	FailureReasonCode               string
	FailureReasonText               string
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

func requireReceiptEvidenceID(metadata ProviderExecutionMetadata) error {
	if metadata.OutcomeState != "success" {
		return fmt.Errorf("planner provider execution did not settle success")
	}
	if metadata.CompletionReceiptEvidenceID == "" {
		return fmt.Errorf("planner provider execution missing completion receipt evidence id")
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
