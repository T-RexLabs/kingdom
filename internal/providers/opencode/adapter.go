// Package opencode provides the OpenCode execution adapter surface.
//
// It defines the request and stream contract the runtime uses to dispatch work,
// includes the session-backed adapter that speaks to the OpenCode binary, and
// keeps a small in-memory implementation for tests that need provider behavior
// without an external process.
package opencode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"kingdom/internal/core/verdict"
)

const (
	AttemptEventTypeStarted   = "attempt.started"
	AttemptEventTypeProgress  = "attempt.progress"
	AttemptEventTypeSucceeded = "attempt.succeeded"
	AttemptEventTypeFailed    = "attempt.failed"
	AttemptEventTypeCancelled = "attempt.cancelled"
)

type Adapter interface {
	Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error)
	Cancel(ctx context.Context, req CancelRequest) error
	Stream(ctx context.Context, req StreamRequest) (<-chan AttemptStreamEvent, <-chan error, error)
}

const CompletionProtocolVersion = "v1"

type ExportCheckPhase string

const (
	ExportCheckPhaseForegroundReconcile ExportCheckPhase = "foreground_reconcile"
	ExportCheckPhaseDeferredVerify      ExportCheckPhase = "deferred_verify"
	ExportCheckPhaseLateRepair          ExportCheckPhase = "late_repair"
)

type ExportCheckStatus string

const (
	ExportCheckStatusResolvedSuccess  ExportCheckStatus = "resolved_success"
	ExportCheckStatusResolvedFailure  ExportCheckStatus = "resolved_failure"
	ExportCheckStatusUnresolved       ExportCheckStatus = "unresolved"
	ExportCheckStatusMalformedExport  ExportCheckStatus = "malformed_export"
	ExportCheckStatusExporterError    ExportCheckStatus = "exporter_error"
	ExportCheckStatusDeadlineExceeded ExportCheckStatus = "deadline_exceeded"
	ExportCheckStatusCancelled        ExportCheckStatus = "cancelled"
)

type ExportCheck struct {
	ProviderSessionID          string            `json:"-"`
	ProviderInvocationID       string            `json:"-"`
	Phase                      ExportCheckPhase  `json:"-"`
	ExportMode                 string            `json:"-"`
	Status                     ExportCheckStatus `json:"-"`
	SalvageUsed                bool              `json:"-"`
	AssistantFound             bool              `json:"-"`
	AssistantOwnedByInvocation bool              `json:"-"`
	AssistantCompletedAt       string            `json:"-"`
	AssistantFinishReason      string            `json:"-"`
	AssistantHasError          bool              `json:"-"`
	StdoutBytes                int               `json:"-"`
	StderrBytes                int               `json:"-"`
	DiagnosticText             string            `json:"-"`
	StdoutCapture              string            `json:"-"`
	StderrCapture              string            `json:"-"`
	StartedAt                  string            `json:"-"`
	FinishedAt                 string            `json:"-"`
	DurationMillis             int64             `json:"-"`
}

type ExecuteRequest struct {
	SessionID                 string `json:"session_id"`
	ProviderSessionID         string `json:"provider_session_id,omitempty"`
	AttemptID                 string `json:"attempt_id"`
	RunID                     string `json:"run_id"`
	NodeRunID                 string `json:"node_run_id"`
	Model                     string `json:"model,omitempty"`
	Variant                   string `json:"variant,omitempty"`
	Agent                     string `json:"agent,omitempty"`
	ForceNewSession           bool   `json:"force_new_session,omitempty"`
	WorkspacePath             string `json:"workspace_path"`
	ConfigDir                 string `json:"config_dir,omitempty"`
	DatabasePath              string `json:"database_path,omitempty"`
	Prompt                    string `json:"prompt"`
	OccurredAt                string `json:"occurred_at"`
	InvocationID              string `json:"invocation_id,omitempty"`
	InvocationIssuedAt        string `json:"invocation_issued_at,omitempty"`
	CompletionProtocolVersion string `json:"completion_protocol_version,omitempty"`
	CompletionCapability      string `json:"completion_capability,omitempty"`
	CompletionReceiptDir      string `json:"completion_receipt_dir,omitempty"`

	// OnProgress is called with each text chunk as the LLM streams output.
	// Optional — if nil, progress events are still stored internally but not forwarded.
	OnProgress func(text string) `json:"-"`

	// OnProviderSession is called when OpenCode emits a provider-native session id.
	// It may be called more than once if the provider session rotates during retries.
	OnProviderSession func(sessionID string) `json:"-"`

	// OnProcessStarted is called after the provider command successfully starts
	// but before stream decoding begins.
	OnProcessStarted func() `json:"-"`

	// OnLifecycle is called for structured provider execution lifecycle observations
	// that are not plain text progress, such as export-based reconciliation phases.
	OnLifecycle func(observation ExecuteLifecycleObservation) `json:"-"`

	// OnActivity is called for real provider-emitted activity such as tool use
	// or reasoning observations. Callers must not fabricate additional detail.
	OnActivity func(observation ExecuteActivityObservation) `json:"-"`

	// OnCompletionReceipt persists a runtime-owned settlement receipt before the
	// adapter returns success for runtime-backed execution.
	OnCompletionReceipt func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) `json:"-"`
}

type ExecuteLifecyclePhase string

const (
	ExecuteLifecyclePhaseProviderExportReconciliationStarted    ExecuteLifecyclePhase = "provider_export_reconciliation_started"
	ExecuteLifecyclePhaseProviderExportReconciliationSucceeded  ExecuteLifecyclePhase = "provider_export_reconciliation_succeeded"
	ExecuteLifecyclePhaseProviderExportReconciliationFailed     ExecuteLifecyclePhase = "provider_export_reconciliation_failed"
	ExecuteLifecyclePhaseProviderExportReconciliationUnresolved ExecuteLifecyclePhase = "provider_export_reconciliation_unresolved"
)

type ExecuteLifecycleObservation struct {
	Phase              ExecuteLifecyclePhase `json:"phase"`
	OccurredAt         string                `json:"occurred_at,omitempty"`
	ProviderSessionID  string                `json:"provider_session_id,omitempty"`
	AmbiguityReason    string                `json:"ambiguity_reason,omitempty"`
	ResolvedState      string                `json:"resolved_state,omitempty"`
	ResolvedReasonCode string                `json:"resolved_reason_code,omitempty"`
	EvidenceSource     string                `json:"evidence_source,omitempty"`
	Error              string                `json:"error,omitempty"`
}

type ExecuteActivityKind string

const (
	ExecuteActivityKindTool      ExecuteActivityKind = "tool"
	ExecuteActivityKindReasoning ExecuteActivityKind = "reasoning"
	ExecuteActivityKindProgress  ExecuteActivityKind = "progress"
)

type ExecuteActivityObservation struct {
	Kind       ExecuteActivityKind `json:"kind"`
	OccurredAt string              `json:"occurred_at,omitempty"`
	ToolName   string              `json:"tool_name,omitempty"`
	ToolStatus string              `json:"tool_status,omitempty"`
	ToolTitle  string              `json:"tool_title,omitempty"`
	ToolInput  string              `json:"tool_input,omitempty"`
	Label      string              `json:"label,omitempty"`
	Summary    string              `json:"summary,omitempty"`
	RawText    string              `json:"raw_text,omitempty"`
}

type CompletionReceipt struct {
	ProtocolVersion       string `json:"protocol_version"`
	AttemptID             string `json:"attempt_id"`
	InvocationID          string `json:"invocation_id"`
	CompletionCapability  string `json:"completion_capability"`
	FinalText             string `json:"final_text"`
	StructuredPayloadJSON string `json:"structured_payload_json,omitempty"`
}

type CompletionReceiptAcceptance struct {
	EvidenceID            string `json:"evidence_id,omitempty"`
	FinalText             string `json:"final_text,omitempty"`
	StructuredPayloadJSON string `json:"structured_payload_json,omitempty"`
	Idempotent            bool   `json:"idempotent,omitempty"`
	Rejected              bool   `json:"rejected,omitempty"`
	InvalidatesInvocation bool   `json:"invalidates_invocation,omitempty"`
	RejectedReason        string `json:"rejected_reason,omitempty"`
}

type mockExecutionArtifact struct {
	Path string `json:"path"`
	Name string `json:"name"`
	MIME string `json:"mime"`
	Body string `json:"-"`
}

type TokenUsage struct {
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
}

// ExecutionOutcome carries the canonical provider terminal outcome.
// Callers must persist this instead of re-deriving reason codes from error text.
type ExecutionOutcome struct {
	State          string `json:"state,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
	FailureText    string `json:"failure_text,omitempty"`
	EvidenceSource string `json:"evidence_source,omitempty"`
}

type ExecuteResponse struct {
	Accepted                        bool             `json:"accepted"`
	OutputText                      string           `json:"output_text,omitempty"`
	Tokens                          TokenUsage       `json:"tokens"`
	ProviderSessionID               string           `json:"provider_session_id,omitempty"`
	SuppressEmptyOutputRetry        bool             `json:"suppress_empty_output_retry,omitempty"`
	ProviderInvocationID            string           `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt            string           `json:"provider_invocation_at,omitempty"`
	Outcome                         ExecutionOutcome `json:"outcome,omitempty"`
	CompletionReceiptAccepted       bool             `json:"completion_receipt_accepted,omitempty"`
	CompletionReceiptEvidenceID     string           `json:"completion_receipt_evidence_id,omitempty"`
	CompletionStructuredPayloadJSON string           `json:"completion_structured_payload_json,omitempty"`
	ProviderDiagnosticsJSON         string           `json:"provider_diagnostics_json,omitempty"`
	ExportChecks                    []ExportCheck    `json:"-"`
}

type CancelRequest struct {
	SessionID  string `json:"session_id"`
	AttemptID  string `json:"attempt_id"`
	Reason     string `json:"reason"`
	OccurredAt string `json:"occurred_at"`
}

type StreamRequest struct {
	SessionID string
	AttemptID string
}

type AttemptStreamEvent struct {
	AttemptID   string `json:"attempt_id"`
	Seq         int    `json:"seq"`
	EventType   string `json:"event_type"`
	PayloadJSON string `json:"payload_json"`
	OccurredAt  string `json:"occurred_at"`
}

type AttemptNotFoundError struct {
	AttemptID string
}

func (e *AttemptNotFoundError) Error() string {
	return fmt.Sprintf("attempt not found: %q", e.AttemptID)
}

type MemoryAdapter struct {
	mu     sync.Mutex
	events map[string][]AttemptStreamEvent
}

func NewMemoryAdapter() *MemoryAdapter {
	return &MemoryAdapter{events: make(map[string][]AttemptStreamEvent)}
}

func (a *MemoryAdapter) Execute(_ context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	if err := validateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	if strings.TrimSpace(req.WorkspacePath) == "" {
		return ExecuteResponse{}, fmt.Errorf("workspace path is required")
	}
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.events[req.AttemptID]; !exists {
		a.events[req.AttemptID] = []AttemptStreamEvent{
			{
				AttemptID:   req.AttemptID,
				Seq:         1,
				EventType:   AttemptEventTypeStarted,
				PayloadJSON: `{}`,
				OccurredAt:  req.OccurredAt,
			},
		}
	}

	summary, err := buildMemoryOutput(req)
	if err != nil {
		return ExecuteResponse{}, err
	}
	accepted, err := acceptSyntheticCompletionReceipt(req, summary)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if req.OnCompletionReceipt != nil {
		disposition, err := completionReceiptDispositionForHandlerResult(accepted, nil)
		if err != nil {
			return ExecuteResponse{}, err
		}
		if disposition == completionReceiptDispositionRejected {
			reason := firstNonEmpty(strings.TrimSpace(accepted.RejectedReason), "completion receipt rejected")
			return ExecuteResponse{}, fmt.Errorf("synthetic completion receipt rejected: %s", reason)
		}
	}

	return ExecuteResponse{
		Accepted:                        true,
		OutputText:                      summary,
		ProviderInvocationID:            strings.TrimSpace(req.InvocationID),
		ProviderInvocationAt:            strings.TrimSpace(req.InvocationIssuedAt),
		CompletionReceiptAccepted:       accepted.EvidenceID != "" || accepted.Idempotent,
		CompletionReceiptEvidenceID:     strings.TrimSpace(accepted.EvidenceID),
		CompletionStructuredPayloadJSON: strings.TrimSpace(accepted.StructuredPayloadJSON),
	}, nil
}

func (a *MemoryAdapter) Cancel(_ context.Context, req CancelRequest) error {
	if req.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	if req.OccurredAt == "" {
		return fmt.Errorf("occurred_at is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	events, ok := a.events[req.AttemptID]
	if !ok {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}

	nextSeq := len(events) + 1
	a.events[req.AttemptID] = append(events, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		Seq:         nextSeq,
		EventType:   AttemptEventTypeCancelled,
		PayloadJSON: `{"reason":"` + req.Reason + `"}`,
		OccurredAt:  req.OccurredAt,
	})

	return nil
}

func (a *MemoryAdapter) Stream(ctx context.Context, req StreamRequest) (<-chan AttemptStreamEvent, <-chan error, error) {
	if req.AttemptID == "" {
		return nil, nil, fmt.Errorf("attempt id is required")
	}

	a.mu.Lock()
	events, ok := a.events[req.AttemptID]
	if !ok {
		a.mu.Unlock()
		return nil, nil, &AttemptNotFoundError{AttemptID: req.AttemptID}
	}

	snapshot := make([]AttemptStreamEvent, len(events))
	copy(snapshot, events)
	a.mu.Unlock()

	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Seq < snapshot[j].Seq
	})

	eventCh := make(chan AttemptStreamEvent)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)

		for _, event := range snapshot {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case eventCh <- event:
			}
		}
	}()

	return eventCh, errCh, nil
}

func (a *MemoryAdapter) PublishEvent(req AttemptStreamEvent) error {
	if req.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	if req.Seq <= 0 {
		return fmt.Errorf("seq must be positive")
	}
	if req.EventType == "" {
		return fmt.Errorf("event type is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	events, ok := a.events[req.AttemptID]
	if !ok {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}

	a.events[req.AttemptID] = append(events, req)
	return nil
}

func validateExecuteRequest(req ExecuteRequest) error {
	if req.SessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if req.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	if req.RunID == "" {
		return fmt.Errorf("run id is required")
	}
	if req.NodeRunID == "" {
		return fmt.Errorf("node run id is required")
	}
	if req.Prompt == "" {
		return fmt.Errorf("prompt is required")
	}
	if req.OccurredAt == "" {
		return fmt.Errorf("occurred_at is required")
	}

	return nil
}

func acceptSyntheticCompletionReceipt(req ExecuteRequest, finalText string) (CompletionReceiptAcceptance, error) {
	if req.OnCompletionReceipt == nil {
		return CompletionReceiptAcceptance{}, nil
	}
	if strings.TrimSpace(req.InvocationID) == "" || strings.TrimSpace(req.CompletionCapability) == "" {
		return CompletionReceiptAcceptance{}, fmt.Errorf("invocation id and completion capability are required when completion receipt handling is enabled")
	}
	accepted, err := req.OnCompletionReceipt(CompletionReceipt{
		ProtocolVersion:       firstNonEmpty(strings.TrimSpace(req.CompletionProtocolVersion), CompletionProtocolVersion),
		AttemptID:             strings.TrimSpace(req.AttemptID),
		InvocationID:          strings.TrimSpace(req.InvocationID),
		CompletionCapability:  strings.TrimSpace(req.CompletionCapability),
		FinalText:             strings.TrimSpace(finalText),
		StructuredPayloadJSON: structuredPayloadJSONForOutputText(finalText),
	})
	if err != nil {
		return CompletionReceiptAcceptance{}, err
	}
	if _, err := completionReceiptDispositionForHandlerResult(accepted, nil); err != nil {
		return CompletionReceiptAcceptance{}, err
	}
	if accepted.Rejected {
		return accepted, nil
	}
	if strings.TrimSpace(accepted.FinalText) == "" {
		accepted.FinalText = strings.TrimSpace(finalText)
	}
	if strings.TrimSpace(accepted.StructuredPayloadJSON) == "" {
		accepted.StructuredPayloadJSON = structuredPayloadJSONForOutputText(finalText)
	}
	return accepted, nil
}

func structuredPayloadJSONForOutputText(outputText string) string {
	verdict := extractReceiptVerdict(outputText)
	if verdict == "" {
		return "{}"
	}
	return fmt.Sprintf(`{"verdict":%q}`, verdict)
}

func extractReceiptVerdict(outputText string) string {
	return verdict.ExtractNearTop(outputText)
}

func buildMemoryOutput(req ExecuteRequest) (string, error) {
	prompt := strings.ToLower(req.Prompt)
	implementationAuthorized := strings.Contains(prompt, "roleid: builder-berserker")

	summary := fmt.Sprintf("mock engine completed %s", req.NodeRunID)
	if !implementationAuthorized {
		if strings.Contains(prompt, "nodeclass: execution_review") {
			return "VERDICT: PASS\nExecution review completed with no blocking gaps.", nil
		}
		if strings.Contains(prompt, "nodeclass: validation_gates") {
			return "VERDICT: PASS\nMock validation completed with no blockers.", nil
		}
		if strings.Contains(prompt, "nodeclass: royal_closure") {
			return "ROYAL_VERDICT: APPROVE\nRoyal closure approved based on provided evidence.", nil
		}
		return summary, nil
	}

	artifactPath := fmt.Sprintf("deliverables/%s.txt", req.NodeRunID)
	if implementationAuthorized && (strings.Contains(prompt, "homepage") || strings.Contains(prompt, "index.html")) {
		artifactPath = "deliverables/index.html"
		summary = "mock engine generated homepage"
		artifacts := []mockExecutionArtifact{{
			Path: artifactPath,
			Name: "index.html",
			MIME: "text/html",
			Body: "<!doctype html><html><body><h1>Hello world</h1><p>Generated by mock engine.</p></body></html>",
		}}
		return summary, writeMockArtifacts(req.WorkspacePath, artifacts)
	}

	artifacts := []mockExecutionArtifact{{
		Path: artifactPath,
		Name: filepath.Base(artifactPath),
		MIME: inferMockMIME(artifactPath),
		Body: fmt.Sprintf("completed %s", req.NodeRunID),
	}}
	return summary, writeMockArtifacts(req.WorkspacePath, artifacts)
}

func inferMockMIME(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".md":
		return "text/markdown"
	case ".html":
		return "text/html"
	default:
		return "text/plain"
	}
}

func writeMockArtifacts(workspacePath string, artifacts []mockExecutionArtifact) error {
	for _, artifact := range artifacts {
		artifactPath := filepath.Join(workspacePath, artifact.Path)
		if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
			return fmt.Errorf("create mock artifact directory: %w", err)
		}
		if err := os.WriteFile(artifactPath, []byte(artifact.Body), 0o600); err != nil {
			return fmt.Errorf("write mock artifact: %w", err)
		}
	}
	return nil
}
