// Package opencode provides the OpenCode execution adapter surface.
//
// It defines the request and stream contract the runtime uses to dispatch work,
// includes the session-backed adapter that speaks to the OpenCode binary, and
// keeps a small in-memory implementation for tests that need provider behavior
// without an external process.
package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/processguardian"
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

type ExecuteRequest struct {
	CompletionMode            CompletionMode `json:"completion_mode"`
	SessionID                 string         `json:"session_id"`
	ProviderSessionID         string         `json:"provider_session_id,omitempty"`
	AttemptID                 string         `json:"attempt_id"`
	RunID                     string         `json:"run_id"`
	NodeRunID                 string         `json:"node_run_id"`
	Model                     string         `json:"model,omitempty"`
	Variant                   string         `json:"variant,omitempty"`
	Agent                     string         `json:"agent,omitempty"`
	ForceNewSession           bool           `json:"force_new_session,omitempty"`
	WorkspacePath             string         `json:"workspace_path"`
	ConfigDir                 string         `json:"config_dir,omitempty"`
	DatabasePath              string         `json:"database_path,omitempty"`
	InlineConfigContent       string         `json:"-"`
	Prompt                    string         `json:"prompt"`
	OccurredAt                string         `json:"occurred_at"`
	InvocationID              string         `json:"invocation_id,omitempty"`
	InvocationIssuedAt        string         `json:"invocation_issued_at,omitempty"`
	CompletionProtocolVersion string         `json:"completion_protocol_version,omitempty"`
	CompletionCapability      string         `json:"completion_capability,omitempty"`
	CompletionReceiptDir      string         `json:"completion_receipt_dir,omitempty"`

	// OnProgress is called with each text chunk as the LLM streams output.
	// Optional — if nil, progress events are still stored internally but not forwarded.
	OnProgress func(text string) `json:"-"`

	// OnProviderSession is called when OpenCode emits a provider-native session id.
	// It may be called more than once if the provider session rotates during retries.
	OnProviderSession func(sessionID string) `json:"-"`

	// OnProcessStarted is called after the provider command successfully starts
	// but before stream decoding begins.
	OnProcessStarted func() `json:"-"`

	// OnProcessGuardOwnership durably records guardian ownership before the
	// provider launch gate is released. The raw quiescence capability is limited
	// to this orchestration callback and is never included in diagnostics.
	OnProcessGuardOwnership func(context.Context, ProcessGuardOwnership) error `json:"-"`

	// OnProcessGuardQuiescence records the guardian's terminal proof after the
	// command tree is quiescent and its streams have been drained.
	OnProcessGuardQuiescence func(context.Context, ProcessGuardQuiescence) error `json:"-"`

	// OnActivity is called for real provider-emitted activity such as tool use
	// or reasoning observations. Callers must not fabricate additional detail.
	OnActivity func(observation ExecuteActivityObservation) `json:"-"`
}

// GuardianRunner starts provider command trees behind a durable launch gate.
// It is intentionally narrow so adapter tests can supply deterministic handles.
type GuardianRunner interface {
	Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error)
}

type ProcessGuardOwnership struct {
	CommandOrdinal int
	StartedAt      string
	AcknowledgedAt string
	Ownership      processguardian.Ownership
}

type ProcessGuardQuiescence struct {
	CommandOrdinal int
	ExitedAt       string
	QuiescentAt    string
	Result         processguardian.Result
}

// CompletionMode defines the single, server-owned authority a provider
// invocation uses to settle an attempt.
type CompletionMode string

const (
	CompletionModeServerResponse  CompletionMode = "server_response"
	CompletionModeReceiptRequired CompletionMode = "receipt_required"
)

type ExecuteActivityKind string

const (
	ExecuteActivityKindTool      ExecuteActivityKind = "tool"
	ExecuteActivityKindReasoning ExecuteActivityKind = "reasoning"
	ExecuteActivityKindProgress  ExecuteActivityKind = "progress"
)

type ExecuteActivityObservation struct {
	Kind           ExecuteActivityKind `json:"kind"`
	ProviderPartID string              `json:"provider_part_id,omitempty"`
	OccurredAt     string              `json:"occurred_at,omitempty"`
	ToolName       string              `json:"tool_name,omitempty"`
	ToolStatus     string              `json:"tool_status,omitempty"`
	ToolTitle      string              `json:"tool_title,omitempty"`
	ToolInput      string              `json:"tool_input,omitempty"`
	Label          string              `json:"label,omitempty"`
	Summary        string              `json:"summary,omitempty"`
	RawText        string              `json:"raw_text,omitempty"`
}

type mockExecutionArtifact struct {
	Path string `json:"path"`
	Name string `json:"name"`
	MIME string `json:"mime"`
	Body string `json:"-"`
}

type TokenUsage = providerexecution.TokenUsage

// InvocationStreamObservation is the complete live-stream evidence for one
// provider invocation. It is diagnostic evidence only until the execution
// store validates its lineage and settles it transactionally.
type InvocationStreamObservation = providerexecution.InvocationStreamObservation

type InvocationToolObservation = providerexecution.InvocationToolObservation

type ProcessGuardObservation = providerexecution.ProcessGuardObservation

// ExecutionOutcome carries the canonical provider terminal outcome.
// Callers must persist this instead of re-deriving reason codes from error text.
type ExecutionOutcome struct {
	State          string `json:"state,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
	FailureText    string `json:"failure_text,omitempty"`
	EvidenceSource string `json:"evidence_source,omitempty"`
}

type ExecuteResponse struct {
	Accepted                bool                        `json:"accepted"`
	OutputText              string                      `json:"output_text,omitempty"`
	Tokens                  TokenUsage                  `json:"tokens"`
	ProviderSessionID       string                      `json:"provider_session_id,omitempty"`
	ProviderInvocationID    string                      `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt    string                      `json:"provider_invocation_at,omitempty"`
	Outcome                 ExecutionOutcome            `json:"outcome,omitempty"`
	ProviderDiagnosticsJSON string                      `json:"provider_diagnostics_json,omitempty"`
	StreamObservation       InvocationStreamObservation `json:"stream_observation"`
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
	if err := invokeSyntheticGuardCallbacks(req); err != nil {
		return ExecuteResponse{}, err
	}
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}
	providerSessionID := "memory_" + strings.TrimSpace(req.InvocationID)
	if req.OnProviderSession != nil {
		req.OnProviderSession(providerSessionID)
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

	zero := 0
	return ExecuteResponse{
		Accepted:             true,
		OutputText:           summary,
		ProviderSessionID:    providerSessionID,
		ProviderInvocationID: strings.TrimSpace(req.InvocationID),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
		Outcome: ExecutionOutcome{
			State:          string(attemptCompletionStateSuccess),
			ReasonCode:     "execution_completed",
			EvidenceSource: string(completionEvidenceSourceStream),
		},
		StreamObservation: InvocationStreamObservation{
			ProviderSessionID:      providerSessionID,
			FinalTurnID:            "turn_" + firstNonEmpty(strings.TrimSpace(req.InvocationID), strings.TrimSpace(req.AttemptID)),
			FinalStepID:            "step_" + firstNonEmpty(strings.TrimSpace(req.InvocationID), strings.TrimSpace(req.AttemptID)),
			FinalSequence:          2,
			RawFinishReason:        "stop",
			NormalizedFinishReason: "stop",
			CanonicalTerminalText:  summary,
			ProcessExited:          true,
			ProcessExitCode:        &zero,
			StdoutDrained:          true,
			StderrDrained:          true,
			ProcessQuiescent:       true,
			ProcessGuards:          []ProcessGuardObservation{syntheticProcessGuardObservation(req)},
		},
	}, nil
}

// invokeSyntheticGuardCallbacks keeps the in-memory test double compatible
// with the durable guard contract without pretending that it launches a host
// provider process. Production OpenCode commands always use SessionAdapter.
func invokeSyntheticGuardCallbacks(req ExecuteRequest) error {
	guardID := "memory_guard_" + firstNonEmpty(strings.TrimSpace(req.InvocationID), strings.TrimSpace(req.AttemptID))
	ownership := processguardian.Ownership{GuardID: guardID, GuardianPID: 1, GuardianIdentityToken: "memory-guardian", ChildPID: 1, ChildIdentityToken: "memory-child", TreeID: 1, TreeIdentityToken: "memory-child", Platform: "unix_process_group", QuiescenceCapability: "memory-guardian-capability"}
	if req.OnProcessGuardOwnership != nil {
		if err := req.OnProcessGuardOwnership(context.Background(), ProcessGuardOwnership{CommandOrdinal: 1, StartedAt: req.OccurredAt, AcknowledgedAt: req.OccurredAt, Ownership: ownership}); err != nil {
			return err
		}
	}
	result := processguardian.Result{Ownership: ownership, ExitCode: 0, Quiescent: true}
	evidence, err := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: result})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(evidence)
	result.EvidenceJSON = string(evidence)
	result.EvidenceSHA256 = fmt.Sprintf("%x", sum[:])
	if req.OnProcessGuardQuiescence != nil {
		if err := req.OnProcessGuardQuiescence(context.Background(), ProcessGuardQuiescence{CommandOrdinal: 1, ExitedAt: req.OccurredAt, QuiescentAt: req.OccurredAt, Result: result}); err != nil {
			return err
		}
	}
	return nil
}

func syntheticProcessGuardObservation(req ExecuteRequest) ProcessGuardObservation {
	return ProcessGuardObservation{CommandOrdinal: 1, GuardID: "memory_guard_" + firstNonEmpty(strings.TrimSpace(req.InvocationID), strings.TrimSpace(req.AttemptID)), GuardianPID: 1, ChildPID: 1, TreeID: 1, Platform: "unix_process_group", Quiescent: true}
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
	switch req.CompletionMode {
	case CompletionModeServerResponse:
		if strings.TrimSpace(req.CompletionProtocolVersion) != "" {
			return fmt.Errorf("completion protocol version is only valid for receipt_required completion mode")
		}
		if strings.TrimSpace(req.CompletionCapability) != "" {
			return fmt.Errorf("completion capability is only valid for receipt_required completion mode")
		}
		if strings.TrimSpace(req.CompletionReceiptDir) != "" {
			return fmt.Errorf("completion receipt dir is only valid for receipt_required completion mode")
		}
	case CompletionModeReceiptRequired:
		if strings.TrimSpace(req.InvocationID) == "" {
			return fmt.Errorf("invocation id is required for receipt_required completion mode")
		}
		if strings.TrimSpace(req.CompletionCapability) == "" {
			return fmt.Errorf("completion capability is required for receipt_required completion mode")
		}
		if strings.TrimSpace(req.CompletionReceiptDir) == "" {
			return fmt.Errorf("completion receipt dir is required for receipt_required completion mode")
		}
	default:
		return fmt.Errorf("completion mode must be %q or %q", CompletionModeServerResponse, CompletionModeReceiptRequired)
	}
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
