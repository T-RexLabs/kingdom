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
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// Provider runs requests on a shared model server. Prepare fixes a request's
// provider identity without sending work; the caller binds that identity to its
// canonical invocation, and only then Execute sends the work. Cancel interrupts
// one attempt's request, and Discard releases a prepared request that will not
// run.
type Provider interface {
	Prepare(ctx context.Context, req ExecuteRequest) (Prepared, error)
	Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error)
	Cancel(ctx context.Context, req CancelRequest) error
	Discard(ctx context.Context, invocationID string) error
}

type ExecuteRequest struct {
	CompletionMode      CompletionMode `json:"completion_mode"`
	SessionID           string         `json:"session_id"`
	ProviderSessionID   string         `json:"provider_session_id,omitempty"`
	AttemptID           string         `json:"attempt_id"`
	RunID               string         `json:"run_id"`
	NodeRunID           string         `json:"node_run_id"`
	Model               string         `json:"model,omitempty"`
	Variant             string         `json:"variant,omitempty"`
	Agent               string         `json:"agent,omitempty"`
	ForceNewSession     bool           `json:"force_new_session,omitempty"`
	WorkspacePath       string         `json:"workspace_path"`
	ProjectRootBoundary string         `json:"-"`
	ConfigDir           string         `json:"config_dir,omitempty"`
	DatabasePath        string         `json:"database_path,omitempty"`
	InlineConfigContent string         `json:"-"`
	// System is the exact instruction pinned into the request's managed
	// profile. It is the only instruction channel Kingdom authors.
	System string `json:"-"`
	// KeepSession retains the provider session's captured history after the
	// request, because a later request will continue it.
	KeepSession bool `json:"-"`
	// FirstEventTimeout and StreamInactivityTimeout bound how long a request
	// may stay silent before it is interrupted.
	FirstEventTimeout       time.Duration `json:"-"`
	StreamInactivityTimeout time.Duration `json:"-"`
	Prompt                  string        `json:"prompt"`
	// OpenPrompt and WritePrompt are replayable sources: each Execute opens or
	// invokes its source once, closes opened readers, and streams to stdin. Set
	// exactly one of Prompt, OpenPrompt, or WritePrompt. Writers must honor ctx.
	OpenPrompt  func(context.Context) (io.ReadCloser, error) `json:"-"`
	WritePrompt func(context.Context, io.Writer) error       `json:"-"`
	// OpenContinuationPrompt supplies only the new input for a request that
	// continues a reused provider session, whose history already holds the rest.
	OpenContinuationPrompt func(context.Context) (io.ReadCloser, error) `json:"-"`
	OccurredAt             string                                       `json:"occurred_at"`
	InvocationID           string                                       `json:"invocation_id,omitempty"`
	InvocationIssuedAt     string                                       `json:"invocation_issued_at,omitempty"`

	// OnProgress is called with each text chunk as the LLM streams output.
	// Optional — if nil, progress events are still stored internally but not forwarded.
	OnProgress func(text string) `json:"-"`

	// OnProviderSession is called when OpenCode emits a provider-native session id.
	// It may be called more than once if the provider session rotates during retries.
	OnProviderSession func(sessionID string) `json:"-"`

	// OnProcessStarted is called after the provider command successfully starts
	// but before stream decoding begins.
	OnProcessStarted func() `json:"-"`

	// OnInvocationStarted is called once the provider has durably accepted the
	// request, which is when the shared server begins working on it.
	OnInvocationStarted func() `json:"-"`

	// OnProcessGuardOwnership durably records guardian ownership before the
	// provider launch gate is released. The raw quiescence capability is limited
	// to this orchestration callback and is never included in diagnostics.
	// Both guardian callbacks are set together, and only for requests whose
	// profile may run commands; a request without them refuses every command.
	OnProcessGuardOwnership func(context.Context, ProcessGuardOwnership) error `json:"-"`

	// OnProcessGuardQuiescence records the guardian's terminal proof after the
	// command tree is quiescent and its streams have been drained.
	OnProcessGuardQuiescence func(context.Context, ProcessGuardQuiescence) error `json:"-"`

	// OnActivity is called for real provider-emitted activity such as tool use
	// or reasoning observations. Callers must not fabricate additional detail.
	// Content sources are valid only through this synchronous callback; persist
	// their bytes before returning rather than retaining the process-local opener.
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
	CompletionModeServerResponse CompletionMode = "server_response"
)

type ExecuteActivityKind = string

const (
	ExecuteActivityKindMessage   = "message"
	ExecuteActivityKindTool      = "tool"
	ExecuteActivityKindReasoning = "reasoning"
	ExecuteActivityKindProgress  = "progress"
)

type ExecuteActivityObservation = providerexecution.ActivityObservation

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

// ExecutionOutcome carries the canonical provider terminal outcome.
// Callers must persist this instead of re-deriving reason codes from error text.
type ExecutionOutcome struct {
	State          string `json:"state,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
	FailureText    string `json:"failure_text,omitempty"`
	EvidenceSource string `json:"evidence_source,omitempty"`
}

type ExecuteResponse struct {
	Accepted   bool   `json:"accepted"`
	OutputText string `json:"output_text,omitempty"`
	// Referenced answers are opened with providerexecution.OpenTerminalText.
	// OutputPreview is bounded display data, never terminal authority.
	OutputPreview           string                      `json:"output_preview,omitempty"`
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

// buildMemoryOutput is the canned answer of the in-memory fixture provider.
func buildMemoryOutput(req ExecuteRequest) (string, error) {
	prompt := strings.ToLower(req.Prompt)
	implementationAuthorized := strings.Contains(prompt, "roleid: builder")

	summary := fmt.Sprintf("mock engine completed %s", req.NodeRunID)
	if !implementationAuthorized {
		if strings.Contains(prompt, "nodeclass: execution_review") {
			return "VERDICT: PASS\nExecution review completed with no blocking gaps.", nil
		}
		if strings.Contains(prompt, "nodeclass: validation_gates") {
			return "VERDICT: PASS\nMock validation completed with no blockers.", nil
		}
		if strings.Contains(prompt, "nodeclass: completion") {
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
