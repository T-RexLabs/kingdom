// Package opencode runs Kingdom's provider work on one shared stock OpenCode V2
// server.
//
// Provider is the contract the runtime dispatches through: a request is
// prepared, bound to its canonical invocation, executed, and settled from the
// evidence the server returns. Service is the Provider that owns the shared V2
// server and serves every workspace through it. FixtureProvider is the
// deterministic in-memory Provider for tests that need provider behavior
// without a server.
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

const AttemptEventTypeProgress = "attempt.progress"

// Provider runs requests on a shared model server. Prepare fixes a request's
// provider identity without sending work; the caller binds that identity to its
// canonical invocation, and only then Execute sends the work. Cancel interrupts
// one attempt's request, and Discard releases a prepared request that will not
// run.
type Provider interface {
	Prepare(ctx context.Context, req ExecuteRequest) (Prepared, error)
	Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error)
	Cancel(ctx context.Context, attemptID string) error
	Discard(ctx context.Context, invocationID string) error
}

type ExecuteRequest struct {
	AttemptID    string
	InvocationID string
	// ProviderSessionID names a session to continue; ForceNewSession starts a
	// new one instead.
	ProviderSessionID string
	ForceNewSession   bool
	// ResumeFrom explicitly adopts a checkpointed interrupted run conversation.
	// It never falls back to a new session if the saved context is unavailable.
	ResumeFrom    *SessionResume
	Model         string
	Variant       string
	Agent         string
	WorkspacePath string
	// System is the exact instruction pinned into the request's managed
	// profile. It is the only instruction channel Kingdom authors.
	System string
	// KeepSession retains the provider session's captured history after the
	// request, because a later request will continue it.
	KeepSession bool
	// FirstEventTimeout and StreamInactivityTimeout bound how long a request
	// may stay silent before it is interrupted.
	FirstEventTimeout       time.Duration
	StreamInactivityTimeout time.Duration
	// OpenPrompt is the replayable prompt source: each Execute opens it once,
	// sends the text and closes the reader.
	OpenPrompt func(context.Context) (io.ReadCloser, error)
	// OpenContinuationPrompt supplies only the new input for a request that
	// continues a reused provider session, whose history already holds the rest.
	OpenContinuationPrompt func(context.Context) (io.ReadCloser, error)

	// OnProgress is called with each text chunk as the LLM streams output.
	// Optional — if nil, progress events are still stored internally but not forwarded.
	OnProgress func(text string)

	// OnInvocationStarted is called once the provider has durably accepted the
	// request, which is when the shared server begins working on it.
	OnInvocationStarted func()

	// OnCommandGuardianOwnership durably records guardian ownership before the
	// provider launch gate is released. The raw quiescence capability is limited
	// to this orchestration callback and is never included in diagnostics.
	// Both guardian callbacks are set together, and only for requests whose
	// profile may run commands; a request without them refuses every command.
	OnCommandGuardianOwnership func(context.Context, CommandGuardianOwnership) error

	// OnCommandGuardianQuiescence records the guardian's terminal proof after the
	// command tree is quiescent and its streams have been drained.
	OnCommandGuardianQuiescence func(context.Context, CommandGuardianQuiescence) error

	// OnActivity is called for real provider-emitted activity such as tool use
	// or reasoning observations. Callers must not fabricate additional detail.
	// Content sources are valid only through this synchronous callback; persist
	// their bytes before returning rather than retaining the process-local opener.
	OnActivity func(observation ExecuteActivityObservation)
}

// GuardianRunner starts provider command trees behind a durable launch gate.
// It is intentionally narrow so adapter tests can supply deterministic handles.
type GuardianRunner interface {
	Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error)
}

type CommandGuardianOwnership struct {
	CommandOrdinal int
	StartedAt      string
	AcknowledgedAt string
	Ownership      processguardian.Ownership
}

type CommandGuardianQuiescence struct {
	CommandOrdinal int
	ExitedAt       string
	QuiescentAt    string
	Result         processguardian.Result
}

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
	OutputText string `json:"output_text,omitempty"`
	// Referenced answers are opened with providerexecution.OpenTerminalText.
	// OutputPreview is bounded display data, never terminal authority.
	OutputPreview        string                      `json:"output_preview,omitempty"`
	Tokens               TokenUsage                  `json:"tokens"`
	ProviderSessionID    string                      `json:"provider_session_id,omitempty"`
	ProviderInvocationID string                      `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt string                      `json:"provider_invocation_at,omitempty"`
	Outcome              ExecutionOutcome            `json:"outcome,omitempty"`
	StreamObservation    InvocationStreamObservation `json:"stream_observation"`
}

type AttemptNotFoundError struct {
	AttemptID string
}

func (e *AttemptNotFoundError) Error() string {
	return fmt.Sprintf("attempt not found: %q", e.AttemptID)
}

// buildMemoryOutput is the canned answer of the in-memory fixture provider.
func buildMemoryOutput(req ExecuteRequest, prompt string) (string, error) {
	prompt = strings.ToLower(prompt)
	implementationAuthorized := strings.Contains(prompt, "roleid: builder")

	summary := fmt.Sprintf("mock engine completed %s", req.AttemptID)
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

	artifactPath := fmt.Sprintf("deliverables/%s.txt", req.AttemptID)
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
		Body: fmt.Sprintf("completed %s", req.AttemptID),
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
