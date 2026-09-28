package opencode

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateExecuteRequestRequiresExplicitCompletionMode(t *testing.T) {
	validReceiptRequest := func() ExecuteRequest {
		return ExecuteRequest{
			CompletionMode:       CompletionModeReceiptRequired,
			SessionID:            "session_1",
			AttemptID:            "attempt_1",
			RunID:                "run_1",
			NodeRunID:            "node_run_1",
			Prompt:               "complete this work",
			OccurredAt:           "2026-03-01T12:00:00Z",
			InvocationID:         "invocation_1",
			CompletionCapability: "capability_1",
			CompletionReceiptDir: t.TempDir(),
		}
	}

	tests := []struct {
		name string
		req  ExecuteRequest
		want string
	}{
		{
			name: "missing mode",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.CompletionMode = ""; return req }(),
			want: "completion mode must be",
		},
		{
			name: "unknown mode",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.CompletionMode = "unknown"; return req }(),
			want: "completion mode must be",
		},
		{
			name: "server response protocol",
			req: func() ExecuteRequest {
				req := validReceiptRequest()
				req.CompletionMode = CompletionModeServerResponse
				req.CompletionProtocolVersion = CompletionProtocolVersion
				req.CompletionCapability = ""
				req.CompletionReceiptDir = ""
				return req
			}(),
			want: "completion protocol version",
		},
		{
			name: "server response capability",
			req: func() ExecuteRequest {
				req := validReceiptRequest()
				req.CompletionMode = CompletionModeServerResponse
				req.CompletionReceiptDir = ""
				return req
			}(),
			want: "completion capability",
		},
		{
			name: "server response receipt directory",
			req: func() ExecuteRequest {
				req := validReceiptRequest()
				req.CompletionMode = CompletionModeServerResponse
				req.CompletionCapability = ""
				return req
			}(),
			want: "completion receipt dir",
		},
		{
			name: "receipt required invocation",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.InvocationID = ""; return req }(),
			want: "invocation id",
		},
		{
			name: "receipt required capability",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.CompletionCapability = ""; return req }(),
			want: "completion capability",
		},
		{
			name: "receipt required receipt directory",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.CompletionReceiptDir = ""; return req }(),
			want: "completion receipt dir",
		},
		{
			name: "receipt required attempt",
			req:  func() ExecuteRequest { req := validReceiptRequest(); req.AttemptID = ""; return req }(),
			want: "attempt id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateExecuteRequest(test.req)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate execute request error = %v, want %q", err, test.want)
			}
		})
	}

	if err := validateExecuteRequest(validReceiptRequest()); err != nil {
		t.Fatalf("expected receipt mode without callback to validate: %v", err)
	}
}

func TestValidateExecuteRequestAcceptsLongPrompt(t *testing.T) {
	request := func(prompt string) ExecuteRequest {
		return ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_prompt_boundary",
			AttemptID:      "attempt_prompt_boundary",
			RunID:          "run_prompt_boundary",
			NodeRunID:      "node_prompt_boundary",
			Prompt:         prompt,
			OccurredAt:     "2026-03-01T12:00:00Z",
		}
	}
	prompt := strings.Repeat("large prompt context ", 4096)
	if err := validateExecuteRequest(request(prompt)); err != nil {
		t.Fatalf("long prompt rejected: %v", err)
	}
}

func TestMemoryAdapterReceiptModeDoesNotTouchReceiptPath(t *testing.T) {
	t.Parallel()

	receiptPath := filepath.Join(t.TempDir(), "opaque-receipt-path")
	const sentinel = "not a receipt directory"
	if err := os.WriteFile(receiptPath, []byte(sentinel), 0o600); err != nil {
		t.Fatalf("write receipt path sentinel: %v", err)
	}

	res, err := NewMemoryAdapter().Execute(context.Background(), ExecuteRequest{
		CompletionMode:            CompletionModeReceiptRequired,
		SessionID:                 "session_receipt_observation",
		AttemptID:                 "attempt_receipt_observation",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "observe process only",
		OccurredAt:                "2026-03-01T12:00:00Z",
		InvocationID:              "invocation_receipt_observation",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_observation",
		CompletionReceiptDir:      receiptPath,
	})
	if err != nil {
		t.Fatalf("execute receipt-mode process observation: %v", err)
	}
	if !res.Accepted || res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected process-only success candidate, got %#v", res)
	}
	raw, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatalf("read receipt path sentinel: %v", err)
	}
	if string(raw) != sentinel {
		t.Fatalf("receipt path was modified: %q", raw)
	}
}

func TestMemoryAdapterExecuteCancelAndStreamContract(t *testing.T) {
	t.Parallel()

	adapter := NewMemoryAdapter()

	_, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_1",
		AttemptID:      "attempt_1",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "build it",
		OccurredAt:     "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	err = adapter.PublishEvent(AttemptStreamEvent{
		AttemptID:   "attempt_1",
		Seq:         2,
		EventType:   AttemptEventTypeProgress,
		PayloadJSON: `{"delta":"working"}`,
		OccurredAt:  "2026-03-01T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("publish progress event: %v", err)
	}

	err = adapter.Cancel(context.Background(), CancelRequest{
		SessionID:  "session_1",
		AttemptID:  "attempt_1",
		Reason:     "operator_cancel",
		OccurredAt: "2026-03-01T12:00:10Z",
	})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	events, errs, err := adapter.Stream(context.Background(), StreamRequest{
		SessionID: "session_1",
		AttemptID: "attempt_1",
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	received := make([]AttemptStreamEvent, 0, 3)
	for event := range events {
		received = append(received, event)
	}

	for streamErr := range errs {
		if streamErr != nil {
			t.Fatalf("stream error: %v", streamErr)
		}
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 stream events, got %d", len(received))
	}
	if received[0].EventType != AttemptEventTypeStarted {
		t.Fatalf("expected first event type %q, got %q", AttemptEventTypeStarted, received[0].EventType)
	}
	if received[1].EventType != AttemptEventTypeProgress {
		t.Fatalf("expected second event type %q, got %q", AttemptEventTypeProgress, received[1].EventType)
	}
	if received[2].EventType != AttemptEventTypeCancelled {
		t.Fatalf("expected terminal event type %q, got %q", AttemptEventTypeCancelled, received[2].EventType)
	}
}

func TestMemoryAdapterCancelUnknownAttemptReturnsNotFound(t *testing.T) {
	t.Parallel()

	adapter := NewMemoryAdapter()

	err := adapter.Cancel(context.Background(), CancelRequest{
		SessionID:  "session_1",
		AttemptID:  "unknown_attempt",
		Reason:     "operator_cancel",
		OccurredAt: "2026-03-01T12:00:10Z",
	})
	if err == nil {
		t.Fatal("expected cancel to fail for unknown attempt")
	}

	var notFoundErr *AttemptNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected AttemptNotFoundError, got %T", err)
	}
}

func TestMemoryAdapterRoleAwareArtifactBehavior(t *testing.T) {
	t.Parallel()

	adapter := NewMemoryAdapter()
	docWorkspace := t.TempDir()

	_, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_doc",
		AttemptID:      "attempt_doc",
		RunID:          "run_1",
		NodeRunID:      "nr_doc",
		WorkspacePath:  docWorkspace,
		Prompt:         "RoleID: king",
		OccurredAt:     "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute doc role: %v", err)
	}
	if _, err := os.Stat(filepath.Join(docWorkspace, "deliverables", "nr_doc.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected non-builder role to return response only, stat err=%v", err)
	}

	builderWorkspace := t.TempDir()
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_builder",
		AttemptID:      "attempt_builder",
		RunID:          "run_1",
		NodeRunID:      "nr_builder",
		WorkspacePath:  builderWorkspace,
		Prompt:         "RoleID: builder-berserker",
		OccurredAt:     "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute builder role: %v", err)
	}
	if _, err := os.Stat(filepath.Join(builderWorkspace, "deliverables", "nr_builder.txt")); err != nil {
		t.Fatalf("expected non-markdown artifact for builder role: %v", err)
	}
}
