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
	validServerResponseRequest := func() ExecuteRequest {
		return ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_1",
			AttemptID:      "attempt_1",
			RunID:          "run_1",
			NodeRunID:      "node_run_1",
			Prompt:         "complete this work",
			OccurredAt:     "2026-03-01T12:00:00Z",
		}
	}

	tests := []struct {
		name   string
		mutate func(*ExecuteRequest)
		want   string
	}{
		{
			name:   "missing mode",
			mutate: func(req *ExecuteRequest) { req.CompletionMode = "" },
			want:   "completion mode must be",
		},
		{
			name:   "unknown mode",
			mutate: func(req *ExecuteRequest) { req.CompletionMode = "unknown" },
			want:   "completion mode must be",
		},
		{
			name:   "missing session id",
			mutate: func(req *ExecuteRequest) { req.SessionID = "" },
			want:   "session id",
		},
		{
			name:   "missing attempt id",
			mutate: func(req *ExecuteRequest) { req.AttemptID = "" },
			want:   "attempt id",
		},
		{
			name:   "missing run id",
			mutate: func(req *ExecuteRequest) { req.RunID = "" },
			want:   "run id",
		},
		{
			name:   "missing node run id",
			mutate: func(req *ExecuteRequest) { req.NodeRunID = "" },
			want:   "node run id",
		},
		{
			name:   "missing prompt",
			mutate: func(req *ExecuteRequest) { req.Prompt = "" },
			want:   "prompt",
		},
		{
			name:   "missing occurred at",
			mutate: func(req *ExecuteRequest) { req.OccurredAt = "" },
			want:   "occurred_at",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validServerResponseRequest()
			test.mutate(&request)
			err := validateExecuteRequest(request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate execute request error = %v, want %q", err, test.want)
			}
		})
	}

	if err := validateExecuteRequest(validServerResponseRequest()); err != nil {
		t.Fatalf("expected server-response request to validate: %v", err)
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
		Prompt:         "RoleID: governance",
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
		Prompt:         "RoleID: builder",
		OccurredAt:     "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute builder role: %v", err)
	}
	if _, err := os.Stat(filepath.Join(builderWorkspace, "deliverables", "nr_builder.txt")); err != nil {
		t.Fatalf("expected non-markdown artifact for builder role: %v", err)
	}
}
