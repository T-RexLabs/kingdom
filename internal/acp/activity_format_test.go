package acp

import "testing"

func TestToolActivityLabelUsesFriendlyVerbsAndTargets(t *testing.T) {
	label := ToolActivityLabel("webfetch", "completed", "", `{"url":"https://example.com"}`)
	if label != "Fetched https://example.com" {
		t.Fatalf("expected fetched label, got %q", label)
	}

	label = ToolActivityLabel("bash", "running", "", `{"command":"go test ./..."}`)
	if label != "Running go test ./..." {
		t.Fatalf("expected running command label, got %q", label)
	}
}

func TestPhaseActivityLabelIncludesProviderLaunchLifecycle(t *testing.T) {
	tests := map[string]string{
		"preparation_admitted":  "Preparation queued",
		"workspace_preparing":   "Preparing workspace",
		"workspace_prepared":    "Workspace prepared",
		"provider_launching":    "Launching provider",
		"provider_session_open": "Opening provider session",
		"finalizing":            "Finalizing response",
	}

	for phase, want := range tests {
		got, ok := PhaseActivityLabel(phase)
		if !ok {
			t.Fatalf("expected phase %q to be recognized", phase)
		}
		if got != want {
			t.Fatalf("expected phase %q label %q, got %q", phase, want, got)
		}
	}
}

func TestNormalizeProgressActivityLabelRewritesProviderStepStarted(t *testing.T) {
	got, ok := NormalizeProgressActivityLabel("Provider step started")
	if !ok {
		t.Fatalf("expected progress label to be recognized")
	}
	if got != "Processing" {
		t.Fatalf("expected normalized progress label, got %q", got)
	}
}

func TestToolActivityLabelTodowriteShowsChecklistProgress(t *testing.T) {
	input := `{"todos":[{"content":"a","status":"completed"},{"content":"b","status":"in_progress"},{"content":"c","status":"pending"},{"content":"d","status":"completed"}]}`
	got := ToolActivityLabel("todowrite", "completed", "", input)
	if got != "Checklist · 2/4" {
		t.Fatalf("ToolActivityLabel(todowrite) = %q, want %q", got, "Checklist · 2/4")
	}
}

func TestToolActivityLabelTodowriteFallsBackWithoutTodos(t *testing.T) {
	if got := ToolActivityLabel("todowrite", "completed", "", ""); got != "Updated checklist" {
		t.Fatalf("empty todowrite label = %q, want %q", got, "Updated checklist")
	}
	if got := ToolActivityLabel("todowrite", "completed", "", "not json"); got != "Updated checklist" {
		t.Fatalf("malformed todowrite label = %q, want %q", got, "Updated checklist")
	}
}
