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
