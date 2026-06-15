package state

import "testing"

// Fine-grained provider activity is observability-only and must never advance the
// run-activity projection (which drives operator-facing "last activity" and run
// collection events).
func TestShouldTrackRunActivityExcludesAttemptActivity(t *testing.T) {
	tracked, err := ShouldTrackRunActivity(AttemptActivityEventType, "provider_activity", `{"kind":"kingdom.reasoning","detail":"thinking"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tracked {
		t.Fatalf("attempt.activity must not be tracked as run activity")
	}
}

// Guard the coarse lifecycle events that must continue to advance run activity, so a
// future refactor cannot silently drop them alongside the activity exclusion.
func TestShouldTrackRunActivityKeepsLifecycleEvents(t *testing.T) {
	for _, eventType := range []string{"attempt.started", "attempt.succeeded", "attempt.failed", "node.state_changed"} {
		tracked, err := ShouldTrackRunActivity(eventType, "", "{}")
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", eventType, err)
		}
		if !tracked {
			t.Fatalf("expected %q to advance run activity", eventType)
		}
	}
}
