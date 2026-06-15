package state

import "testing"

// Fine-grained agent activity advances the run's last_activity_at (so the Throne
// Room / Runs cards stay fresh while an agent works) but must not project run
// collection events.
func TestShouldTrackRunActivityAdvancesOnAttemptActivity(t *testing.T) {
	tracked, err := ShouldTrackRunActivity(AttemptActivityEventType, "provider_activity", `{"kind":"kingdom.reasoning","detail":"thinking"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tracked {
		t.Fatalf("attempt.activity must advance run last_activity_at")
	}
	if ShouldProjectRunCollectionEvent(AttemptActivityEventType) {
		t.Fatalf("attempt.activity must not project run collection events")
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
