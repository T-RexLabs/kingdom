package state

import "testing"

func TestPreparationProgressOrder(t *testing.T) {
	phases := []struct{ event, reason string }{
		{"attempt.retry_queued", ""},
		{"attempt.progress", "preparation_admitted"},
		{"attempt.progress", "workspace_preparing"},
		{"attempt.progress", "workspace_prepared"},
		{"node.state_changed", "provider_launching"},
		{"attempt.started", ""},
		{"attempt.progress", "provider_invoked"},
	}
	previous := -1
	for _, phase := range phases {
		marker, ok := attemptExecutionProgressMarkerForEvent(phase.event, phase.reason, "")
		if !ok || marker.Rank <= previous {
			t.Fatalf("phase %v does not advance progress: %#v after %d", phase, marker, previous)
		}
		previous = marker.Rank
	}
}
