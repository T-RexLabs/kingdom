package planning

import (
	"context"
	"errors"
	"testing"
)

type scopedSessionAgent struct{ mockAgent }

func (scopedSessionAgent) RequiresFreshSession() bool { return true }
func (scopedSessionAgent) RequiresFreshSessionForOperation(_ context.Context, id string) (bool, error) {
	switch id {
	case "local":
		return false, nil
	case "remote":
		return true, nil
	default:
		return false, errors.New("missing source")
	}
}
func TestPlanningSessionPolicyUsesRetainedOperationBackend(t *testing.T) {
	for id, want := range map[string]bool{"local": false, "remote": true, "missing": true} {
		if got := requiresFreshProviderSession(context.Background(), &scopedSessionAgent{}, id); got != want {
			t.Fatalf("%s: got %v want %v", id, got, want)
		}
	}
}
