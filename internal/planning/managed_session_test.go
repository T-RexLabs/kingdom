package planning

import (
	"context"
	"kingdom/internal/core/providerexecution"
	"strings"
	"testing"
)

type managedPlannerRecorder struct{ recordingPlannerAdapter }

func (*managedPlannerRecorder) ExecutionBackend() providerexecution.Backend {
	return providerexecution.BackendOpenShell
}

func TestManagedPlannerReconstructsConversationInFreshSession(t *testing.T) {
	adapter := &managedPlannerRecorder{}
	input := AgentContext{PromptCatalog: testPromptCatalog(t), SessionID: "session", Messages: continuationTestConversation(), CurrentMessageID: "msg-3", ProviderInvocation: adapter.invocation("ses_existing")}
	turn, err := NewOpenCodeAgent(adapter, deterministicID()).ContinueTurnStream(context.Background(), input, nil)
	defer turn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if adapter.request.ProviderSessionID != "" || !adapter.request.ForceNewSession || adapter.request.OpenContinuationPrompt != nil {
		t.Fatal("managed request reused a disposed provider session")
	}
	for _, text := range []string{"earlier-user-turn", "earlier-planner-turn", "newest-user-turn"} {
		if !strings.Contains(adapter.prompt, text) {
			t.Fatalf("canonical conversation missing %s", text)
		}
	}
}
