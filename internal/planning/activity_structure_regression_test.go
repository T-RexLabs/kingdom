package planning

import (
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
	"testing"
)

func TestPlannerActivityPayloadNeverInfersToolStatusFromUnrecognizedInput(t *testing.T) {
	activity := ProviderActivity{Kind: "tool", ToolName: "custom-tool", Input: providerexecution.ActivityText(`{"arbitrary":"input"}`)}
	payload := plannerActivityPayload(state.PlanningOperation{OperationID: "op"}, "part", activity)
	if payload["tool_name"] != "custom-tool" || payload["tool_status"] != nil {
		t.Fatalf("invented provider tool metadata: %#v", payload)
	}
}
