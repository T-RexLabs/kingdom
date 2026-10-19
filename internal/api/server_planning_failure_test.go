package api

import (
	"testing"

	"kingdom/internal/state"
)

func TestFailedPlanningTurnIsExplainedInOperatorWords(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure state.PlanningTurnFailure
		want    string
	}{
		{"the provider's own words win", state.PlanningTurnFailure{ReasonCode: "provider_unknown_failure", ProviderMessage: " Attempted to access resource from outside project geography EU "}, "Attempted to access resource from outside project geography EU"},
		{"a known code", state.PlanningTurnFailure{ReasonCode: "agent_unresponsive"}, "The planner stopped responding before the reply finished."},
		{"an unknown code", state.PlanningTurnFailure{ReasonCode: "internal_error"}, "The planner could not finish this reply."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, err := planningMessageFromState(state.PlanningMessage{
				PlanningMessageID: "m", PlanningSessionID: "s", Role: state.PlanningMessageRoleAssistant,
				QuestionPayloadJSON: "{}", AnswerPayloadJSON: "{}", CreatedAt: "2026-04-21T17:10:00Z", Failure: &tc.failure,
			})
			if err != nil {
				t.Fatal(err)
			}
			if message.Failure == nil || message.Failure.ReasonCode != tc.failure.ReasonCode || message.Failure.Message != tc.want {
				t.Fatalf("failure=%+v", message.Failure)
			}
		})
	}
}
