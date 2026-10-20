package planning

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

// completedDuringCancellationPlanner makes the provider terminal available
// before shutdown, then returns it only after the runtime requests cancellation.
// This pins the race without relying on scheduling or a provider transport.
type completedDuringCancellationPlanner struct {
	mockAgent
	started chan struct{}
	outcome string
}

func (p *completedDuringCancellationPlanner) ContinueTurnStream(ctx context.Context, input AgentContext, _ func(string)) (AgentTurn, error) {
	var metadata ProviderExecutionMetadata
	var err error
	if p.outcome == providerexecution.RequestOutcomeFailed {
		metadata, err = failedProviderMetadata(context.Background(), input.ProviderInvocation, ProviderExecutionMetadata{OutcomeState: "failed"})
	} else {
		metadata, err = testSuccessMetadata(input, "Completed before cancellation.")
	}
	if err != nil {
		return AgentTurn{}, err
	}
	close(p.started)
	<-ctx.Done()
	return AgentTurn{ContentMarkdown: metadata.FinalText, Metadata: metadata}, nil
}

func TestPlanningCancellationPreservesConcurrentProviderCompletion(t *testing.T) {
	for _, outcome := range []string{providerexecution.RequestOutcomeSucceeded, providerexecution.RequestOutcomeFailed} {
		t.Run(outcome, func(t *testing.T) {
			projectRoot := t.TempDir()
			writeTestFile(t, projectRoot, "README.md", "# Project\n")
			planner := &completedDuringCancellationPlanner{started: make(chan struct{}), outcome: outcome}
			db := stateTestDB(t)
			store := state.NewPlanningStore(db)
			service := NewService(store, planner, projectRoot, func() string { return "2026-04-26T12:00:00Z" }, deterministicID())
			session, err := service.CreateSession(context.Background(), "Cancellation Race", `{"model":"test-model"}`)
			if err != nil {
				t.Fatal(err)
			}
			op, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
				Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Reply before shutdown.", AnswerPayloadJSON: `{}`,
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-planner.started:
			case <-time.After(5 * time.Second):
				t.Fatal("planner did not prepare its terminal")
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := service.Close(closeCtx); err != nil {
				t.Fatalf("completed provider request must drain on cancellation: %v", err)
			}
			stored, found, err := store.GetOperation(context.Background(), op.Operation.OperationID)
			if err != nil || !found || stored.Status != state.PlanningOperationStatusCancelled {
				t.Fatalf("operation did not settle as cancelled: %#v %v", stored, err)
			}
			var raw string
			if err := db.QueryRow(`SELECT e.payload_json FROM planning_provider_completion_evidence e JOIN planning_provider_completion_settlements s ON s.primary_evidence_id=e.evidence_id WHERE s.planning_operation_id=?`, stored.OperationID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var evidence struct {
				Observation providerexecution.InvocationStreamObservation `json:"stream_observation"`
			}
			if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
				t.Fatal(err)
			}
			if !evidence.Observation.Cancelled || evidence.Observation.Request == nil || evidence.Observation.Request.Terminal.Outcome != outcome {
				t.Fatalf("cancellation rewrote provider truth or lost the runtime decision: %s", raw)
			}
		})
	}
}
