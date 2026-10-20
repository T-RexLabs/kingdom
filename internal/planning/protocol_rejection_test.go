package planning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

type rejectedPlannerAdapter struct {
	stagedPlannerAdapter
	store    *state.PlanningStore
	ref      providerexecution.ContentReference
	released bool
}

func (a *rejectedPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	response, err := a.stagedPlannerAdapter.Execute(ctx, req)
	if err != nil {
		return response, err
	}
	identity, err := a.store.AbandonPlanningProviderRequest(ctx, req.InvocationID)
	if err != nil {
		return response, err
	}
	observation, err := providerexecution.AbandonedObservation(identity, req.InvocationID)
	if err != nil {
		return response, err
	}
	diagnostic := response.StreamObservation.StagedDiagnosticContent[0]
	a.ref = diagnostic.Reference
	observation.DiagnosticContent = append(observation.DiagnosticContent, diagnostic.Reference)
	observation.StagedDiagnosticContent = append(observation.StagedDiagnosticContent, diagnostic)
	release := observation.ReleaseContent
	observation.ReleaseContent = func() error {
		var body strings.Builder
		if err := a.store.CopyPlanningProviderContent(context.Background(), diagnostic.Reference, &body); err != nil {
			return fmt.Errorf("release before durable diagnostic ingestion: %w", err)
		}
		a.released = true
		return errors.Join(release(), os.Remove(diagnostic.Path))
	}
	return opencode.ExecuteResponse{
		ProviderSessionID: identity.SessionID, StreamObservation: observation,
		Outcome: opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_protocol_error"},
	}, &opencode.ProtocolError{Operation: "request verification", Cause: errors.New("projected finish conflicts with events")}
}

func TestPlanningPersistsRejectedEvidenceWithoutPublishingItsReply(t *testing.T) {
	store := state.NewPlanningStore(stateTestDB(t))
	adapter := &rejectedPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Unverified provider reply."}, store: store}
	service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
	defer service.Close(context.Background())
	session, err := service.CreateSession(context.Background(), "Rejected evidence", "{}")
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Input"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	op, _, err := store.GetOperation(context.Background(), created.Operation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != state.PlanningOperationStatusFailed || op.CanonicalSettlementID == nil || !adapter.released {
		t.Fatalf("rejection was not durably settled and ingested: %#v released=%v", op, adapter.released)
	}
	var body strings.Builder
	if err := store.CopyPlanningProviderContent(context.Background(), adapter.ref, &body); err != nil || !strings.Contains(body.String(), adapter.text) {
		t.Fatalf("rejected diagnostic replay: %q %v", body.String(), err)
	}
	request, err := decodeOperationRequest(created.Operation.RequestPayloadJSON)
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.GetMessageInSession(context.Background(), session.PlanningSessionID, request.AssistantMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if message.ContentMarkdown != "" || message.Failure == nil || message.Failure.ReasonCode != "provider_protocol_error" {
		t.Fatalf("unverified reply escaped the diagnostic boundary: %#v", message)
	}
}
