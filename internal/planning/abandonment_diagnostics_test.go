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

// unprovenPlannerAdapter returns rejected diagnostics without a terminal. The
// request's effects have drained by the time the runner retries abandonment.
type unprovenPlannerAdapter struct {
	stagedPlannerAdapter
	store    *state.PlanningStore
	ref      providerexecution.ContentReference
	released bool
}

func (a *unprovenPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	response, err := a.stagedPlannerAdapter.Execute(ctx, req)
	if err != nil {
		return response, err
	}
	diagnostic := response.StreamObservation.StagedDiagnosticContent[0]
	a.ref = diagnostic.Reference
	observation := providerexecution.InvocationStreamObservation{
		ProviderSessionID:       response.ProviderSessionID,
		DiagnosticContent:       []providerexecution.ContentReference{diagnostic.Reference},
		StagedDiagnosticContent: []providerexecution.StagedContent{diagnostic},
		MalformedProtocolCount:  1,
	}
	observation.ReleaseContent = func() error {
		var body strings.Builder
		if err := a.store.CopyPlanningProviderContent(context.Background(), diagnostic.Reference, &body); err != nil {
			return fmt.Errorf("diagnostics released before ingestion: %w", err)
		}
		a.released = true
		return os.Remove(diagnostic.Path)
	}
	return opencode.ExecuteResponse{
		ProviderSessionID: response.ProviderSessionID, StreamObservation: observation,
		Outcome: opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_protocol_error", FailureText: "Rejected snapshots."},
	}, &opencode.ProtocolError{Operation: "request verification", Cause: errors.New("unclassified provider evidence")}
}

func TestPlanningRetainsRejectedDiagnosticsWhenRunnerAbandonsTheRequest(t *testing.T) {
	store := state.NewPlanningStore(stateTestDB(t))
	adapter := &unprovenPlannerAdapter{stagedPlannerAdapter: stagedPlannerAdapter{root: t.TempDir(), text: "Unverified provider text."}, store: store}
	service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
	defer service.Close(context.Background())
	session, err := service.CreateSession(context.Background(), "Deferred Abandonment", "{}")
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
	if err != nil || op.Status != state.PlanningOperationStatusFailed || op.CanonicalSettlementID == nil {
		t.Fatalf("unproven request did not settle: %#v %v", op, err)
	}
	var body strings.Builder
	if err := store.CopyPlanningProviderContent(context.Background(), adapter.ref, &body); err != nil || !strings.Contains(body.String(), adapter.text) {
		t.Fatalf("rejected diagnostics lost during abandonment: %q %v", body.String(), err)
	}
	if !adapter.released {
		t.Fatal("durably ingested diagnostics were not retired")
	}
	request, err := decodeOperationRequest(created.Operation.RequestPayloadJSON)
	if err != nil {
		t.Fatal(err)
	}
	message, err := store.GetMessageInSession(context.Background(), session.PlanningSessionID, request.AssistantMessageID)
	if err != nil || message.ContentMarkdown != "" || message.Failure == nil || message.Failure.ReasonCode != "provider_protocol_error" {
		t.Fatalf("abandonment published rejected text or lost its reason: %#v %v", message, err)
	}
}
