package opencode

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
)

// Native location movement is needed when Resume gives response-only work a
// new per-attempt workspace. This exercises the provider contract only; it does
// not relax canonical checkpoint or ownership admission.
func TestStockRestartedSessionMoveDoesNotReplayOldInput(t *testing.T) {
	h := newStockHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first := h.planningRequest("inv-location-before", "op-location-before", "SLEEP:30 TEXT:original")
	first.FirstEventTimeout, first.StreamInactivityTimeout = time.Second, time.Second
	prepared, response, err := h.run("session-location", first.AttemptID, first.InvocationID, first)
	if !errors.Is(err, context.DeadlineExceeded) || response.StreamObservation.Request == nil || response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
		t.Fatalf("first response did not prove interruption: %#v %v", response.StreamObservation.Request, err)
	}
	if err := h.service.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	h.startService(t)
	started, err := h.service.ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newWorkspace := filepath.Join(filepath.Dir(h.workspace), "next-attempt")
	if err := os.MkdirAll(newWorkspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := started.capture.track(prepared.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	if err := requireEmptyInbox(ctx, started.client, h.workspace, prepared.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	// The previous profile was retired and no profile has yet been registered
	// in the destination. Moving must apply only the control item, never replay
	// the original task under the old profile or at the new location.
	if err := started.client.send(ctx, started.client.http, http.MethodPost, sessionPath(prepared.ProviderSessionID, "/move"), atLocation(h.workspace), map[string]string{"directory": newWorkspace, "delivery": "steer"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := started.client.Wait(ctx, newWorkspace, prepared.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	info, err := started.client.Session(ctx, newWorkspace, prepared.ProviderSessionID)
	if err != nil || info.Location.Directory != newWorkspace {
		t.Fatalf("session did not move: %#v %v", info, err)
	}
	if calls := h.model.Calls(); len(calls) != 1 {
		t.Fatalf("moving replayed prior model input: %#v", calls)
	}
	second := h.planningRequest("inv-location-after", "op-location-after", "TEXT:bootstrap-must-not-run")
	second.WorkspacePath = newWorkspace
	second.ProviderSessionID = prepared.ProviderSessionID
	next, err := h.service.Prepare(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	defer h.service.Discard(context.Background(), second.InvocationID)
	if next.ProviderSessionID != prepared.ProviderSessionID || next.Identity.Location != newWorkspace || next.Identity.ProfileID == prepared.Identity.ProfileID {
		t.Fatalf("location continuation identity: %#v", next)
	}
	if err := requireEmptyInbox(ctx, started.client, newWorkspace, next.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	if err := started.client.Prompt(ctx, newWorkspace, next.ProviderSessionID, next.Identity.RequestID, "TEXT:continued-at-new-location"); err != nil {
		t.Fatal(err)
	}
	if err := started.client.Wait(ctx, newWorkspace, next.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	calls := h.model.Calls()
	if len(calls) != 2 || calls[1].LastUser != "TEXT:continued-at-new-location" || calls[1].Messages <= calls[0].Messages {
		t.Fatalf("history did not continue exactly once: %#v", calls)
	}
	system := strings.Join(calls[1].System, "\n")
	if !strings.Contains(system, "PINNED SYSTEM inv-location-after") || strings.Contains(system, "PINNED SYSTEM inv-location-before") {
		t.Fatalf("moved session used old instructions: %q", system)
	}
}
