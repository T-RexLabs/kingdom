package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode/protocol"
)

// This contract test deliberately enrolls a known, interrupted session in the
// new capture. Production adoption still needs a durable checkpoint and closed
// predecessor ownership; enrolling arbitrary session IDs is not a recovery API.
func TestStockRestartContinuationKeepsHistoryAndVerifiesOnlyNewRequest(t *testing.T) {
	for _, parked := range []bool{false, true} {
		name := "empty inbox"
		if parked {
			name = "parked old input"
		}
		t.Run(name, func(t *testing.T) { stockRestartContinuation(t, parked) })
	}
}

func stockRestartContinuation(t *testing.T, parked bool) {
	h := newStockHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h.runOwner("attempt-before", "inv-before")
	first := h.builderRequest("inv-before", "attempt-before", "WRITE:partial.txt PAUSE_AFTER_TOOL:30")
	prepared, err := h.service.Prepare(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	h.bindRun(prepared, first.AttemptID, first.InvocationID)
	type result struct {
		response ExecuteResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := h.service.Execute(ctx, first)
		done <- result{response, err}
	}()
	// Wait until the model has received the write result and is paused. The
	// interruption therefore occurs after real partial progress, not dispatch.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(h.model.Calls()) < 2 {
		select {
		case early := <-done:
			t.Fatalf("request ended before interruption: %v", early.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	oldServer, err := h.service.ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	queuedID := "msg_queued_before_interruption"
	if parked {
		var queued json.RawMessage
		if err := oldServer.client.send(ctx, oldServer.client.http, http.MethodPost, sessionPath(prepared.ProviderSessionID, "/prompt"), atLocation(h.workspace), map[string]string{
			"id": queuedID, "text": "WRITE:must-not-replay.txt", "delivery": "queue",
		}, &queued); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.service.Cancel(ctx, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	interrupted := <-done
	if interrupted.response.StreamObservation.Request == nil {
		t.Fatalf("missing interruption evidence: %v", interrupted.err)
	}
	if !parked && (!errors.Is(interrupted.err, context.Canceled) || interrupted.response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted) {
		t.Fatalf("interruption not proven: %#v %v", interrupted.response.StreamObservation.Request, interrupted.err)
	}
	if parked && (interrupted.err == nil || interrupted.response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeFailed || len(interrupted.response.StreamObservation.DiagnosticContent) == 0) {
		t.Fatalf("unexpected inbox input must remain a diagnostic failure: %#v %v", interrupted.response.StreamObservation, interrupted.err)
	}
	if err := providerrequest.RequireResourcesClosedTx(ctx, h.db, "binding-inv-before"); err != nil {
		t.Fatal(err)
	}
	if err := h.service.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	callsBeforeRestart := len(h.model.Calls())
	h.startService(t)
	started, err := h.service.ready(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expectedPending := 0
	if parked {
		expectedPending = 1
	}
	if pending, err := started.client.PendingInbox(ctx, h.workspace, prepared.ProviderSessionID); err != nil || pending != expectedPending {
		t.Fatalf("parked input must survive restart: pending=%d err=%v", pending, err)
	}
	if err := requireEmptyInbox(ctx, started.client, h.workspace, prepared.ProviderSessionID); parked && err == nil {
		t.Fatal("pending old input must prevent session reuse")
	}
	if err := started.client.send(ctx, started.client.http, http.MethodDelete, sessionPath(prepared.ProviderSessionID, "/inbox/"+queuedID), atLocation(h.workspace), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := requireEmptyInbox(ctx, started.client, h.workspace, prepared.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	// The event subscription is already connected. Only the new request's
	// interval is evidence; old projected messages remain conversation context.
	if err := started.capture.track(prepared.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	h.runOwner("attempt-after", "inv-after")
	second := h.builderRequest("inv-after", "attempt-after", "TEXT:must-not-replay-original-task")
	second.ProviderSessionID = prepared.ProviderSessionID
	second.OpenContinuationPrompt = openText("TEXT:continued")
	next, err := h.service.Prepare(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if next.ProviderSessionID != prepared.ProviderSessionID || next.Identity.InstanceID == prepared.Identity.InstanceID || next.Identity.Generation == prepared.Identity.Generation || next.Identity.ProfileID == prepared.Identity.ProfileID || next.Identity.RequestID == prepared.Identity.RequestID {
		t.Fatalf("restart must preserve session with fresh ownership: before=%#v after=%#v", prepared, next)
	}
	defer h.service.Discard(context.Background(), second.InvocationID)
	// Exercise the native provider contract, not production recovery admission.
	// The current canonical binder intentionally forbids this cross-generation
	// reuse until Resume can supply a validated checkpoint. Do not weaken that
	// guard merely to make a native capability test pass.
	if err := started.client.Prompt(ctx, h.workspace, next.ProviderSessionID, next.Identity.RequestID, "TEXT:continued"); err != nil {
		t.Fatal(err)
	}
	if err := started.client.Wait(ctx, h.workspace, next.ProviderSessionID); err != nil {
		t.Fatal(err)
	}
	watermark, err := started.client.Log(ctx, h.workspace, next.ProviderSessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := started.capture.await(ctx, next.ProviderSessionID, watermark); err != nil {
		t.Fatal(err)
	}
	stage := func(write func(io.Writer) error) protocol.Source {
		t.Helper()
		writer, err := newStagedWriter("kingdom-resume-contract-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(writer.discard)
		if err := write(writer); err != nil {
			t.Fatal(err)
		}
		content, err := writer.finish(second.InvocationID)
		if err != nil {
			t.Fatal(err)
		}
		return stagedSource(content)
	}
	events := stage(func(out io.Writer) error {
		return started.capture.writeInterval(next.ProviderSessionID, next.Identity.RequestID, watermark, out)
	})
	execution := &execution{runtime: h.service.requests.executionRuntime(), request: h.service.requests.registry.invocation(second.InvocationID), req: second}
	messages := stage(func(out io.Writer) error { return execution.writeMessages(ctx, out) })
	var text bytes.Buffer
	verified, err := protocol.Verify(ctx, next.Identity, protocol.Evidence{
		InvocationID: second.InvocationID, Events: events, Messages: messages,
		Watermark: watermark, CaptureHealthy: started.capture.healthy() == nil, WaitCompleted: true,
	}, &text)
	if err != nil {
		t.Fatal(err)
	}
	if text.String() != "continued" || verified.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded || len(verified.Terminal.Tools) != 0 {
		t.Fatalf("new terminal includes old work: text=%q terminal=%#v", text.String(), verified.Terminal)
	}
	calls := h.model.Calls()
	if len(calls) != callsBeforeRestart+1 {
		t.Fatalf("restart replayed model input: before=%d after=%d", callsBeforeRestart, len(calls))
	}
	last := calls[len(calls)-1]
	if last.LastUser != "TEXT:continued" || len(last.ToolResults) != 1 || last.Messages <= calls[0].Messages {
		t.Fatalf("saved conversation was not continued: %#v", last)
	}
	system := strings.Join(last.System, "\n")
	if !strings.Contains(system, "PINNED SYSTEM inv-after") || strings.Contains(system, "PINNED SYSTEM inv-before") {
		t.Fatalf("old profile instructions survived: %q", system)
	}
	var grants int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&grants); err != nil || grants != 1 {
		t.Fatalf("old write replayed: grants=%d err=%v", grants, err)
	}
	if raw, err := os.ReadFile(filepath.Join(h.workspace, "partial.txt")); err != nil || string(raw) != "hello\n" {
		t.Fatalf("partial workspace lost: %q %v", raw, err)
	}
	if _, err := os.Stat(filepath.Join(h.workspace, "must-not-replay.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("parked old work executed: %v", err)
	}
}
