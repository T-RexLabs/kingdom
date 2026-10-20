package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

func TestSessionCheckpointRequiresClosedOwnershipBeforeProviderCalls(t *testing.T) {
	for _, pending := range []string{"admission", "native receipt", "command claim"} {
		t.Run(pending, func(t *testing.T) {
			h := newBoundRun(t)
			call := providerrequest.NativeCall{Identity: h.identity, MessageID: "assistant", CallID: "call", Tool: "write", Input: json.RawMessage(`{"path":"partial.txt","content":"partial"}`)}
			if pending == "native receipt" {
				if _, err := h.authority.AdmitNative(context.Background(), call); err != nil {
					t.Fatal(err)
				}
			}
			if pending == "command claim" {
				call.Tool = "shell"
				if _, err := h.authority.ClaimCommand(context.Background(), call); err != nil {
					t.Fatal(err)
				}
			}
			if pending != "admission" {
				if err := h.authority.CloseAdmission(context.Background(), h.identity); err != nil {
					t.Fatal(err)
				}
			}
			// A service without a configured startup can only pass this test by
			// refusing the request before contacting or interrupting OpenCode.
			service := &Service{requests: &requestRuntime{authority: h.authority}}
			if _, err := service.CaptureSessionCheckpoint(context.Background(), h.identity); !errors.Is(err, providerrequest.ErrResourcesOpen) {
				t.Fatalf("pending %s was not rejected: %v", pending, err)
			}
		})
	}
}

func TestSessionCheckpointRejectsUnexpectedContext(t *testing.T) {
	for _, tc := range []struct {
		name, workspace, profile, inbox, messages, want string
		changed                                         bool
		stopFailed                                      string
	}{
		{name: "workspace", workspace: "/other", want: "different workspace"},
		{name: "profile", profile: "other-profile", want: "different workspace or profile"},
		{name: "pending input", inbox: `[{"id":"pending"}]`, want: "undelivered input"},
		{name: "missing source", messages: `[{"id":"assistant","type":"assistant"}]`, want: "lacks the source"},
		{name: "newer input", messages: `[{"id":"newer","type":"user"},{"id":"msg_test","type":"user"}]`, want: "input after"},
		{name: "concurrent mutation", messages: `[{"id":"msg_test","type":"user"}]`, changed: true, want: "changed while capturing"},
		{name: "interrupt failure", stopFailed: "interrupt", want: "resources remain open"},
		{name: "wait failure", stopFailed: "wait", want: "resources remain open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBoundRun(t)
			if err := h.authority.CloseAdmission(context.Background(), h.identity); err != nil {
				t.Fatal(err)
			}
			logs := 0
			client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case sessionPath(h.identity.SessionID, ""):
					workspace, profile := h.workspace, h.identity.ProfileID
					if tc.workspace != "" {
						workspace = tc.workspace
					}
					if tc.profile != "" {
						profile = tc.profile
					}
					fmt.Fprintf(w, `{"data":{"id":%q,"agent":%q,"location":{"directory":%q}}}`, h.identity.SessionID, profile, workspace)
				case sessionPath(h.identity.SessionID, "/interrupt"):
					if tc.stopFailed == "interrupt" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					if r.URL.Query().Get("resume") != "false" {
						t.Error("checkpoint must park the queue")
					}
					io.WriteString(w, `{"interrupted":false}`)
				case "/api/experimental/session/" + h.identity.SessionID + "/wait":
					if tc.stopFailed == "wait" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				case sessionPath(h.identity.SessionID, "/inbox"):
					inbox := tc.inbox
					if inbox == "" {
						inbox = `[]`
					}
					fmt.Fprintf(w, `{"data":%s}`, inbox)
				case sessionPath(h.identity.SessionID, "/message"):
					fmt.Fprintf(w, `{"data":%s,"cursor":{}}`, tc.messages)
				case "/api/experimental/session/" + h.identity.SessionID + "/log":
					logs++
					seq := 1
					if tc.changed {
						seq = logs
					}
					fmt.Fprintf(w, "data: {\"type\":\"log.synced\",\"seq\":%d}\n\n", seq)
				default:
					t.Errorf("unexpected provider mutation: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			done := make(chan struct{})
			close(done)
			service := &Service{requests: &requestRuntime{authority: h.authority}, attempt: &serviceStart{done: done, client: client}}
			if _, err := service.CaptureSessionCheckpoint(context.Background(), h.identity); err == nil || !strings.Contains(err.Error(), tc.want) || (tc.stopFailed != "" && !errors.Is(err, providerrequest.ErrResourcesOpen)) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestProjectedCheckpointDigestNormalizesKeysAndPreservesLargeNumbers(t *testing.T) {
	identity := providerexecution.RequestIdentity{Location: "/workspace", SessionID: "session", RequestID: "request"}
	projected := `[{"type":"assistant","id":"latest","metadata":{"n":9007199254740993}},{"id":"request","type":"user","text":"original"}]`
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"data":%s,"cursor":{}}`, projected)
	})
	first, err := captureProjectedCheckpoint(context.Background(), client, identity)
	if err != nil {
		t.Fatal(err)
	}
	projected = `[{"metadata":{"n":9007199254740993},"id":"latest","type":"assistant"},{"text":"original","type":"user","id":"request"}]`
	reordered, err := captureProjectedCheckpoint(context.Background(), client, identity)
	if err != nil || reordered != first {
		t.Fatalf("key ordering changed checkpoint: %#v %#v %v", first, reordered, err)
	}
	projected = strings.ReplaceAll(projected, "9007199254740993", "9007199254740992")
	changed, err := captureProjectedCheckpoint(context.Background(), client, identity)
	if err != nil || changed.MessagesSHA256 == first.MessagesSHA256 || changed.MessageID != "latest" {
		t.Fatalf("history change was lost: %#v %#v %v", first, changed, err)
	}
}

func TestStockSessionCheckpointSurvivesRestart(t *testing.T) {
	h := newStockHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req := h.planningRequest("inv-checkpoint", "op-checkpoint", "SLEEP:30 TEXT:original")
	req.FirstEventTimeout, req.StreamInactivityTimeout = time.Second, time.Second
	prepared, response, err := h.run("session-checkpoint", req.AttemptID, req.InvocationID, req)
	if !errors.Is(err, context.DeadlineExceeded) || response.StreamObservation.Request == nil || response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
		t.Fatalf("source interruption not proven: %#v %v", response.StreamObservation.Request, err)
	}
	before, err := h.service.CaptureSessionCheckpoint(ctx, prepared.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.service.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	h.startService(t)
	after, err := h.service.CaptureSessionCheckpoint(ctx, prepared.Identity)
	if err != nil || before != after {
		t.Fatalf("restart changed checkpoint: before=%#v after=%#v err=%v", before, after, err)
	}
	if before.MessageID == "" || len(before.MessagesSHA256) != 64 {
		t.Fatalf("incomplete checkpoint: %#v", before)
	}
	if calls := h.model.Calls(); len(calls) != 1 {
		t.Fatalf("checkpoint replayed model input: %#v", calls)
	}
}
