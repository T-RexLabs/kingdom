package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

func TestRejectedRequestRetainsUnverifiedDiagnostics(t *testing.T) {
	h := newOwnerFixture(t, t.TempDir())
	var fixture struct {
		Identity  providerexecution.RequestIdentity `json:"identity"`
		Events    []map[string]any                  `json:"events"`
		Messages  []map[string]any                  `json:"messages"`
		Watermark int64                             `json:"watermark"`
	}
	raw, err := os.ReadFile("protocol/testdata/success.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	fixture.Identity.Location = h.workspace
	// A mismatched projected finish must fail verification without losing
	// either the events or the rejected projection.
	for _, message := range fixture.Messages {
		if message["type"] == "assistant" {
			message["finish"] = "length"
		}
	}
	c := &capture{dir: t.TempDir(), changed: make(chan struct{}), sessions: map[string]*capturedSession{}}
	if err := c.track(fixture.Identity.SessionID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.release(fixture.Identity.SessionID) })
	for _, event := range fixture.Events {
		if location, ok := event["location"].(map[string]any); ok {
			location["directory"] = h.workspace
		}
		raw, _ := json.Marshal(event)
		if err := c.record(raw); err != nil {
			t.Fatal(err)
		}
	}
	client := sessionTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/log"):
			fmt.Fprintf(w, "data: {\"type\":\"log.synced\",\"seq\":%d}\n\n", fixture.Watermark)
		case strings.HasSuffix(r.URL.Path, "/message"):
			messages := append([]map[string]any(nil), fixture.Messages...)
			for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
				messages[i], messages[j] = messages[j], messages[i]
			}
			json.NewEncoder(w).Encode(map[string]any{"data": messages})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	h.planningOwner("session", "operation", "invocation")
	h.bindPlanning(Prepared{Identity: fixture.Identity, ProviderSessionID: fixture.Identity.SessionID}, "operation", "invocation")
	authority := providerrequest.NewAuthority(h.db, time.Now)
	if err := authority.IssueDispatch(context.Background(), fixture.Identity); err != nil {
		t.Fatal(err)
	}
	e := &execution{
		service: &Service{requests: &requestRuntime{authority: authority, commands: newCommandBridge(authority, nil, nil), outboxDir: t.TempDir(), settleLimit: time.Second}},
		request: &request{identity: fixture.Identity, directory: h.workspace, started: &serviceStart{client: client, capture: c}},
		req:     ExecuteRequest{InvocationID: "invocation"},
	}
	response, err := e.finalize(context.Background(), "now")
	if err == nil {
		t.Fatal("conflicting projection was accepted")
	}
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Errorf("rejection is not a typed protocol error: %v", err)
	}
	if response.Outcome.ReasonCode != "provider_protocol_error" {
		t.Errorf("reason=%q", response.Outcome.ReasonCode)
	}
	observation := response.StreamObservation
	if observation.Request == nil || !observation.Request.Terminal.Abandoned() {
		t.Fatalf("rejection must abandon rather than certify provider output: %#v", observation.Request)
	}
	if observation.CanonicalTerminalTextRef != nil {
		t.Fatal("unverified reply became canonical text")
	}
	if len(observation.DiagnosticContent) != 3 || len(observation.StagedDiagnosticContent) != 3 {
		t.Fatalf("rejected evidence lost: %#v", observation.DiagnosticContent)
	}
	for _, content := range observation.StagedDiagnosticContent {
		reader, err := content.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if content.Reference != providerexecution.ReferenceText("invocation", string(body)) {
			t.Fatal("diagnostic digest mismatch")
		}
	}
	if err := observation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
	for _, content := range observation.StagedDiagnosticContent {
		if _, err := os.Stat(content.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("diagnostic staging not released: %v", err)
		}
	}
}
