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

func TestRequestLifecycleRetainsVerifiedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		reject, failClosure, failExport bool
	}{
		{name: "rejected", reject: true}, {name: "rejected_unclosed", reject: true, failClosure: true},
		{name: "success"}, {name: "success_unclosed", failClosure: true}, {name: "export_failed", failExport: true},
	} {
		t.Run(tc.name, func(t *testing.T) { testRequestLifecycle(t, tc.reject, tc.failClosure, tc.failExport) })
	}
}

func testRequestLifecycle(t *testing.T, reject, failClosure, failExport bool) {
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
		if reject && message["type"] == "assistant" {
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
		runtime: (&requestRuntime{authority: authority, commands: newCommandBridge(authority, nil, nil), outboxDir: t.TempDir(), settleLimit: time.Second}).executionRuntime(),
		request: &request{identity: fixture.Identity, directory: h.workspace, started: &requestSession{client: client, capture: c}},
		req:     ExecuteRequest{InvocationID: "invocation"},
	}
	closed, exported := false, false
	exportFailure := errors.New("workspace export failed")
	closureFailure := errors.New("server closure remains unproven")
	e.runtime.lifecycle = testExecutionLifecycle{closeFn: func(ctx context.Context) error {
		var recorded int
		if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM provider_requests WHERE terminal_sha256 IS NOT NULL`).Scan(&recorded); err != nil {
			t.Fatal(err)
		}
		if recorded != 0 {
			t.Fatal("abandonment preceded backend closure")
		}
		closed = true
		if failClosure {
			return closureFailure
		}
		return nil
	}, exportFn: func(context.Context) error {
		if !closed {
			t.Fatal("workspace exported before closure")
		}
		exported = true
		if failExport {
			return exportFailure
		}
		return nil
	}}
	response, err := e.finalize(context.Background(), "now")
	if !closed {
		t.Fatal("request skipped backend closure")
	}
	if failClosure {
		if !errors.Is(err, closureFailure) {
			t.Fatalf("closure failure lost: %v", err)
		}
		if response.StreamObservation.Request != nil {
			t.Fatal("unclosed resources produced terminal evidence")
		}
		if response.StreamObservation.ReleaseContent != nil {
			_ = response.StreamObservation.ReleaseContent()
		}
		return
	}
	if !reject && !failExport {
		if err != nil {
			t.Fatal(err)
		}
		if !exported || response.Outcome.State != "success" || response.StreamObservation.Request == nil {
			t.Fatal("verified success lost export or terminal")
		}
		if response.StreamObservation.ReleaseContent != nil {
			_ = response.StreamObservation.ReleaseContent()
		}
		return
	}
	if failExport && (!exported || !errors.Is(err, exportFailure)) {
		t.Fatalf("export failure lost: %v", err)
	}
	if reject && exported {
		t.Fatal("rejected projection exported workspace")
	}
	if err == nil {
		t.Fatal("conflicting projection or export was accepted")
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

// Tests install effects without replacing the protocol verifier or authority.
type testExecutionLifecycle struct {
	closeFn  func(context.Context) error
	exportFn func(context.Context) error
}

func (l testExecutionLifecycle) close(ctx context.Context) error { return l.closeFn(ctx) }
func (l testExecutionLifecycle) export(ctx context.Context) error {
	if l.exportFn != nil {
		return l.exportFn(ctx)
	}
	return nil
}
func (testExecutionLifecycle) retire(context.Context, *execution) error { return nil }
