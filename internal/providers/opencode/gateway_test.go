package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/execution/providerrequest"
)

func newTestGateway(t *testing.T, bound *boundRun) *gateway {
	t.Helper()
	g, err := newGateway(bound.authority, newCommandBridge(bound.authority, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.close(context.Background()) })
	return g
}

func (g *gateway) post(t *testing.T, route, token string, body any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, g.url()+route, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	_ = json.NewDecoder(response.Body).Decode(&result)
	return response.StatusCode, result
}

func admission(bound *boundRun, callID string) map[string]any {
	return map[string]any{"identity": bound.identity, "messageID": "msg_assistant", "callID": callID, "tool": "write", "input": map[string]any{"path": "a.txt", "content": "x"}}
}

func TestGatewayRequiresTheServiceCredential(t *testing.T) {
	bound := newBoundRun(t)
	g := newTestGateway(t, bound)
	for name, token := range map[string]string{"missing": "", "wrong": "not-the-token"} {
		t.Run(name, func(t *testing.T) {
			status, _ := g.post(t, "/v1/native/admit", token, admission(bound, "call-1"))
			if status != http.StatusForbidden {
				t.Fatalf("status %d", status)
			}
		})
	}
	var grants int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&grants); err != nil || grants != 0 {
		t.Fatalf("an unauthenticated call created a grant: %d %v", grants, err)
	}
	request, _ := http.NewRequest(http.MethodGet, g.url()+"/v1/native/admit", nil)
	request.Header.Set("Authorization", "Bearer "+g.token)
	if response, err := http.DefaultClient.Do(request); err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("a non-POST request was served: %v %v", response, err)
	}
}

func TestGatewayAdmitsOnceAndReportsReceipts(t *testing.T) {
	bound := newBoundRun(t)
	g := newTestGateway(t, bound)
	status, grant := g.post(t, "/v1/native/admit", g.token, admission(bound, "call-1"))
	if status != http.StatusOK || grant["execute"] != true {
		t.Fatalf("first admission %d %v", status, grant)
	}
	if status, replay := g.post(t, "/v1/native/admit", g.token, admission(bound, "call-1")); status != http.StatusOK || replay["execute"] != false || replay["grantID"] != grant["grantID"] {
		t.Fatalf("a replay must not execute again: %d %v", status, replay)
	}
	receipt := map[string]any{"identity": bound.identity, "grantID": grant["grantID"], "identityDigest": grant["identityDigest"], "outcome": "completed", "nativeStarted": true}
	if status, result := g.post(t, "/v1/native/finish", g.token, receipt); status != http.StatusOK || result["acknowledged"] != true {
		t.Fatalf("receipt %d %v", status, result)
	}
	foreign := bound.identity
	foreign.RequestID = "msg_other"
	receipt["identity"] = foreign
	if status, _ := g.post(t, "/v1/native/finish", g.token, receipt); status != http.StatusConflict {
		t.Fatalf("a receipt for another request identity was accepted: %d", status)
	}
}

func TestGatewayRefusesWorkOutsideTheBoundRequest(t *testing.T) {
	bound := newBoundRun(t)
	g := newTestGateway(t, bound)
	foreign := admission(bound, "call-1")
	identity := bound.identity
	identity.ProfileID = "kd_other"
	foreign["identity"] = identity
	if status, _ := g.post(t, "/v1/native/admit", g.token, foreign); status != http.StatusConflict {
		t.Fatalf("an unknown profile was admitted: %d", status)
	}
	unknown := admission(bound, "call-1")
	unknown["surprise"] = true
	if status, _ := g.post(t, "/v1/native/admit", g.token, unknown); status != http.StatusConflict {
		t.Fatalf("an unknown field was accepted: %d", status)
	}
	if err := bound.authority.CloseAdmission(context.Background(), bound.identity); err != nil {
		t.Fatal(err)
	}
	if status, _ := g.post(t, "/v1/native/admit", g.token, admission(bound, "call-2")); status != http.StatusConflict {
		t.Fatalf("a tool was admitted after admission closed: %d", status)
	}
}

func TestRecoverReceiptsDeliversPersistedReceiptsOnce(t *testing.T) {
	bound := newBoundRun(t)
	ctx := context.Background()
	grant, err := bound.authority.AdmitNative(ctx, providerrequest.NativeCall{Identity: bound.identity, MessageID: "msg_assistant", CallID: "call-1", Tool: "write", Input: json.RawMessage(`{"path":"a.txt"}`)})
	if err != nil {
		t.Fatal(err)
	}
	started := true
	outbox := t.TempDir()
	receipt := nativeReceipt{Identity: bound.identity, GrantID: grant.ID, IdentityDigest: grant.Digest, Outcome: "completed", NativeStarted: &started}
	raw, _ := json.Marshal(receipt)
	path := filepath.Join(outbox, grant.ID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverReceipts(ctx, outbox, bound.authority, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("a delivered receipt stayed in the outbox: %v", err)
	}
	var open int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_native_grants WHERE outcome IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatalf("recovered receipt did not close the grant: %d %v", open, err)
	}
	// A receipt the plugin already delivered is acknowledged again harmlessly.
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverReceipts(ctx, outbox, bound.authority, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverReceiptsKeepsReceiptsItCannotDeliver(t *testing.T) {
	bound := newBoundRun(t)
	outbox := t.TempDir()
	started := true
	unknown, _ := json.Marshal(nativeReceipt{Identity: bound.identity, GrantID: "native_unknown", IdentityDigest: "digest", Outcome: "completed", NativeStarted: &started})
	for name, content := range map[string][]byte{"unknown.json": unknown, "garbage.json": []byte("{")} {
		if err := os.WriteFile(filepath.Join(outbox, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := recoverReceipts(context.Background(), outbox, bound.authority, nil); err == nil {
		t.Fatal("undeliverable receipts must be reported")
	}
	for _, name := range []string{"unknown.json", "garbage.json"} {
		if _, err := os.Stat(filepath.Join(outbox, name)); err != nil {
			t.Fatalf("%s was discarded: %v", name, err)
		}
	}
}

func TestPluginIsInstalledAsALocalPackageAndPreservesUserPlugins(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "plugin")
	for range 2 {
		if err := installPlugin(directory); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"index.js", "package.json"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	env, err := serviceEnvironment([]string{`OPENCODE_CONFIG_CONTENT={"plugins":["/user/plugin"],"providers":{}}`}, directory)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Plugins   []string        `json:"plugins"`
		Formatter bool            `json:"formatter"`
		Providers json.RawMessage `json:"providers"`
	}
	for _, entry := range env {
		if raw, ok := bytes.CutPrefix([]byte(entry), []byte("OPENCODE_CONFIG_CONTENT=")); ok {
			if err := json.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(config.Plugins) != 2 || config.Plugins[0] != "/user/plugin" || config.Plugins[1] != directory || config.Formatter || string(config.Providers) != "{}" {
		t.Fatalf("effective configuration %#v", config)
	}
}

func TestRecoverReceiptsForOneRequestIgnoresAnotherRequestsReceipts(t *testing.T) {
	bound := newBoundRun(t)
	outbox := t.TempDir()
	started := true
	stranger := bound.identity
	stranger.RequestID = "msg_stranger"
	raw, _ := json.Marshal(nativeReceipt{Identity: stranger, GrantID: "native_unknown", IdentityDigest: "digest", Outcome: "completed", NativeStarted: &started})
	if err := os.WriteFile(filepath.Join(outbox, "stranger.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recoverReceipts(context.Background(), outbox, bound.authority, &bound.identity); err != nil {
		t.Fatalf("an unrelated undeliverable receipt blocked this request: %v", err)
	}
}
