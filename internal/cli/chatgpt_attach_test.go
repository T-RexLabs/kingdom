package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/personal"
	"kingdom/internal/securefs"
	"kingdom/internal/subscription/chatgpt"
	_ "modernc.org/sqlite"
)

type attachTransport func(*http.Request) (*http.Response, error)

func (f attachTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type attachFixture struct {
	t                              *testing.T
	db                             *sql.DB
	connections                    *connections.Store
	target                         *chatgpt.Service
	handoffs                       *personal.Handoffs
	owner, session, host, root, ca string
	server                         *httptest.Server
	attempt                        personal.SignIn
	authorizations, exchanges      atomic.Int32
	mu                             sync.Mutex
	query                          url.Values
	hosts                          []string
	payloads                       [][]byte
	loseComplete                   bool
	transport                      attachTransport
}

func newAttachFixture(t *testing.T) *attachFixture {
	t.Helper()
	f := &attachFixture{t: t, session: uuid.NewString(), root: filepath.Join(t.TempDir(), "helper")}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{91}, 32))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "attach-test"))
	if err != nil {
		t.Fatal(err)
	}
	f.transport = func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "auth.openai.com" {
			t.Errorf("unexpected OAuth origin: %s", r.URL.Host)
			return nil, chatgpt.ErrDenied
		}
		var body any
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			body = map[string]any{"issuer": "https://auth.openai.com", "authorization_endpoint": "https://auth.openai.com/api/accounts/authorize", "token_endpoint": "https://auth.openai.com/api/accounts/oauth/token", "jwks_uri": "https://auth.openai.com/keys", "revocation_endpoint": "https://auth.openai.com/revoke", "id_token_signing_alg_values_supported": []string{"EdDSA"}}
		case "/keys":
			body = jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: key.Public(), KeyID: "attach-test", Algorithm: "EdDSA", Use: "sig"}}}
		case "/api/accounts/oauth/token":
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			f.mu.Lock()
			q := f.query
			f.mu.Unlock()
			hash := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if r.Form.Get("grant_type") != "authorization_code" || r.Form.Get("client_id") != "oaiapp_kingdom_attach" || r.Form.Get("resource") != "https://api.openai.com/v1" || r.Form.Get("redirect_uri") != q.Get("redirect_uri") || base64.RawURLEncoding.EncodeToString(hash[:]) != q.Get("code_challenge") {
				t.Error("OAuth exchange lost its exact binding")
				return nil, chatgpt.ErrDenied
			}
			n := f.exchanges.Add(1)
			claims, _ := json.Marshal(map[string]any{"iss": "https://auth.openai.com", "aud": "oaiapp_kingdom_attach", "sub": "controlled-account", "nonce": q.Get("nonce"), "exp": time.Now().Add(time.Hour).Unix()})
			signed, err := signer.Sign(claims)
			if err != nil {
				return nil, err
			}
			id, err := signed.CompactSerialize()
			if err != nil {
				return nil, err
			}
			body = map[string]any{"access_token": "controlled-access-token", "refresh_token": fmt.Sprintf("controlled-refresh-%d", n), "id_token": id, "expires_in": 3600, "token_type": "Bearer", "scope": "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct"}
		case "/revoke":
			body = map[string]any{}
		default:
			t.Errorf("unexpected OAuth path: %s", r.URL.Path)
			return nil, chatgpt.ErrDenied
		}
		raw, _ := json.Marshal(body)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)), Request: r}, nil
	}
	f.db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if err != nil {
		t.Fatal(err)
	}
	f.db.SetMaxOpenConns(1)
	t.Cleanup(func() { f.db.Close() })
	ctx := context.Background()
	identity := catalog.Identity{Issuer: "https://fixture.example", Subject: "operator"}
	c, err := catalog.New(ctx, f.db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := c.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	f.owner = owner.ID
	f.connections, err = connections.New(ctx, f.db, bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	vault, err := chatgpt.NewVault(ctx, f.db, bytes.Repeat([]byte{5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	f.host = vault.HostID()
	f.target, err = chatgpt.NewService(ctx, vault, &http.Client{Transport: f.transport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.target.Close(ctx) })
	accounts, err := personal.New(ctx, personal.Config{DB: f.db, Key: bytes.Repeat([]byte{6}, 32), ChatGPT: f.target})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { accounts.Close(ctx) })
	registry, err := capability.New(ctx, capability.Config{DatabasePath: filepath.Join(t.TempDir(), "capabilities.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close(ctx) })
	f.handoffs, err = personal.NewHandoffs(ctx, personal.HandoffConfig{Accounts: accounts, Connections: f.connections, Registry: registry, HostID: f.host, CheckSession: func(_ context.Context, actor, session string) error {
		if actor != f.owner || session != f.session {
			return personal.ErrDenied
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.nextAttempt()
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/central/v1/personal-accounts/handoff" || r.Header.Get("Authorization") != "Bearer "+f.attempt.PairingCode {
			http.Error(w, "denied", 403)
			return
		}
		var req struct {
			Action   string          `json:"action"`
			Transfer json.RawMessage `json:"transfer"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		var result any
		var err error
		switch req.Action {
		case "prepare":
			result, err = f.handoffs.Prepare(r.Context(), f.attempt.PairingCode)
		case "complete":
			f.mu.Lock()
			f.payloads = append(f.payloads, bytes.Clone(req.Transfer))
			f.mu.Unlock()
			result, err = f.handoffs.Complete(r.Context(), f.attempt.PairingCode, req.Transfer)
			if err == nil && f.loseComplete {
				f.loseComplete = false
				conn, _, e := w.(http.Hijacker).Hijack()
				if e == nil {
					conn.Close()
				}
				return
			}
		default:
			err = personal.ErrDenied
		}
		if err != nil {
			http.Error(w, "unavailable", 403)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	t.Cleanup(f.server.Close)
	f.ca = filepath.Join(t.TempDir(), "ca.crt")
	if err = os.WriteFile(f.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	oldRead, oldOpen, oldNew := readChatGPTPairingCode, openChatGPTBrowser, newChatGPTAttachService
	readChatGPTPairingCode = func(io.Writer) (string, error) { return f.attempt.PairingCode, nil }
	newChatGPTAttachService = func(ctx context.Context, v *chatgpt.Vault) (*chatgpt.Service, error) {
		return chatgpt.NewService(ctx, v, &http.Client{Transport: f.transport})
	}
	openChatGPTBrowser = func(ctx context.Context, raw string) error {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Host != "auth.openai.com" || u.Path != "/api/accounts/authorize" {
			return chatgpt.ErrDenied
		}
		q := u.Query()
		callback, err := url.Parse(q.Get("redirect_uri"))
		if err != nil || callback.Scheme != "http" || callback.Hostname() != "127.0.0.1" || callback.Path != "/auth/callback" {
			return chatgpt.ErrDenied
		}
		f.mu.Lock()
		f.query = q
		f.hosts = append(f.hosts, q.Get("ext_agent_host_id"))
		f.mu.Unlock()
		f.authorizations.Add(1)
		callback.RawQuery = url.Values{"state": {q.Get("state")}, "code": {"fixture-code"}, "client_id": {"oaiapp_kingdom_attach"}}.Encode()
		req, err := http.NewRequestWithContext(ctx, "GET", callback.String(), nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		io.Copy(io.Discard, res.Body)
		if res.StatusCode != 200 {
			return chatgpt.ErrDenied
		}
		return nil
	}
	t.Cleanup(func() { readChatGPTPairingCode, openChatGPTBrowser, newChatGPTAttachService = oldRead, oldOpen, oldNew })
	return f
}
func (f *attachFixture) nextAttempt() {
	f.t.Helper()
	var err error
	f.attempt, err = f.handoffs.Begin(context.Background(), f.owner, f.session, "Controlled ChatGPT", uuid.NewString())
	if err != nil {
		f.t.Fatal(err)
	}
}
func (f *attachFixture) run() (string, error) {
	f.t.Helper()
	var out bytes.Buffer
	err := runChatGPTCommand(context.Background(), &out, []string{"attach", "--service", f.server.URL, "--ca-file", f.ca, "--state-root", f.root})
	return out.String(), err
}
func (f *attachFixture) profile() string {
	hash := sha256.Sum256([]byte(f.server.URL))
	return filepath.Join(f.root, hex.EncodeToString(hash[:16]))
}
func (f *attachFixture) state() chatGPTAttachState {
	f.t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.profile(), f.attempt.ID, "state.json"))
	if err != nil {
		f.t.Fatal(err)
	}
	var state chatGPTAttachState
	if json.Unmarshal(raw, &state) != nil {
		f.t.Fatal("invalid helper state")
	}
	return state
}

func TestChatGPTAttachRecoversLostCompletionWithoutReauthorization(t *testing.T) {
	f := newAttachFixture(t)
	f.loseComplete = true
	output, err := f.run()
	if err == nil {
		t.Fatal("lost target response reported success")
	}
	state := f.state()
	key, err := os.ReadFile(filepath.Join(f.profile(), "key"))
	if err != nil {
		t.Fatal(err)
	}
	vault, err := chatgpt.OpenFileVault(context.Background(), f.profile(), key)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := vault.Get(context.Background(), f.owner, state.RegistrationID)
	vault.Close()
	if err != nil || reg.State != "reauth" {
		t.Fatalf("source was not fenced: %+v %v", reg, err)
	}
	again, err := f.run()
	output += again
	if err != nil {
		t.Fatal(err)
	}
	if f.authorizations.Load() != 1 || f.exchanges.Load() != 1 {
		t.Fatal("retry reauthorized a transferred session")
	}
	if len(f.payloads) != 2 || !bytes.Equal(f.payloads[0], f.payloads[1]) {
		t.Fatal("retry changed the selected session")
	}
	list, err := f.connections.List(context.Background(), f.owner)
	if err != nil || len(list) != 1 {
		t.Fatalf("target connection duplicated: %d %v", len(list), err)
	}
	_, ref, err := f.connections.ResolveAccount(context.Background(), f.owner, list[0].ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := f.target.Access(context.Background(), f.owner, ref.ID)
	if err != nil || token.Bearer() != "controlled-access-token" {
		t.Fatalf("target did not own transferred session: %v", err)
	}
	if strings.Contains(output, f.attempt.PairingCode) || strings.Contains(output, "controlled-access") || strings.Contains(output, "controlled-refresh") || strings.Contains(output, "auth.openai.com") {
		t.Fatal("helper output exposed credentials or authorization URL")
	}
	if !f.state().Acknowledged {
		t.Fatal("source acknowledgement was not retained")
	}
}

func TestChatGPTAttachRecoversStateFailureAfterVaultAcknowledgement(t *testing.T) {
	f := newAttachFixture(t)
	old := saveChatGPTAttachState
	failed := false
	saveChatGPTAttachState = func(root *securefs.Root, state chatGPTAttachState) error {
		if state.Acknowledged && !failed {
			failed = true
			return io.ErrClosedPipe
		}
		return old(root, state)
	}
	t.Cleanup(func() { saveChatGPTAttachState = old })
	if _, err := f.run(); err == nil {
		t.Fatal("failed local acknowledgement write reported success")
	}
	if !f.state().Completed || f.state().Acknowledged {
		t.Fatal("completion proof was not saved before token cleanup")
	}
	if _, err := f.run(); err != nil {
		t.Fatalf("acknowledged source could not recover: %v", err)
	}
	if f.authorizations.Load() != 1 || f.exchanges.Load() != 1 || len(f.payloads) != 1 {
		t.Fatal("ack recovery replaced the transferred session")
	}
	if !f.state().Acknowledged {
		t.Fatal("ack recovery did not finish state")
	}
}

func TestChatGPTAttachKeepsSourceHostStableAcrossAttempts(t *testing.T) {
	f := newAttachFixture(t)
	if _, err := f.run(); err != nil {
		t.Fatal(err)
	}
	f.nextAttempt()
	if _, err := f.run(); err != nil {
		t.Fatal(err)
	}
	if len(f.hosts) != 2 || f.hosts[0] != f.hosts[1] || f.hosts[0] == f.host {
		t.Fatal("helper host changed per attempt or copied the target host")
	}
}

func TestChatGPTAttachRejectsRedirectsAndForeignOrigins(t *testing.T) {
	for _, origin := range []string{"http://example.com", "https://user:password@example.com", "https://example.com/path", "https://example.com?", "https://example.com#", "https://example.com/?token=value"} {
		if _, err := chatGPTServiceOrigin(origin); err == nil {
			t.Fatalf("non-origin service accepted: %q", origin)
		}
	}
	var leaked atomic.Int32
	sink := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); w.WriteHeader(200) }))
	defer sink.Close()
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
	}))
	defer first.Close()
	ca := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	code := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{33}, 32))
	old := readChatGPTPairingCode
	readChatGPTPairingCode = func(io.Writer) (string, error) { return code, nil }
	t.Cleanup(func() { readChatGPTPairingCode = old })
	var out bytes.Buffer
	if err := runChatGPTCommand(context.Background(), &out, []string{"attach", "--service", first.URL, "--ca-file", ca, "--state-root", filepath.Join(t.TempDir(), "helper")}); err == nil {
		t.Fatal("redirect accepted")
	}
	if leaked.Load() != 0 || strings.Contains(out.String(), code) {
		t.Fatal("pairing credential followed a redirect or reached output")
	}
}

func TestChatGPTAttachRejectsReadableStateAndKey(t *testing.T) {
	for _, name := range []string{"key", "state.json"} {
		t.Run(name, func(t *testing.T) {
			f := newAttachFixture(t)
			if _, err := f.run(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.profile(), name)
			if name == "state.json" {
				path = filepath.Join(f.profile(), f.attempt.ID, name)
			}
			if err := os.Chmod(path, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := f.run(); !errors.Is(err, chatgpt.ErrDenied) {
				t.Fatalf("readable %s accepted: %v", name, err)
			}
			if f.authorizations.Load() != 1 {
				t.Fatal("invalid local storage caused another sign-in")
			}
		})
	}
}
