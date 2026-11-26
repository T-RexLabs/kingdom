package personal

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/subscription/chatgpt"
)

func setupHandoffs(t *testing.T) (*signinFixture, *Handoffs, *capability.Registry) {
	t.Helper()
	f := setupSignIns(t)
	ctx := context.Background()
	v, err := chatgpt.NewVault(ctx, f.db, bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	service, err := chatgpt.NewService(ctx, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close(ctx) })
	f.s.cfg.Accounts.chatgpt = service
	registry, err := capability.New(ctx, capability.Config{DatabasePath: filepath.Join(t.TempDir(), "caps.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close(ctx) })
	h, err := NewHandoffs(ctx, HandoffConfig{Accounts: f.s.cfg.Accounts, Connections: f.s.cfg.Connections, CheckSession: f.s.cfg.CheckSession, HostID: v.HostID(), Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	return f, h, registry
}
func TestHandoffRequiresLiveOwnerSessionAndEncryptsPairingCode(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	ctx := context.Background()
	key := uuid.NewString()
	v, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", key)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.PairingCode) != 43 || v.Status != "pending" {
		t.Fatal("pairing unavailable")
	}
	var encrypted []byte
	if err = f.db.QueryRow(`SELECT code FROM central_chatgpt_handoffs WHERE id=?`, v.ID).Scan(&encrypted); err != nil || bytes.Contains(encrypted, []byte(v.PairingCode)) {
		t.Fatal("pairing secret is not encrypted")
	}
	target, err := h.Prepare(ctx, v.PairingCode)
	if err != nil || target.OwnerID != f.owner || target.AttemptID != v.ID || !strings.HasPrefix(target.HostID, "urn:uuid:") {
		t.Fatal("pairing target differs")
	}
	again, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", key)
	if err != nil || again.ID != v.ID || again.PairingCode != v.PairingCode {
		t.Fatal("pairing retry changed identity")
	}
	if _, err = h.Get(ctx, f.owner, "foreign-session", v.ID); err == nil {
		t.Fatal("foreign session inspected pairing")
	}
	if err = h.Cancel(ctx, f.owner, "foreign-session", v.ID); err == nil {
		t.Fatal("foreign session canceled pairing")
	}
	f.valid.Store(false)
	if _, err = h.Prepare(ctx, v.PairingCode); err == nil {
		t.Fatal("revoked login still admits transfer")
	}
}
func TestHandoffRejectsMalformedTransferWithoutConsumingAttempt(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	ctx := context.Background()
	v, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`null`, `{}`, `{"version":1,"version":2}`} {
		if _, err = h.Complete(ctx, v.PairingCode, json.RawMessage(payload)); err == nil {
			t.Fatal("malformed transfer admitted")
		}
	}
	r, err := h.read(ctx, f.owner, f.session, v.ID)
	if err != nil || r.state != "pending" || len(r.transferHash) != 0 {
		t.Fatal("malformed request consumed pairing")
	}
}
func TestHandoffCanceledStateRemainsCanceledAfterExpiry(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	ctx := context.Background()
	v, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Cancel(ctx, f.owner, f.session, v.ID); err != nil {
		t.Fatal(err)
	}
	r, err := h.read(ctx, f.owner, f.session, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.expires = time.Now().Add(-time.Minute).UnixNano()
	view, err := h.view(ctx, r, false)
	if err != nil || view.Status != "canceled" {
		t.Fatalf("cancellation changed to %s", view.Status)
	}
}
func TestClosedAccountServiceCannotBeginHandoff(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	ctx := context.Background()
	if err := f.s.cfg.Accounts.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", uuid.NewString()); !errors.Is(err, ErrDenied) {
		t.Fatal("closed account service admitted a pairing")
	}
}
func TestPortalRejectsCrossProviderReuseOfIdempotencyKey(t *testing.T) {
	f, h, registry := setupHandoffs(t)
	portal := &Portal{Copilot: f.s, ChatGPT: h, Accounts: f.s.cfg.Accounts, Connections: f.s.cfg.Connections, Registry: registry}
	key := uuid.NewString()
	ctx := context.Background()
	if _, err := portal.Begin(ctx, f.owner, f.session, "openai", "Personal Account", key); err != nil {
		t.Fatal(err)
	}
	if _, err := portal.Begin(ctx, f.owner, f.session, "github-copilot", "Personal Account", key); err == nil {
		t.Fatal("same key created another provider authorization")
	}
	if f.begins.Load() != 0 {
		t.Fatal("conflicting retry reached native authorization")
	}
}
func TestHandoffFingerprintIsNotAnUnkeyedTokenDigest(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	r := handoffRecord{id: uuid.NewString(), owner: f.owner}
	digest := sha256.Sum256([]byte("protected transfer"))
	sealed, err := h.sealFingerprint(r, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, digest[:]) {
		t.Fatal("unkeyed credential digest stored")
	}
	r.transferHash = sealed
	if !h.matchesFingerprint(r, digest[:]) {
		t.Fatal("exact fingerprint does not match")
	}
	r.id = uuid.NewString()
	if h.matchesFingerprint(r, digest[:]) {
		t.Fatal("fingerprint was not bound to exact handoff")
	}
}

func controlledHandoffTransfer(t *testing.T, f *signinFixture, h *Handoffs) json.RawMessage {
	t.Helper()
	ctx := context.Background()
	if err := f.s.cfg.Accounts.chatgpt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	v, err := chatgpt.NewVault(ctx, f.db, bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{4}, 32))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", "controlled"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := chatgpt.NewService(ctx, v, &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body any
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			body = map[string]any{"issuer": "https://auth.openai.com", "authorization_endpoint": "https://auth.openai.com/api/accounts/authorize", "token_endpoint": "https://auth.openai.com/api/accounts/oauth/token", "jwks_uri": "https://auth.openai.com/keys", "revocation_endpoint": "https://auth.openai.com/revoke", "id_token_signing_alg_values_supported": []string{"EdDSA"}}
		case "/keys":
			body = jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: key.Public(), KeyID: "controlled", Algorithm: "EdDSA", Use: "sig"}}}
		case "/revoke":
			body = map[string]any{}
		default:
			t.Errorf("unexpected controlled provider path %s", r.URL.Path)
			return nil, errors.New("unexpected request")
		}
		raw, _ := json.Marshal(body)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw)), Request: r}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	f.s.cfg.Accounts.chatgpt = service
	t.Cleanup(func() { service.Close(ctx) })
	claims, _ := json.Marshal(map[string]any{"iss": "https://auth.openai.com", "aud": "oaiapp_controlled", "sub": "controlled-personal-account", "email": "account@example.test", "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := signer.Sign(claims)
	if err != nil {
		t.Fatal(err)
	}
	idToken, err := signed.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"version": 1, "transfer_id": uuid.NewString(), "target_host_id": h.config.HostID, "client_id": "oaiapp_controlled", "subject": "controlled-personal-account", "email": "account@example.test", "access_token": "controlled-access-token", "refresh_token": "controlled-refresh-token", "id_token": idToken, "scope": "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct", "expires_at": time.Now().Add(30 * time.Minute).Unix(), "earliest_refresh_at": 0})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestHandoffImportsOneVerifiedAccountAndCannotReplayAfterDisconnect(t *testing.T) {
	f, h, registry := setupHandoffs(t)
	ctx := context.Background()
	payload := controlledHandoffTransfer(t, f, h)
	pending, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	done, err := h.Complete(ctx, pending.PairingCode, payload)
	if err != nil || done.Status != "complete" || done.Connection == nil {
		t.Fatalf("verified transfer failed: %v", err)
	}
	again, err := h.Complete(ctx, pending.PairingCode, payload)
	if err != nil || again.Connection == nil || again.Connection.ID != done.Connection.ID {
		t.Fatal("exact transfer retry changed connection")
	}
	m, ref, err := f.s.cfg.Connections.ResolveAccount(ctx, f.owner, done.Connection.ID, 1)
	if err != nil || m.Definition.Provider != "openai" {
		t.Fatal("imported connection differs")
	}
	token, reg, err := f.s.cfg.Accounts.chatgpt.Access(ctx, f.owner, ref.ID)
	if err != nil || token.Bearer() != "controlled-access-token" || reg.Revision != ref.Revision {
		t.Fatal("imported account did not retain exact token owner")
	}
	portal := &Portal{ChatGPT: h, Accounts: f.s.cfg.Accounts, Connections: f.s.cfg.Connections, Registry: registry}
	disconnectKey := uuid.NewString()
	confirmed, err := portal.Disconnect(ctx, f.owner, m.ID, disconnectKey)
	if err != nil || !confirmed {
		t.Fatal("controlled revocation failed")
	}
	if _, err = h.Complete(ctx, pending.PairingCode, payload); err == nil {
		t.Fatal("signed-out account replay reported usable completion")
	}
	if confirmed, err = portal.Disconnect(ctx, f.owner, m.ID, disconnectKey); err != nil || !confirmed {
		t.Fatal("exact disconnect retry did not retain disabled account authority")
	}
}

type blockedHandoffRegistry struct {
	entered chan string
	release chan struct{}
}

func (b *blockedHandoffRegistry) RevokeConnection(ctx context.Context, id string, _ int64) error {
	b.entered <- id
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHandoffSessionLossAfterPublicationFencesAndJoinsBeforeReturn(t *testing.T) {
	f, h, _ := setupHandoffs(t)
	ctx := context.Background()
	payload := controlledHandoffTransfer(t, f, h)
	pending, err := h.Begin(ctx, f.owner, f.session, "Personal ChatGPT", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	var checks atomic.Int64
	h.config.CheckSession = func(ctx context.Context, owner, session string) error {
		if checks.Add(1) >= 4 {
			return ErrDenied
		}
		return f.s.cfg.CheckSession(ctx, owner, session)
	}
	registry := &blockedHandoffRegistry{entered: make(chan string, 1), release: make(chan struct{})}
	h.config.Registry = registry
	completed := make(chan error, 1)
	go func() { _, err := h.Complete(ctx, pending.PairingCode, payload); completed <- err }()
	var connection string
	select {
	case connection = <-registry.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("session loss did not revoke issued connection")
	}
	items, err := f.s.cfg.Connections.List(ctx, f.owner)
	if err != nil || len(items) != 1 || items[0].ID != connection || !items[0].Disabled {
		t.Fatal("connection was not fenced before joining model admissions")
	}
	select {
	case <-completed:
		t.Fatal("handoff returned before model admissions joined")
	default:
	}
	close(registry.release)
	if err = <-completed; !errors.Is(err, ErrDenied) {
		t.Fatal("session loss returned a completed account")
	}
	_, ref, err := f.s.cfg.Connections.ResolveAccountForDisconnect(ctx, f.owner, connection, 1)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := f.s.cfg.Accounts.chatgpt.Registration(ctx, f.owner, ref.ID)
	if err != nil || reg.State == "ready" {
		t.Fatal("session loss left imported account usable")
	}
}
