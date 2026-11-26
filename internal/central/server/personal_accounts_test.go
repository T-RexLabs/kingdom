package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/personal"
)

type personalAccountsFixture struct {
	called                                         int
	owner, session, provider, label, key, id, code string
	payload                                        json.RawMessage
}

func (p *personalAccountsFixture) Providers() []string { return []string{"openai", "github-copilot"} }
func (p *personalAccountsFixture) Begin(_ context.Context, owner, session, provider, label, key string) (personal.SignIn, error) {
	p.called++
	p.owner, p.session, p.provider, p.label, p.key = owner, session, provider, label, key
	return personal.SignIn{ID: uuid.NewString(), Provider: provider, Status: "pending"}, nil
}
func (p *personalAccountsFixture) Get(_ context.Context, owner, session, id string) (personal.SignIn, error) {
	p.called++
	p.owner, p.session, p.id = owner, session, id
	return personal.SignIn{ID: id, Status: "pending"}, nil
}
func (p *personalAccountsFixture) Cancel(_ context.Context, owner, session, id string) error {
	p.called++
	p.owner, p.session, p.id = owner, session, id
	return nil
}
func (p *personalAccountsFixture) Disconnect(_ context.Context, owner, id, key string) (bool, error) {
	p.called++
	p.owner, p.id, p.key = owner, id, key
	return false, nil
}
func (p *personalAccountsFixture) PrepareHandoff(_ context.Context, code string) (personal.HandoffTarget, error) {
	p.called++
	p.code = code
	return personal.HandoffTarget{AttemptID: uuid.NewString()}, nil
}
func (p *personalAccountsFixture) CompleteHandoff(_ context.Context, code string, payload json.RawMessage) (personal.SignIn, error) {
	p.called++
	p.code, p.payload = code, payload
	return personal.SignIn{ID: uuid.NewString(), Status: "complete"}, nil
}

func TestPersonalAccountRoutesInjectTrustedOwnerAndExactSession(t *testing.T) {
	f := setup(t)
	p := &personalAccountsFixture{}
	f.server.cfg.PersonalAccounts = p
	key := uuid.NewString()
	r := f.request("POST", BasePath+"/personal-accounts/sign-ins", `{"provider":"github-copilot","label":"Personal Copilot"}`, key, false)
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != 201 || p.called != 1 || p.owner != f.member.ID || p.session != f.memberSession.SessionID || p.provider != "github-copilot" || p.key != key {
		t.Fatalf("trusted session not injected: code %d", w.Code)
	}
	id := uuid.NewString()
	r = f.request("GET", BasePath+"/personal-accounts/sign-ins/"+id, "", "", true)
	w = httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != 200 || p.owner != f.owner.ID || p.session != f.ownerSession.SessionID || p.id != id {
		t.Fatal("attempt inspection omitted exact session")
	}
}

func TestPersonalAccountMutationRejectsForgedActorAndMissingCSRF(t *testing.T) {
	for _, body := range []string{`{"provider":"openai","label":"ChatGPT","owner_id":"foreign"}`, `{"provider":"openai","label":"ChatGPT","session_id":"foreign"}`} {
		f := setup(t)
		p := &personalAccountsFixture{}
		f.server.cfg.PersonalAccounts = p
		r := f.request("POST", BasePath+"/personal-accounts/sign-ins", body, uuid.NewString(), true)
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if w.Code != 400 || p.called != 0 {
			t.Fatal("body supplied account authority")
		}
	}
	f := setup(t)
	p := &personalAccountsFixture{}
	f.server.cfg.PersonalAccounts = p
	r := f.request("POST", BasePath+"/personal-accounts/sign-ins", `{"provider":"openai","label":"ChatGPT"}`, uuid.NewString(), true)
	r.Header.Del("X-CSRF-Token")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != 403 || p.called != 0 {
		t.Fatal("account mutation bypassed CSRF")
	}
}

func TestHandoffRouteRequiresTLSAndOneBearerCredential(t *testing.T) {
	for _, change := range []func(*http.Request){func(r *http.Request) { r.TLS = nil }, func(r *http.Request) { r.Header.Del("Authorization") }, func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+strings.Repeat("b", 43)) }} {
		f := setup(t)
		p := &personalAccountsFixture{}
		f.server.cfg.PersonalAccounts = p
		r := httptest.NewRequest("POST", "https://kingdom.example"+BasePath+"/personal-accounts/handoff", strings.NewReader(`{"action":"prepare"}`))
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 43))
		r.Header.Set("Content-Type", "application/json")
		change(r)
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if w.Code != 403 || p.called != 0 {
			t.Fatalf("invalid pairing transport reached account owner: %d", w.Code)
		}
	}
}

func TestHandoffRouteDoesNotAcceptAmbiguousOrMissingTransfer(t *testing.T) {
	for _, body := range []string{`{"action":"complete"}`, `{"action":"prepare","action":"complete","transfer":{}}`, `{"action":"prepare","transfer":{}}`, `{"action":"prepare","owner_id":"foreign"}`} {
		f := setup(t)
		p := &personalAccountsFixture{}
		f.server.cfg.PersonalAccounts = p
		r := httptest.NewRequest("POST", "https://kingdom.example"+BasePath+"/personal-accounts/handoff", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 43))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if w.Code != 400 || p.called != 0 {
			t.Fatalf("ambiguous handoff reached account owner: code %d body %s", w.Code, body)
		}
	}
}
