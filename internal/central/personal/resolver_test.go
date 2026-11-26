package personal

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/accounts"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
)

type routeFunc func(connections.Metadata, string) (modelproxy.Route, error)

func (f routeFunc) PinAccountRoute(_ context.Context, _, _ string, m connections.Metadata, model, _ string) (modelproxy.Route, error) {
	return f(m, model)
}
func TestResolverDoesNotReplacePersonalAuthorizationOrRoute(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	c, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := c.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	member, err := c.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	project, err := c.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	store, err := connections.New(ctx, db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	ref, err := s.SaveCopilot(ctx, member.ID, accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "account-one", Refresh: "account-one"})
	if err != nil {
		t.Fatal(err)
	}
	d := connections.Definition{Label: "My Account", Provider: "github-copilot", AuthKind: "copilot_oauth"}
	m, err := store.CreateAccountRequest(ctx, member.ID, d, ref, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	route := modelproxy.Route{Protocol: "openai_responses", Endpoint: "https://api.githubcopilot.com", Model: "model"}
	r := &Resolver{Connections: store, Accounts: s, Routes: routeFunc(func(got connections.Metadata, model string) (modelproxy.Route, error) {
		if got != m || model != "github-copilot/model" {
			return modelproxy.Route{}, ErrDenied
		}
		return route, nil
	})}
	b := modelproxy.Binding{Access: access.Snapshot{Version: 1, ActorID: member.ID, ProjectID: project.ID, Selection: access.Selection{Model: "github-copilot/model", Connection: access.Connection{ID: m.ID, Revision: m.Revision, OwnerID: m.OwnerID, Scope: "personal", Provider: "github-copilot"}}}}
	if got, err := r.Pin(ctx, b); err != nil || got != route {
		t.Fatal(got, err)
	}
	if _, err = r.Resolve(ctx, b, route); err != nil {
		t.Fatal(err)
	}
	foreign := b
	foreign.Access.ActorID = owner.ID
	if _, err = r.Pin(ctx, foreign); err == nil {
		t.Fatal("team owner used another member's subscription")
	}
	changed := route
	changed.Endpoint = "https://api.individual.githubcopilot.com"
	if _, err = r.Resolve(ctx, b, changed); err == nil {
		t.Fatal("route changed after capability pin")
	}
	second, err := s.SaveCopilot(ctx, member.ID, accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "account-two", Refresh: "account-two"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReviseAccountRequest(ctx, member.ID, m.ID, 1, d, second, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	// Reconnect does not replace the registration in already captured work.
	if err = s.fence(ctx, member.ID, ref); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Resolve(ctx, b, route); err == nil {
		t.Fatal("old work used replacement authorization")
	}
	if _, err = r.Pin(ctx, b); err == nil {
		t.Fatal("disconnected original account admitted")
	}
}

func TestAccountCredentialCommitHasExactRetryIdentity(t *testing.T) {
	ctx := context.Background()
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	s, err := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	ref := connections.AccountReference{ID: uuid.NewString(), Revision: 1}
	credential := accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "first", Refresh: "first"}
	for i := 0; i < 2; i++ {
		if err = s.SaveCopilotReference(ctx, "owner", ref, credential); err != nil {
			t.Fatal(err)
		}
	}
	credential.Access = "replacement"
	credential.Refresh = "replacement"
	if err = s.SaveCopilotReference(ctx, "owner", ref, credential); err == nil {
		t.Fatal("same authorization identity replaced its credential")
	}
	if err = s.SaveCopilotReference(ctx, "other", ref, credential); err == nil {
		t.Fatal("another owner claimed same authorization")
	}
}
