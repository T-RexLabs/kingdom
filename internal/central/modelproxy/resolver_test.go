package modelproxy

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/execution/access"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

type catalogAuthority struct{ catalog *catalog.Store }

func (a catalogAuthority) Check(ctx context.Context, b Binding, _ Stage) error {
	return a.catalog.Authorize(ctx, b.Access.ActorID, b.Access.ProjectID, catalog.UseProject)
}
func TestCatalogRevocationStopsActiveProxyAndKeepsRevisionPinned(t *testing.T) {
	upstreamStopped := make(chan struct{})
	broker, unused, server, _, fake := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer revision-one-secret" {
			t.Error("credential revision drift")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: ready\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamStopped)
	}, true)
	ctx := context.Background()
	pins, identity, e := broker.cfg.Registry.Lookup(ctx, unused.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = broker.Revoke(ctx, unused.ID); e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "catalog.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	external := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	catalogStore, e := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{external}})
	if e != nil {
		t.Fatal(e)
	}
	owner, e := catalogStore.BootstrapOwner(ctx, external, "Owner")
	if e != nil {
		t.Fatal(e)
	}
	member, e := catalogStore.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: external.Issuer, Subject: "member"}, "Member")
	if e != nil {
		t.Fatal(e)
	}
	project, e := catalogStore.CreateProject(ctx, owner.ID, "Project")
	if e != nil {
		t.Fatal(e)
	}
	if e = catalogStore.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); e != nil {
		t.Fatal(e)
	}
	store, e := connections.New(ctx, db, bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	definition := connections.Definition{Label: "Models", Provider: "openai", AuthKind: "api_key", Endpoint: fake.route.Endpoint}
	connection, e := store.Create(ctx, member.ID, connections.Personal, definition, []byte("revision-one-secret"))
	if e != nil {
		t.Fatal(e)
	}
	binding := pins.Binding
	binding.Preparation.Owner.InvocationID = "catalog-invocation"
	binding.Access = access.Snapshot{Version: 1, ActorID: member.ID, ProjectID: project.ID, Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: connection.ID, Revision: 1, OwnerID: member.ID, Scope: "personal", Provider: "openai"}}}
	resolver := &APIKeyResolver{Store: store, Providers: map[string]string{"openai": "openai"}}
	broker.cfg.Resolver = resolver
	broker.cfg.Authorizer = catalogAuthority{catalogStore}
	lease, e := broker.Issue(ctx, binding)
	if e != nil {
		t.Fatal(e)
	}
	if e = broker.Activate(ctx, lease.ID, *identity); e != nil {
		t.Fatal(e)
	}
	if _, e = store.Revise(ctx, member.ID, connection.ID, 1, definition, []byte("revision-two-secret")); e != nil {
		t.Fatal(e)
	}
	route, e := resolver.Pin(ctx, binding)
	if e != nil {
		t.Fatal(e)
	}
	secret, e := resolver.Resolve(ctx, binding, route)
	if e != nil || secret.token != "revision-one-secret" {
		t.Fatal("rotation substituted revision", e)
	}
	foreign := binding
	foreign.Access.ActorID = owner.ID
	if _, e = resolver.Pin(ctx, foreign); e == nil {
		t.Fatal("owner read personal secret")
	}
	response := proxyRequest(t, server, lease, `{"model":"model","stream":true}`)
	defer response.Body.Close()
	buffer := make([]byte, 13)
	if _, e = io.ReadFull(response.Body, buffer); e != nil {
		t.Fatal(e)
	}
	if e = catalogStore.RevokeMembership(ctx, owner.ID, project.ID, member.ID); e != nil {
		t.Fatal(e)
	}
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if e = broker.RevokeAccess(deadline, member.ID, project.ID); e != nil {
		t.Fatal(e)
	}
	select {
	case <-upstreamStopped:
	case <-deadline.Done():
		t.Fatal("revoked member's stream remains open")
	}
	if e = broker.RequireClosed(ctx, binding); e != nil {
		t.Fatal(e)
	}
	if _, e = resolver.Resolve(ctx, binding, route); e == nil {
		t.Fatal("membership revocation bypassed")
	}
	if _, e = broker.Issue(ctx, binding); e == nil {
		t.Fatal("revoked member issued fresh capability")
	}
}
