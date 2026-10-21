package selections

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/execution/access"
	_ "modernc.org/sqlite"
)

type cachedModelsFunc func(context.Context, connections.Metadata, string, string) error

func (f cachedModelsFunc) Validate(ctx context.Context, m connections.Metadata, model, effort string) error {
	return f(ctx, m, model, effort)
}
func TestResolverPinsExactRevisionAndRechecksMembership(t *testing.T) {
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
	definition := connections.Definition{Label: "Models", Provider: "provider", AuthKind: "api_key", Endpoint: "https://models.example/v1"}
	connection, err := store.Create(ctx, member.ID, connections.Personal, definition, []byte("revision-one-secret"))
	if err != nil {
		t.Fatal(err)
	}
	var validatedRevision int64
	resolver, err := New(store, cachedModelsFunc(func(_ context.Context, m connections.Metadata, model, effort string) error {
		validatedRevision = m.Revision
		if model != "provider/model" || effort != "medium" {
			return errors.New("model not cached for revision")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	request := access.RequestedSelection{Model: "provider/model", ThinkingEffort: "medium", Connection: &access.ConnectionReference{ID: connection.ID, Revision: 1}}
	if _, err = resolver.Resolve(ctx, owner.ID, project.ID, request); err == nil {
		t.Fatal("team owner inherited personal connection")
	}
	if _, err = store.Revise(ctx, member.ID, connection.ID, 1, definition, []byte("revision-two-secret")); err != nil {
		t.Fatal(err)
	}
	pinned, err := resolver.Resolve(ctx, member.ID, project.ID, request)
	if err != nil || pinned.Connection.Revision != 1 || validatedRevision != 1 {
		t.Fatal("credential rotation changed pin", pinned, err)
	}
	request.ThinkingEffort = "unsupported"
	if _, err = resolver.Resolve(ctx, member.ID, project.ID, request); err == nil {
		t.Fatal("uncached model effort accepted")
	}
	request.ThinkingEffort = "medium"
	if err = c.RevokeMembership(ctx, owner.ID, project.ID, member.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.Resolve(ctx, member.ID, project.ID, request); err == nil {
		t.Fatal("revoked member created selection")
	}
	if _, _, err = store.ResolveForProject(ctx, member.ID, project.ID, pinned.Connection.ID, pinned.Connection.Revision); err == nil {
		t.Fatal("saved snapshot bypassed revocation at dispatch")
	}
}
