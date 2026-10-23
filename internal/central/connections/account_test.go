package connections

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
)

func accountDefinition() Definition {
	return Definition{Label: "My ChatGPT Account", Provider: "openai", AuthKind: "openai_oauth"}
}

func TestAccountConnectionsRetainPrivateTypedReferences(t *testing.T) {
	s, db, owner, member := setup(t)
	ctx := context.Background()
	c, err := catalog.New(ctx, db, catalog.Config{})
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
	first := AccountReference{ID: uuid.NewString(), Revision: 2}
	key := uuid.NewString()
	m, err := s.CreateAccountRequest(ctx, member.ID, accountDefinition(), first, key)
	if err != nil || m.Scope != Personal {
		t.Fatalf("create account: %+v %v", m, err)
	}
	replay, err := s.CreateAccountRequest(ctx, member.ID, accountDefinition(), first, key)
	if err != nil || replay != m {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	if _, err = s.CreateAccountRequest(ctx, member.ID, accountDefinition(), AccountReference{ID: uuid.NewString(), Revision: 2}, key); !errors.Is(err, mutation.ErrConflict) {
		t.Fatalf("changed retry: %v", err)
	}
	metadata, ref, err := s.ResolveAccountForProject(ctx, member.ID, project.ID, m.ID, 1)
	if err != nil || metadata != m || ref != first {
		t.Fatalf("resolve account: %+v %+v %v", metadata, ref, err)
	}
	encoded, err := json.Marshal(m)
	if err != nil || strings.Contains(string(encoded), first.ID) {
		t.Fatal("private account reference leaked to public metadata")
	}
	var encrypted []byte
	if err = db.QueryRow(`SELECT credential FROM central_connection_revisions WHERE connection_id=?`, m.ID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encrypted), first.ID) {
		t.Fatal("account reference was not encrypted")
	}
	if _, _, err = s.ResolveForProject(ctx, member.ID, project.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("account reference treated as API key: %v", err)
	}
	if _, _, err = s.Resolve(ctx, member.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("account reference exposed as raw credential: %v", err)
	}
	if _, _, err = s.ResolveAccountForProject(ctx, owner.ID, project.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("team owner accessed personal account: %v", err)
	}
	second := AccountReference{ID: uuid.NewString(), Revision: 1}
	updated, err := s.ReviseAccountRequest(ctx, member.ID, m.ID, 1, accountDefinition(), second, uuid.NewString())
	if err != nil || updated.Revision != 2 {
		t.Fatalf("revise account: %+v %v", updated, err)
	}
	for revision, expected := range map[int64]AccountReference{1: first, 2: second} {
		_, actual, err := s.ResolveAccountForProject(ctx, member.ID, project.ID, m.ID, revision)
		if err != nil || actual != expected {
			t.Fatalf("revision %d changed: %+v %v", revision, actual, err)
		}
	}
	if err = s.Disable(ctx, member.ID, m.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.ResolveAccountForProject(ctx, member.ID, project.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled account resolved: %v", err)
	}
	if _, _, err = s.ResolveAccount(ctx, member.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled account resolved without project: %v", err)
	}
	disabled, ref, err := s.ResolveAccountForDisconnect(ctx, member.ID, m.ID, 1)
	if err != nil || !disabled.Disabled || ref != first {
		t.Fatalf("disabled account revocation cannot recover: %+v %+v %v", disabled, ref, err)
	}
	if _, _, err = s.ResolveAccountForDisconnect(ctx, owner.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("team owner read disabled personal account: %v", err)
	}
}

func TestAccountConnectionsRejectRawTokensAndInvalidReferences(t *testing.T) {
	s, _, owner, _ := setup(t)
	ctx := context.Background()
	for _, scope := range []Scope{Personal, Team} {
		if _, err := s.Create(ctx, owner.ID, scope, accountDefinition(), []byte("raw-token")); !errors.Is(err, ErrInvalid) {
			t.Fatalf("raw OAuth credential accepted in %s: %v", scope, err)
		}
	}
	for _, ref := range []AccountReference{{}, {ID: "not-an-id", Revision: 1}, {ID: uuid.NewString()}, {ID: uuid.NewString(), Revision: -1}} {
		if _, err := s.CreateAccountRequest(ctx, owner.ID, accountDefinition(), ref, uuid.NewString()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid reference accepted: %+v %v", ref, err)
		}
	}
	ref := AccountReference{ID: uuid.NewString(), Revision: 1}
	for _, d := range []Definition{definition(), {Label: "Other", Provider: "other", AuthKind: "openai_oauth"}, {Label: "Redirect", Provider: "openai", AuthKind: "openai_oauth", Endpoint: "https://other.example"}} {
		if _, err := s.CreateAccountRequest(ctx, owner.ID, d, ref, uuid.NewString()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid account definition accepted: %+v %v", d, err)
		}
	}
	m, err := s.Create(ctx, owner.ID, Team, definition(), []byte("team-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReviseAccountRequest(ctx, owner.ID, m.ID, 1, accountDefinition(), ref, uuid.NewString()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("team key converted into personal account: %v", err)
	}
}
