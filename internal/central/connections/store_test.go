package connections

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/central/catalog"
	_ "modernc.org/sqlite"
)

func setup(t *testing.T) (*Store, *sql.DB, catalog.User, catalog.User) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "catalog.db"))+"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	identity := catalog.Identity{Issuer: "https://identity.example", Subject: "owner"}
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
	s, err := New(ctx, db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s, db, owner, member
}

func definition() Definition {
	return Definition{Label: "Models", Provider: "openai", AuthKind: "api_key", Endpoint: "https://models.example/v1"}
}

func TestPersonalCredentialsRemainPrivateFromTeamOwner(t *testing.T) {
	s, db, owner, member := setup(t)
	ctx := context.Background()
	m, err := s.Create(ctx, member.ID, Personal, definition(), []byte("private-credential"))
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range []string{owner.ID, "missing"} {
		if _, _, err := s.Resolve(ctx, actor, m.ID, 1); !errors.Is(err, ErrDenied) {
			t.Fatalf("cross-user resolve: %v", err)
		}
		if err := s.Disable(ctx, actor, m.ID); !errors.Is(err, ErrDenied) {
			t.Fatalf("cross-user disable: %v", err)
		}
		if _, err := s.Revise(ctx, actor, m.ID, 1, definition(), []byte("replacement")); !errors.Is(err, ErrDenied) {
			t.Fatalf("cross-user write: %v", err)
		}
	}
	list, err := s.List(ctx, owner.ID)
	if err != nil || len(list) != 0 {
		t.Fatalf("private metadata visible: %+v %v", list, err)
	}
	_, secret, err := s.Resolve(ctx, member.ID, m.ID, 1)
	if err != nil || string(secret.Bytes()) != "private-credential" {
		t.Fatalf("owner resolve: %v", err)
	}
	if strings.Contains(fmt.Sprintf("%v %#v", secret, secret), "private-credential") {
		t.Fatal("secret leaked to formatting")
	}
	if _, err := json.Marshal(secret); err == nil {
		t.Fatal("secret serialized")
	}
	var stored []byte
	if err := db.QueryRow(`SELECT credential FROM central_connection_revisions WHERE connection_id=?`, m.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, secret.Bytes()) {
		t.Fatal("plaintext stored")
	}
}

func TestTeamCredentialAccessAndImmutableRevision(t *testing.T) {
	s, _, owner, member := setup(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, member.ID, Team, definition(), []byte("key")); !errors.Is(err, ErrDenied) {
		t.Fatalf("member team create: %v", err)
	}
	m, err := s.Create(ctx, owner.ID, Team, definition(), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.Revise(ctx, owner.ID, m.ID, 1, definition(), []byte("new"))
	if err != nil || updated.Revision != 2 {
		t.Fatalf("revise: %+v %v", updated, err)
	}
	if _, err := s.Revise(ctx, owner.ID, m.ID, 1, definition(), []byte("stale")); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale revision: %v", err)
	}
	for revision, want := range map[int64]string{1: "old", 2: "new"} {
		_, secret, err := s.Resolve(ctx, member.ID, m.ID, revision)
		if err != nil || string(secret.Bytes()) != want {
			t.Fatalf("revision %d: %v", revision, err)
		}
	}
	if _, _, err := s.Resolve(ctx, member.ID, m.ID, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unpinned resolve: %v", err)
	}
	if err := s.Disable(ctx, member.ID, m.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("member team disable: %v", err)
	}
	if err := s.Disable(ctx, owner.ID, m.ID); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []int64{1, 2} {
		if _, _, err := s.Resolve(ctx, member.ID, m.ID, revision); !errors.Is(err, ErrDenied) {
			t.Fatalf("disabled resolve: %v", err)
		}
	}
}

func TestWrongKeyAndTamperedBindingFailClosed(t *testing.T) {
	s, db, owner, _ := setup(t)
	ctx := context.Background()
	m, err := s.Create(ctx, owner.ID, Personal, definition(), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(ctx, db, bytes.Repeat([]byte{2}, 32)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("wrong key: %v", err)
	}
	reopened, err := New(ctx, db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopened.Resolve(ctx, owner.ID, m.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE central_connection_revisions SET definition='{}' WHERE connection_id=?`, m.ID); err == nil {
		t.Fatal("revision mutation accepted")
	}
	if _, err := db.Exec(`DROP TRIGGER central_connection_revision_immutable`); err != nil {
		t.Fatal(err)
	}
	d := definition()
	d.Provider = "other"
	encoded, _ := json.Marshal(d)
	if _, err := db.Exec(`UPDATE central_connection_revisions SET definition=? WHERE connection_id=?`, string(encoded), m.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Resolve(ctx, owner.ID, m.ID, 1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("swapped binding: %v", err)
	}
}

func TestDisabledUsersAndInvalidInputCannotResolve(t *testing.T) {
	s, db, owner, member := setup(t)
	ctx := context.Background()
	m, err := s.Create(ctx, owner.ID, Team, definition(), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE central_users SET disabled=1 WHERE id=?`, member.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Resolve(ctx, member.ID, m.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled user: %v", err)
	}
	for _, endpoint := range []string{"http://models.example", "https://user:secret@models.example", "https://models.example?key=secret", "file:///tmp/key"} {
		d := definition()
		d.Endpoint = endpoint
		if _, err := s.Create(ctx, owner.ID, Personal, d, []byte("key")); !errors.Is(err, ErrInvalid) {
			t.Fatalf("endpoint accepted: %q %v", endpoint, err)
		}
	}
}

func TestConcurrentRefreshHasOneSuccessor(t *testing.T) {
	s, _, owner, _ := setup(t)
	ctx := context.Background()
	m, err := s.Create(ctx, owner.ID, Personal, definition(), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			_, err := s.Revise(ctx, owner.ID, m.ID, 1, definition(), []byte("new"))
			results <- err
		}()
	}
	close(start)
	first, second := <-results, <-results
	if !(first == nil && errors.Is(second, ErrConflict) || second == nil && errors.Is(first, ErrConflict)) {
		t.Fatalf("concurrent refresh: %v, %v", first, second)
	}
}

func TestOAuthConnectionsCannotRedirectProviderTokens(t *testing.T) {
	s, _, owner, _ := setup(t)
	for _, d := range []Definition{
		{Label: "OpenAI", Provider: "openai", AuthKind: "openai_oauth", Endpoint: "https://other.example"},
		{Label: "Other", Provider: "other", AuthKind: "openai_oauth"},
		{Label: "Copilot", Provider: "github-copilot", AuthKind: "copilot_oauth", Endpoint: "https://other.example"},
	} {
		if _, err := s.Create(context.Background(), owner.ID, Personal, d, []byte("token")); !errors.Is(err, ErrInvalid) {
			t.Fatalf("provider token redirect accepted: %v", err)
		}
	}
}
