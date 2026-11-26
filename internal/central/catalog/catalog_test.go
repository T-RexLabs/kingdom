package catalog

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

var aliceIdentity = Identity{Issuer: "https://identity.example", Subject: "alice"}
var bobIdentity = Identity{Issuer: "https://identity.example", Subject: "bob"}

func testStore(t *testing.T, cfg Config) (*Store, *sql.DB) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "catalog.db")}
	q := uri.Query()
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "foreign_keys(1)")
	uri.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s, err := New(context.Background(), db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, db
}
func bootstrap(t *testing.T, s *Store) User {
	t.Helper()
	u, err := s.BootstrapOwner(context.Background(), aliceIdentity, "Alice")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestBootstrapRequiresExplicitAllowlistAndNeverLinksByDisplayName(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{})
	if _, err := s.BootstrapOwner(ctx, aliceIdentity, "Alice"); !errors.Is(err, ErrDenied) {
		t.Fatalf("unconfigured bootstrap=%v", err)
	}
	s, db := testStore(t, Config{InitialOwners: []Identity{aliceIdentity}})
	owner := bootstrap(t, s)
	if _, err := s.BootstrapOwner(ctx, bobIdentity, "Alice"); !errors.Is(err, ErrDenied) {
		t.Fatalf("unlisted bootstrap=%v", err)
	}
	bob, err := s.CreateUser(ctx, owner.ID, bobIdentity, "Alice")
	if err != nil {
		t.Fatal(err)
	}
	if bob.ID == owner.ID || bob.TeamOwner {
		t.Fatal("display name linked or elevated identity")
	}
	other, err := s.CreateUser(ctx, owner.ID, Identity{Issuer: "https://other.example", Subject: "alice"}, "Alice")
	if err != nil {
		t.Fatal(err)
	}
	if other.ID == owner.ID {
		t.Fatal("subject linked across issuers")
	}
	if _, err := db.Exec(`UPDATE central_identities SET user_id=? WHERE issuer=? AND subject=?`, bob.ID, aliceIdentity.Issuer, aliceIdentity.Subject); err == nil {
		t.Fatal("identity mapping changed")
	}
	if _, err := db.Exec(`DELETE FROM central_identities WHERE issuer=? AND subject=?`, aliceIdentity.Issuer, aliceIdentity.Subject); err == nil {
		t.Fatal("identity mapping deleted")
	}
	if _, err := s.CreateUser(ctx, bob.ID, Identity{Issuer: "x", Subject: "y"}, "Y"); !errors.Is(err, ErrDenied) {
		t.Fatalf("member created user: %v", err)
	}
}

func TestConcurrentBootstrapIsAtomicAcrossStores(t *testing.T) {
	ctx := context.Background()
	cfg := Config{InitialOwners: []Identity{aliceIdentity, bobIdentity}}
	s, db := testStore(t, cfg)
	other, err := New(ctx, db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		user User
		err  error
	}
	results := make(chan outcome, 16)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			store := s
			id := aliceIdentity
			if i%2 == 1 {
				store = other
				id = bobIdentity
			}
			u, e := store.BootstrapOwner(ctx, id, "Owner")
			results <- outcome{u, e}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	winner := ""
	success := 0
	for r := range results {
		if r.err != nil {
			if !errors.Is(r.err, ErrDenied) {
				t.Fatal(r.err)
			}
			continue
		}
		success++
		if winner != "" && winner != r.user.ID {
			t.Fatal("multiple initial owners")
		}
		winner = r.user.ID
	}
	if success != 8 {
		t.Fatalf("successful idempotent winner logins=%d", success)
	}
	var users, claims int
	if err := db.QueryRow(`SELECT count(*) FROM central_users`).Scan(&users); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM central_bootstrap`).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if users != 1 || claims != 1 {
		t.Fatalf("users=%d claims=%d", users, claims)
	}
}

func TestConcurrentDuplicateIdentityKeepsOneUser(t *testing.T) {
	ctx := context.Background()
	s, db := testStore(t, Config{InitialOwners: []Identity{aliceIdentity}})
	owner := bootstrap(t, s)
	var wg sync.WaitGroup
	results := make(chan User, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.CreateUser(ctx, owner.ID, bobIdentity, "Bob")
			results <- u
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	for u := range results {
		if id != "" && id != u.ID {
			t.Fatal("duplicate identity created another user")
		}
		id = u.ID
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM central_users`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("users=%d", count)
	}
}

func TestProjectAuthorizationRevocationAndOwnerScope(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{InitialOwners: []Identity{aliceIdentity}})
	owner := bootstrap(t, s)
	bob, err := s.CreateUser(ctx, owner.ID, bobIdentity, "Bob")
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.CreateProject(ctx, owner.ID, "A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateProject(ctx, owner.ID, "B")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetMembership(ctx, owner.ID, a.ID, bob.ID, Member); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(ctx, bob.ID, a.ID, UseProject); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		user, project string
		permission    Permission
	}{{bob.ID, b.ID, UseProject}, {"unknown", a.ID, UseProject}, {bob.ID, a.ID, ManageProject}, {bob.ID, a.ID, "invented"}} {
		if err := s.Authorize(ctx, check.user, check.project, check.permission); !errors.Is(err, ErrDenied) {
			t.Fatalf("unexpected authorization: %#v error=%v", check, err)
		}
	}
	if err := s.SetMembership(ctx, bob.ID, a.ID, bob.ID, Owner); !errors.Is(err, ErrDenied) {
		t.Fatalf("self promotion=%v", err)
	}
	if err := s.SetMembership(ctx, owner.ID, a.ID, bob.ID, Owner); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateProject(ctx, bob.ID, "Cannot Create"); !errors.Is(err, ErrDenied) {
		t.Fatalf("project owner gained team authority=%v", err)
	}
	if err := s.SetMembership(ctx, bob.ID, b.ID, bob.ID, Owner); !errors.Is(err, ErrDenied) {
		t.Fatalf("cross project grant=%v", err)
	}
	projects, err := s.Projects(ctx, bob.ID)
	if err != nil || len(projects) != 1 || projects[0].ID != a.ID {
		t.Fatalf("project list=%v error=%v", projects, err)
	}
	if err := s.RevokeMembership(ctx, owner.ID, a.ID, bob.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(ctx, bob.ID, a.ID, UseProject); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked member authorized=%v", err)
	}
	if err := s.SetMembership(ctx, bob.ID, a.ID, bob.ID, Owner); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked owner reinstated itself=%v", err)
	}
	projects, err = s.Projects(ctx, bob.ID)
	if err != nil || len(projects) != 0 {
		t.Fatalf("revoked project listed=%v error=%v", projects, err)
	}
}

func TestCatalogSurvivesReopenWithoutReassigningOwner(t *testing.T) {
	ctx := context.Background()
	s, db := testStore(t, Config{InitialOwners: []Identity{aliceIdentity}})
	owner := bootstrap(t, s)
	project, err := s.CreateProject(ctx, owner.ID, "Persisted")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(ctx, db, Config{InitialOwners: []Identity{bobIdentity}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.ResolveIdentity(ctx, aliceIdentity)
	if err != nil || got != owner {
		t.Fatalf("identity=%v error=%v", got, err)
	}
	if _, err := reopened.BootstrapOwner(ctx, bobIdentity, "Bob"); !errors.Is(err, ErrDenied) {
		t.Fatalf("new config reassigned owner: %v", err)
	}
	if err := reopened.Authorize(ctx, owner.ID, project.ID, ManageProject); err != nil {
		t.Fatal(err)
	}
}

func TestProjectRetainsAnOwner(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t, Config{InitialOwners: []Identity{aliceIdentity}})
	owner := bootstrap(t, s)
	p, err := s.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeMembership(ctx, owner.ID, p.ID, owner.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("removed final owner: %v", err)
	}
	if err := s.SetMembership(ctx, owner.ID, p.ID, owner.ID, Member); !errors.Is(err, ErrDenied) {
		t.Fatalf("demoted final owner: %v", err)
	}
	if err := s.Authorize(ctx, owner.ID, p.ID, ManageProject); err != nil {
		t.Fatal(err)
	}
}
