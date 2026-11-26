package resultsource

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/publishedresult"
	_ "modernc.org/sqlite"
)

type resultReader struct {
	calls   int
	close   func()
	resolve func()
}

func (r *resultReader) Resolve(context.Context, string) (publishedresult.ResultSnapshot, error) {
	r.calls++
	if r.resolve != nil {
		r.resolve()
	}
	return publishedresult.ResultSnapshot{RunID: "run", SnapshotID: "snapshot"}, nil
}
func (r *resultReader) Open(context.Context, string, string, string) (io.ReadCloser, error) {
	r.calls++
	return resultBody{Reader: strings.NewReader("canonical"), close: r.close}, nil
}

type resultBody struct {
	io.Reader
	close func()
}

func (b resultBody) Close() error {
	if b.close != nil {
		b.close()
	}
	return nil
}

type projectReader struct{ reader publishedresult.Reader }

func (p projectReader) PublishedResults() publishedresult.Reader   { return p.reader }
func (projectReader) ServeHTTP(http.ResponseWriter, *http.Request) {}
func TestResultSourceChecksCurrentProjectMembershipBeforeAndAfterStartup(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	authority, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := authority.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	member, err := authority.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	project, err := authority.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := auth.New(ctx, auth.Config{DB: db, Catalog: authority, PublicOrigin: "https://kingdom.example"})
	if err != nil {
		t.Fatal(err)
	}
	defer sessions.Close()
	reader := new(resultReader)
	factoryCalls := 0
	revokeOnStartup := false
	source, err := New(authority, sessions, func(_ context.Context, id string, a api.Authenticator) (http.Handler, error) {
		factoryCalls++
		if id != project.ID || a == nil {
			t.Fatal("untrusted project lookup")
		}
		if revokeOnStartup {
			if err := authority.RevokeMembership(ctx, owner.ID, project.ID, member.ID); err != nil {
				t.Fatal(err)
			}
		}
		return projectReader{reader}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.Resolve(ctx, member.ID, project.ID, "run"); err == nil || factoryCalls != 0 {
		t.Fatal("foreign member reached runtime")
	}
	if err = authority.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	result, err := source.Resolve(ctx, member.ID, project.ID, "run")
	if err != nil || result.SnapshotID != "snapshot" {
		t.Fatal(err)
	}
	revokeOnStartup = true
	if _, err = source.Resolve(ctx, member.ID, project.ID, "run"); err == nil {
		t.Fatal("startup revocation ignored")
	}
	if reader.calls != 1 {
		t.Fatal("revoked member reached result reader")
	}
	revokeOnStartup = false
	if err = authority.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	reader.resolve = func() {
		if err := authority.RevokeMembership(ctx, owner.ID, project.ID, member.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = source.Resolve(ctx, member.ID, project.ID, "run"); err == nil {
		t.Fatal("metadata read revocation ignored")
	}
}
