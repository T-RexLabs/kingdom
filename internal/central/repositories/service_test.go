package repositories

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
	"kingdom/internal/central/sources"
	_ "modernc.org/sqlite"
)

type unusedSource struct{}

func (unusedSource) ReplayRepository(context.Context, string, string, string, int64, string) (sources.Metadata, bool, error) {
	return sources.Metadata{}, false, nil
}

func (unusedSource) ImportRepository(context.Context, string, string, string, sources.RepositoryOrigin, io.Reader) (sources.Metadata, error) {
	return sources.Metadata{}, ErrUnavailable
}
func TestImportOperationPinsCommitAcrossRetryAndRestart(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	authority, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := authority.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	project, err := authority.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	client := fixture(t)
	client.grants[project.ID] = Grant{ProjectID: project.ID, InstallationID: 2, RepositoryID: 3}
	service, err := New(ctx, db, authority, client, unusedSource{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := uuid.NewString()
	id, pin, err := service.importOperation(ctx, owner.ID, project.ID, key, "main", "")
	if err != nil || pin != "" || id == "" {
		t.Fatalf("begin: %s %s %v", id, pin, err)
	}
	first := strings.Repeat("a", 40)
	_, pin, err = service.importOperation(ctx, owner.ID, project.ID, key, "main", first)
	if err != nil || pin != first {
		t.Fatal("commit not pinned")
	}
	service, err = New(ctx, db, authority, client, unusedSource{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	replay, pin, err := service.importOperation(ctx, owner.ID, project.ID, key, "main", strings.Repeat("b", 40))
	if err != nil || pin != first || replay != id {
		t.Fatal("branch rotation changed retry")
	}
	if _, _, err = service.importOperation(ctx, owner.ID, project.ID, key, "other", ""); !errors.Is(err, mutation.ErrConflict) {
		t.Fatal("key reuse allowed")
	}
	if _, _, err = service.importOperation(ctx, "foreign", project.ID, key, "main", ""); !errors.Is(err, catalog.ErrDenied) {
		t.Fatalf("foreign actor: %v", err)
	}
	service.sources = recoveredSource{expected: id}
	client.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
		t.Fatal("committed import contacted GitHub")
		return nil, nil
	})
	if result, err := service.Import(ctx, owner.ID, project.ID, key, "main"); err != nil || result.ID != "committed-source" {
		t.Fatalf("committed source recovery: %+v %v", result, err)
	}
	client.grants[project.ID] = Grant{ProjectID: project.ID, InstallationID: 2, RepositoryID: 4}
	if _, _, err = service.importOperation(ctx, owner.ID, project.ID, key, "main", ""); !errors.Is(err, mutation.ErrConflict) {
		t.Fatal("grant changed retained operation")
	}
}

type recoveredSource struct {
	unusedSource
	expected string
}

func (s recoveredSource) ReplayRepository(_ context.Context, actor, project, key string, repository int64, commit string) (sources.Metadata, bool, error) {
	if key != s.expected || repository != 3 || commit != strings.Repeat("a", 40) {
		return sources.Metadata{}, false, ErrInvalid
	}
	return sources.Metadata{ID: "committed-source"}, true, nil
}
