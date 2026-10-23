package server

import (
	"context"
	"github.com/google/uuid"
	"kingdom/internal/central/repositories"
	"kingdom/internal/central/sources"
	"net/http/httptest"
	"testing"
)

type repositoryFixture struct {
	calls int
	actor string
}

func (s *repositoryFixture) Status(context.Context, string, string) (repositories.RepositoryStatus, error) {
	s.calls++
	return repositories.RepositoryStatus{Available: true, RepositoryID: 42}, nil
}
func (s *repositoryFixture) Import(_ context.Context, actor, project, key, ref string) (sources.Metadata, error) {
	s.calls++
	s.actor = actor
	return sources.Metadata{Kind: "github", Repository: &sources.RepositoryOrigin{RepositoryID: 42}}, nil
}
func TestRepositoryImportRequiresOwnerAndCSRFWithoutStartingRuntime(t *testing.T) {
	f := setup(t)
	service := &repositoryFixture{}
	f.server.cfg.Repositories = service
	path := BasePath + "/projects/" + f.project.ID + "/repository"
	if w := f.call("GET", path, "", "", false); w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, owner := range []bool{false, true} {
		r := f.request("POST", path+"/import", `{"ref":"main"}`, uuid.NewString(), owner)
		r.Header.Set("X-Actor-ID", f.owner.ID)
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if owner {
			if w.Code != 201 || service.actor != f.owner.ID {
				t.Fatal(w.Code)
			}
		} else if w.Code != 403 {
			t.Fatal("member imported", w.Code)
		}
	}
	r := f.request("POST", path+"/import", `{"ref":"main"}`, uuid.NewString(), true)
	r.Header.Del("X-CSRF-Token")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != 403 || service.calls != 2 || f.factoryCalls.Load() != 0 {
		t.Fatal("repository boundary bypassed", w.Code, service.calls)
	}
}
