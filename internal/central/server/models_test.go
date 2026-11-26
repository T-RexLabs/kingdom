package server

import (
	"context"
	"github.com/google/uuid"
	"kingdom/internal/central/modelcatalog"
	"net/http/httptest"
	"testing"
)

type modelDiscoveryFixture struct {
	calls int
	actor string
}

func (m *modelDiscoveryFixture) List(_ context.Context, actor, project, id string, revision int64) (modelcatalog.Snapshot, error) {
	m.calls++
	m.actor = actor
	return modelcatalog.Snapshot{ConnectionID: id, Revision: revision, Models: []modelcatalog.Model{}}, nil
}
func (m *modelDiscoveryFixture) Refresh(ctx context.Context, actor, project, id string, revision int64) (modelcatalog.Snapshot, error) {
	return m.List(ctx, actor, project, id, revision)
}
func TestCentralModelDiscoveryUsesSessionBeforeProjectRuntime(t *testing.T) {
	f := setup(t)
	models := &modelDiscoveryFixture{}
	f.server.cfg.Models = models
	path := BasePath + "/projects/" + f.project.ID + "/connections/" + uuid.NewString() + "/revisions/1/models"
	for _, method := range []string{"GET", "POST"} {
		target := path
		if method == "POST" {
			target += "/refresh"
		}
		r := f.request(method, target, "", "", false)
		r.Header.Set("X-Actor-ID", f.owner.ID)
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if w.Code != 200 || models.actor != f.member.ID || f.factoryCalls.Load() != 0 {
			t.Fatal(w.Code, w.Body.String(), models.actor)
		}
	}
	r := f.request("POST", path+"/refresh", "", "", false)
	r.Header.Del("X-CSRF-Token")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code == 200 || models.calls != 2 {
		t.Fatal("csrf bypassed discovery boundary")
	}
	if err := f.catalog.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	f.server.ServeHTTP(w, f.request("GET", path, "", "", false))
	if w.Code == 200 || models.calls != 2 {
		t.Fatal("revoked user reached discovery")
	}
}
