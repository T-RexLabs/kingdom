package server

import (
	"context"
	"github.com/google/uuid"
	"io"
	"kingdom/internal/central/sources"
	"net/http/httptest"
	"testing"
)

type sourceFixture struct {
	calls int
	actor string
}

func (s *sourceFixture) Status(context.Context, string, string) (sources.Status, error) {
	s.calls++
	return sources.Status{State: "empty"}, nil
}
func (s *sourceFixture) Upload(_ context.Context, actor, _, _, _, _ string, _ io.Reader) (sources.Metadata, error) {
	s.calls++
	s.actor = actor
	return sources.Metadata{ID: uuid.NewString()}, nil
}
func TestSourceRoutesRequireProjectOwnerAndCSRFBeforeRuntime(t *testing.T) {
	f := setup(t)
	source := &sourceFixture{}
	f.server.cfg.Sources = source
	path := BasePath + "/projects/" + f.project.ID + "/source"
	if w := f.call("GET", path, "", "", false); w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, owner := range []bool{false, true} {
		r := f.request("POST", path+"/file?name=a.txt", "file", uuid.NewString(), owner)
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("X-Actor-ID", f.owner.ID)
		w := httptest.NewRecorder()
		f.server.ServeHTTP(w, r)
		if owner {
			if w.Code != 201 || source.actor != f.owner.ID {
				t.Fatal(w.Code)
			}
		} else if w.Code != 403 {
			t.Fatal("member write allowed", w.Code)
		}
	}
	r := f.request("POST", path+"/zip", "zip", uuid.NewString(), true)
	r.Header.Set("Content-Type", "application/zip")
	r.Header.Del("X-CSRF-Token")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != 403 || source.calls != 2 || f.factoryCalls.Load() != 0 {
		t.Fatal("source boundary bypassed", w.Code, source.calls)
	}
}
