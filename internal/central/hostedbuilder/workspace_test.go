package hostedbuilder

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	data "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

type workspaceFake struct {
	request       *native.CreateWorkspaceRequest
	id            string
	creates, gets int
	lost          bool
}

func (f *workspaceFake) CreateWorkspace(_ context.Context, r *native.CreateWorkspaceRequest, _ ...grpc.CallOption) (*native.CreateWorkspaceResponse, error) {
	f.creates++
	if f.request != nil && (f.request.Name != r.Name || f.request.RequestId != r.RequestId) {
		return nil, errors.New("changed replay")
	}
	f.request = r
	if f.lost {
		f.lost = false
		return nil, errors.New("lost reply")
	}
	return &native.CreateWorkspaceResponse{Workspace: &data.Workspace{Metadata: &data.ObjectMeta{Id: f.id, Name: r.Name}}}, nil
}
func (f *workspaceFake) GetWorkspace(_ context.Context, r *native.GetWorkspaceRequest, _ ...grpc.CallOption) (*native.GetWorkspaceResponse, error) {
	f.gets++
	return &native.GetWorkspaceResponse{Workspace: &data.Workspace{Metadata: &data.ObjectMeta{Id: f.id, Name: r.Name}}}, nil
}
func mappingDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	id := uuid.NewString()
	if _, e = db.Exec(`CREATE TABLE central_projects(id TEXT PRIMARY KEY);INSERT INTO central_projects VALUES(?)`, id); e != nil {
		t.Fatal(e)
	}
	if e = initializeMappings(context.Background(), db); e != nil {
		t.Fatal(e)
	}
	return db, id
}
func TestWorkspaceReplayKeepsIdentityAndRejectsDatabaseReplacement(t *testing.T) {
	ctx := context.Background()
	db, id := mappingDB(t)
	f := &workspaceFake{id: uuid.NewString(), lost: true}
	now := time.Now()
	if _, e := workspace(ctx, db, f, id, "gateway", "db-instance", now); e == nil {
		t.Fatal("lost reply accepted")
	}
	first := *f.request
	m, e := workspace(ctx, db, f, id, "gateway", "db-instance", now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Name) > 19 || m.Name != first.Name || m.RequestID != first.RequestId || f.creates != 2 {
		t.Fatal("changed replay")
	}
	if _, e = workspace(ctx, db, f, id, "gateway", "db-instance", now.Add(48*time.Hour)); e != nil || f.creates != 2 || f.gets != 1 {
		t.Fatal("known mapping recreated", e)
	}
	if _, e = workspace(ctx, db, f, id, "gateway", "replacement", now); e == nil {
		t.Fatal("replacement database adopted old allocations")
	}
	f.id = uuid.NewString()
	if _, e = workspace(ctx, db, f, id, "gateway", "db-instance", now); e == nil {
		t.Fatal("replacement native workspace adopted")
	}
	if _, e = db.Exec(`DELETE FROM hosted_project_workspaces`); e == nil {
		t.Fatal("mapping reusable")
	}
}
func TestUncertainWorkspaceBeyondReplayWindowNeverRetries(t *testing.T) {
	ctx := context.Background()
	db, id := mappingDB(t)
	f := &workspaceFake{id: uuid.NewString(), lost: true}
	now := time.Now()
	workspace(ctx, db, f, id, "gateway", "db", now)
	if _, e := workspace(ctx, db, f, id, "gateway", "db", now.Add(24*time.Hour)); e == nil {
		t.Fatal("expired replay allowed")
	}
	if f.creates != 1 {
		t.Fatal("sent after replay retention")
	}
}
func TestWorkerRoutingNeverBuildsOrCrossesProject(t *testing.T) {
	id := uuid.NewString()
	calls := 0
	s := &Service{handlers: map[string]http.Handler{id: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) })}}
	for _, tc := range []struct {
		path string
		tls  bool
		want int
	}{{"/worker/projects/" + id + "/model/v1/responses", true, 204}, {"/worker/projects/" + uuid.NewString() + "/plugin/v1/native/invoke", true, 404}, {"/worker/projects/" + id + "/plugin/v1/native/invoke", false, 404}, {"/worker/projects/" + id + "/plugin/v1/native/admit", true, 404}, {"/worker/projects/" + id + "/model", true, 404}} {
		r := httptest.NewRequest("POST", tc.path, nil)
		if tc.tls {
			r.TLS = &tls.ConnectionState{}
		}
		w := httptest.NewRecorder()
		s.Workers(w, r)
		if w.Code != tc.want {
			t.Fatalf("status %d", w.Code)
		}
	}
	if calls != 1 {
		t.Fatal("wrong project reached handler")
	}
}
