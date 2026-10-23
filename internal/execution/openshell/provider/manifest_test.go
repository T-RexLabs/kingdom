package provider

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	_ "modernc.org/sqlite"
	"strings"
	"testing"
	"time"
)

type sourceFixture struct {
	captures int
	ref      remote.SourceArchive
}

func (s *sourceFixture) Capture(context.Context, providerrequest.Preparation) (remote.SourceArchive, error) {
	s.captures++
	return s.ref, nil
}
func (*sourceFixture) Open(context.Context, remote.SourceArchive) (worker.Archive, error) {
	return nil, errors.New("unused")
}
func manifestFixture(t *testing.T) (manifestStore, providerrequest.Preparation, *sourceFixture) {
	t.Helper()
	db, e := sql.Open("sqlite", t.TempDir()+"/state.db")
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	names, e := dbmigrations.List()
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range names {
		raw, e := dbmigrations.Read(name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec(string(raw)); e != nil {
			t.Fatal(name, e)
		}
	}
	now := time.Now()
	stamp := now.Format(time.RFC3339Nano)
	expiry := now.Add(time.Hour).Format(time.RFC3339Nano)
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES('plan','Plan','custom','active','processing','/project','{}',?,?)`, []any{stamp, stamp}},
		{`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at)VALUES('operation','acp','plan','reply','running','{}','worker',1,?,'session',?)`, []any{expiry, stamp}},
		{`INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at)VALUES('inv','operation','plan','conversation','session','issued','server_response',?,?,?)`, []any{stamp, stamp, stamp}},
		{`UPDATE planning_operations SET active_provider_invocation_id='inv'`, nil}, {`UPDATE planning_sessions SET current_operation_id='operation'`, nil},
		{`INSERT INTO provider_preparations(owner_kind,invocation_id,lease_owner,lease_version)VALUES('planning','inv','worker',1)`, nil},
	} {
		if _, e = db.Exec(q.sql, q.args...); e != nil {
			t.Fatal(e)
		}
	}
	source := &sourceFixture{ref: remote.SourceArchive{ID: uuid.NewString(), SHA256: strings.Repeat("a", 64), Bytes: 100}}
	return manifestStore{db: db, source: source, pins: []byte(`{"image":"fixed"}`), now: func() time.Time { return now }}, providerrequest.Preparation{ResourceID: "operation", Owner: providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: "inv"}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}, source
}
func TestManifestRestartUsesOriginalCaptureAndOperationIDs(t *testing.T) {
	store, p, source := manifestFixture(t)
	ctx := context.Background()
	first, e := store.ensure(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	source.ref.ID = uuid.NewString()
	source.ref.SHA256 = strings.Repeat("b", 64)
	restart := store
	again, e := restart.ensure(ctx, p)
	if e != nil {
		t.Fatal(e)
	}
	if source.captures != 1 || first.Source != again.Source || first.ImportID != again.ImportID || first.ExportID != again.ExportID || first.CreationID != again.CreationID {
		t.Fatal("restart recaptured or changed IDs")
	}
	restart.pins = []byte(`{"image":"changed"}`)
	if _, e = restart.ensure(ctx, p); e == nil {
		t.Fatal("image pins changed")
	}
	if source.captures != 1 {
		t.Fatal("mismatch recaptured")
	}
	p.Lease.Version++
	if _, e = store.ensure(ctx, p); e == nil {
		t.Fatal("new lease adopted manifest")
	}
}
func TestManifestCaptureCannotCommitUnderExpiredLease(t *testing.T) {
	store, p, source := manifestFixture(t)
	store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if _, e := store.ensure(context.Background(), p); e == nil {
		t.Fatal("expired lease committed capture")
	}
	var count int
	if e := store.db.QueryRow(`SELECT count(*) FROM remote_worker_manifests`).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if count != 0 || source.captures != 1 {
		t.Fatal("capture must remain an unreferenced private orphan")
	}
}
func TestKeyedLifecycleLockHonorsCancellationAndReleasesEntries(t *testing.T) {
	p := &Provider{locks: map[string]*gate{}}
	unlock, e := p.lock(context.Background(), "inv")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = p.lock(ctx, "inv"); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	unlock()
	if len(p.locks) != 0 {
		t.Fatal("lock leaked")
	}
}
func TestCloseInvocationWithoutAllocationOnlyFencesPreparation(t *testing.T) {
	store, prep, _ := manifestFixture(t)
	m, e := store.ensure(context.Background(), prep)
	if e != nil {
		t.Fatal(e)
	}
	p := &Provider{c: Config{DB: store.db, Now: store.now}, manifests: store, locks: map[string]*gate{}, handles: map[string]*handle{}}
	if e = p.CloseInvocation(context.Background(), m.Owner.InvocationID); e != nil {
		t.Fatal(e)
	}
	var closed bool
	if e = store.db.QueryRow(`SELECT admission_closed FROM provider_preparations WHERE invocation_id='inv'`).Scan(&closed); e != nil || !closed {
		t.Fatalf("closed=%v %v", closed, e)
	}
	if e = p.DiscardInvocation(context.Background(), "inv"); e != nil {
		t.Fatal(e)
	}
	var runtimes int
	if e = store.db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&runtimes); e != nil || runtimes != 0 {
		t.Fatal("cleanup provisioned runtime", e)
	}
}

func TestCloseBeforeCaptureCommitFencesLateSource(t *testing.T) {
	store, prep, _ := manifestFixture(t)
	p := &Provider{c: Config{DB: store.db, Now: store.now}, manifests: store, locks: map[string]*gate{}, handles: map[string]*handle{}}
	if e := p.CloseInvocation(context.Background(), prep.Owner.InvocationID); e != nil {
		t.Fatal(e)
	}
	if _, e := store.ensure(context.Background(), prep); e == nil {
		t.Fatal("late capture reopened a closed invocation")
	}
}
