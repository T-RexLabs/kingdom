package personal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/accounts"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
)

type fakeLogin struct {
	client     *accounts.Client
	closed     atomic.Bool
	closeError atomic.Bool
}

func (f *fakeLogin) Client(context.Context) (*accounts.Client, error) { return f.client, nil }
func (f *fakeLogin) Close(context.Context) error {
	f.closed.Store(true)
	if f.closeError.Load() {
		return errors.New("unproven stop")
	}
	return nil
}

type signinFixture struct {
	s        *SignIns
	db       *sql.DB
	owner    string
	session  string
	valid    atomic.Bool
	complete atomic.Bool
	begins   atomic.Int64
	resolves atomic.Int64
	fake     *fakeLogin
	labelMu  sync.Mutex
	label    string
}

func setupSignIns(t *testing.T) *signinFixture {
	t.Helper()
	ctx := context.Background()
	db, e := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "signin.db")+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	id := catalog.Identity{Issuer: "https://identity.example", Subject: "owner"}
	cat, e := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{id}})
	if e != nil {
		t.Fatal(e)
	}
	owner, e := cat.BootstrapOwner(ctx, id, "Owner")
	if e != nil {
		t.Fatal(e)
	}
	store, e := connections.New(ctx, db, bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	personal, e := New(ctx, Config{DB: db, Key: bytes.Repeat([]byte{2}, 32)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { personal.Close(ctx) })
	f := &signinFixture{db: db, owner: owner.ID, session: "trusted-session", fake: &fakeLogin{}}
	f.valid.Store(true)
	s, e := NewSignIns(ctx, SignInsConfig{DB: db, Accounts: personal, Connections: store, Catalog: cat, BinaryPath: "/operator/opencode", RuntimeRoot: filepath.Join(t.TempDir(), "private"), CheckSession: func(ctx context.Context, owner, session string) error {
		if !f.valid.Load() || owner != f.owner || session != f.session {
			return ErrDenied
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	f.s = s
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := s.Close(closeCtx); e != nil {
			t.Error(e)
		}
	})
	var client *accounts.Client
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respond := func(data any) {
			_ = json.NewEncoder(w).Encode(map[string]any{"location": map[string]string{"directory": "/private/work"}, "data": data})
		}
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/integration/github-copilot/connect/oauth":
			f.begins.Add(1)
			var in struct {
				Label string `json:"label"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			f.labelMu.Lock()
			f.label = in.Label
			f.labelMu.Unlock()
			respond(accounts.Attempt{AttemptID: "native-attempt", URL: "https://github.com/login/device", Instructions: "Enter controlled code", Mode: "auto", Time: accounts.AttemptTime{Created: time.Now().UnixMilli(), Expires: time.Now().Add(5 * time.Minute).UnixMilli()}})
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/oauth/native-attempt"):
			status := "pending"
			if f.complete.Load() {
				status = "complete"
			}
			respond(accounts.AttemptStatus{Status: status})
		case r.Method == "DELETE":
			w.WriteHeader(204)
		case r.URL.Path == "/api/integration/github-copilot":
			f.labelMu.Lock()
			label := f.label
			f.labelMu.Unlock()
			respond(map[string]any{"id": accounts.CopilotProvider, "connections": []accounts.Connection{{Type: "credential", ID: "native-credential", Method: "oauth", Label: label}}})
		case r.URL.Path == "/api/rpc/kingdom.accounts/resolve":
			f.resolves.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"output": accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "private-copilot-token", Refresh: "private-copilot-token"}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, e = accounts.NewClient(server.URL, "private-server-password", "/private/work")
	if e != nil {
		t.Fatal(e)
	}
	f.fake.client = client
	s.newProcess = func(accounts.ProcessConfig) (loginProcess, error) { return f.fake, nil }
	return f
}
func waitSignIn(t *testing.T, f *signinFixture, id, status string) SignIn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v, e := f.s.Get(context.Background(), f.owner, f.session, id)
		if e == nil && v.Status == status {
			return v
		}
		time.Sleep(10 * time.Millisecond)
	}
	v, e := f.s.Get(context.Background(), f.owner, f.session, id)
	t.Fatalf("wanted %s got %s err %v", status, v.Status, e)
	return SignIn{}
}
func TestSignInCompletesOnlyAfterEncryptedCommitAndNativeCleanup(t *testing.T) {
	f := setupSignIns(t)
	f.complete.Store(true)
	v, e := f.s.Begin(context.Background(), f.owner, f.session, "Personal Copilot", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	v = waitSignIn(t, f, v.ID, "complete")
	if v.Connection == nil || v.Connection.Scope != connections.Personal || !f.fake.closed.Load() {
		t.Fatal("completion lacks personal connection and proven cleanup")
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private-copilot-token") {
		t.Fatal("public sign-in exposes token")
	}
	var encrypted []byte
	if e = f.db.QueryRow(`SELECT credential FROM central_personal_accounts`).Scan(&encrypted); e != nil || bytes.Contains(encrypted, []byte("private-copilot-token")) {
		t.Fatal("credential not encrypted")
	}
}
func TestSignInIsBoundToExactSessionAndIdempotentRequest(t *testing.T) {
	f := setupSignIns(t)
	key := uuid.NewString()
	v, e := f.s.Begin(context.Background(), f.owner, f.session, "Personal Copilot", key)
	if e != nil {
		t.Fatal(e)
	}
	again, e := f.s.Begin(context.Background(), f.owner, f.session, "Personal Copilot", key)
	if e != nil || again.ID != v.ID || f.begins.Load() != 1 {
		t.Fatal("native authorization repeated")
	}
	if _, e = f.s.Get(context.Background(), f.owner, "foreign-session", v.ID); e == nil {
		t.Fatal("foreign session inspected login")
	}
	if e = f.s.Cancel(context.Background(), f.owner, "foreign-session", v.ID); e == nil {
		t.Fatal("foreign session canceled login")
	}
	if _, e = f.s.Begin(context.Background(), f.owner, f.session, "Changed", key); e == nil {
		t.Fatal("changed idempotency request accepted")
	}
	if e = f.s.Cancel(context.Background(), f.owner, f.session, v.ID); e != nil {
		t.Fatal(e)
	}
	waitSignIn(t, f, v.ID, "canceled")
	if !f.fake.closed.Load() {
		t.Fatal("cancel completed before process closure")
	}
}
func TestSignInDoesNotResolveAfterSessionRevocation(t *testing.T) {
	f := setupSignIns(t)
	v, e := f.s.Begin(context.Background(), f.owner, f.session, "Personal Copilot", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	f.valid.Store(false)
	f.complete.Store(true)
	deadline := time.Now().Add(4 * time.Second)
	for !f.fake.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !f.fake.closed.Load() || f.resolves.Load() != 0 {
		t.Fatal("revoked session resolved or retained native login")
	}
	if _, e = f.s.Get(context.Background(), f.owner, f.session, v.ID); e == nil {
		t.Fatal("revoked session can inspect login")
	}
	var count int
	_ = f.db.QueryRow(`SELECT COUNT(*) FROM central_personal_accounts`).Scan(&count)
	if count != 0 {
		t.Fatal("revoked session saved account")
	}
}
func TestSignInCleanupFailureDoesNotPublishCompletion(t *testing.T) {
	f := setupSignIns(t)
	f.fake.closeError.Store(true)
	f.complete.Store(true)
	v, e := f.s.Begin(context.Background(), f.owner, f.session, "Personal Copilot", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	waitSignIn(t, f, v.ID, "cleanup")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = f.s.Close(ctx); e == nil {
		t.Fatal("unproven native cleanup reported closed")
	}
	f.fake.closeError.Store(false)
}
func TestSignInHTTPWaitCancellationDoesNotCancelAuthorization(t *testing.T) {
	f := setupSignIns(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := f.s.Begin(ctx, f.owner, f.session, "Personal Copilot", uuid.NewString()); e == nil {
		t.Fatal("canceled caller admitted")
	}
	if f.begins.Load() != 0 {
		t.Fatal("canceled caller launched native authorization")
	}
}

func TestRecoveryNeverRepeatsAnAmbiguousNativeBegin(t *testing.T) {
	f := setupSignIns(t)
	id := uuid.NewString()
	key := uuid.NewString()
	_, e := f.db.Exec(`INSERT INTO central_personal_signins(id,owner_id,session_id,label,request_key,status,expires_at) VALUES(?,?,?,?,?,'starting',?)`, id, f.owner, f.session, "Interrupted", key, time.Now().Add(time.Minute).UnixMilli())
	if e != nil {
		t.Fatal(e)
	}
	recovered, e := NewSignIns(context.Background(), f.s.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer recovered.Close(context.Background())
	v, e := recovered.Begin(context.Background(), f.owner, f.session, "Interrupted", key)
	if e != nil || v.ID != id || v.Status != "failed" || f.begins.Load() != 0 {
		t.Fatalf("ambiguous authorization repeated: %s %v", v.Status, e)
	}
}

func TestRecoveryPublishesOnlyTheRetainedEncryptedAccountReference(t *testing.T) {
	f := setupSignIns(t)
	ctx := context.Background()
	ref, e := f.s.cfg.Accounts.SaveCopilot(ctx, f.owner, accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "retained-token", Refresh: "retained-token"})
	if e != nil {
		t.Fatal(e)
	}
	id := uuid.NewString()
	_, e = f.db.Exec(`INSERT INTO central_personal_signins(id,owner_id,session_id,label,request_key,status,expires_at,account_id) VALUES(?,?,?,?,?,'publishing',?,?)`, id, f.owner, f.session, "Retained", uuid.NewString(), time.Now().Add(time.Minute).UnixMilli(), ref.ID)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		recovered, e := NewSignIns(ctx, f.s.cfg)
		if e != nil {
			t.Fatal(e)
		}
		v, e := recovered.Get(ctx, f.owner, f.session, id)
		if e != nil || v.Status != "complete" || v.Connection == nil {
			t.Fatalf("retained publication did not recover: %s %v", v.Status, e)
		}
		_ = recovered.Close(ctx)
	}
	list, e := f.s.cfg.Connections.List(ctx, f.owner)
	if e != nil || len(list) != 1 || f.begins.Load() != 0 {
		t.Fatal("recovery repeated native login or central publication")
	}
}

func TestSignInLimitsPendingAttemptsBeforeLaunchingNativeProcess(t *testing.T) {
	f := setupSignIns(t)
	for i := 0; i < 8; i++ {
		_, e := f.db.Exec(`INSERT INTO central_personal_signins(id,owner_id,session_id,label,request_key,status,expires_at) VALUES(?,?,?,?,?,'starting',?)`, uuid.NewString(), f.owner, f.session, "Reserved", uuid.NewString(), time.Now().Add(time.Minute).UnixMilli())
		if e != nil {
			t.Fatal(e)
		}
	}
	if _, e := f.s.Begin(context.Background(), f.owner, f.session, "Overflow", uuid.NewString()); e == nil {
		t.Fatal("unbounded native login admitted")
	}
	if f.begins.Load() != 0 {
		t.Fatal("limit checked after native authorization")
	}
}
