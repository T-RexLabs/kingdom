// This fast acceptance test seeds canonical run rows and captures real source /
// result snapshots. It verifies publication eligibility and byte integrity; it
// does not execute a model or certify OpenShell. The opt-in managed acceptance
// test covers that separate execution boundary.
package repositories_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/repositories"
	"kingdom/internal/central/resultsource"
	centralserver "kingdom/internal/central/server"
	"kingdom/internal/central/sources"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/publishedresult"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/testutil/githubemu"
	_ "modernc.org/sqlite"
)

func acceptanceMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type acceptanceHandler struct {
	mu      sync.RWMutex
	handler http.Handler
}

func (h *acceptanceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	next := h.handler
	h.mu.RUnlock()
	if next == nil {
		http.Error(w, "not ready", 503)
		return
	}
	next.ServeHTTP(w, r)
}

type canonicalRuntime struct{ reader publishedresult.Reader }

func (r canonicalRuntime) PublishedResults() publishedresult.Reader         { return r.reader }
func (r canonicalRuntime) ServeHTTP(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }

type repositoryAcceptance struct {
	t                           *testing.T
	ctx                         context.Context
	path, inputPath, workspace  string
	db, projectDB               *sql.DB
	authority                   *catalog.Store
	sessions                    *auth.Manager
	source                      *sources.Service
	client                      *repositories.Client
	service                     *repositories.Service
	result                      *resultsource.Service
	store                       *runtimefs.Store
	emulator                    *githubemu.Emulator
	key                         *rsa.PrivateKey
	owner, member               catalog.User
	project, other              catalog.Project
	ownerSession, memberSession auth.Session
	ownerCookie, memberCookie   *http.Cookie
	http                        *httptest.Server
	handler                     *acceptanceHandler
}

func newRepositoryAcceptance(t *testing.T) *repositoryAcceptance {
	t.Helper()
	f := &repositoryAcceptance{t: t, ctx: context.Background(), path: filepath.Join(t.TempDir(), "central.db"), inputPath: filepath.Join(t.TempDir(), "inputs"), workspace: t.TempDir(), handler: &acceptanceHandler{}}
	var err error
	f.key, err = rsa.GenerateKey(rand.Reader, 2048)
	acceptanceMust(t, err)
	f.emulator, err = githubemu.New(githubemu.Config{AppID: 1, InstallationID: 2, RepositoryID: 3, PublicKey: &f.key.PublicKey, Owner: "fixture", Name: "repository", Files: map[string]githubemu.File{"input.txt": {Data: []byte("original\n"), Mode: "100644"}, "nested/keep.sh": {Data: []byte("#!/bin/sh\necho preserved\n"), Mode: "100755"}}})
	acceptanceMust(t, err)
	f.http = httptest.NewTLSServer(f.handler)
	t.Cleanup(f.http.Close)
	f.projectDB, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "project.db"))
	acceptanceMust(t, err)
	f.projectDB.SetMaxOpenConns(1)
	t.Cleanup(func() { f.projectDB.Close() })
	names, err := dbmigrations.List()
	acceptanceMust(t, err)
	var migrations []state.SQLMigration
	for _, name := range names {
		if strings.HasSuffix(name, ".up.sql") {
			raw, err := dbmigrations.Read(name)
			acceptanceMust(t, err)
			migrations = append(migrations, state.SQLMigration{Name: name, SQL: raw})
		}
	}
	acceptanceMust(t, state.ApplySQLMigrations(f.ctx, f.projectDB, migrations, time.Now()))
	f.store, err = runtimefs.NewStore(f.projectDB, t.TempDir())
	acceptanceMust(t, err)
	f.open(true)
	t.Cleanup(f.close)
	return f
}
func (f *repositoryAcceptance) close() {
	if f.sessions != nil {
		f.sessions.Close()
	}
	if f.source != nil {
		f.source.Close()
	}
	if f.client != nil {
		f.client.Close()
	}
	if f.db != nil {
		f.db.Close()
	}
}
func (f *repositoryAcceptance) open(initial bool) {
	t := f.t
	var err error
	f.db, err = sql.Open("sqlite", "file:"+f.path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	acceptanceMust(t, err)
	f.db.SetMaxOpenConns(1)
	identity := catalog.Identity{Issuer: "https://fixture.invalid", Subject: "owner"}
	f.authority, err = catalog.New(f.ctx, f.db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	acceptanceMust(t, err)
	if initial {
		f.owner, err = f.authority.BootstrapOwner(f.ctx, identity, "Owner")
		acceptanceMust(t, err)
		f.member, err = f.authority.CreateUser(f.ctx, f.owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
		acceptanceMust(t, err)
		f.project, err = f.authority.CreateProject(f.ctx, f.owner.ID, "Repository Acceptance")
		acceptanceMust(t, err)
		f.other, err = f.authority.CreateProject(f.ctx, f.owner.ID, "Other")
		acceptanceMust(t, err)
		acceptanceMust(t, f.authority.SetMembership(f.ctx, f.owner.ID, f.project.ID, f.member.ID, catalog.Member))
	}
	f.sessions, err = auth.New(f.ctx, auth.Config{DB: f.db, Catalog: f.authority, PublicOrigin: f.http.URL})
	acceptanceMust(t, err)
	f.ownerSession, f.ownerCookie, err = f.sessions.CreateSession(f.ctx, f.owner.ID)
	acceptanceMust(t, err)
	f.memberSession, f.memberCookie, err = f.sessions.CreateSession(f.ctx, f.member.ID)
	acceptanceMust(t, err)
	vault, err := connections.New(f.ctx, f.db, bytes.Repeat([]byte{7}, 32))
	acceptanceMust(t, err)
	f.source, err = sources.New(f.ctx, f.db, f.authority, f.inputPath)
	acceptanceMust(t, err)
	f.client, err = repositories.NewTestClient(repositories.Config{AppID: 1, PrivateKey: f.key, Grants: []repositories.Grant{{ProjectID: f.project.ID, InstallationID: 2, RepositoryID: 3, Publish: true}}}, f.emulator.Transport())
	acceptanceMust(t, err)
	factory := func(ctx context.Context, project string, _ api.Authenticator) (http.Handler, error) {
		if project != f.project.ID {
			return nil, publishedresult.ErrUnavailable
		}
		if err := f.source.PrepareInitial(ctx, project, f.workspace); err != nil {
			return nil, err
		}
		return canonicalRuntime{reader: f.store}, nil
	}
	f.result, err = resultsource.New(f.authority, f.sessions, factory)
	acceptanceMust(t, err)
	f.service, err = repositories.New(f.ctx, f.db, f.authority, f.client, f.source, f.result)
	acceptanceMust(t, err)
	login, err := auth.NewLogin(f.ctx, auth.LoginConfig{Sessions: f.sessions, GitHub: &auth.GitHubConfig{ClientID: "unused-fixture", ClientSecret: "unused-fixture"}})
	acceptanceMust(t, err)
	server, err := centralserver.New(centralserver.Config{Sessions: f.sessions, Login: login, Catalog: f.authority, Connections: vault, Sources: f.source, Repositories: f.service, ProjectFactory: factory})
	acceptanceMust(t, err)
	f.handler.mu.Lock()
	f.handler.handler = server
	f.handler.mu.Unlock()
}
func (f *repositoryAcceptance) pathFor(project string) string {
	return centralserver.BasePath + "/projects/" + project + "/repository"
}
func (f *repositoryAcceptance) request(method, path string, body any, key, actor string, csrf bool) (int, []byte) {
	f.t.Helper()
	var input io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		acceptanceMust(f.t, err)
		input = bytes.NewReader(raw)
	}
	r, err := http.NewRequest(method, f.http.URL+path, input)
	acceptanceMust(f.t, err)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	session, cookie := f.ownerSession, f.ownerCookie
	if actor == "member" {
		session, cookie = f.memberSession, f.memberCookie
	}
	if actor != "anonymous" {
		r.AddCookie(cookie)
	}
	if csrf {
		r.Header.Set("Origin", f.http.URL)
		r.Header.Set("X-CSRF-Token", session.CSRFToken)
	}
	response, err := f.http.Client().Do(r)
	acceptanceMust(f.t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	acceptanceMust(f.t, err)
	if bytes.Contains(raw, []byte("private-emulator")) {
		f.t.Fatal("private upstream failure exposed")
	}
	return response.StatusCode, raw
}
func expect(t *testing.T, actual, want int, raw []byte) {
	t.Helper()
	if actual != want {
		t.Fatalf("status=%d want=%d body=%s", actual, want, raw)
	}
}
func (f *repositoryAcceptance) seedCanonicalResult() {
	t := f.t
	acceptanceMust(t, f.source.PrepareInitial(f.ctx, f.project.ID, f.workspace))
	now := "2026-10-25T00:00:00Z"
	_, err := f.projectDB.Exec(`INSERT INTO work_orders(work_order_id,work_order_title,work_order_text,metadata_json,created_at)VALUES('order','Fixture','Fixture','{}',?);INSERT INTO workflow_definitions(workflow_definition_id,workflow_name,workflow_version,definition_json,created_at)VALUES('workflow','fixture','1','{}',?);INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,row_version,created_at,updated_at)VALUES('run','order','workflow','running','approved',1,?,?)`, now, now, now, now)
	acceptanceMust(t, err)
	_, err = f.store.CaptureSnapshot(f.ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "source", RunID: "run", SnapshotKind: "baseline", RootPath: f.workspace, CreatedAt: now})
	acceptanceMust(t, err)
	acceptanceMust(t, os.WriteFile(filepath.Join(f.workspace, "verification.txt"), []byte("verified\n"), 0644))
	_, err = f.store.CaptureSnapshot(f.ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "result", RunID: "run", ParentSnapshotID: "source", SnapshotKind: "trusted_promotion", RootPath: f.workspace, CreatedAt: now})
	acceptanceMust(t, err)
	_, err = f.store.EnsureExecutionTarget(f.ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run", TargetMode: "direct", SourceRoot: f.workspace, ExecutionRoot: f.workspace, PublishRoot: f.workspace, SourceSnapshotID: "source", TrustedSnapshotID: "result", PublishedSnapshotID: "result", State: "trusted", CreatedAt: now, UpdatedAt: now})
	acceptanceMust(t, err)
	manifest, err := f.store.OpenSnapshotManifest(f.ctx, "result")
	acceptanceMust(t, err)
	entry, found, err := manifest.Lookup(f.ctx, "verification.txt")
	acceptanceMust(t, err)
	manifest.Close()
	if !found {
		t.Fatal("canonical file missing")
	}
	_, err = f.projectDB.Exec(`INSERT INTO run_publication_paths(run_id,path,artifact_action,blob_id,sha256,mode_bits,updated_at)VALUES('run','verification.txt','write',?,?,?,?);UPDATE workflow_runs SET run_status='succeeded' WHERE run_id='run'`, entry.BlobID, entry.BlobID, entry.ModeBits, now)
	acceptanceMust(t, err)
	snapshot, err := f.store.Resolve(f.ctx, "run")
	acceptanceMust(t, err)
	if len(snapshot.Files) != 3 || len(snapshot.Changes) != 1 {
		t.Fatal("canonical fixture result differs")
	}
	// Deliberately corrupt the live tree. Publication must use retained canonical
	// blobs and mode facts, never mutable project files.
	acceptanceMust(t, os.WriteFile(filepath.Join(f.workspace, "verification.txt"), []byte("unpublished live mutation"), 0644))
}
func TestEmulatedRepositoryAcceptanceRestartAndAuthority(t *testing.T) {
	f := newRepositoryAcceptance(t)
	path := f.pathFor(f.project.ID)
	input := map[string]string{"ref": "main"}
	key := uuid.NewString()
	for _, tc := range []struct {
		actor   string
		csrf    bool
		project string
		status  int
	}{{"anonymous", true, f.project.ID, 401}, {"owner", false, f.project.ID, 403}, {"member", true, f.project.ID, 403}, {"member", true, f.other.ID, 403}} {
		status, raw := f.request("POST", f.pathFor(tc.project)+"/import", input, key, tc.actor, tc.csrf)
		expect(t, status, tc.status, raw)
	}
	if f.emulator.Snapshot().Requests != 0 {
		t.Fatal("unauthorized HTTP request reached GitHub")
	}
	initial := f.emulator.Snapshot()
	sum := sha1.Sum([]byte("blob 9\x00original\n"))
	f.emulator.FailNext("GET", "/repos/fixture/repository/git/blobs/"+hex.EncodeToString(sum[:]))
	status, raw := f.request("POST", path+"/import", input, key, "owner", true)
	expect(t, status, 503, raw)
	status, raw = f.request("POST", path+"/import", input, key, "owner", true)
	expect(t, status, 201, raw)
	var imported sources.Metadata
	acceptanceMust(t, json.Unmarshal(raw, &imported))
	if imported.Files != 2 || imported.Repository == nil || imported.Repository.CommitSHA != initial.MainCommit {
		t.Fatal("source provenance differs")
	}
	requests := f.emulator.Snapshot().Requests
	status, replay := f.request("POST", path+"/import", input, key, "owner", true)
	expect(t, status, 201, replay)
	if !bytes.Equal(raw, replay) || f.emulator.Snapshot().Requests != requests {
		t.Fatal("import replay contacted GitHub or changed source")
	}
	f.seedCanonicalResult()
	status, raw = f.request("POST", path+"/import", input, uuid.NewString(), "owner", true)
	expect(t, status, 409, raw)
	publishKey := uuid.NewString()
	payload := repositories.PublishRequest{RunID: "run", BaseBranch: "main", Title: "Verified Result", Body: "Controlled repository acceptance"}
	beforeDenied := f.emulator.Snapshot().Requests
	for _, tc := range []struct {
		actor  string
		csrf   bool
		status int
	}{{"anonymous", true, 401}, {"owner", false, 403}, {"member", true, 403}} {
		status, raw := f.request("POST", path+"/publish", payload, publishKey, tc.actor, tc.csrf)
		expect(t, status, tc.status, raw)
	}
	if f.emulator.Snapshot().Requests != beforeDenied {
		t.Fatal("publication authority failure reached GitHub")
	}
	f.emulator.DenyWrites(true)
	status, raw = f.request("POST", path+"/publish", payload, publishKey, "owner", true)
	expect(t, status, 503, raw)
	if f.emulator.Snapshot().Writes != 0 {
		t.Fatal("permission denial mutated repository")
	}
	f.emulator.DenyWrites(false)
	f.emulator.FailNext("POST", "/repos/fixture/repository/git/blobs")
	status, raw = f.request("POST", path+"/publish", payload, publishKey, "owner", true)
	expect(t, status, 503, raw)
	if f.emulator.Snapshot().Writes != 0 {
		t.Fatal("ambiguous pre-write failure mutated repository")
	}
	f.emulator.HidePulls(true)
	f.emulator.LoseNextPullReply(true)
	status, raw = f.request("POST", path+"/publish", payload, publishKey, "owner", true)
	expect(t, status, 503, raw)
	committed := f.emulator.Snapshot()
	if committed.PullCreates != 1 || committed.RefCreates != 1 || committed.MainWrites != 0 || committed.Refs["main"] != initial.MainCommit {
		t.Fatal("publication writes differ")
	}
	// Reopen the actual central database and rebuild all authority/source/repository
	// services. Emulator remote state survives while no client retry state does.
	f.close()
	f.open(false)
	status, raw = f.request("POST", path+"/publish", payload, publishKey, "owner", true)
	expect(t, status, 202, raw)
	var pending repositories.Publication
	acceptanceMust(t, json.Unmarshal(raw, &pending))
	if pending.State != "pending" || pending.PullRequest != 0 {
		t.Fatal("hidden PR lookup invented success")
	}
	if current := f.emulator.Snapshot(); current.Writes != committed.Writes || current.RefCreates != 1 || current.PullCreates != 1 {
		t.Fatal("restart duplicated branch or PR")
	}
	requestsBeforeConflict := f.emulator.Snapshot().Requests
	changed := payload
	changed.Title = "Changed immutable request"
	status, raw = f.request("POST", path+"/publish", changed, publishKey, "owner", true)
	expect(t, status, 409, raw)
	if f.emulator.Snapshot().Requests != requestsBeforeConflict {
		t.Fatal("conflicting publication contacted GitHub")
	}
	f.emulator.HidePulls(false)
	status, raw = f.request("POST", path+"/publications/"+pending.ID+"/reconcile", map[string]any{}, "", "owner", true)
	expect(t, status, 200, raw)
	var publication repositories.Publication
	acceptanceMust(t, json.Unmarshal(raw, &publication))
	if publication.State != "published" || publication.ID != pending.ID || publication.PullRequest != 1 || publication.URL != "https://github.com/fixture/repository/pull/1" {
		t.Fatal("exact PR recovery failed")
	}
	final := f.emulator.Snapshot()
	if len(final.Violations) != 0 || final.Writes != committed.Writes || final.RefCreates != 1 || final.PullCreates != 1 || final.MainWrites != 0 || final.Refs["main"] != initial.MainCommit {
		t.Fatalf("contract drift: violations=%v writes=%d", final.Violations, final.Writes)
	}
	branch := final.FilesByRef[publication.Branch]
	if string(branch["verification.txt"].Data) != "verified\n" || branch["nested/keep.sh"].Mode != "100755" || !bytes.Equal(branch["nested/keep.sh"].Data, initial.FilesByRef["main"]["nested/keep.sh"].Data) || string(branch["input.txt"].Data) != "original\n" || len(branch) != 3 {
		t.Fatal("canonical result or unrelated bytes/modes changed")
	}
	readScopes, writeScopes := 0, 0
	for _, scope := range final.TokenRequests {
		if scope.RepositoryID != 3 {
			t.Fatal("foreign token scope")
		}
		if scope.Permissions["contents"] == "read" {
			readScopes++
		} else if scope.Permissions["contents"] == "write" && scope.Permissions["pull_requests"] == "write" {
			writeScopes++
		} else {
			t.Fatal("invalid token scope")
		}
	}
	if readScopes == 0 || writeScopes == 0 {
		t.Fatal("missing independent read/write scopes")
	}
	acceptanceMust(t, f.authority.SetMembership(f.ctx, f.owner.ID, f.project.ID, f.member.ID, catalog.Owner))
	acceptanceMust(t, f.authority.RevokeMembership(f.ctx, f.member.ID, f.project.ID, f.owner.ID))
	requests = final.Requests
	status, raw = f.request("POST", path+"/publications/"+pending.ID+"/reconcile", map[string]any{}, "", "owner", true)
	expect(t, status, 403, raw)
	if _, err := f.result.Resolve(f.ctx, f.owner.ID, f.project.ID, "run"); err == nil {
		t.Fatal("revoked actor read canonical result")
	}
	if f.emulator.Snapshot().Requests != requests {
		t.Fatal("revoked actor reached GitHub")
	}
	status, raw = f.request("POST", centralserver.BasePath+"/logout", map[string]any{}, "", "owner", true)
	expect(t, status, 204, raw)
	status, raw = f.request("GET", path, nil, "", "owner", false)
	expect(t, status, 401, raw)
}
