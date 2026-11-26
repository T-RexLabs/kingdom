package repositories_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	gen "kingdom/internal/apigen"
	"kingdom/internal/central/application"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/hostedbuilder"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/repositories"
	"kingdom/internal/central/resultsource"
	"kingdom/internal/central/sources"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/idempotency"
	"kingdom/internal/testutil/githubemu"
	_ "modernc.org/sqlite"
)

// TestManagedRepositoryAcceptance is deliberately opt-in. It opens only the
// operator-specified local managed gateway and fixture listeners. GitHub and
// model inference are emulated; Kingdom, OpenCode, and OpenShell are real.
// Evidence and unresolved cleanup debt are retained outside t.TempDir.
func TestManagedRepositoryAcceptance(t *testing.T) {
	if os.Getenv("KINGDOM_REPOSITORY_MANAGED_ACCEPTANCE") != "1" {
		t.Skip("set KINGDOM_REPOSITORY_MANAGED_ACCEPTANCE=1 with an isolated managed environment and fresh evidence directory")
	}
	env, out := loadManagedEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	saveManaged(t, out, "environment", env)
	pins := readManagedJSON[managedPins](t, filepath.Join(env.root, "image-pins.json"))
	saveManaged(t, out, "image-pins", pins)
	profileRaw := readManagedFile(t, filepath.Join(env.root, "profile.sha256"))
	profile, err := hex.DecodeString(strings.TrimSpace(string(profileRaw)))
	managedOK(t, err)
	if len(profile) != 32 {
		t.Fatal("managed profile digest must contain 32 bytes")
	}
	ca := readManagedFile(t, filepath.Join(env.root, "tls", "ca.crt"))
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(env.root, "tls", "server.crt"), filepath.Join(env.root, "tls", "server.key"))
	managedOK(t, err)
	model := new(managedModel)
	serveManaged(t, env.ModelBind, cert, model)
	var active atomic.Pointer[application.Application]
	serveManaged(t, env.CallbackBind, cert, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if app := active.Load(); app != nil {
			app.ServeHTTP(w, r)
		} else {
			http.Error(w, "Acceptance application unavailable.", 503)
		}
	}))
	_, callbackPort, err := net.SplitHostPort(env.CallbackBind)
	managedOK(t, err)
	httpTransport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", callbackPort))
	}}
	httpClient := &http.Client{Transport: httpTransport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(httpTransport.CloseIdleConnections)
	master := make([]byte, 32)
	_, err = rand.Read(master)
	managedOK(t, err)
	managedOK(t, os.WriteFile(filepath.Join(out, "master.key"), master, 0600))
	appKey, err := rsa.GenerateKey(rand.Reader, 2048)
	managedOK(t, err)
	initial := map[string]githubemu.File{
		"input.txt":            {Data: []byte("trusted synthetic fixture input\n"), Mode: "100644"},
		"scripts/keep.sh":      {Data: []byte("#!/bin/sh\nprintf 'unchanged\\n'\n"), Mode: "100755"},
		"nested/untouched.txt": {Data: []byte("unrelated repository content\n"), Mode: "100644"},
	}
	emu, err := githubemu.New(githubemu.Config{AppID: 101, InstallationID: 202, RepositoryID: 303, PublicKey: &appKey.PublicKey, Owner: "fixture-owner", Name: "managed-repository", Files: initial})
	managedOK(t, err)
	before := emu.Snapshot()
	var shared application.Shared
	var source *sources.Service
	var results *resultsource.Service
	var project catalog.Project
	var owner catalog.User
	var connection connections.Metadata
	var app *application.Application
	var session auth.Session
	var cookie *http.Cookie
	cfg := application.Config{DataRoot: filepath.Join(out, "central"), PublicOrigin: env.CallbackOrigin, MasterKey: master, InitialOwners: []catalog.Identity{{Issuer: "repository-acceptance", Subject: "synthetic-owner"}}, GitHub: &auth.GitHubConfig{ClientID: "unused-fixture-login", ClientSecret: "unused-fixture-login-secret"}}
	cfg.Projects = func(s application.Shared) (application.ProjectServices, error) {
		shared = s
		var err error
		owner, err = s.Catalog.BootstrapOwner(ctx, cfg.InitialOwners[0], "Synthetic Acceptance Owner")
		if err != nil {
			return application.ProjectServices{}, err
		}
		if project.ID == "" {
			project, err = s.Catalog.CreateProject(ctx, owner.ID, "Managed Repository Acceptance")
			if err != nil {
				return application.ProjectServices{}, err
			}
		}
		source, err = sources.New(ctx, s.CatalogDB, s.Catalog, filepath.Join(cfg.DataRoot, "inputs"))
		if err != nil {
			return application.ProjectServices{}, err
		}
		builder, err := hostedbuilder.New(ctx, hostedbuilder.Config{PrepareInitial: source.PrepareInitial, RootDir: filepath.Join(cfg.DataRoot, "projects"), PublicOrigin: env.CallbackOrigin, GatewayID: env.GatewayID, WorkerID: "repository-acceptance", CatalogDB: s.CatalogDB, Catalog: s.Catalog, Connections: s.Connections, Registry: s.Registry, RequestKey: func(_ context.Context, id string) ([]byte, error) { return s.RequestKey(id) }, Models: func(context.Context, string) (hostedbuilder.Models, error) {
			return hostedbuilder.Models{Catalog: managedModels{}, Cached: managedModels{}, Resolver: &modelproxy.APIKeyResolver{Store: s.Connections, Providers: map[string]string{"openai": "openai"}}}, nil
		}, Endpoint: env.endpoint(), Image: pins.WorkerImage, ProfileSHA256: profile, Server: worker.ServerProfile{LauncherSHA256: pins.WorkerSHA256["kingdom-provider-launcher"], OpenCodeSHA256: pins.WorkerSHA256["opencode"], Lifetime: 3 * time.Minute, ContextLimit: 128000, OutputLimit: 16000}, HelperSHA256: pins.WorkerSHA256["kingdom-workspace-helper"], FileToolSHA256: pins.WorkerSHA256["kingdom-file-tool-helper"], AllowedIPs: []string{env.WorkerIP}, ModelOrigins: []modelproxy.OriginPolicy{{Origin: env.ModelOrigin, AllowedPrivateAddresses: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}, ModelTLSRoots: roots})
		if err != nil {
			source.Close()
			return application.ProjectServices{}, err
		}
		results, err = resultsource.New(s.Catalog, s.Sessions, s.ProjectFactory)
		if err != nil {
			source.Close()
			return application.ProjectServices{}, err
		}
		client, err := repositories.NewTestClient(repositories.Config{AppID: 101, PrivateKey: appKey, Grants: []repositories.Grant{{ProjectID: project.ID, InstallationID: 202, RepositoryID: 303, Publish: true}}}, emu.Transport())
		if err != nil {
			source.Close()
			return application.ProjectServices{}, err
		}
		service, err := repositories.New(ctx, s.CatalogDB, s.Catalog, client, source, results)
		if err != nil {
			client.Close()
			source.Close()
			return application.ProjectServices{}, err
		}
		if connection.ID == "" {
			connection, err = s.Connections.Create(ctx, owner.ID, connections.Personal, connections.Definition{Label: "Emulated GPT-6 Luna", Provider: "openai", Endpoint: env.ModelOrigin + "/v1", AuthKind: "api_key"}, []byte("synthetic-disposable"))
			if err != nil {
				client.Close()
				source.Close()
				return application.ProjectServices{}, err
			}
		}
		return application.ProjectServices{Build: builder.Build, Workers: http.HandlerFunc(builder.Workers), Sources: source, Repositories: service, Close: func(context.Context) error { client.Close(); return source.Close() }}, nil
	}
	start := func() {
		var err error
		app, err = application.New(ctx, cfg)
		managedOK(t, err)
		active.Store(app)
		session, cookie, err = shared.Sessions.CreateSession(ctx, owner.ID)
		managedOK(t, err)
	}
	closeApp := func() error {
		if app == nil {
			return nil
		}
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		err := app.Close(cleanup)
		if err == nil {
			active.Store(nil)
			app = nil
		}
		return err
	}
	t.Cleanup(func() {
		if err := closeApp(); err != nil {
			t.Errorf("managed application cleanup retained debt: %v", err)
		}
	})
	start()
	saveManaged(t, out, "project", project)
	requestKey, err := shared.RequestKey(project.ID)
	managedOK(t, err)
	managedOK(t, os.WriteFile(filepath.Join(out, "request.key"), requestKey, 0600))
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		if err := closeApp(); err != nil {
			t.Errorf("failed acceptance retains application cleanup debt: %v", err)
			return
		}
		if _, err := os.Stat(filepath.Join(cfg.DataRoot, "projects", project.ID, "project.db")); err == nil {
			verifyManagedCleanupMode(t, out, env, pins, profile, project.ID, requestKey, false)
		}
	})
	call := func(method, path, key string, body, result any) int {
		t.Helper()
		raw, err := json.Marshal(body)
		managedOK(t, err)
		r, err := http.NewRequestWithContext(ctx, method, env.CallbackOrigin+path, bytes.NewReader(raw))
		managedOK(t, err)
		r.AddCookie(cookie)
		if method != "GET" {
			r.Header.Set("Origin", env.CallbackOrigin)
			r.Header.Set("X-CSRF-Token", session.CSRFToken)
			r.Header.Set("Content-Type", "application/json")
		}
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		response, err := httpClient.Do(r)
		managedOK(t, err)
		responseBody, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		managedOK(t, err)
		managedOK(t, response.Body.Close())
		if response.StatusCode >= 400 {
			saveManaged(t, out, "http-failure", map[string]any{"method": method, "path": path, "status": response.StatusCode, "response": string(responseBody)})
		}
		if result != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			managedOK(t, json.Unmarshal(responseBody, result))
		}
		return response.StatusCode
	}
	repoPath := "/api/central/v1/projects/" + project.ID + "/repository"
	importKey := uuid.NewString()
	var imported sources.Metadata
	if status := call("POST", repoPath+"/import", importKey, map[string]string{"ref": "main"}, &imported); status != 201 {
		t.Fatalf("repository import HTTP %d", status)
	}
	if imported.Repository == nil || imported.Repository.CommitSHA != before.MainCommit || imported.Files != len(initial) {
		t.Fatal("import did not pin the emulated repository source")
	}
	saveManaged(t, out, "repository-import", imported)
	var replay sources.Metadata
	if status := call("POST", repoPath+"/import", importKey, map[string]string{"ref": "main"}, &replay); status != 201 || !reflect.DeepEqual(imported, replay) {
		t.Fatal("repository import retry changed its immutable input")
	}
	projectPath := "/api/projects/" + project.ID + "/api/v1"
	projectKey := func() string {
		key, err := idempotency.Generate("repository-acceptance", time.Now())
		managedOK(t, err)
		return key
	}
	selection := map[string]any{"model": "openai/gpt-6-luna", "thinking_effort": "medium", "connection": map[string]any{"id": connection.ID, "revision": connection.Revision}}
	var order gen.CreateWorkOrderResult
	if status := call("POST", projectPath+"/work-orders", projectKey(), map[string]string{"work_order_title": "Managed Repository Result", "work_order_text": "Create result.txt with exactly managed result followed by newline. Preserve all existing files and executable modes. Verify result.txt using a confined shell command."}, &order); status != 201 && status != 200 {
		t.Fatalf("work order HTTP %d", status)
	}
	var run map[string]any
	if status := call("POST", projectPath+"/runs", projectKey(), map[string]any{"work_order_id": order.WorkOrderId, "model_selection": map[string]any{"global": selection}, "publish_mode": "direct", "autonomous_approvals": true, "skip_reviews_and_validations": true}, &run); status < 200 || status >= 300 {
		t.Fatalf("managed run HTTP %d", status)
	}
	runID, _ := run["run_id"].(string)
	if runID == "" {
		t.Fatal("managed run ID absent")
	}
	saveManaged(t, out, "run-intent", run)
	for {
		if status := call("GET", projectPath+"/runs/"+runID, "", nil, &run); status != 200 {
			t.Fatalf("run observation HTTP %d", status)
		}
		state, _ := run["status"].(string)
		if state == "succeeded" {
			break
		}
		if state == "failed" || state == "cancelled" {
			saveManaged(t, out, "run-failed", run)
			t.Fatalf("managed run ended %s", state)
		}
		select {
		case <-ctx.Done():
			saveManaged(t, out, "run-timeout", run)
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	saveManaged(t, out, "run-completed", run)
	snapshot, err := results.Resolve(ctx, owner.ID, project.ID, runID)
	managedOK(t, err)
	if len(snapshot.Changes) != 1 || snapshot.Changes[0].Path != "result.txt" || snapshot.Changes[0].Before != nil || snapshot.Changes[0].After == nil {
		t.Fatalf("unexpected canonical result change set: %+v", snapshot.Changes)
	}
	resultFile, err := results.Open(ctx, owner.ID, project.ID, runID, snapshot.SnapshotID, "result.txt")
	managedOK(t, err)
	resultBytes, err := io.ReadAll(resultFile)
	managedOK(t, err)
	managedOK(t, resultFile.Close())
	if string(resultBytes) != "managed result\n" {
		t.Fatal("canonical managed result differs")
	}
	saveManaged(t, out, "canonical-result", snapshot)
	projectRoot := filepath.Join(cfg.DataRoot, "projects", project.ID, "project")
	for path, file := range initial {
		actual := readManagedFile(t, filepath.Join(projectRoot, filepath.FromSlash(path)))
		info, err := os.Stat(filepath.Join(projectRoot, filepath.FromSlash(path)))
		managedOK(t, err)
		mode := os.FileMode(0644)
		if file.Mode == "100755" {
			mode = 0755
		}
		if !bytes.Equal(actual, file.Data) || info.Mode().Perm() != mode {
			t.Fatalf("imported input changed: %s", path)
		}
	}
	status, err := source.Status(ctx, owner.ID, project.ID)
	managedOK(t, err)
	if status.Source == nil || !reflect.DeepEqual(*status.Source, imported) {
		t.Fatal("immutable source changed after managed execution")
	}
	saveManaged(t, out, "source-preservation", map[string]any{"source_metadata_unchanged": true, "files_and_modes_preserved": len(initial)})
	// The first PR write commits in the emulator but loses its reply. Hide the
	// readback, retry, then restart the complete central application before recovery.
	emu.HidePulls(true)
	emu.LoseNextPullReply(true)
	publishKey := uuid.NewString()
	publish := repositories.PublishRequest{RunID: runID, BaseBranch: "main", Title: "Managed Acceptance Result"}
	if status := call("POST", repoPath+"/publish", publishKey, publish, nil); status != 503 {
		t.Fatalf("lost publication reply HTTP %d", status)
	}
	committed := emu.Snapshot()
	if committed.RefCreates != 1 || committed.PullCreates != 1 || len(committed.Pulls) != 1 {
		t.Fatal("lost reply did not retain exactly one branch and pull request")
	}
	assertReadbackOnly := func(stage string) {
		current := emu.Snapshot()
		if current.Writes != committed.Writes || current.RefCreates != 1 || current.PullCreates != 1 {
			t.Fatalf("%s repeated a repository write", stage)
		}
	}
	var pending repositories.Publication
	if status := call("POST", repoPath+"/publish", publishKey, publish, &pending); status != 202 || pending.State != "pending" || emu.Snapshot().PullCreates != 1 {
		t.Fatal("uncertain publication repeated a PR write")
	}
	assertReadbackOnly("same-key retry")
	saveManaged(t, out, "publication-pending", pending)
	managedOK(t, closeApp())
	verifyManagedCleanup(t, out, env, pins, profile, project.ID, requestKey)
	start()
	if status := call("POST", repoPath+"/publish", publishKey, publish, &pending); status != 202 || emu.Snapshot().PullCreates != 1 {
		t.Fatal("central restart repeated an uncertain PR write")
	}
	assertReadbackOnly("application restart")
	emu.HidePulls(false)
	var published repositories.Publication
	if status := call("POST", repoPath+"/publications/"+pending.ID+"/reconcile", uuid.NewString(), map[string]any{}, &published); status != 200 || published.State != "published" || published.PullRequest <= 0 {
		t.Fatalf("exact publication recovery failed: HTTP %d %+v", status, published)
	}
	assertReadbackOnly("exact reconciliation")
	final := emu.Snapshot()
	if final.MainCommit != before.MainCommit || final.Refs["main"] != before.Refs["main"] || len(final.Refs) != 2 || final.Refs[published.Branch] == "" || final.MainWrites != 0 || final.RefCreates != 1 || final.PullCreates != 1 || len(final.Pulls) != 1 {
		t.Fatal("publication changed the source branch or repeated its pull request")
	}
	if final.Pulls[0].HeadRef != published.Branch || final.Pulls[0].HeadSHA != final.Refs[published.Branch] || final.Pulls[0].BaseRef != "main" {
		t.Fatal("recovered pull request differs from its exact branch and head commit")
	}
	publishedFiles := final.FilesByRef[published.Branch]
	if len(publishedFiles) != len(initial)+1 || string(publishedFiles["result.txt"].Data) != "managed result\n" || publishedFiles["result.txt"].Mode != "100644" {
		t.Fatal("published branch differs from the canonical result")
	}
	for path, file := range initial {
		if !reflect.DeepEqual(publishedFiles[path], file) {
			t.Fatalf("publication changed unrelated source file %s", path)
		}
	}
	if len(final.Violations) != 0 {
		t.Fatalf("GitHub emulator contract violations: %v", final.Violations)
	}
	saveManaged(t, out, "publication-recovered", published)
	saveManaged(t, out, "github-emulator", final)
	managedOK(t, closeApp())
	verifyManagedCleanup(t, out, env, pins, profile, project.ID, requestKey)
	if model.count() == 0 {
		t.Fatal("no actual OpenCode request reached the emulated model")
	}
	saveManaged(t, out, "acceptance", map[string]any{"passed": true, "real_kingdom_application": true, "real_project_manager": true, "real_opencode": true, "real_openshell": true, "github_emulated": true, "model_emulated": true, "model_label": "OpenAI - GPT-6 Luna", "thinking_effort": "medium", "model_calls": model.count(), "publication_recovered_after_application_restart": true, "source_branch_unchanged": true, "typed_cleanup_validated": true})
}

type managedEnvironment struct {
	root            string
	GatewayID       string `json:"gateway_id"`
	EndpointAddress string `json:"endpoint_address"`
	WorkerIP        string `json:"worker_ip"`
	CallbackBind    string `json:"callback_bind"`
	CallbackOrigin  string `json:"callback_origin"`
	ModelBind       string `json:"model_bind"`
	ModelOrigin     string `json:"model_origin"`
}
type managedPins struct {
	WorkerImage  string            `json:"worker_image"`
	WorkerSHA256 map[string]string `json:"worker_sha256"`
}

func (e managedEnvironment) endpoint() worker.Endpoint {
	return worker.Endpoint{Address: e.EndpointAddress, CAFile: filepath.Join(e.root, "tls", "ca.crt"), CertificateFile: filepath.Join(e.root, "tls", "operator.crt"), KeyFile: filepath.Join(e.root, "tls", "operator.key")}
}
func managedOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func readManagedFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	managedOK(t, err)
	return data
}
func readManagedJSON[T any](t *testing.T, path string) T {
	t.Helper()
	var value T
	managedOK(t, json.Unmarshal(readManagedFile(t, path), &value))
	return value
}
func saveManaged(t *testing.T, out, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	managedOK(t, err)
	managedOK(t, os.WriteFile(filepath.Join(out, name+".json"), append(data, '\n'), 0600))
}
func loadManagedEnvironment(t *testing.T) (managedEnvironment, string) {
	t.Helper()
	root, out := os.Getenv("KINGDOM_REPOSITORY_ACCEPTANCE_ENV"), os.Getenv("KINGDOM_REPOSITORY_ACCEPTANCE_DIR")
	if !filepath.IsAbs(root) || !filepath.IsAbs(out) {
		t.Fatal("enabled acceptance requires absolute environment and fresh evidence paths")
	}
	cwd, err := os.Getwd()
	managedOK(t, err)
	for {
		if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(cwd)
		if parent == cwd {
			t.Fatal("repository root absent")
		}
		cwd = parent
	}
	rel, err := filepath.Rel(filepath.Join(cwd, "kd-sandbox"), out)
	managedOK(t, err)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatal("evidence must use a fresh directory under this repository's kd-sandbox")
	}
	env := readManagedJSON[managedEnvironment](t, filepath.Join(root, "config.json"))
	env.root = root
	for _, address := range []string{env.EndpointAddress, env.CallbackBind, env.ModelBind} {
		_, port, err := net.SplitHostPort(address)
		if err != nil || port == "8080" || port == "0" {
			t.Fatal("explicit non-8080 managed acceptance ports required")
		}
	}
	for _, origin := range []string{env.CallbackOrigin, env.ModelOrigin} {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			t.Fatal("explicit HTTPS fixture origins required")
		}
	}
	if env.GatewayID == "" || net.ParseIP(env.WorkerIP) == nil {
		t.Fatal("managed gateway identity and host IP required")
	}
	managedOK(t, os.Mkdir(out, 0700))
	return env, out
}
func serveManaged(t *testing.T, address string, cert tls.Certificate, handler http.Handler) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	managedOK(t, err)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}}
	go func() { _ = server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	t.Cleanup(func() { _ = server.Close() })
}

type managedDisposal struct{ db *sql.DB }

func (d managedDisposal) RequireClosed(ctx context.Context, owner remote.Owner, ref remote.Reference) error {
	return providerrequest.RequireRemoteDisposalPrerequisites(ctx, d.db, owner, ref)
}

// Use the canonical typed recovery path after shutdown, then re-read and
// validate every historical allocation. Row counts alone do not prove closure.
func verifyManagedCleanup(t *testing.T, out string, env managedEnvironment, pins managedPins, profile []byte, project string, key []byte) {
	verifyManagedCleanupMode(t, out, env, pins, profile, project, key, true)
}
func verifyManagedCleanupMode(t *testing.T, out string, env managedEnvironment, pins managedPins, profile []byte, project string, key []byte, strict bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(out, "central", "projects", project, "project.db")+"?_pragma=foreign_keys(1)")
	managedOK(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	rows, err := db.QueryContext(ctx, "SELECT allocation FROM remote_runtimes ORDER BY allocation")
	managedOK(t, err)
	var ids []int64
	for rows.Next() {
		var id int64
		managedOK(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	managedOK(t, rows.Err())
	managedOK(t, rows.Close())
	if len(ids) == 0 {
		if !strict {
			return
		}
		t.Fatal("no real managed allocation was retained")
	}
	requests, err := remote.NewRequestStore(key)
	managedOK(t, err)
	central, err := sql.Open("sqlite", filepath.Join(out, "central", "central.db"))
	managedOK(t, err)
	defer central.Close()
	var workspace string
	managedOK(t, central.QueryRowContext(ctx, "SELECT name FROM hosted_project_workspaces WHERE project_id=?", project).Scan(&workspace))
	type proof struct {
		Allocation                                                   int64 `json:"allocation"`
		Commands                                                     int   `json:"commands"`
		Import, Export, Output, Capabilities, Archive, TypedDeletion bool
		NeverCreated                                                 bool `json:"never_created,omitempty"`
	}
	var proofs []proof
	created := 0
	for _, id := range ids {
		reservation, err := remote.ReadRuntime(ctx, db, id)
		managedOK(t, err)
		owner, ref := reservation.Owner, reservation.Reference
		never, err := remote.ReadRuntimeNeverCreated(ctx, db, owner, ref)
		managedOK(t, err)
		if never {
			proofs = append(proofs, proof{Allocation: id, NeverCreated: true})
			continue
		}
		created++
		gateway, err := worker.Connect(db, requests, worker.Config{Target: ref.Target, Workspace: workspace, Image: pins.WorkerImage, ProfileSHA256: profile, Egress: worker.Egress{ModelURL: env.CallbackOrigin + "/worker/projects/" + project + "/model/v1", CallbackURL: env.CallbackOrigin + "/worker/projects/" + project + "/plugin", AllowedIPs: []string{env.WorkerIP}}}, env.endpoint())
		managedOK(t, err)
		disposer, err := worker.NewDisposer(gateway.Worker, managedDisposal{db})
		managedOK(t, err)
		for {
			err = disposer.Dispose(ctx, reservation)
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				gateway.Close()
				t.Fatalf("allocation %d retains disposal debt: %v", id, err)
			case <-time.After(time.Second):
			}
		}
		managedOK(t, gateway.Close())
		if strict {
			managedOK(t, remote.RequireWorkspaceImportTx(ctx, db, owner, ref))
			managedOK(t, remote.RequireWorkspaceExport(ctx, db, owner, ref))
			managedOK(t, remote.RequireWorkspaceOutputApplied(ctx, db, owner, ref))
		}
		managedOK(t, providerrequest.RequireRemoteCapabilitiesClosed(ctx, db, providerrequest.Owner{Kind: providerrequest.OwnerKind(owner.Kind), InvocationID: owner.InvocationID}, id))
		_, commands, err := remote.DisposalInventory(ctx, db, owner, ref)
		managedOK(t, err)
		_, err = remote.ReadRuntimeArchive(ctx, db, owner, ref)
		managedOK(t, err)
		managedOK(t, remote.RequireRuntimeDisposed(ctx, db, owner, ref))
		managedOK(t, providerrequest.RequireRemoteDisposalPrerequisites(ctx, db, owner, ref))
		proofs = append(proofs, proof{Allocation: id, Commands: len(commands), Import: strict, Export: strict, Output: strict, Capabilities: true, Archive: true, TypedDeletion: true})
	}
	if strict && created == 0 {
		t.Fatal("no real managed runtime was created")
	}
	if strict {
		var shellCommands, completedShellCommands int
		managedOK(t, db.QueryRowContext(ctx, `SELECT count(*) FROM remote_commands`).Scan(&shellCommands))
		managedOK(t, db.QueryRowContext(ctx, `SELECT count(*) FROM remote_commands r JOIN provider_remote_command_completions c ON c.operation_id=r.command_id AND c.claim_id=r.claim_id`).Scan(&completedShellCommands))
		if shellCommands < 1 || completedShellCommands != shellCommands {
			t.Fatal("confined shell execution lacks exact canonical completion evidence")
		}
		saveManaged(t, out, "managed-commands", map[string]int{"remote_shell_commands": shellCommands, "matching_remote_shell_completions": completedShellCommands})
	}
	name := "managed-cleanup"
	if !strict {
		name = "managed-failure-cleanup"
	}
	saveManaged(t, out, name, proofs)
}

type managedModels struct{}

func (managedModels) List(context.Context, bool) ([]gen.ProviderModel, error) {
	return []gen.ProviderModel{{Id: "openai/gpt-6-luna", ModelName: "GPT-6 Luna", ProviderId: "openai", ProviderName: "OpenAI", ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "medium", Label: "Medium"}}}}, nil
}
func (m managedModels) Snapshot(ctx context.Context, force bool) (api.ProviderModelCatalogSnapshot, error) {
	items, err := m.List(ctx, force)
	return api.ProviderModelCatalogSnapshot{Items: items, Status: api.ProviderModelCatalogStatusFresh}, err
}
func (managedModels) Validate(_ context.Context, m connections.Metadata, model, effort string) error {
	if m.Definition.Provider != "openai" || model != "openai/gpt-6-luna" || effort != "medium" {
		return fmt.Errorf("emulated model selection rejected")
	}
	return nil
}
