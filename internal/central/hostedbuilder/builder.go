// Package hostedbuilder assembles isolated hosted project runtimes. It owns no
// listener, central credentials, shared registry, or central database lifetime.
package hostedbuilder

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/projects"
	"kingdom/internal/central/selections"
	"kingdom/internal/central/workerauthority"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/stack"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	kingdom "kingdom/internal/orchestration"
	runtime "kingdom/internal/projectruntime"
	"kingdom/internal/state"
	"kingdom/internal/theme"
	_ "modernc.org/sqlite"
)

type Models struct {
	Catalog  api.ProviderModelCatalog
	Cached   selections.CachedModels
	Resolver modelproxy.Resolver
}
type Config struct {
	PrepareInitial                             func(context.Context, string, string) error
	RootDir, PublicOrigin, GatewayID, WorkerID string
	// WorkerOrigin is the sandbox-reachable TLS origin. Empty uses PublicOrigin.
	WorkerOrigin                 string
	CatalogDB                    *sql.DB
	Catalog                      *catalog.Store
	Connections                  *connections.Store
	Registry                     *capability.Registry
	RequestKey                   func(context.Context, string) ([]byte, error)
	Models                       func(context.Context, string) (Models, error)
	Endpoint                     worker.Endpoint
	Image                        string
	ProfileSHA256                []byte
	Server                       worker.ServerProfile
	HelperSHA256, FileToolSHA256 string
	AllowedIPs                   []string
	ModelOrigins                 []modelproxy.OriginPolicy
	ModelTLSRoots                *x509.CertPool
	TransferTimeout, StopTimeout time.Duration
}
type Service struct {
	c        Config
	mu       sync.RWMutex
	handlers map[string]http.Handler
}

func New(ctx context.Context, c Config) (*Service, error) {
	var e error
	if c.WorkerOrigin == "" {
		c.WorkerOrigin = c.PublicOrigin
	}
	if c.CatalogDB == nil || c.Catalog == nil || c.Connections == nil || c.Registry == nil || c.RequestKey == nil || c.Models == nil || !filepath.IsAbs(c.RootDir) || c.GatewayID == "" || c.WorkerID == "" || !validOrigin(c.PublicOrigin) || !validOrigin(c.WorkerOrigin) {
		return nil, errors.New("explicit hosted builder configuration required")
	}
	if !digest(c.HelperSHA256) || !digest(c.FileToolSHA256) || !digest(c.Server.LauncherSHA256) || !digest(c.Server.OpenCodeSHA256) || len(c.ProfileSHA256) != 32 || strings.Count(c.Image, "@sha256:") != 1 || !digest(strings.Split(c.Image, "@sha256:")[1]) || c.Server.Lifetime <= 0 || c.Server.ContextLimit <= 0 || c.Server.OutputLimit <= 0 {
		return nil, errors.New("immutable managed worker pins required")
	}
	if e = privateDir(c.RootDir); e != nil {
		return nil, e
	}
	if e = initializeMappings(ctx, c.CatalogDB); e != nil {
		return nil, e
	}
	if c.TransferTimeout == 0 {
		c.TransferTimeout = time.Minute
	}
	if c.StopTimeout == 0 {
		c.StopTimeout = time.Minute
	}
	return &Service{c: c, handlers: map[string]http.Handler{}}, nil
}
func (c Config) workerBase(project string) string {
	origin := c.WorkerOrigin
	if origin == "" {
		origin = c.PublicOrigin
	}
	return origin + "/worker/projects/" + project
}

func validOrigin(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.Path == "" && u.RawPath == "" && u.Opaque == "" && u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && !strings.Contains(raw, "#")
}

func privateDir(path string) error {
	if e := os.MkdirAll(path, 0700); e != nil {
		return e
	}
	info, e := os.Lstat(path)
	if e != nil {
		return e
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("private hosted directory required")
	}
	return nil
}

// Workers never constructs runtimes or waits for project startup.
func (s *Service) Workers(w http.ResponseWriter, r *http.Request) {
	const prefix = "/worker/projects/"
	if r.TLS == nil || !strings.HasPrefix(r.URL.Path, prefix) || r.URL.RawPath != "" {
		http.NotFound(w, r)
		return
	}
	id, route, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, prefix), "/")
	parsed, e := uuid.Parse(id)
	if !ok || e != nil || parsed.String() != id {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	switch route {
	case "model/v1/responses", "model/v1/chat/completions", "plugin/v1/native/invoke", "plugin/v1/command/invoke", "plugin/v1/command/cancel":
	default:
		http.NotFound(w, r)
		return
	}
	s.mu.RLock()
	h := s.handlers[id]
	s.mu.RUnlock()
	if h == nil {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}
func (s *Service) Build(b projects.BuildContext, id string, auth api.Authenticator) (result projects.Runtime, err error) {
	parsed, e := uuid.Parse(id)
	if e != nil || parsed == uuid.Nil || parsed.String() != id || auth == nil || b.Startup == nil || b.Lifetime == nil {
		return nil, projects.ErrUnavailable
	}
	var exists int
	if e = s.c.CatalogDB.QueryRowContext(b.Startup, `SELECT count(*) FROM central_projects WHERE id=?`, id).Scan(&exists); e != nil || exists != 1 {
		return nil, projects.ErrUnavailable
	}
	root := filepath.Join(s.c.RootDir, id)
	for _, dir := range []string{root, filepath.Join(root, "project"), filepath.Join(root, "runtime"), filepath.Join(root, "source"), filepath.Join(root, "stage"), filepath.Join(root, "spool")} {
		if e = privateDir(dir); e != nil {
			return nil, e
		}
	}
	home := filepath.Join(root, "runtime")
	projectRoot := filepath.Join(root, "project")
	if s.c.PrepareInitial != nil {
		if e = s.c.PrepareInitial(b.Startup, id, projectRoot); e != nil {
			return nil, e
		}
	}
	if e = bootstrapassets.WriteScaffold(home, false); e != nil {
		return nil, e
	}

	dbPath := filepath.Join(root, "project.db")
	file, openErr := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if openErr == nil {
		if e = file.Close(); e != nil {
			return nil, e
		}
	} else if !os.IsExist(openErr) {
		return nil, openErr
	}
	info, e := os.Lstat(dbPath)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("private project database required")
	}
	db, e := sql.Open("sqlite", databaseDSN(dbPath))
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	owned := &project{db: db, service: s, id: id, closeToken: make(chan struct{}, 1)}
	owned.closeToken <- struct{}{}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), s.c.StopTimeout)
			defer cancel()
			if closeErr := owned.Close(cleanup); closeErr != nil {
				result = owned
				err = errors.Join(err, closeErr)
			}
		}
	}()
	names, e := dbmigrations.List()
	if e != nil {
		return nil, e
	}
	migrations := make([]state.SQLMigration, 0, len(names))
	for _, name := range names {
		raw, e := dbmigrations.Read(name)
		if e != nil {
			return nil, e
		}
		migrations = append(migrations, state.SQLMigration{Name: name, SQL: raw})
	}
	if e = state.ApplySQLMigrations(b.Startup, db, migrations, time.Now().UTC()); e != nil {
		return nil, e
	}
	if _, e = db.ExecContext(b.Startup, `CREATE TABLE IF NOT EXISTS hosted_database_identity(slot INTEGER PRIMARY KEY CHECK(slot=1),id TEXT NOT NULL UNIQUE);CREATE TRIGGER IF NOT EXISTS hosted_database_identity_immutable BEFORE UPDATE ON hosted_database_identity BEGIN SELECT RAISE(ABORT,'database identity immutable'); END;CREATE TRIGGER IF NOT EXISTS hosted_database_identity_retained BEFORE DELETE ON hosted_database_identity BEGIN SELECT RAISE(ABORT,'database identity retained'); END;INSERT OR IGNORE INTO hosted_database_identity VALUES(1,?)`, uuid.NewString()); e != nil {
		return nil, e
	}
	var databaseID string
	if e = db.QueryRowContext(b.Startup, `SELECT id FROM hosted_database_identity WHERE slot=1`).Scan(&databaseID); e != nil {
		return nil, e
	}
	tx, e := db.BeginTx(b.Startup, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	if e = access.BindProjectTx(b.Startup, tx, id); e != nil {
		return nil, e
	}
	if e = s.c.Registry.BindProjectTx(b.Startup, tx); e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	key, e := s.c.RequestKey(b.Startup, id)
	if e != nil {
		return nil, e
	}
	requests, e := remote.NewRequestStore(key)
	clear(key)
	if e != nil {
		return nil, e
	}
	gateway, e := worker.Dial(s.c.Endpoint)
	if e != nil {
		return nil, e
	}
	owned.gateway = gateway
	mapping, e := workspace(b.Startup, s.c.CatalogDB, gateway.Native, id, s.c.GatewayID, databaseID, time.Now())
	if e != nil {
		return nil, e
	}
	base := s.c.workerBase(id)
	modelURL := base + "/model/v1"
	pluginURL := base + "/plugin"
	target := remote.Target{WorkerID: s.c.WorkerID, GatewayID: s.c.GatewayID, WorkspaceID: mapping.ID}
	authority := providerrequest.NewAuthority(db, time.Now)
	authorizer, e := workerauthority.New(authority, s.c.Connections)
	if e != nil {
		return nil, e
	}
	modelConfig, e := s.c.Models(b.Startup, id)
	if e != nil {
		return nil, e
	}
	if modelConfig.Catalog == nil || modelConfig.Cached == nil || modelConfig.Resolver == nil {
		return nil, errors.New("project model catalog unavailable")
	}
	selection, e := selections.New(s.c.Connections, modelConfig.Cached)
	if e != nil {
		return nil, e
	}
	pipeline, e := stack.New(stack.Config{DB: db, Requests: requests, Gateway: gateway, Registry: s.c.Registry, Authority: authority, Worker: worker.Config{Target: target, Workspace: mapping.Name, Image: s.c.Image, ProfileSHA256: s.c.ProfileSHA256, Egress: worker.Egress{ModelURL: modelURL, CallbackURL: pluginURL, AllowedIPs: s.c.AllowedIPs}}, Server: s.c.Server, Root: root, RuntimeRoot: home, HelperSHA256: s.c.HelperSHA256, FileToolSHA256: s.c.FileToolSHA256, ModelAuthorizer: workerauthority.Model{Authority: authorizer}, PluginAuthorizer: workerauthority.Plugin{Authority: authorizer}, Resolver: modelConfig.Resolver, ModelOrigins: s.c.ModelOrigins, ModelTLSRoots: s.c.ModelTLSRoots, TransferTimeout: s.c.TransferTimeout, StopTimeout: s.c.StopTimeout})
	owned.models, owned.sources, owned.provider = pipeline.Models, pipeline.Sources, pipeline.Provider
	if e != nil {
		return nil, e
	}
	managed, models, callback := pipeline.Provider, pipeline.Models, pipeline.Callback
	themes, e := theme.OpenStore(home)
	if e != nil {
		return nil, e
	}
	owned.themes = themes
	mux := http.NewServeMux()
	mux.Handle("/worker/projects/"+id+"/model/", http.StripPrefix("/worker/projects/"+id+"/model", models.Handler()))
	mux.Handle("/worker/projects/"+id+"/plugin/", http.StripPrefix("/worker/projects/"+id+"/plugin", callback))
	s.mu.Lock()
	if s.handlers[id] != nil {
		s.mu.Unlock()
		return nil, projects.ErrUnavailable
	}
	s.handlers[id] = mux
	owned.registered = true
	s.mu.Unlock()
	instance, e := runtime.New(b.Startup, runtime.Config{Mode: runtime.Hosted, LifetimeContext: b.Lifetime, DB: db, ThemeStore: themes, Paths: paths(home), ProjectRoot: projectRoot, YoloOutputRoot: projectRoot, RuntimeRoot: home, BasePath: "/api/v1", PublicAPIBase: "/api/projects/" + id + "/api/v1", ProviderEngine: "opencode", GovernanceMode: "autonomous", PublishMode: "direct", Authenticator: auth, ProviderService: managed, ProviderModelCatalog: modelConfig.Catalog, HostedSelection: &api.HostedSelectionConfig{ProjectID: id, Resolver: selection}})
	owned.instance = instance
	if e != nil {
		return nil, e
	}
	if e = instance.Start(b.Lifetime); e != nil {
		return nil, e
	}
	return owned, nil
}

func paths(home string) kingdom.BootstrapPaths {
	return kingdom.BootstrapPaths{SchemaDir: filepath.Join(home, "schemas"), RetryPolicyPath: filepath.Join(home, "policies/retry_policy.yaml"), RetryPolicySchemaPath: filepath.Join(home, "schemas/retry_policy.schema.json"), ProviderPolicyPath: filepath.Join(home, "policies/opencode_provider_policy.yaml"), ProviderPolicySchemaPath: filepath.Join(home, "schemas/opencode_provider_policy.schema.json"), RolePolicyPath: filepath.Join(home, "policies/role_policy.yaml"), NetworkPolicyPath: filepath.Join(home, "policies/network_policy.yaml")}
}

func digest(s string) bool {
	raw, e := hex.DecodeString(s)
	return e == nil && len(raw) == 32 && hex.EncodeToString(raw) == s
}

func databaseDSN(path string) string {
	query := url.Values{}
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	return (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String()
}
