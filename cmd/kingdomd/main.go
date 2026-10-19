package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"kingdom/internal/api"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/providerrequest"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/localauth"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/processguardian"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/shutdown"
	"kingdom/internal/state"
	"kingdom/internal/theme"
	webui "kingdom/web"
)

// Exit codes. Operators / supervisors / CI scripts can distinguish
// "clean shutdown" from "shutdown drain failed" via the exit code,
// not log scraping.
const (
	exitOK           = 0
	exitFatalStartup = 1
	exitDrainFailure = 2
	exitServeFailure = 3
)

func main() {
	if exitCode, handled := processguardian.DispatchHiddenMode(os.Args); handled {
		os.Exit(exitCode)
	}
	os.Exit(run())
}

// run owns the daemon lifecycle and returns a non-zero exit code on
// any failure that operators need to act on. Defers fire on natural
// return, which is critical so db.Close runs after the planner drain
// (otherwise the planner's decoupled-ctx terminal writes would hit a
// closed DB).
//
// The named exitCode return lets the deferred shutdown safety-net
// (set up further down, after apiServer/server are constructed)
// surface a non-clean drain via exit code on routes that don't
// already do so themselves — most importantly the bind-failure /
// serve-error paths, where the safety-net is the only Coordinator.Run
// invocation. The SIGINT path captures CloseResult and sets exitCode
// directly, so the safety-net's identical observation (cached via
// sync.Once) is a no-op there. exitCode == exitOK guard preserves
// any earlier exitFatalStartup so a successful drain after a
// construction failure doesn't downgrade the exit code.
func run() (exitCode int) {
	var (
		dbPath               string
		listen               string
		basePath             string
		providerEngine       string
		governanceMode       string
		autonomousApprovals  bool
		providerBinary       string
		credentialFile       string
		githubSharedSlug     string
		githubSharedClientID string
	)

	flag.StringVar(&dbPath, "db-path", "./.kingdom/kingdom.db", "sqlite database path")
	flag.StringVar(&listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flag.StringVar(&basePath, "base-path", "/api/v1", "API base path")
	flag.StringVar(&providerEngine, "provider-engine", envOrDefault("KINGDOM_PROVIDER_ENGINE", "opencode"), "provider engine: opencode")
	flag.StringVar(&governanceMode, "governance-mode", envOrDefault("KINGDOM_GOVERNANCE_MODE", "autonomous"), "governance mode (MVP): autonomous")
	flag.BoolVar(&autonomousApprovals, "autonomous-approvals", envBoolOrDefault("KINGDOM_AUTONOMOUS_APPROVALS", true), "autonomous governance approvals for plan and final closure")
	flag.StringVar(&providerBinary, "provider-binary", envOrDefault("KINGDOM_PROVIDER_BINARY", "opencode"), "opencode binary path")
	flag.StringVar(&credentialFile, "credential-file", "./.kingdom/secrets/operator-token", "local operator credential file")
	flag.StringVar(&githubSharedSlug, "github-shared-app-slug", envOrDefaultAllowEmpty("KINGDOM_GITHUB_SHARED_APP_SLUG", githubexternal.OfficialSharedAppSlug), "public shared GitHub App slug (paired with client id)")
	flag.StringVar(&githubSharedClientID, "github-shared-app-client-id", envOrDefaultAllowEmpty("KINGDOM_GITHUB_SHARED_APP_CLIENT_ID", githubexternal.OfficialSharedAppClientID), "public shared GitHub App client id (paired with slug)")
	flag.Parse()
	if (strings.TrimSpace(githubSharedSlug) == "") != (strings.TrimSpace(githubSharedClientID) == "") {
		log.Printf("github shared app slug and client id must both be set or both be blank")
		return exitFatalStartup
	}
	validatedBasePath, err := api.ValidateBasePath(basePath)
	if err != nil {
		log.Printf("invalid API base path: %v", err)
		return exitFatalStartup
	}
	basePath = validatedBasePath
	operatorCredential, err := localauth.ReadCredential(credentialFile)
	if err != nil {
		log.Printf("read operator credential: %v", err)
		return exitFatalStartup
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Printf("create database directory: %v", err)
		return exitFatalStartup
	}

	dsn, err := kingdomdSQLiteDSN(dbPath)
	if err != nil {
		log.Printf("resolve sqlite database path: %v", err)
		return exitFatalStartup
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Printf("open sqlite database: %v", err)
		return exitFatalStartup
	}
	defer db.Close()

	themeStore, err := theme.OpenStore(filepath.Dir(dbPath))
	if err != nil {
		log.Printf("open theme storage: %v", err)
		return exitFatalStartup
	}
	defer themeStore.Close()

	if err := ensureSchema(db); err != nil {
		log.Printf("ensure schema: %v", err)
		return exitFatalStartup
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		log.Printf("resolve working directory: %v", err)
		return exitFatalStartup
	}

	bootstrapPaths := kingdom.DefaultBootstrapPaths(repoRoot)
	providerServiceRoot := opencode.ServiceRuntimeRoot(filepath.Join(filepath.Dir(dbPath), "runtime"))
	providerService, err := opencode.NewService(opencode.ServiceConfig{
		BinaryPath:  providerBinary,
		WorkDir:     repoRoot,
		RuntimeRoot: providerServiceRoot,
		Requests:    providerrequest.NewAuthority(db, nil),
	})
	if err != nil {
		log.Printf("configure OpenCode service: %v", err)
		return exitFatalStartup
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providerService.CloseContext(closeCtx); err != nil {
			log.Printf("OpenCode service close error: %v", err)
			if exitCode == exitOK {
				exitCode = exitDrainFailure
			}
		}
	}()
	runtime, err := kingdom.NewRuntime(kingdom.Config{
		DB:                  db,
		Paths:               bootstrapPaths,
		ProviderEngine:      providerEngine,
		GovernanceMode:      governanceMode,
		AutonomousApprovals: &autonomousApprovals,
		WorkspaceRoot:       filepath.Join(filepath.Dir(dbPath), "runtime"),
		GuardianRuntimeRoot: providerServiceRoot,
		Provider:            providerService,
	})
	if err != nil {
		log.Printf("bootstrap runtime: %v", err)
		return exitFatalStartup
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	apiServer, err := api.NewHandler(api.ServerConfig{
		DB:                         db,
		ThemeStore:                 themeStore,
		StartupContext:             ctx,
		BasePath:                   basePath,
		AutonomousApprovalsDefault: &autonomousApprovals,
		ProviderService:            providerService,
		ProviderModelCatalog:       api.NewOpenCodeProviderModelCatalog(providerService, nil, api.ProviderModelCatalogTTL),
		ProviderPolicyPath:         bootstrapPaths.ProviderPolicyPath,
		ProviderPolicySchemaPath:   bootstrapPaths.ProviderPolicySchemaPath,
		ProjectRoot:                repoRoot,
		RuntimeRoot:                filepath.Join(filepath.Dir(dbPath), "runtime"),
		RetryEngine:                runtime.RetryEngine(),
		OperatorCredential:         operatorCredential,
		GitHubSharedAppSlug:        githubSharedSlug,
		GitHubSharedAppClientID:    githubSharedClientID,
	})
	if err != nil {
		log.Printf("build api handler: %v", err)
		return exitFatalStartup
	}
	runtime.SetPreDispatchRecovery(apiServer.RecoverPlanningOperations)
	if err := runtime.Start(ctx); err != nil {
		_ = apiServer.Close(context.Background())
		log.Printf("start runtime: %v", err)
		return exitFatalStartup
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			log.Printf("runtime close error: %v", err)
		}
	}()
	runtimeCloser := shutdown.CompositeCloser{Closers: []shutdown.Closer{
		apiServer,
		shutdown.CloseFunc(runtime.GracefulClose),
	}}

	// Concurrent shutdown coordinator: cancel the planning service in
	// parallel with HTTP server shutdown so a long-lived SSE/planner
	// request cannot consume the entire HTTP grace window before the
	// planner is ever cancelled. The Coordinator's sync.Once means a
	// follow-up call (e.g. an unexpected serveErr path) is a true
	// no-op — no second wg.Wait, no double-paid drain.
	var shutdownCoord shutdown.Coordinator

	// API paths go to the handler, everything else falls through to the
	// embedded SPA (so deep-link refreshes keep working). The mux is
	// constructed by api.NewSPAMux so both kingdomd and the install_mode
	// summon flow route via the same predicate — no chance of drifting
	// trailing-slash semantics between the two front doors.
	mux := api.NewSPAMux(apiServer, webui.Handler(basePath), basePath)

	server := api.NewHTTPServer(listen, mux)

	// Safety-net drain. Every exit path from run() — SIGINT (which
	// also calls shutdownCoord.Run explicitly to capture the result
	// for the exit code), serve-error, ErrServerClosed, panic — fires
	// this deferred call. The Coordinator's sync.Once guarantees the
	// underlying teardowns run at most once: an explicit SIGINT call
	// observes the result; the deferred call is a real no-op via the
	// cached CloseResult. Without this, a bind-failure ListenAndServe
	// returns past the planning service's recovery loop with db.Close
	// firing seconds later — leaving runner goroutines mutating a
	// closed DB.
	defer func() {
		// Coordinator's once-guard means this call returns the cached
		// CloseResult when the explicit graceful path already invoked
		// Run (SIGINT case). On bind-failure / serve-error paths where
		// the safety-net is the only call, this is the actual drain.
		// Either way, surface a non-clean drain via exit code on routes
		// that don't already do so. Coordinator.Run already logs each
		// subsystem error per-call (api server close error, server
		// shutdown error) so we don't double-log here.
		result := shutdownCoord.Run(10*time.Second, runtimeCloser, server, log.Printf)
		if (result.Closer != nil || result.Shutdowner != nil) && exitCode == exitOK {
			exitCode = exitDrainFailure
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
		// Drive both teardowns concurrently. apiServer.Close cancels
		// the recovery loop and waits for in-flight runner goroutines
		// to finalize (each as `cancelled` rather than `failed` via
		// the decoupled-ctx terminal write); server.Shutdown stops
		// accepting new requests and waits for in-flight HTTP
		// connections (including SSE) to finish. Running in parallel
		// means SSE handlers see the planner's `cancelled` events and
		// exit promptly rather than holding HTTP shutdown open until
		// its own deadline.
		result := shutdownCoord.Run(10*time.Second, runtimeCloser, server, log.Printf)
		// Operators / supervisors need to know on the wire — not via
		// log scraping — when shutdown didn't drain cleanly. Either
		// side erroring is enough for a non-zero exit.
		if result.Closer != nil || result.Shutdowner != nil {
			return exitDrainFailure
		}
		return exitOK
	case serveErr := <-errCh:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return exitOK
		}
		log.Printf("serve http: %v", serveErr)
		return exitServeFailure
	}
}

func kingdomdSQLiteDSN(dbPath string) (string, error) {
	absolutePath, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("resolve sqlite path: %w", err)
	}
	sqliteURL := &url.URL{Scheme: "file", Path: absolutePath}
	query := sqliteURL.Query()
	for _, pragma := range []string{"busy_timeout(5000)", "foreign_keys(ON)", "journal_mode(WAL)", "synchronous(NORMAL)"} {
		query.Add("_pragma", pragma)
	}
	sqliteURL.RawQuery = query.Encode()
	return sqliteURL.String(), nil
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envOrDefaultAllowEmpty(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envBoolOrDefault(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func ensureSchema(db *sql.DB) error {
	migrationNames, err := dbmigrations.List()
	if err != nil {
		return fmt.Errorf("list canonical migrations: %w", err)
	}
	migrations := make([]state.SQLMigration, 0, len(migrationNames))
	for _, name := range migrationNames {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			return fmt.Errorf("read canonical migration %s: %w", name, err)
		}
		migrations = append(migrations, state.SQLMigration{Name: name, SQL: raw})
	}

	if err := state.ApplySQLMigrations(context.Background(), db, migrations, time.Now().UTC()); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}
