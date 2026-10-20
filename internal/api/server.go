// Package api exposes Kingdom's HTTP boundary.
//
// It builds the validated API handler, translates requests into calls on the
// planning, state, workflow, and execution layers, and keeps the public
// contract aligned across OpenAPI validation, idempotency, SSE, preview
// surfaces, and the canonical JSON error envelope.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/planning"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
	"kingdom/internal/theme"
	"kingdom/internal/workspace"
)

const (
	defaultAPIBasePath   = "/api/v1"
	defaultTimelineLimit = 50
	maxTimelineLimit     = 200
	// fixedUTCTimestampLayout preserves chronological order when SQLite compares
	// provenance timestamps as text.
	fixedUTCTimestampLayout = "2006-01-02T15:04:05.000000000Z"
)

var generatedIDCounter uint64

type ServerConfig struct {
	// ThemeStore is owned by the runtime that constructs the API server.
	ThemeStore                 *theme.Store
	DB                         *sql.DB
	StartupContext             context.Context
	BasePath                   string
	ArtifactDownloadURL        string
	AutonomousApprovalsDefault *bool
	ProviderEngine             string
	// ProviderService runs planner requests. It is shared with run execution and
	// model discovery, and is closed by the process that created it.
	ProviderService          *opencode.Service
	ProviderModelCatalog     ProviderModelCatalog
	ProviderPolicyPath       string
	ProviderPolicySchemaPath string
	SchemaDir                string
	ProjectRoot              string
	RuntimeRoot              string
	DefaultPublishMode       string
	Now                      func() time.Time
	IDGenerator              func(prefix string) string
	PlannerAgent             planning.Agent
	RetryEngine              *retryengine.Engine
	ExternalAdapter          external.Adapter
	GitHubKeyStore           *githubexternal.KeyStore
	GitHubUserTokenStore     *githubexternal.UserTokenStore
	GitHubSetupClient        *githubexternal.SetupClient
	GitHubDeviceOAuthClient  *githubexternal.DeviceOAuthClient
	GitHubSharedAppSlug      string
	GitHubSharedAppClientID  string
	// OperatorCredential is the local bearer credential read from a protected
	// file by the process entrypoint. It is never persisted by the API.
	OperatorCredential []byte
}

type Server struct {
	themes                     *theme.Store
	db                         *sql.DB
	now                        func() time.Time
	idGenerator                func(prefix string) string
	basePath                   string
	artifactDownloadURL        string
	autonomousApprovalsDefault bool
	providerEngine             string
	providerModelCatalog       ProviderModelCatalog
	projectRoot                string
	defaultPublishMode         string
	lifecycleCtx               context.Context
	lifecycleCancel            context.CancelFunc

	eventIngestor           *state.AttemptEventIngestor
	transitionService       *state.TransitionService
	artifactService         *state.ArtifactService
	tokenUsageStore         *state.TokenUsageStore
	planningStore           *state.PlanningStore
	planningService         *planning.Service
	externalStore           *state.ExternalStore
	externalRegistry        *external.Registry
	githubKeys              *githubexternal.KeyStore
	githubUserTokens        *githubexternal.UserTokenStore
	appKeyLifecycle         connectionCredentialLifecycleStore
	userTokenLifecycle      connectionCredentialLifecycleStore
	githubSetup             *githubexternal.SetupClient
	githubDeviceOAuth       *githubexternal.DeviceOAuthClient
	githubSharedAppSlug     string
	githubSharedAppClientID string
	manifestFlows           *manifestFlowManager
	deviceFlows             *deviceFlowManager
	workflowCompiler        *workflow.Compiler
	cursors                 *cursorSigner
	auth                    *authManager
	// handler is the fully composed http handler tree (mux + middleware).
	// We keep it on Server so *Server itself can satisfy http.Handler and
	// callers can hold a single object that owns both serving and
	// shutdown.
	handler http.Handler
}

// ServeHTTP delegates to the composed mux/middleware tree. Having Server
// implement http.Handler directly means callers don't need a parallel
// handler reference — the same object exposes serving and Close().
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The request context remains the parent, so client disconnects and request
	// deadlines retain their normal cancellation behavior. Server.Close also
	// cancels this derived context, allowing graceful HTTP shutdown to drain
	// long-lived handlers such as SSE streams.
	ctx, cancel := context.WithCancel(r.Context())
	stopLifecycleCancel := context.AfterFunc(s.lifecycleCtx, cancel)
	defer cancel()
	defer stopLifecycleCancel()

	s.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Close releases resources owned by the server. The recovery goroutine in
// the planning service is the main one that needs an explicit signal —
// without it, abrupt shutdown can interrupt an in-flight RecoverOperations
// call mid-transaction.
func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if s.lifecycleCancel != nil {
		s.lifecycleCancel()
	}
	var closeErr error
	if s.planningService != nil {
		closeErr = errors.Join(closeErr, s.planningService.Close(ctx))
	}
	return closeErr
}

// Warm pays this process's one-time provider cost up front, so the first
// operator to arrive does not.
//
// Listing models starts the dedicated provider service lazily, and request
// execution shares that server.
//
// Warming is best-effort by construction. A failure here means the operator
// pays the cost lazily exactly as they used to, and the real diagnosis reaches
// them through the model catalog notice — so it must never fail startup, and
// it must never gate readiness.
func (s *Server) Warm(ctx context.Context) {
	if s == nil {
		return
	}
	if s.providerModelCatalog != nil {
		if _, err := s.providerModelCatalog.Snapshot(ctx, false); err != nil {
			log.Printf("kingdom: warm provider model catalog: %v", err)
		}
	}
}

// ForceClose is the hard-stop shutdown path used when summon is escalated
// from graceful drain to immediate teardown.
func (s *Server) ForceClose() {
	if s == nil {
		return
	}
	if s.lifecycleCancel != nil {
		s.lifecycleCancel()
	}
	if s.planningService != nil {
		s.planningService.ForceClose()
	}
}

func NewHandler(cfg ServerConfig) (*Server, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("api database handle is required")
	}

	startupCtx := cfg.StartupContext
	if startupCtx == nil {
		startupCtx = context.Background()
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(startupCtx)
	startupCtx = lifecycleCtx
	lifecycleTransferred := false
	defer func() {
		if !lifecycleTransferred {
			lifecycleCancel()
		}
	}()

	basePath, err := ValidateBasePath(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("invalid API base path: %w", err)
	}

	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	idGenerator := cfg.IDGenerator
	if idGenerator == nil {
		idGenerator = func(prefix string) string {
			return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UTC().UnixNano(), atomic.AddUint64(&generatedIDCounter, 1))
		}
	}

	artifactDownloadURL := strings.TrimSpace(cfg.ArtifactDownloadURL)
	if artifactDownloadURL == "" {
		artifactDownloadURL = basePath + "/artifacts"
	}
	artifactDownloadURL = strings.TrimSuffix(artifactDownloadURL, "/")

	autonomousApprovalsDefault := true
	if cfg.AutonomousApprovalsDefault != nil {
		autonomousApprovalsDefault = *cfg.AutonomousApprovalsDefault
	}

	providerEngine := strings.ToLower(strings.TrimSpace(cfg.ProviderEngine))
	if providerEngine == "" {
		providerEngine = "opencode"
	}
	if providerEngine != "opencode" {
		return nil, fmt.Errorf("unsupported provider engine %q (expected opencode)", cfg.ProviderEngine)
	}
	if cfg.ProviderModelCatalog == nil {
		return nil, fmt.Errorf("provider model catalog is required")
	}

	schemaDir, err := resolveSchemaDir(cfg.SchemaDir)
	if err != nil {
		return nil, err
	}

	schemaSet, err := schemas.Load(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("load workflow schemas: %w", err)
	}

	promptCatalog, err := state.CanonicalPromptCatalog()
	if err != nil {
		return nil, fmt.Errorf("load canonical prompt catalog: %w", err)
	}
	roleCommPolicy := rolecomm.FromCatalog(promptCatalog)

	workflowParser := workflow.NewParser(state.CanonicalRoleIDs())
	workflowCompiler := workflow.NewCompiler(schemaSet, workflowParser)
	planningStore := state.NewPlanningStore(cfg.DB)

	cursorSigner, err := newCursorSigner()
	if err != nil {
		return nil, err
	}
	auth, err := newAuthManager(cfg.OperatorCredential, now)
	if err != nil {
		return nil, fmt.Errorf("initialize local authentication: %w", err)
	}

	projectRoot := strings.TrimSpace(cfg.ProjectRoot)
	rawDefaultPublishMode := strings.ToLower(strings.TrimSpace(cfg.DefaultPublishMode))
	if rawDefaultPublishMode != "" && !workspace.IsSupportedPublishMode(rawDefaultPublishMode) {
		return nil, fmt.Errorf("unsupported default publish mode %q", cfg.DefaultPublishMode)
	}
	defaultPublishMode := workspace.NormalizePublishMode(rawDefaultPublishMode)
	githubKeys := cfg.GitHubKeyStore
	sharedSlug := strings.TrimSpace(cfg.GitHubSharedAppSlug)
	sharedClientID := strings.TrimSpace(cfg.GitHubSharedAppClientID)
	if (sharedSlug == "") != (sharedClientID == "") {
		return nil, fmt.Errorf("github shared app slug and client id must both be set or both be blank")
	}
	if sharedSlug != "" && (!appSlugPatternAPI.MatchString(sharedSlug) || !githubClientIDPattern.MatchString(sharedClientID)) {
		return nil, fmt.Errorf("github shared app slug or client id is invalid")
	}
	externalStore := state.NewExternalStore(cfg.DB)
	if githubKeys == nil && projectRoot != "" {
		githubKeys, err = githubexternal.NewKeyStore(filepath.Join(projectRoot, ".kingdom", "secrets"))
		if err != nil {
			return nil, fmt.Errorf("initialize github key store: %w", err)
		}
	}
	githubUserTokens := cfg.GitHubUserTokenStore
	if githubUserTokens == nil && projectRoot != "" {
		githubUserTokens, err = githubexternal.NewUserTokenStore(filepath.Join(projectRoot, ".kingdom", "secrets"))
		if err != nil {
			return nil, fmt.Errorf("initialize github user token store: %w", err)
		}
	}
	githubDeviceOAuth := cfg.GitHubDeviceOAuthClient
	if sharedSlug != "" && githubDeviceOAuth == nil {
		githubDeviceOAuth, err = githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{Now: now})
		if err != nil {
			return nil, fmt.Errorf("initialize github device oauth client: %w", err)
		}
	}
	var externalAdapter external.Adapter = cfg.ExternalAdapter
	if externalAdapter == nil && githubKeys != nil {
		clientConfig := githubexternal.ClientConfig{Keys: githubKeys, Now: now}
		if sharedSlug != "" {
			clientConfig.UserTokens, clientConfig.DeviceOAuth, clientConfig.SharedClientID = githubUserTokens, githubDeviceOAuth, sharedClientID
			clientConfig.UserCredentialGeneration = func(ctx context.Context, connectionID string) (int, error) {
				publication, err := externalStore.GetCredentialPublication(ctx, connectionID)
				if err != nil {
					return 0, err
				}
				if publication.CredentialStrategy != "user_access_token" {
					return 0, nil
				}
				return publication.CredentialGeneration, nil
			}
		}
		externalAdapter, err = githubexternal.NewClient(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("initialize github adapter: %w", err)
		}
	}
	adapters := map[string]external.Adapter{}
	if externalAdapter != nil {
		adapters["github"] = externalAdapter
	}
	githubSetup := cfg.GitHubSetupClient
	if githubSetup == nil {
		githubSetup, err = githubexternal.NewSetupClient(githubexternal.SetupClientConfig{Now: now})
		if err != nil {
			return nil, fmt.Errorf("initialize github setup client: %w", err)
		}
	}

	s := &Server{
		themes:                     cfg.ThemeStore,
		db:                         cfg.DB,
		now:                        now,
		idGenerator:                idGenerator,
		basePath:                   basePath,
		artifactDownloadURL:        artifactDownloadURL,
		eventIngestor:              state.NewAttemptEventIngestor(cfg.DB, schemaSet).WithRoleCommunicationPolicy(roleCommPolicy),
		transitionService:          state.NewTransitionService(cfg.DB, schemaSet).WithRoleCommunicationPolicy(roleCommPolicy),
		artifactService:            state.NewArtifactService(cfg.DB),
		tokenUsageStore:            state.NewTokenUsageStore(cfg.DB),
		planningStore:              planningStore,
		externalStore:              externalStore,
		externalRegistry:           external.NewRegistry(adapters),
		githubKeys:                 githubKeys,
		githubUserTokens:           githubUserTokens,
		appKeyLifecycle:            githubKeys,
		userTokenLifecycle:         githubUserTokens,
		githubSetup:                githubSetup,
		githubDeviceOAuth:          githubDeviceOAuth,
		githubSharedAppSlug:        sharedSlug,
		githubSharedAppClientID:    sharedClientID,
		manifestFlows:              newManifestFlowManager(now),
		deviceFlows:                newDeviceFlowManager(now),
		workflowCompiler:           workflowCompiler,
		autonomousApprovalsDefault: autonomousApprovalsDefault,
		providerEngine:             providerEngine,
		providerModelCatalog:       cfg.ProviderModelCatalog,
		projectRoot:                projectRoot,
		defaultPublishMode:         defaultPublishMode,
		lifecycleCtx:               lifecycleCtx,
		lifecycleCancel:            lifecycleCancel,
		cursors:                    cursorSigner,
		auth:                       auth,
	}
	var plannerAgent planning.Agent
	if githubKeys != nil || githubUserTokens != nil {
		selectedGeneration := func(strategy string) func(string) (int, error) {
			return func(connectionID string) (int, error) {
				publication, publicationErr := s.externalStore.GetCredentialPublication(startupCtx, connectionID)
				if errors.Is(publicationErr, state.ErrExternalNotFound) {
					return 0, nil
				}
				if publicationErr != nil {
					return 0, publicationErr
				}
				if publication.CredentialStrategy != strategy || (publication.Lifecycle != "enabled" && publication.Lifecycle != "disabled") || publication.CredentialGeneration <= 0 {
					return 0, nil
				}
				return publication.CredentialGeneration, nil
			}
		}
		if githubKeys != nil {
			if reconcileErr := githubKeys.ReconcileGenerations(selectedGeneration("app_private_key")); reconcileErr != nil {
				return nil, fmt.Errorf("reconcile github key generations: %w", reconcileErr)
			}
		}
		if githubUserTokens != nil {
			if reconcileErr := githubUserTokens.ReconcileGenerations(selectedGeneration("user_access_token")); reconcileErr != nil {
				return nil, fmt.Errorf("reconcile github user token generations: %w", reconcileErr)
			}
		}
		connections, listErr := s.externalStore.ListConnectionsAwaitingRemovalReconciliation(startupCtx)
		if listErr != nil {
			return nil, fmt.Errorf("list connections for removal reconciliation: %w", listErr)
		}
		for _, connection := range connections {
			lifecycleStore, storeErr := s.connectionCredentialLifecycleStore(connection.CredentialStrategy)
			if storeErr != nil {
				return nil, fmt.Errorf("reconcile connection removal: %w", storeErr)
			}
			reconcile := func() error {
				if err := lifecycleStore.RemoveAllWhileLocked(connection.ConnectionID); err != nil {
					return err
				}
				if connection.Lifecycle == "removed" {
					return nil
				}
				at := now().UTC().Format(time.RFC3339Nano)
				return s.externalStore.UpdateConnectionStateAudited(startupCtx, connection.ConnectionID, "removed", "removed", "", at, false, "system", idGenerator("external_audit"), "connection.removal_reconciled")
			}
			if reconcileErr := lifecycleStore.WithConnectionLock(connection.ConnectionID, reconcile); reconcileErr != nil {
				return nil, fmt.Errorf("reconcile connection removal: %w", reconcileErr)
			}
		}
	}
	if cfg.PlannerAgent != nil {
		plannerAgent = cfg.PlannerAgent
	} else if providerEngine == "opencode" {
		if cfg.ProviderService == nil {
			return nil, fmt.Errorf("provider service is required for the OpenCode planner")
		}
		if strings.TrimSpace(cfg.ProviderPolicyPath) == "" || strings.TrimSpace(cfg.ProviderPolicySchemaPath) == "" {
			return nil, fmt.Errorf("provider policy and schema paths must both be set for the OpenCode planner")
		}
		providerPolicy, loadErr := providerpolicy.LoadPolicyFromFile(cfg.ProviderPolicyPath, cfg.ProviderPolicySchemaPath)
		if loadErr != nil {
			return nil, loadErr
		}
		firstEventTimeout, streamInactivityTimeout, _, _, timeoutErr := providerPolicy.ExecutionTimeouts()
		if timeoutErr != nil {
			return nil, fmt.Errorf("parse provider execution timeouts: %w", timeoutErr)
		}
		plannerAgent = planning.NewOpenCodeAgent(cfg.ProviderService, idGenerator, planning.WithRequestLimits(firstEventTimeout, streamInactivityTimeout))
	}
	s.planningService = planning.NewServiceWithRetryEngine(planningStore, plannerAgent, strings.TrimSpace(cfg.ProjectRoot), func() string {
		return now().UTC().Format(time.RFC3339Nano)
	}, idGenerator, cfg.RetryEngine)
	// Lifecycle ownership: from this point on, NewHandler owns
	// s.planningService and must close it on any subsequent error
	// path. Without this, a downstream constructor failure
	// (RecoverOperations, EnsureCanonicalRoles, OpenAPI validator
	// bootstrap) would leak the planning service's recovery loop and
	// runner goroutines while the caller saw a clean error return.
	// constructorOK flips to true on the success path; the deferred
	// closure stands down then.
	constructorOK := false
	defer func() {
		if constructorOK {
			return
		}
		lifecycleCancel()
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(startupCtx), 10*time.Second)
		defer cancel()
		_ = s.planningService.Close(closeCtx)
	}()

	if err := state.EnsureCanonicalRoles(startupCtx, cfg.DB, now()); err != nil {
		return nil, fmt.Errorf("seed canonical roles: %w", err)
	}

	inner := http.NewServeMux()
	s.registerRoutes(inner)

	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		return nil, err
	}

	idempotency := NewIdempotencyMiddleware(state.NewIdempotencyStore(cfg.DB), now)
	wrapped := s.authenticationMiddleware(limitThemeRequestBody(validator(idempotency(inner))))

	// SSE streaming endpoints bypass idempotency middleware (which buffers the full response)
	// but still go through OpenAPI validation.
	streamingMux := http.NewServeMux()
	streamingMux.HandleFunc("GET /acp/runs/{runId}/events", s.handleStreamAcpRunEvents)
	wrappedStreaming := s.authenticationMiddleware(validator(streamingMux))

	mux := http.NewServeMux()
	mux.Handle("GET "+basePath+"/acp/runs/{runId}/events", http.StripPrefix(basePath, wrappedStreaming))
	mux.Handle(basePath+"/", http.StripPrefix(basePath, wrapped))

	s.handler = mux
	constructorOK = true
	lifecycleTransferred = true
	return s, nil
}

func (s *Server) RecoverPlanningOperations(ctx context.Context) error {
	if s == nil || s.planningService == nil {
		return nil
	}
	if err := s.planningService.RecoverOperations(ctx); err != nil {
		return err
	}
	s.planningService.StartRecoveryLoop()
	return nil
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/session", s.handleCreateAuthSession)
	mux.HandleFunc("GET /mutation-outcomes/{key}", s.handleGetMutationOutcome)
	s.registerThemeRoutes(mux)
	mux.HandleFunc("GET /health", s.handleGetHealth)
	mux.HandleFunc("GET /roles", s.handleListRoles)
	mux.HandleFunc("GET /agents", s.handleListAgents)
	mux.HandleFunc("GET /agent-configuration", s.handleGetAgentConfiguration)
	mux.HandleFunc("PUT /agent-configuration/workspace-voice", s.handleUpdateWorkspaceAgentVoice)
	mux.HandleFunc("PUT /agent-configuration/roles/{roleId}", s.handleUpdateAgentRoleConfiguration)
	mux.HandleFunc("POST /agent-configuration/roles/{roleId}/guidance/reset", s.handleResetAgentRoleGuidance)
	mux.HandleFunc("GET /provider/models", s.handleListProviderModels)
	mux.HandleFunc("GET /workspace-capabilities", s.handleGetWorkspaceCapabilities)
	mux.HandleFunc("GET /connections", s.handleListConnections)
	mux.HandleFunc("POST /connections", s.handleCreateConnection)
	mux.HandleFunc("GET /connections/github/setup-options", s.handleGitHubSetupOptions)
	mux.HandleFunc("GET /connections/github/device-flows", s.handleCurrentGitHubDeviceFlow)
	mux.HandleFunc("POST /connections/github/device-flows", s.handleStartGitHubDeviceFlow)
	mux.HandleFunc("GET /connections/github/device-flows/{id}", s.handleGetGitHubDeviceFlow)
	mux.HandleFunc("DELETE /connections/github/device-flows/{id}", s.handleDiscardGitHubDeviceFlow)
	mux.HandleFunc("POST /connections/github/device-flows/{id}/poll", s.handlePollGitHubDeviceFlow)
	mux.HandleFunc("POST /connections/github/device-flows/{id}/candidates", s.handleRefreshGitHubDeviceCandidates)
	mux.HandleFunc("POST /connections/github/device-flows/{id}/finalize", s.handleFinalizeGitHubDeviceFlow)
	mux.HandleFunc("GET /connections/github/manifest-flows", s.handleCurrentGitHubManifestFlow)
	mux.HandleFunc("POST /connections/github/manifest-flows", s.handleStartGitHubManifestFlow)
	mux.HandleFunc("GET /connections/github/manifest-flows/{id}", s.handleGetGitHubManifestFlow)
	mux.HandleFunc("DELETE /connections/github/manifest-flows/{id}", s.handleDiscardGitHubManifestFlow)
	mux.HandleFunc("POST /connections/github/manifest-flows/{id}/conversion", s.handleConvertGitHubManifestFlow)
	mux.HandleFunc("POST /connections/github/manifest-flows/{id}/installation", s.handleGetGitHubManifestInstallationURL)
	mux.HandleFunc("POST /connections/github/manifest-flows/{id}/authorization", s.handleAuthorizeGitHubManifestFlow)
	mux.HandleFunc("POST /connections/github/manifest-flows/{id}/completion", s.handleCompleteGitHubManifestFlow)
	mux.HandleFunc("GET /connections/github/manifest-flows/manifest-callback", s.handleGitHubManifestCallback)
	mux.HandleFunc("GET /connections/github/manifest-flows/setup-callback", s.handleGitHubSetupCallback)
	mux.HandleFunc("GET /connections/github/manifest-flows/oauth-callback", s.handleGitHubOAuthCallback)
	mux.HandleFunc("GET /connections/{id}", s.handleGetConnection)
	mux.HandleFunc("PATCH /connections/{id}", s.handleUpdateConnection)
	mux.HandleFunc("DELETE /connections/{id}", s.handleDeleteConnection)
	mux.HandleFunc("POST /connections/{id}/health-checks", s.handleConnectionHealthCheck)
	mux.HandleFunc("GET /connections/{id}/installation-candidates", s.handleListConnectionInstallationCandidates)
	mux.HandleFunc("PUT /connections/{id}/installations", s.handleSetConnectionInstallations)
	mux.HandleFunc("GET /connections/{id}/repositories", s.handleListConnectionRepositories)
	mux.HandleFunc("GET /connections/{id}/repositories/{repositoryId}/issues", s.handleListConnectionIssues)
	mux.HandleFunc("GET /connections/{id}/repositories/{repositoryId}/issues/{number}/preview", s.handlePreviewConnectionIssue)
	mux.HandleFunc("POST /external-source-captures", s.handleCaptureExternalSource)
	mux.HandleFunc("GET /external-source-snapshots/{id}", s.handleGetExternalSourceSnapshot)
	mux.HandleFunc("GET /external-source-snapshots/{id}/comments", s.handleGetExternalSourceSnapshotComments)
	mux.HandleFunc("GET /external-source-snapshots/{id}/work-order-text", s.handleGetExternalSourceWorkOrderText)
	mux.HandleFunc("GET /external-source-links/{id}/history", s.handleGetExternalSourceLinkHistory)
	mux.HandleFunc("POST /external-source-links/{id}/refreshes", s.handleRefreshExternalSourceLink)
	mux.HandleFunc("POST /external-source-links/{id}/adoptions", s.handleAdoptExternalSourceLink)
	mux.HandleFunc("POST /external-source-links/{id}/detachments", s.handleDetachExternalSourceLink)
	mux.HandleFunc("POST /work-orders", s.handleCreateWorkOrder)
	mux.HandleFunc("GET /work-orders/{id}", s.handleGetWorkOrder)
	mux.HandleFunc("POST /planning-sessions", s.handleCreatePlanningSession)
	mux.HandleFunc("GET /planning-sessions", s.handleListPlanningSessions)
	mux.HandleFunc("GET /planning-sessions/{id}", s.handleGetPlanningSession)
	mux.HandleFunc("PATCH /planning-sessions/{id}", s.handleUpdatePlanningSession)
	mux.HandleFunc("DELETE /planning-sessions/{id}", s.handleDeletePlanningSession)
	mux.HandleFunc("POST /planning-sessions/{id}/external-sources", s.handleAttachPlanningExternalSource)
	mux.HandleFunc("GET /acp/agents", s.handleListAcpAgents)
	mux.HandleFunc("GET /acp/agents/{name}", s.handleGetAcpAgent)
	mux.HandleFunc("POST /acp/runs", s.handleCreateAcpRun)
	mux.HandleFunc("GET /acp/runs/{runId}", s.handleGetAcpRun)
	mux.HandleFunc("GET /acp/runs/{runId}/events", s.handleStreamAcpRunEvents)
	mux.HandleFunc("GET /acp/runs/{runId}/activity", s.handleGetActivity)
	mux.HandleFunc("POST /acp/runs/{runId}/cancel", s.handleCancelAcpRun)
	mux.HandleFunc("GET /planning-sessions/{id}/messages", s.handleListPlanningSessionMessages)
	mux.HandleFunc("PUT /planning-sessions/{id}/messages/{messageId}/activate", s.handleActivatePlanningMessageVariant)
	mux.HandleFunc("GET /planning-sessions/{id}/messages/{messageId}/variants", s.handleListPlanningMessageVariants)
	mux.HandleFunc("GET /planning-sessions/{id}/plan", s.handleGetPlanningSessionPlan)
	mux.HandleFunc("POST /planning-sessions/{id}/context/refresh", s.handleRefreshPlanningSessionContext)
	mux.HandleFunc("GET /planning-sessions/{id}/context/{snapshotId}/entries", s.handleListPlanningContextEntries)
	mux.HandleFunc("POST /workflows/publish", s.handlePublishWorkflow)
	mux.HandleFunc("POST /runs", s.handleCreateRun)
	mux.HandleFunc("GET /runs", s.handleListRuns)
	mux.HandleFunc("GET /runs/events/stream", s.handleStreamRunCollectionEvents)
	mux.HandleFunc("GET /runs/{id}", s.handleGetRun)
	mux.HandleFunc("POST /runs/{id}/cancel", s.handleCancelRun)
	mux.HandleFunc("POST /runs/{id}/executions/{attemptId}/stop-retry", s.handleRetryExecutionStop)
	mux.HandleFunc("GET /runs/{id}/plan", s.handleGetRunPlan)
	mux.HandleFunc("GET /runs/{id}/plan-reviews", s.handleListRunPlanReviews)
	mux.HandleFunc("GET /runs/{id}/governance-decisions", s.handleListRunGovernanceDecisions)
	mux.HandleFunc("GET /runs/{id}/artifacts", s.handleListRunArtifacts)
	mux.HandleFunc("GET /runs/{id}/artifacts/archive", s.handleGetRunArtifactsArchive)
	mux.HandleFunc("GET /runs/{id}/graph", s.handleGetRunGraph)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}/activity", s.handleGetActivity)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}/activity/{eventId}/content/{field}", s.handleGetActivityContent)
	mux.HandleFunc("GET /acp/runs/{runId}/activity/{eventId}/content/{field}", s.handleGetActivityContent)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}/publication/{batchId}/entries", s.handleListRunPublicationEntries)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}/instructions", s.handleGetExecutionInstructions)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}", s.handleGetRunExecution)
	mux.HandleFunc("GET /runs/{id}/executions", s.handleListRunExecutions)
	mux.HandleFunc("GET /runs/{id}/nodes/{nodeId}/attempts", s.handleListRunNodeAttempts)
	mux.HandleFunc("GET /runs/{id}/agents", s.handleListRunAgents)
	mux.HandleFunc("GET /runs/{id}/timeline", s.handleGetRunTimeline)
	mux.HandleFunc("GET /runs/{id}/mailbox/messages", s.handleListRunMailboxMessages)
	mux.HandleFunc("GET /runs/{id}/mailbox/messages/{messageId}", s.handleGetRunMailboxMessage)
	mux.HandleFunc("GET /runs/{id}/mailbox/messages/{messageId}/content", s.handleGetRunMailboxMessageContent)
	mux.HandleFunc("POST /runs/{id}/mailbox/messages/{messageId}/read", s.handleMarkRunMailboxMessageRead)
	mux.HandleFunc("GET /runs/{id}/resume", s.handleGetRunResumePlan)
	mux.HandleFunc("POST /runs/{id}/resume", s.handleResumeRun)
	mux.HandleFunc("POST /runs/{id}/rerun-from/{nodeKey}", s.handleRerunFromNode)
	mux.HandleFunc("POST /runs/{id}/plan-approval", s.handleCreatePlanApproval)
	mux.HandleFunc("POST /runs/{id}/final-approval", s.handleCreateFinalApproval)
	mux.HandleFunc("GET /artifacts/{id}", s.handleGetArtifact)
	mux.HandleFunc("GET /artifacts/{id}/content", s.handleGetArtifactContent)
	mux.HandleFunc("GET /events/stream", s.handleStreamEvents)
	mux.HandleFunc("GET /metrics/runtime", s.handleGetMetricsRuntime)
	mux.HandleFunc("GET /dashboard/snapshot", s.handleGetDashboardSnapshot)
	mux.HandleFunc("GET /analytics/snapshot", s.handleGetAnalyticsSnapshot)
	mux.HandleFunc("GET /analytics/execution-detail", s.handleGetAnalyticsExecutionDetailSnapshot)
}

type runListResponse struct {
	Items      []gen.RunSummary `json:"items"`
	NextCursor *string          `json:"next_cursor,omitempty"`
}

type rolesResponse struct {
	Items []roleItem `json:"items"`
}

type roleItem struct {
	RoleID                         string `json:"role_id"`
	DisplayName                    string `json:"display_name"`
	Description                    string `json:"description"`
	CanSpawn                       bool   `json:"can_spawn"`
	AgentModelSelectorVisible      bool   `json:"agent_model_selector_visible"`
	AgentModelSelectorSectionID    string `json:"agent_model_selector_section_id"`
	AgentModelSelectorSectionLabel string `json:"agent_model_selector_section_label"`
	AgentModelSelectorSectionOrder int    `json:"agent_model_selector_section_order"`
	AgentModelSelectorRoleOrder    int    `json:"agent_model_selector_role_order"`
}

type runAgentsResponse struct {
	RunID string               `json:"run_id"`
	Items []runParticipantItem `json:"items"`
}

type agentsResponse struct {
	Items []agentItem `json:"items"`
}

type runParticipantItem struct {
	AgentID       string    `json:"agent_id"`
	RoleID        string    `json:"role_id"`
	DisplayName   string    `json:"display_name"`
	Status        string    `json:"status"`
	ParentAgentID *string   `json:"parent_agent_id,omitempty"`
	SpawnDepth    int       `json:"spawn_depth"`
	FirstSeenAt   time.Time `json:"first_seen_at"`
}

type agentItem struct {
	AgentID       string    `json:"agent_id"`
	RoleID        string    `json:"role_id"`
	DisplayName   string    `json:"display_name"`
	Status        string    `json:"status"`
	ParentAgentID *string   `json:"parent_agent_id,omitempty"`
	SpawnDepth    int       `json:"spawn_depth"`
	CreatedAt     time.Time `json:"created_at"`
}

type runExecutionItem struct {
	AttemptID          string     `json:"attempt_id"`
	NodeRunID          string     `json:"node_run_id"`
	NodeID             string     `json:"node_id"`
	Attempt            int        `json:"attempt"`
	AgentID            string     `json:"agent_id"`
	RequiredRole       string     `json:"required_role"`
	ExecutionMode      string     `json:"execution_mode"`
	Status             string     `json:"status"`
	LogicalSessionID   *string    `json:"logical_session_id,omitempty"`
	ProviderSessionID  *string    `json:"provider_session_id,omitempty"`
	QueuedAt           time.Time  `json:"queued_at"`
	ActivityAt         time.Time  `json:"activity_at"`
	ProgressEventType  *string    `json:"progress_event_type,omitempty"`
	ProgressReasonCode *string    `json:"progress_reason_code,omitempty"`
	ProgressAt         *time.Time `json:"progress_at,omitempty"`
	LaunchStartedAt    *time.Time `json:"launch_started_at,omitempty"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
	TerminalObservedAt *time.Time `json:"terminal_observed_at,omitempty"`
	CommittedAt        *time.Time `json:"committed_at,omitempty"`
	FailureCode        *string    `json:"failure_code,omitempty"`
	FailureText        *string    `json:"failure_text,omitempty"`
	HandoffState       *string    `json:"handoff_state,omitempty"`
	StopRetryAvailable bool       `json:"stop_retry_available"`
}

type runExecutionSettlementDetailItem struct {
	Reason *string `json:"reason,omitempty"`
}

type runExecutionDetailItem struct {
	runExecutionItem
	ProviderInvocationID *string                           `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt *time.Time                        `json:"provider_invocation_at,omitempty"`
	SettlementStatus     string                            `json:"settlement_status"`
	SettlementDetail     *runExecutionSettlementDetailItem `json:"settlement_detail,omitempty"`
	SettledAt            *time.Time                        `json:"settled_at,omitempty"`
	OutputStatus         string                            `json:"output_status"`
	OutputText           *string                           `json:"output_text,omitempty"`
	OutputVerdict        *string                           `json:"output_verdict,omitempty"`
	OutputVerdictStatus  *string                           `json:"output_verdict_status,omitempty"`
	Publication          *gen.RunExecutionPublication      `json:"publication,omitempty"`
}

func (s *Server) handleGetHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, gen.HealthResponse{Status: gen.HealthResponseStatusOk})
}

func (s *Server) handleGetWorkOrder(w http.ResponseWriter, r *http.Request) {
	workOrderID := strings.TrimSpace(r.PathValue("id"))
	if workOrderID == "" {
		s.writeBadRequest(w, "work order id is required")
		return
	}

	var detail gen.WorkOrderDetail
	var createdAt string
	var metadataJSON string
	err := s.db.QueryRowContext(
		r.Context(),
		`SELECT work_order_id, work_order_title, metadata_json, created_at FROM work_orders WHERE work_order_id = ?`,
		workOrderID,
	).Scan(&detail.WorkOrderId, &detail.WorkOrderTitle, &metadataJSON, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "work order not found", "api.get_work_order", map[string]any{"work_order_id": workOrderID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch work order failed")
		return
	}

	detail.CreatedAt = mustParseRFC3339(createdAt)
	if metadataJSON != "" && metadataJSON != "{}" {
		var metadata map[string]interface{}
		if jsonErr := json.Unmarshal([]byte(metadataJSON), &metadata); jsonErr == nil {
			detail.Metadata = &metadata
		}
	}
	links, linkErr := s.externalStore.GetLinksForTarget(r.Context(), "work_order", workOrderID)
	if linkErr != nil {
		s.writeInternalError(w, "fetch work order provenance failed")
		return
	}
	detail.ExternalSources = externalProvenanceListFromState(links, false)

	text, err := state.OpenWorkOrderText(r.Context(), s.db, workOrderID)
	if err != nil {
		s.writeInternalError(w, "fetch work order text failed")
		return
	}
	defer text.Close()
	metadata, err := json.Marshal(detail)
	if err != nil {
		s.writeInternalError(w, "encode work order metadata failed")
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &fields); err != nil {
		s.writeInternalError(w, "encode work order metadata failed")
		return
	}
	delete(fields, "work_order_text")
	prefix, err := json.Marshal(fields)
	if err != nil {
		s.writeInternalError(w, "encode work order metadata failed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	err = writeExternalResponse(w, func(out io.Writer) error {
		if _, err := out.Write(prefix[:len(prefix)-1]); err != nil {
			return err
		}
		if _, err := io.WriteString(out, `,"work_order_text":`); err != nil {
			return err
		}
		if err := providerexecution.WriteJSONString(r.Context(), out, text); err != nil {
			return err
		}
		_, err := io.WriteString(out, "}")
		return err
	})
	if err != nil {
		s.writeInternalError(w, "stream work order failed")
	}
}

func externalProvenanceListFromState(links []state.ExternalLinkSummary, planning bool) []gen.ExternalProvenanceSummary {
	result := make([]gen.ExternalProvenanceSummary, 0, len(links))
	for _, link := range links {
		result = append(result, externalProvenanceFromState(link, planning))
	}
	return result
}

func externalProvenanceFromState(link state.ExternalLinkSummary, planning bool) gen.ExternalProvenanceSummary {
	result := gen.ExternalProvenanceSummary{LinkId: link.LinkID, Provider: gen.ExternalProvenanceSummaryProvider(link.Provider), Repository: link.Repository, IssueNumber: link.IssueNumber, CanonicalUrl: link.CanonicalURL, UsedSnapshotId: link.UsedSnapshotID, LatestSnapshotId: link.LatestSnapshotID, Status: gen.ExternalSourceStatus(link.Status)}
	if planning {
		active := link.ActiveSnapshotID
		if active == "" {
			active = link.UsedSnapshotID
		}
		result.ActiveSnapshotId = &active
	}
	return result
}

func (s *Server) handleListRoles(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(
		r.Context(),
		`SELECT role_id, display_name, description, can_spawn, agent_model_selector_visible, agent_model_selector_section_id, agent_model_selector_section_label, agent_model_selector_section_order, agent_model_selector_role_order
		 FROM roles
		 ORDER BY role_id ASC`,
	)
	if err != nil {
		s.writeInternalError(w, "list roles failed")
		return
	}
	defer rows.Close()

	items := make([]roleItem, 0)
	for rows.Next() {
		var item roleItem
		var canSpawn, overrideEligible int
		if err := rows.Scan(&item.RoleID, &item.DisplayName, &item.Description, &canSpawn, &overrideEligible, &item.AgentModelSelectorSectionID, &item.AgentModelSelectorSectionLabel, &item.AgentModelSelectorSectionOrder, &item.AgentModelSelectorRoleOrder); err != nil {
			s.writeInternalError(w, "decode role row failed")
			return
		}
		item.CanSpawn = canSpawn == 1
		item.AgentModelSelectorVisible = overrideEligible == 1
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		s.writeInternalError(w, "iterate role rows failed")
		return
	}

	writeJSON(w, http.StatusOK, rolesResponse{Items: items})
}

func (s *Server) handleListAgents(w http.ResponseWriter, r *http.Request) {
	registry := state.NewAgentRegistry(s.db)
	agents, err := registry.ListAllAgents(r.Context())
	if err != nil {
		s.writeInternalError(w, "list agents failed")
		return
	}

	items := make([]agentItem, 0, len(agents))
	for _, agent := range agents {
		item := agentItem{
			AgentID:     agent.AgentID,
			RoleID:      agent.RoleID,
			DisplayName: agent.DisplayName,
			Status:      string(agent.Status),
			SpawnDepth:  agent.SpawnDepth,
			CreatedAt:   mustParseRFC3339(agent.CreatedAt),
		}
		if agent.ParentAgentID.Valid {
			item.ParentAgentID = &agent.ParentAgentID.String
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, agentsResponse{Items: items})
}

func (s *Server) handleListProviderModels(w http.ResponseWriter, r *http.Request) {
	refresh := false
	rawRefresh := strings.TrimSpace(r.URL.Query().Get("refresh"))
	if rawRefresh != "" {
		parsed, err := strconv.ParseBool(rawRefresh)
		if err != nil {
			s.writeBadRequest(w, "refresh must be a boolean")
			return
		}
		refresh = parsed
	}

	snapshot, err := s.providerModelCatalog.Snapshot(r.Context(), refresh)
	if err != nil {
		s.writeInternalError(w, "list provider models failed")
		return
	}
	response := gen.ProviderModelsResponse{
		Items:         snapshot.Items,
		CatalogStatus: gen.ProviderModelsResponseCatalogStatus(snapshot.Status),
	}
	if snapshot.LastSuccessAt != nil && !snapshot.LastSuccessAt.IsZero() {
		lastSuccessAt := *snapshot.LastSuccessAt
		response.LastSuccessAt = &lastSuccessAt
	}
	if snapshot.FailureReason != ProviderModelCatalogFailureReasonNone {
		failureReason := gen.ProviderModelsResponseFailureReason(snapshot.FailureReason)
		response.FailureReason = &failureReason
	}
	if strings.TrimSpace(snapshot.FailureMessage) != "" {
		failureMessage := strings.TrimSpace(snapshot.FailureMessage)
		response.FailureMessage = &failureMessage
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) runExists(ctx context.Context, runID string) bool {
	var marker int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM workflow_runs WHERE run_id = ? LIMIT 1`, runID).Scan(&marker)
	return err == nil
}

func (s *Server) requirePrincipal(w http.ResponseWriter, r *http.Request, authority string) (string, string, bool) {
	principal, ok := authenticatedPrincipal(r.Context())
	if !ok || strings.TrimSpace(principal.ActorID) == "" {
		writeCanonicalError(w, http.StatusUnauthorized, errorcatalog.CodePolicyViolation, "missing authenticated principal", authority, nil)
		return "", "", false
	}
	return principal.ActorID, operatorRoleID, true
}

func (s *Server) writeBadRequest(w http.ResponseWriter, message string) {
	writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, message, "api.bad_request", nil)
}

func (s *Server) writeInternalError(w http.ResponseWriter, message string) {
	writeCanonicalError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, message, "api.internal", nil)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func decodeStrictJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}

	if decoder.More() {
		return fmt.Errorf("decode request body: multiple JSON values are not allowed")
	}

	return nil
}

func mustParseRFC3339(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return parsed.UTC()
}

func formatFixedUTCTimestamp(value time.Time) string {
	return value.UTC().Format(fixedUTCTimestampLayout)
}

func parseLimitQuery(raw string, fallback int) (int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil || parsed < 1 || parsed > maxTimelineLimit {
		return 0, fmt.Errorf("limit must be an integer between 1 and 200")
	}
	return parsed, nil
}

func isUniqueConstraintError(err error) bool {
	return state.IsUniqueViolation(err)
}

func isForeignKeyError(err error) bool {
	return strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

func resolveSchemaDir(configuredPath string) (string, error) {
	trimmed := strings.TrimSpace(configuredPath)
	if trimmed != "" {
		return trimmed, nil
	}

	candidates := make([]string, 0, 4)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates,
			filepath.Join(wd, "contracts", "schemas"),
			filepath.Join(wd, "..", "contracts", "schemas"),
			filepath.Join(wd, "..", "..", "contracts", "schemas"),
		)
	}

	if _, currentFile, _, ok := runtime.Caller(0); ok {
		candidates = append(candidates, filepath.Join(filepath.Dir(currentFile), "..", "..", "contracts", "schemas"))
	}

	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		schemaFile := filepath.Join(candidate, "workflow.schema.json")
		if _, err := os.Stat(schemaFile); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("resolve schema directory: contracts/schemas not found")
}
