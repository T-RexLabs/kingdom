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
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/planning"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

const (
	defaultAPIBasePath   = "/api/v1"
	defaultTimelineLimit = 50
	maxTimelineLimit     = 200
	defaultPreviewTTL    = 30 * time.Minute

	roleIDHeader = "X-Role-Id"
)

var generatedIDCounter uint64

type ServerConfig struct {
	DB                         *sql.DB
	StartupContext             context.Context
	BasePath                   string
	ArtifactDownloadURL        string
	PreviewSessionTTL          time.Duration
	AutonomousApprovalsDefault *bool
	ProviderEngine             string
	OpenCodeBinaryPath         string
	ProviderModelCatalog       ProviderModelCatalog
	SchemaDir                  string
	RoleCommPolicyPath         string
	RoleCommSchemaPath         string
	ProjectRoot                string
	DefaultPublishMode         string
	Now                        func() time.Time
	IDGenerator                func(prefix string) string
	PlannerAgent               planning.Agent
}

type Server struct {
	db                         *sql.DB
	now                        func() time.Time
	idGenerator                func(prefix string) string
	basePath                   string
	artifactDownloadURL        string
	previewSessions            *previewSessionStore
	autonomousApprovalsDefault bool
	providerEngine             string
	providerModelCatalog       ProviderModelCatalog
	projectRoot                string
	defaultPublishMode         string

	eventIngestor     *state.AttemptEventIngestor
	transitionService *state.TransitionService
	artifactService   *state.ArtifactService
	tokenUsageStore   *state.TokenUsageStore
	planningStore     *state.PlanningStore
	planningService   *planning.Service
	workflowCompiler  *workflow.Compiler
	cursors           *cursorSigner
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
	s.handler.ServeHTTP(w, r)
}

// Close releases resources owned by the server. The recovery goroutine in
// the planning service is the main one that needs an explicit signal —
// without it, abrupt shutdown can interrupt an in-flight RecoverOperations
// call mid-transaction.
func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var closeErr error
	if s.planningService != nil {
		closeErr = errors.Join(closeErr, s.planningService.Close(ctx))
	}
	return closeErr
}

// ForceClose is the hard-stop shutdown path used when summon is escalated
// from graceful drain to immediate teardown.
func (s *Server) ForceClose() {
	if s == nil {
		return
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

	basePath := NormalizeBasePath(cfg.BasePath)

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

	previewSessionTTL := cfg.PreviewSessionTTL
	if previewSessionTTL <= 0 {
		previewSessionTTL = defaultPreviewTTL
	}

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

	schemaDir, err := resolveSchemaDir(cfg.SchemaDir)
	if err != nil {
		return nil, err
	}

	schemaSet, err := schemas.Load(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("load workflow schemas: %w", err)
	}

	roleCommPolicy := rolecomm.Policy{}
	if strings.TrimSpace(cfg.RoleCommPolicyPath) != "" || strings.TrimSpace(cfg.RoleCommSchemaPath) != "" {
		if strings.TrimSpace(cfg.RoleCommPolicyPath) == "" || strings.TrimSpace(cfg.RoleCommSchemaPath) == "" {
			return nil, fmt.Errorf("role communication policy and schema paths must both be set")
		}
		loadedPolicy, loadErr := rolecomm.LoadPolicyFromFile(cfg.RoleCommPolicyPath, cfg.RoleCommSchemaPath, state.CanonicalRoleIDs())
		if loadErr != nil {
			return nil, loadErr
		}
		roleCommPolicy = loadedPolicy
	}

	workflowParser := workflow.NewParser(state.CanonicalRoleIDs())
	workflowCompiler := workflow.NewCompiler(schemaSet, workflowParser)
	planningStore := state.NewPlanningStore(cfg.DB)
	if err := planningStore.NormalizePlannerAcpRunIDs(context.Background()); err != nil {
		return nil, fmt.Errorf("normalize planner acp run ids: %w", err)
	}

	cursorSigner, err := newCursorSigner()
	if err != nil {
		return nil, err
	}

	projectRoot := strings.TrimSpace(cfg.ProjectRoot)
	rawDefaultPublishMode := strings.ToLower(strings.TrimSpace(cfg.DefaultPublishMode))
	if rawDefaultPublishMode != "" && !workspace.IsSupportedPublishMode(rawDefaultPublishMode) {
		return nil, fmt.Errorf("unsupported default publish mode %q", cfg.DefaultPublishMode)
	}
	defaultPublishMode := workspace.NormalizePublishMode(rawDefaultPublishMode)

	s := &Server{
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
		workflowCompiler:           workflowCompiler,
		previewSessions:            newPreviewSessionStore(previewSessionTTL, now, idGenerator),
		autonomousApprovalsDefault: autonomousApprovalsDefault,
		providerEngine:             providerEngine,
		providerModelCatalog:       cfg.ProviderModelCatalog,
		projectRoot:                projectRoot,
		defaultPublishMode:         defaultPublishMode,
		cursors:                    cursorSigner,
	}
	var plannerAgent planning.Agent
	if cfg.PlannerAgent != nil {
		plannerAgent = cfg.PlannerAgent
	} else if providerEngine == "opencode" {
		plannerInlineConfig, inlineConfigErr := opencode.PlannerSessionInlineConfig()
		if inlineConfigErr != nil {
			return nil, fmt.Errorf("build planner opencode inline config: %w", inlineConfigErr)
		}
		if projectRoot != "" {
			if err := bootstrapassets.ValidateManagedOpenCodePlannerAssets(bootstrapassets.ManagedOpenCodeRoot(projectRoot)); err != nil {
				return nil, fmt.Errorf("validate managed opencode planner assets: %w", err)
			}
		}
		plannerAdapter, adapterErr := opencode.NewSessionAdapter(opencode.SessionAdapterConfig{
			BinaryPath:          cfg.OpenCodeBinaryPath,
			WorkDir:             strings.TrimSpace(cfg.ProjectRoot),
			ConfigDir:           bootstrapassets.ManagedOpenCodeRoot(cfg.ProjectRoot),
			DatabasePath:        bootstrapassets.ManagedOpenCodeDatabasePath(cfg.ProjectRoot),
			InlineConfigContent: plannerInlineConfig,
			Agent:               "planner",
			SkipPermissions:     true,
		})
		if adapterErr != nil {
			return nil, fmt.Errorf("init planner adapter: %w", adapterErr)
		}
		plannerAgent = planning.NewOpenCodeAgent(plannerAdapter, idGenerator)
	}
	s.planningService = planning.NewService(planningStore, plannerAgent, strings.TrimSpace(cfg.ProjectRoot), func() string {
		return now().UTC().Format(time.RFC3339)
	}, idGenerator)
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
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(startupCtx), 10*time.Second)
		defer cancel()
		_ = s.planningService.Close(closeCtx)
	}()
	if err := s.planningService.RecoverOperations(startupCtx); err != nil {
		return nil, fmt.Errorf("recover planning operations: %w", err)
	}
	if s.providerModelCatalog == nil {
		s.providerModelCatalog = NewOpenCodeProviderModelCatalog(cfg.OpenCodeBinaryPath, now, 60*time.Second)
	}

	if err := state.EnsureCanonicalRoles(startupCtx, cfg.DB, now()); err != nil {
		return nil, fmt.Errorf("seed canonical roles: %w", err)
	}

	inner := http.NewServeMux()
	s.registerRoutes(inner)

	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		return nil, err
	}

	idempotency := NewIdempotencyMiddleware(state.NewIdempotencyStore(cfg.DB))
	wrapped := validator(idempotency(inner))

	// SSE streaming endpoints bypass idempotency middleware (which buffers the full response)
	// but still go through OpenAPI validation.
	streamingMux := http.NewServeMux()
	streamingMux.HandleFunc("GET /acp/runs/{runId}/events", s.handleStreamAcpRunEvents)
	wrappedStreaming := validator(streamingMux)

	mux := http.NewServeMux()
	mux.Handle("GET "+basePath+"/preview/{token}", http.StripPrefix(basePath, wrapped))
	// Nested preview assets keep the raw outer route because slash-preserving
	// asset paths are browser content URLs, not a good fit for strict OpenAPI
	// request validation without changing the public route contract.
	mux.HandleFunc("GET "+basePath+"/preview/{token}/{assetPath...}", s.handlePreviewArtifactContent)
	mux.Handle("GET "+basePath+"/acp/runs/{runId}/events", http.StripPrefix(basePath, wrappedStreaming))
	mux.Handle(basePath+"/", http.StripPrefix(basePath, wrapped))

	s.handler = mux
	constructorOK = true
	return s, nil
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", s.handleGetHealth)
	mux.HandleFunc("GET /roles", s.handleListRoles)
	mux.HandleFunc("GET /agents", s.handleListAgents)
	mux.HandleFunc("GET /provider/models", s.handleListProviderModels)
	mux.HandleFunc("GET /workspace-capabilities", s.handleGetWorkspaceCapabilities)
	mux.HandleFunc("POST /decrees", s.handleCreateDecree)
	mux.HandleFunc("GET /decrees/{id}", s.handleGetDecree)
	mux.HandleFunc("POST /planning-sessions", s.handleCreatePlanningSession)
	mux.HandleFunc("GET /planning-sessions", s.handleListPlanningSessions)
	mux.HandleFunc("GET /planning-sessions/{id}", s.handleGetPlanningSession)
	mux.HandleFunc("PATCH /planning-sessions/{id}", s.handleUpdatePlanningSession)
	mux.HandleFunc("DELETE /planning-sessions/{id}", s.handleDeletePlanningSession)
	mux.HandleFunc("GET /acp/agents", s.handleListAcpAgents)
	mux.HandleFunc("GET /acp/agents/{name}", s.handleGetAcpAgent)
	mux.HandleFunc("POST /acp/runs", s.handleCreateAcpRun)
	mux.HandleFunc("GET /acp/runs/{runId}", s.handleGetAcpRun)
	mux.HandleFunc("GET /acp/runs/{runId}/events", s.handleStreamAcpRunEvents)
	mux.HandleFunc("GET /acp/runs/{runId}/activity", s.handleGetAcpRunActivity)
	mux.HandleFunc("POST /acp/runs/{runId}/cancel", s.handleCancelAcpRun)
	mux.HandleFunc("GET /planning-sessions/{id}/messages", s.handleListPlanningSessionMessages)
	mux.HandleFunc("PUT /planning-sessions/{id}/messages/{messageId}/activate", s.handleActivatePlanningMessageVariant)
	mux.HandleFunc("GET /planning-sessions/{id}/messages/{messageId}/variants", s.handleListPlanningMessageVariants)
	mux.HandleFunc("GET /planning-sessions/{id}/plan", s.handleGetPlanningSessionPlan)
	mux.HandleFunc("POST /planning-sessions/{id}/context/refresh", s.handleRefreshPlanningSessionContext)
	mux.HandleFunc("POST /workflows/publish", s.handlePublishWorkflow)
	mux.HandleFunc("POST /runs", s.handleCreateRun)
	mux.HandleFunc("GET /runs", s.handleListRuns)
	mux.HandleFunc("GET /runs/events/stream", s.handleStreamRunCollectionEvents)
	mux.HandleFunc("GET /runs/{id}", s.handleGetRun)
	mux.HandleFunc("POST /runs/{id}/cancel", s.handleCancelRun)
	mux.HandleFunc("GET /runs/{id}/plan", s.handleGetRunPlan)
	mux.HandleFunc("GET /runs/{id}/plan-reviews", s.handleListRunPlanReviews)
	mux.HandleFunc("GET /runs/{id}/governance-decisions", s.handleListRunGovernanceDecisions)
	mux.HandleFunc("GET /runs/{id}/artifacts", s.handleListRunArtifacts)
	mux.HandleFunc("GET /runs/{id}/artifacts/archive", s.handleGetRunArtifactsArchive)
	mux.HandleFunc("GET /runs/{id}/graph", s.handleGetRunGraph)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}/activity", s.handleGetRunExecutionActivity)
	mux.HandleFunc("GET /runs/{id}/executions/{attemptId}", s.handleGetRunExecution)
	mux.HandleFunc("GET /runs/{id}/executions", s.handleListRunExecutions)
	mux.HandleFunc("GET /runs/{id}/agents", s.handleListRunAgents)
	mux.HandleFunc("GET /runs/{id}/timeline", s.handleGetRunTimeline)
	mux.HandleFunc("GET /runs/{id}/mailbox/messages", s.handleListRunMailboxMessages)
	mux.HandleFunc("GET /runs/{id}/mailbox/messages/{messageId}", s.handleGetRunMailboxMessage)
	mux.HandleFunc("POST /runs/{id}/mailbox/messages/{messageId}/read", s.handleMarkRunMailboxMessageRead)
	mux.HandleFunc("POST /runs/{id}/nodes/{nodeKey}/retry", s.handleRetryNode)
	mux.HandleFunc("POST /runs/{id}/rerun-from/{nodeKey}", s.handleRerunFromNode)
	mux.HandleFunc("POST /runs/{id}/plan-approval", s.handleCreatePlanApproval)
	mux.HandleFunc("POST /runs/{id}/final-approval", s.handleCreateFinalApproval)
	mux.HandleFunc("GET /artifacts/{id}", s.handleGetArtifact)
	mux.HandleFunc("GET /artifacts/{id}/content", s.handleGetArtifactContent)
	mux.HandleFunc("POST /artifacts/{id}/preview-session", s.handleCreateArtifactPreviewSession)
	mux.HandleFunc("GET /preview/{token}", s.handlePreviewArtifactContent)
	mux.HandleFunc("GET /events/stream", s.handleStreamEvents)
	mux.HandleFunc("GET /metrics/runtime", s.handleGetMetricsRuntime)
	mux.HandleFunc("GET /dashboard/snapshot", s.handleGetDashboardSnapshot)
	mux.HandleFunc("GET /realm-analytics/snapshot", s.handleGetRealmAnalyticsSnapshot)
	mux.HandleFunc("GET /realm-analytics/execution-detail", s.handleGetRealmAnalyticsExecutionDetailSnapshot)
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

type runExecutionsResponse struct {
	RunID string             `json:"run_id"`
	Items []runExecutionItem `json:"items"`
}

type agentsResponse struct {
	Items []agentItem `json:"items"`
}

type runParticipantItem struct {
	AgentID         string     `json:"agent_id"`
	RoleID          string     `json:"role_id"`
	DisplayName     string     `json:"display_name"`
	Status          string     `json:"status"`
	ParentAgentID   *string    `json:"parent_agent_id,omitempty"`
	SpawnDepth      int        `json:"spawn_depth"`
	FirstSeenAt     time.Time  `json:"first_seen_at"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
}

type agentItem struct {
	AgentID         string     `json:"agent_id"`
	RoleID          string     `json:"role_id"`
	DisplayName     string     `json:"display_name"`
	Status          string     `json:"status"`
	ParentAgentID   *string    `json:"parent_agent_id,omitempty"`
	SpawnDepth      int        `json:"spawn_depth"`
	CreatedAt       time.Time  `json:"created_at"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
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
	LastHeartbeatAt    *time.Time `json:"last_heartbeat_at,omitempty"`
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
}

type runExecutionSettlementDetailItem struct {
	Reason *string `json:"reason,omitempty"`
}

type runExecutionDetailItem struct {
	runExecutionItem
	ProviderInvocationID *string                            `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt *time.Time                         `json:"provider_invocation_at,omitempty"`
	SettlementStatus     string                             `json:"settlement_status"`
	SettlementDetail     *runExecutionSettlementDetailItem  `json:"settlement_detail,omitempty"`
	SettledAt            *time.Time                         `json:"settled_at,omitempty"`
	OutputStatus         string                             `json:"output_status"`
	OutputText           *string                            `json:"output_text,omitempty"`
	OutputVerdict        *string                            `json:"output_verdict,omitempty"`
	OutputVerdictStatus  *string                            `json:"output_verdict_status,omitempty"`
	PublicationStatus    *string                            `json:"publication_status,omitempty"`
	PublicationSummary   *string                            `json:"publication_summary,omitempty"`
	PublicationEntries   []runExecutionPublicationEntryItem `json:"publication_entries,omitempty"`
}

type runExecutionPublicationEntryItem struct {
	Path       string `json:"path"`
	Action     string `json:"action"`
	Status     string `json:"status"`
	ReasonCode string `json:"reason_code,omitempty"`
	ReasonText string `json:"reason_text,omitempty"`
}

func (s *Server) handleGetHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, gen.HealthResponse{Status: gen.HealthResponseStatusOk})
}

func (s *Server) handleGetDecree(w http.ResponseWriter, r *http.Request) {
	decreeID := strings.TrimSpace(r.PathValue("id"))
	if decreeID == "" {
		s.writeBadRequest(w, "decree id is required")
		return
	}

	var detail gen.DecreeDetail
	var createdAt string
	var metadataJSON string
	err := s.db.QueryRowContext(
		r.Context(),
		`SELECT decree_id, decree_title, decree_text, metadata_json, created_at FROM decrees WHERE decree_id = ?`,
		decreeID,
	).Scan(&detail.DecreeId, &detail.DecreeTitle, &detail.DecreeText, &metadataJSON, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "decree not found", "api.get_decree", map[string]any{"decree_id": decreeID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch decree failed")
		return
	}

	detail.CreatedAt = mustParseRFC3339(createdAt)
	if metadataJSON != "" && metadataJSON != "{}" {
		var metadata map[string]interface{}
		if jsonErr := json.Unmarshal([]byte(metadataJSON), &metadata); jsonErr == nil {
			detail.Metadata = &metadata
		}
	}

	writeJSON(w, http.StatusOK, detail)
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
		if agent.LastHeartbeatAt.Valid {
			parsed := mustParseRFC3339(agent.LastHeartbeatAt.String)
			item.LastHeartbeatAt = &parsed
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

func (s *Server) roleExists(ctx context.Context, roleID string) bool {
	var marker int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM roles WHERE role_id = ? LIMIT 1`, roleID).Scan(&marker)
	return err == nil
}

func (s *Server) requireActorRoleHeaders(w http.ResponseWriter, r *http.Request, authority string) (string, string, bool) {
	actorID := strings.TrimSpace(r.Header.Get(actorIDHeader))
	roleID := strings.TrimSpace(r.Header.Get(roleIDHeader))
	if actorID == "" || roleID == "" {
		writeCanonicalError(w, http.StatusUnauthorized, errorcatalog.CodePolicyViolation, "missing authentication headers", authority, nil)
		return "", "", false
	}
	if !s.roleExists(r.Context(), roleID) {
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "unknown role", authority, map[string]any{"role_id": roleID})
		return "", "", false
	}
	return actorID, roleID, true
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
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return parsed.UTC()
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
