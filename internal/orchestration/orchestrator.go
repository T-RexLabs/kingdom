package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

const DefaultDispatchInterval = 2 * time.Second
const defaultCompletionProjectionInterval = 250 * time.Millisecond
const instructionSectionSeparator = "----"

var nonAlphaNumPattern = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`[^a-z0-9]+`)
})

var implementationRoles = map[string]struct{}{
	"builder": {},
}

func isImplementationRole(roleID string) bool {
	_, ok := implementationRoles[strings.TrimSpace(roleID)]
	return ok
}

type orchestrationLoop struct {
	db                                 *sql.DB
	transition                         *state.TransitionService
	registry                           *state.AgentRegistry
	mailbox                            *state.MailboxStore
	delegation                         *state.DelegationPolicyEngine
	scheduler                          *workflow.Scheduler
	sessions                           *state.SessionManager
	events                             *state.AttemptEventIngestor
	provider                           opencode.Provider
	fsStore                            *runtimefs.Store
	schemas                            *schemas.Set
	validator                          *state.WorkerOutputValidator
	tokenUsage                         *state.TokenUsageStore
	planningOutputs                    *state.RunPlanningOutputStore
	executionStore                     *executionstore.Store
	nodeTimeouts                       map[string]time.Duration
	firstEventTimeout                  time.Duration
	streamInactivity                   time.Duration
	cancelGrace                        time.Duration
	cancelForce                        time.Duration
	yoloRoot                           string
	publishMode                        string
	roleComm                           rolecomm.Policy
	providerPolicy                     providerpolicy.Policy
	retryEngine                        *retryengine.Engine
	governanceMode                     string
	autonomousApprovals                bool
	runWorktrees                       *workspace.RunWorktreeStore
	worktreeRetention                  time.Duration
	gitRunner                          gitCommandRunner
	now                                func() time.Time
	sleep                              func(time.Duration)
	providerLeaseHeartbeatInterval     time.Duration
	newProviderLeaseTicker             func(time.Duration) providerLeaseTicker
	preparationLimit                   int
	preparations                       map[string]*providerAttemptPreparation
	lastPreparationRunID               string
	maintenance                        maintenanceState
	execMu                             sync.Mutex
	eventSeqMu                         sync.Mutex
	attemptCancelMu                    sync.Mutex
	attemptCancelCursor                executionstore.AttemptCancelCursor
	attemptCancelHighWatermark         executionstore.AttemptCancelCursor
	terminalHandoffFileLocks           keyedLock
	terminalHandoffCursor              string
	terminalHandoffFileCursor          string
	terminalHandoffCleanupCursor       string
	responseOnlyWorkspaceCleanupCursor string
	workspaceLocks                     sync.Map
	inflightExec                       map[string]struct{}
	inflightRunIDs                     map[string]string
	inflightSettlements                map[string]string
	executionResults                   map[string]chan asyncExecutionResult
	asyncDrain                         *executionDrainTracker
	notifyCompletionReady              func()
	closeMode                          *runtimeCloseMode
	guardianRuntimeRoot                string
	// serverAbsent replaces the real process-identity check in tests.
	serverAbsent          func(pid, treeID int, childToken, treeToken string) bool
	commandGuardianAbsent func(executionstore.ProviderCommandGuardian) bool
}

type keyedLockEntry struct {
	mu         sync.Mutex
	references int
}

type keyedLock struct {
	mu      sync.Mutex
	entries map[string]*keyedLockEntry
}

func (l *keyedLock) Lock(key string) func() {
	key = strings.TrimSpace(key)
	if key == "" {
		return func() {}
	}
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[string]*keyedLockEntry)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &keyedLockEntry{}
		l.entries[key] = entry
	}
	entry.references++
	l.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.references--
		if entry.references == 0 {
			delete(l.entries, key)
		}
		l.mu.Unlock()
	}
}

func (o *orchestrationLoop) transitionService() *state.TransitionService {
	if o.transition != nil {
		return o.transition
	}
	return state.NewTransitionService(o.db, nil)
}

func (o *orchestrationLoop) planningOutputStore() *state.RunPlanningOutputStore {
	if o.planningOutputs != nil {
		return o.planningOutputs
	}
	return state.NewRunPlanningOutputStore(o.db)
}

func (o *orchestrationLoop) canonicalExecutionStore() *executionstore.Store {
	if o == nil || o.db == nil {
		return nil
	}
	if o.executionStore != nil {
		return o.executionStore
	}
	return executionstore.New(o.db)
}

func (o *orchestrationLoop) tryLockRunWorkspace(runID string) (func(), bool) {
	if o == nil || strings.TrimSpace(runID) == "" {
		return func() {}, true
	}
	value, _ := o.workspaceLocks.LoadOrStore(runID, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	if !lock.TryLock() {
		return nil, false
	}
	return lock.Unlock, true
}

func (o *orchestrationLoop) executionAttemptInflight(attemptID string) bool {
	if o == nil || strings.TrimSpace(attemptID) == "" {
		return false
	}
	o.execMu.Lock()
	defer o.execMu.Unlock()
	_, ok := o.inflightExec[strings.TrimSpace(attemptID)]
	return ok
}

func (o *orchestrationLoop) hasInflightRunExecution(runID string) bool {
	return o.hasInflightRunExecutionExcept(runID, "")
}

func (o *orchestrationLoop) hasInflightRunExecutionExcept(runID, attemptID string) bool {
	if o == nil || strings.TrimSpace(runID) == "" {
		return false
	}
	runID = strings.TrimSpace(runID)
	o.execMu.Lock()
	defer o.execMu.Unlock()
	for inflightAttemptID, inflightRunID := range o.inflightRunIDs {
		if inflightRunID == runID && inflightAttemptID != attemptID {
			return true
		}
	}
	return false
}

type runSnapshot struct {
	RunID                 string
	RunStatus             string
	PlanState             string
	AutonomousApprovals   bool
	PublishMode           string
	ModelSelection        *gen.RunModelSelection
	WorkflowDefinitionID  string
	CorrelationID         string
	TransitionActorID     string
	TransitionAuthorityID string
}

type nodeDefinition struct {
	NodeID       string
	RequiredRole string
	NodeClass    string
}

type edgeDefinition struct {
	FromNodeID string
	ToNodeID   string
	On         string
}

type nodeAttempt struct {
	NodeRunID             string
	NodeID                string
	Attempt               int
	Wave                  int
	NodeStatus            string
	TransitionReasonCode  string
	TransitionAuthority   string
	RequiredRole          string
	AgentID               string
	StartedAt             sql.NullString
	HasExecutionRecord    bool
	DurableState          string
	ExecutionMode         executionstore.ExecutionMode
	CompletionMode        executionstore.ProviderCompletionMode
	ProviderTimeoutMillis int64
	SessionRef            string
	ProviderSessionID     string
	ProviderInvocationID  string
	LeaseOwner            string
	LeaseVersion          int64
}

type asyncExecutionResult struct {
	run                                  runSnapshot
	node                                 nodeDefinition
	attempt                              nodeAttempt
	outboxID                             string
	attemptID                            string
	sessionID                            string
	providerSessionID                    string
	providerInvocationID                 string
	providerInvocationAt                 string
	terminalHandoffID                    string
	launchObservedAt                     string
	completionMode                       executionstore.ProviderCompletionMode
	expectedLeaseOwner                   string
	expectedLeaseVersion                 int64
	expectedLeaseExpiresAt               string
	providerLeaseDuration                time.Duration
	providerDiagnosticsJSON              string
	providerStreamObservation            opencode.InvocationStreamObservation
	baseSnapshotID                       string
	resultSnapshotID                     string
	workspaceID                          string
	workspacePath                        string
	ephemeralWorkspace                   bool
	responseOnlyWorkspaceCleanupMarkedAt string
	delegator                            string
	openPrompt                           opencodePromptOpener
	system                               string
	model                                string
	variant                              string
	agent                                string
	occurredAt                           string
	outputText                           string
	tokens                               opencode.TokenUsage
	outcome                              opencode.ExecutionOutcome
	launchObserved                       bool
	err                                  error
}

type communicationMessageRequest struct {
	MessageID         string
	RunID             string
	FromAgentID       string
	ToAgentID         string
	ThreadID          string
	CommunicationType string
	NodeID            string
	NodeRunID         string
	AttemptID         string
	ReasonCode        string
	ReasonText        string
	AuthoritySource   string
	OccurredAt        string
	DelegatorScoped   string
	InstructionFull   string
	InstructionRef    *state.RequestContentReference
	ResponseFull      string
	ResponseRef       *providerexecution.ContentReference
	ResponseRaw       string
}

func marshalExecutionAttemptEventPayload(attemptID, nodeID, nodeRunID string, attempt int, reasonCode string, extra map[string]any) (string, error) {
	payload := map[string]any{
		"attempt_id":  strings.TrimSpace(attemptID),
		"node_id":     strings.TrimSpace(nodeID),
		"node_run_id": strings.TrimSpace(nodeRunID),
		"attempt":     attempt,
	}
	if strings.TrimSpace(reasonCode) != "" {
		payload["reason_code"] = strings.TrimSpace(reasonCode)
	}
	for key, value := range extra {
		payload[key] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func marshalAsyncExecutionAttemptEventPayload(result asyncExecutionResult, reasonCode string, extra map[string]any) (string, error) {
	return marshalExecutionAttemptEventPayload(result.attemptID, result.node.NodeID, result.attempt.NodeRunID, result.attempt.Attempt, reasonCode, extra)
}

func newOrchestrationLoop(db *sql.DB, provider opencode.Provider, sessionManager *state.SessionManager, fsStore *runtimefs.Store, outputValidator *state.WorkerOutputValidator, eventSchemas *schemas.Set, nodeTimeouts map[string]time.Duration, streamInactivity time.Duration, cancelGrace time.Duration, cancelForce time.Duration, yoloOutputRoot string, policy rolecomm.Policy, providerPolicy providerpolicy.Policy, retryEngine *retryengine.Engine, governanceMode string, autonomousApprovals bool, publishMode string, worktreeRetention time.Duration, closeMode *runtimeCloseMode) *orchestrationLoop {
	registry := state.NewAgentRegistry(db)
	mailbox := state.NewMailboxStore(db, eventSchemas)
	return &orchestrationLoop{
		db:                     db,
		transition:             state.NewTransitionService(db, eventSchemas).WithRoleCommunicationPolicy(policy),
		registry:               registry,
		mailbox:                mailbox,
		delegation:             state.NewDelegationPolicyEngineWithMailbox(db, registry, mailbox),
		scheduler:              workflow.NewScheduler(workflow.SchedulerConfig{}),
		sessions:               sessionManager,
		events:                 state.NewAttemptEventIngestor(db, eventSchemas).WithRoleCommunicationPolicy(policy),
		provider:               provider,
		fsStore:                fsStore,
		schemas:                eventSchemas,
		validator:              outputValidator,
		tokenUsage:             state.NewTokenUsageStore(db),
		planningOutputs:        state.NewRunPlanningOutputStore(db),
		executionStore:         executionstore.New(db),
		nodeTimeouts:           cloneDurationMap(nodeTimeouts),
		streamInactivity:       streamInactivity,
		cancelGrace:            defaultDuration(cancelGrace, 5*time.Second),
		cancelForce:            defaultDuration(cancelForce, 10*time.Second),
		yoloRoot:               strings.TrimSpace(yoloOutputRoot),
		publishMode:            normalizePublishMode(publishMode),
		roleComm:               policy,
		providerPolicy:         providerPolicy,
		retryEngine:            retryEngine,
		governanceMode:         normalizeGovernanceMode(governanceMode),
		autonomousApprovals:    autonomousApprovals,
		runWorktrees:           workspace.NewRunWorktreeStore(db),
		worktreeRetention:      workspace.NormalizeRunWorktreeRetention(worktreeRetention),
		now:                    time.Now,
		sleep:                  time.Sleep,
		newProviderLeaseTicker: newTimeProviderLeaseTicker,
		inflightExec:           make(map[string]struct{}),
		inflightRunIDs:         make(map[string]string),
		inflightSettlements:    make(map[string]string),
		executionResults:       make(map[string]chan asyncExecutionResult),
		asyncDrain:             newExecutionDrainTracker(),
		closeMode:              closeMode,
	}
}

func (o *orchestrationLoop) forceCloseActive() bool {
	return o != nil && o.closeMode != nil && o.closeMode.IsForced()
}

func (o *orchestrationLoop) inflightAttemptIDs() []string {
	if o == nil {
		return nil
	}
	o.execMu.Lock()
	defer o.execMu.Unlock()
	if len(o.inflightExec) == 0 {
		return nil
	}
	attempts := make([]string, 0, len(o.inflightExec))
	for attemptID := range o.inflightExec {
		attempts = append(attempts, attemptID)
	}
	return attempts
}

func normalizePublishMode(raw string) string {
	return workspace.NormalizePublishMode(raw)
}

func defaultDuration(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func (o *orchestrationLoop) Tick(ctx context.Context) error {
	if _, err := o.scheduleAttemptCancellations(ctx); err != nil {
		return err
	}
	if err := o.scheduleRunWorktreeCleanup(ctx); err != nil {
		return err
	}

	runs, err := o.loadActiveRuns(ctx)
	if err != nil {
		return err
	}

	o.execMu.Lock()
	lastAdmittedRun := o.lastPreparationRunID
	o.execMu.Unlock()
	start := 0
	for i, run := range runs {
		if run.RunID == lastAdmittedRun {
			start = (i + 1) % len(runs)
			break
		}
	}
	for offset := range runs {
		run := runs[(start+offset)%len(runs)]
		if err := o.processRun(ctx, run); err != nil {
			return fmt.Errorf("process run %q: %w", run.RunID, err)
		}
	}
	return nil
}

func (o *orchestrationLoop) waitForAsyncDrain(ctx context.Context) error {
	if o == nil || o.asyncDrain == nil {
		return nil
	}
	return o.asyncDrain.Wait(ctx)
}

func (o *orchestrationLoop) processRun(ctx context.Context, run runSnapshot) error {
	now := o.now().UTC().Format(time.RFC3339Nano)

	if run.RunStatus == "pending" {
		if err := o.transition.Apply(ctx, state.TransitionRequest{
			TransitionID:    fmt.Sprintf("tr_orch_%s_run_running", run.RunID),
			EventID:         fmt.Sprintf("evt_orch_%s_run_running", run.RunID),
			RunID:           run.RunID,
			Entity:          state.EntityTypeRun,
			FromState:       "pending",
			ToState:         "running",
			ActorID:         "runtime_orchestrator",
			RoleID:          "coordinator",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "orchestration_progress",
			ReasonText:      "runtime dispatch started",
			CorrelationID:   "corr_orch_" + run.RunID,
			CausationID:     "caus_orch_" + run.RunID + "_run_start",
			OccurredAt:      now,
			AttemptID:       "run:" + run.RunID + ":status",
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        o.nextEventSeq(ctx, "run:"+run.RunID+":status"),
			PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"status":%q}`, run.RunID, "running"),
		}); err != nil {
			return fmt.Errorf("mark run running: %w", err)
		}
		run.RunStatus = "running"
	}

	if run.RunStatus == "running" {
		if err := o.renewRunSessionLease(ctx, run.RunID, now); err != nil {
			return err
		}
	}

	nodes, edges, err := o.loadWorkflowGraph(ctx, run)
	if err != nil {
		return err
	}

	if err := o.advancePlanProjection(ctx, &run, now, nodes); err != nil {
		return err
	}
	if err := o.autoAdvanceGovernanceDecision(ctx, &run, now); err != nil {
		return err
	}

	if err := o.ensureInitialAttempts(ctx, run.RunID, nodes, now); err != nil {
		return err
	}
	if err := o.ensureDecisionNodeAttempts(ctx, run, nodes, now); err != nil {
		return err
	}

	latestAttempts, err := o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}
	if err := o.enforceRunningAttemptTimeouts(ctx, run, latestAttempts, now); err != nil {
		return err
	}
	if _, err := o.transitionService().ReleaseDueWorkOrderRetries(ctx, run.RunID, now); err != nil {
		return err
	}
	latestAttempts, err = o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}

	if err := o.promoteReadyNodes(ctx, run, nodes, edges, latestAttempts, now); err != nil {
		return err
	}

	if err := o.advancePlanProjection(ctx, &run, now, nodes); err != nil {
		return err
	}
	if err := o.autoAdvanceGovernanceDecision(ctx, &run, now); err != nil {
		return err
	}
	if err := o.ensureDecisionNodeAttempts(ctx, run, nodes, now); err != nil {
		return err
	}

	latestAttempts, err = o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}
	if err := o.enforceRunningAttemptTimeouts(ctx, run, latestAttempts, now); err != nil {
		return err
	}
	if err := o.dispatchReadyNodes(ctx, run, nodes, latestAttempts, now); err != nil {
		return err
	}

	return o.finalizeRunState(ctx, run, nodes, edges)
}

func sanitizeFileToken(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "artifact"
	}
	builder := strings.Builder{}
	builder.Grow(len(raw))
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	value := strings.Trim(builder.String(), "._-")
	if value == "" {
		return "artifact"
	}
	return value
}

func (o *orchestrationLoop) ensureInitialAttempts(ctx context.Context, runID string, nodes []nodeDefinition, occurredAt string) error {
	latest, err := o.loadLatestAttempts(ctx, runID)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if _, exists := latest[node.NodeID]; exists {
			continue
		}
		agentID, err := o.ensureAgentForRole(ctx, node.RequiredRole, occurredAt)
		if err != nil {
			return err
		}
		if err := o.registry.CreateNodeAttempt(ctx, state.CreateNodeAttemptRequest{
			NodeRunID:    fmt.Sprintf("nr_orch_%s_%s_1", runID, node.NodeID),
			RunID:        runID,
			NodeID:       node.NodeID,
			Attempt:      1,
			Wave:         1,
			NodeStatus:   "pending",
			RequiredRole: node.RequiredRole,
			AgentID:      agentID,
			RowVersion:   1,
			CreatedAt:    occurredAt,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (o *orchestrationLoop) ensureAgentForRole(ctx context.Context, roleID, occurredAt string) (string, error) {
	var agentID string
	err := o.db.QueryRowContext(ctx,
		`SELECT agent_id FROM agents WHERE role_id = ? ORDER BY agent_id ASC LIMIT 1`,
		roleID,
	).Scan(&agentID)
	if err == nil {
		return agentID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	agentID = deterministicAgentIDForRole(roleID)
	displayName := roleDisplayNameForRoleID(roleID)
	_, err = o.db.ExecContext(ctx,
		`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at)
		 VALUES (?, ?, ?, 'idle', NULL, 0, ?, ?)`,
		agentID,
		roleID,
		displayName,
		occurredAt,
		occurredAt,
	)
	if err != nil && !state.IsUniqueViolation(err) {
		return "", err
	}

	return agentID, nil
}

func deterministicAgentIDForRole(roleID string) string {
	trimmed := strings.ToLower(strings.TrimSpace(roleID))
	cleaned := nonAlphaNumPattern().ReplaceAllString(trimmed, "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		cleaned = "unknown"
	}
	return "agent_" + cleaned
}

func roleDisplayNameForRoleID(roleID string) string {
	trimmed := strings.TrimSpace(roleID)
	parts := strings.Split(trimmed, "-")
	for idx, part := range parts {
		if part == "" {
			continue
		}
		switch strings.ToLower(part) {
		case "qa", "api", "ui", "id":
			parts[idx] = strings.ToUpper(part)
			continue
		}
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}
	if len(parts) == 0 {
		return "System Agent"
	}
	return strings.Join(parts, " ")
}

func delegationPathForNode(nodeID, roleID string) (string, string) {
	switch roleID {
	case "governance":
		return "", ""
	case "coordinator", "architecture-advisor", "risk-advisor", "systems-advisor", "usability-advisor":
		return deterministicAgentIDForRole("governance"), roleID
	case "plan-author":
		return deterministicAgentIDForRole("coordinator"), roleID
	default:
		return deterministicAgentIDForRole("plan-author"), roleID
	}
}

func delegatorRoleForNode(nodeID, roleID string) string {
	switch roleID {
	case "governance":
		return ""
	case "coordinator", "architecture-advisor", "risk-advisor", "systems-advisor", "usability-advisor":
		return "governance"
	case "plan-author":
		return "coordinator"
	default:
		return "plan-author"
	}
}

func (o *orchestrationLoop) loadActiveRuns(ctx context.Context) ([]runSnapshot, error) {
	rows, err := o.db.QueryContext(ctx,
		`SELECT run_id, run_status, plan_state, COALESCE(autonomous_approvals, 1), COALESCE(publish_mode, 'direct'), COALESCE(model_selection_json, '{}'), workflow_definition_id
		 FROM workflow_runs
		 WHERE run_status IN ('pending', 'running')
		 ORDER BY created_at ASC, run_id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	runs := make([]runSnapshot, 0)
	for rows.Next() {
		var run runSnapshot
		var autonomousApprovalsInt int
		var publishMode string
		var modelSelectionRaw string
		if err := rows.Scan(&run.RunID, &run.RunStatus, &run.PlanState, &autonomousApprovalsInt, &publishMode, &modelSelectionRaw, &run.WorkflowDefinitionID); err != nil {
			return nil, err
		}
		run.AutonomousApprovals = autonomousApprovalsInt != 0
		run.PublishMode = normalizePublishMode(publishMode)
		run.ModelSelection = decodeRunModelSelection(modelSelectionRaw)
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return runs, nil
}

func (o *orchestrationLoop) resolveModelForRole(run runSnapshot, roleID string) (string, string, string) {
	role := strings.TrimSpace(roleID)
	if role != "" && run.ModelSelection != nil && run.ModelSelection.RoleOverrides != nil {
		if entry, ok := (*run.ModelSelection.RoleOverrides)[role]; ok {
			model := strings.TrimSpace(entry.Model)
			variant := ""
			if entry.ThinkingEffort != nil {
				variant = strings.TrimSpace(*entry.ThinkingEffort)
			}
			if model != "" {
				return model, variant, "role_override"
			}
		}
	}
	if run.ModelSelection != nil {
		model := strings.TrimSpace(run.ModelSelection.Global.Model)
		variant := ""
		if run.ModelSelection.Global.ThinkingEffort != nil {
			variant = strings.TrimSpace(*run.ModelSelection.Global.ThinkingEffort)
		}
		if model != "" {
			return model, variant, "global_selection"
		}
	}
	return "", "", "opencode_default"
}

func decodeRunModelSelection(raw string) *gen.RunModelSelection {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var decoded gen.RunModelSelection
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return nil
	}
	if strings.TrimSpace(decoded.Global.Model) == "" && (decoded.RoleOverrides == nil || len(*decoded.RoleOverrides) == 0) {
		return nil
	}
	return &decoded
}

func (o *orchestrationLoop) loadWorkflowGraph(ctx context.Context, run runSnapshot) ([]nodeDefinition, []edgeDefinition, error) {
	nodeRows, err := o.db.QueryContext(ctx,
		`SELECT node_id, required_role, node_class
		 FROM workflow_nodes
		 WHERE workflow_definition_id = ?
		 ORDER BY node_id ASC`,
		run.WorkflowDefinitionID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer nodeRows.Close()

	nodes := make([]nodeDefinition, 0)
	for nodeRows.Next() {
		var node nodeDefinition
		if err := nodeRows.Scan(&node.NodeID, &node.RequiredRole, &node.NodeClass); err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, node)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, nil, err
	}

	edgeRows, err := o.db.QueryContext(ctx,
		`SELECT from_node_id, to_node_id, on_condition
		 FROM workflow_edges
		 WHERE workflow_definition_id = ?
		 ORDER BY from_node_id ASC, to_node_id ASC`,
		run.WorkflowDefinitionID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer edgeRows.Close()

	edges := make([]edgeDefinition, 0)
	for edgeRows.Next() {
		var edge edgeDefinition
		if err := edgeRows.Scan(&edge.FromNodeID, &edge.ToNodeID, &edge.On); err != nil {
			return nil, nil, err
		}
		edges = append(edges, edge)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, nil, err
	}

	return nodes, edges, nil
}

func (o *orchestrationLoop) workflowGraphForRun(ctx context.Context, runID string) (*workflow.Graph, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	var workflowDefinitionID string
	err := o.db.QueryRowContext(ctx, `SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, trimmedRunID).Scan(&workflowDefinitionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("workflow run %q not found", trimmedRunID)
	}
	if err != nil {
		return nil, fmt.Errorf("load workflow definition id for run %q: %w", trimmedRunID, err)
	}
	nodeRows, err := o.db.QueryContext(ctx, `
SELECT node_id, required_role, node_class
FROM workflow_nodes
WHERE workflow_definition_id = ?
ORDER BY node_id ASC`, strings.TrimSpace(workflowDefinitionID))
	if err != nil {
		return nil, fmt.Errorf("list workflow nodes for run %q: %w", trimmedRunID, err)
	}
	defer nodeRows.Close()
	definition := workflow.Definition{WorkflowID: strings.TrimSpace(workflowDefinitionID)}
	for nodeRows.Next() {
		var node workflow.Node
		if err := nodeRows.Scan(&node.NodeID, &node.RequiredRole, &node.NodeClass); err != nil {
			return nil, fmt.Errorf("scan workflow node for run %q: %w", trimmedRunID, err)
		}
		definition.Nodes = append(definition.Nodes, node)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow nodes for run %q: %w", trimmedRunID, err)
	}
	edgeRows, err := o.db.QueryContext(ctx, `
SELECT from_node_id, to_node_id, on_condition
FROM workflow_edges
WHERE workflow_definition_id = ?
ORDER BY from_node_id ASC, to_node_id ASC, on_condition ASC`, strings.TrimSpace(workflowDefinitionID))
	if err != nil {
		return nil, fmt.Errorf("list workflow edges for run %q: %w", trimmedRunID, err)
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var edge workflow.Edge
		if err := edgeRows.Scan(&edge.From, &edge.To, &edge.On); err != nil {
			return nil, fmt.Errorf("scan workflow edge for run %q: %w", trimmedRunID, err)
		}
		definition.Edges = append(definition.Edges, edge)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow edges for run %q: %w", trimmedRunID, err)
	}
	graph, err := workflow.NewParser(nil).ParseDefinition(definition)
	if err != nil {
		return nil, fmt.Errorf("build workflow graph for run %q: %w", trimmedRunID, err)
	}
	return graph, nil
}

func (o *orchestrationLoop) loadLatestAttempts(ctx context.Context, runID string) (map[string]nodeAttempt, error) {
	rows, err := o.db.QueryContext(ctx,
		`WITH latest_node_transitions AS (
			SELECT t.node_run_id,
			       COALESCE(t.reason_code, '') AS reason_code,
			       COALESCE(t.authority_source, '') AS authority_source,
			       ROW_NUMBER() OVER (PARTITION BY t.node_run_id ORDER BY t.created_at DESC, t.transition_id DESC) AS rn
			FROM transitions t
			WHERE t.run_id = ?
			  AND t.entity_type = 'node'
		), canonical_attempts AS (
			SELECT nr.node_run_id,
			       ROW_NUMBER() OVER (
			         PARTITION BY nr.node_id
			         ORDER BY CASE
			           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
			           ELSE 1
			         END ASC,
			         nr.attempt DESC,
			         nr.node_run_id DESC
			       ) AS canonical_rank
			FROM node_runs nr
			WHERE nr.run_id = ?
		)
		SELECT nr.node_run_id, nr.node_id, nr.attempt, nr.wave, nr.node_status,
		        COALESCE(lnt.reason_code, '') AS transition_reason_code,
		        COALESCE(lnt.authority_source, '') AS transition_authority_source,
		        nr.required_role, nr.agent_id, nr.started_at,
		        CASE WHEN ae.attempt_id IS NULL THEN 0 ELSE 1 END AS has_execution_record,
		        COALESCE(ae.state, '') AS durable_state,
		        COALESCE(ae.execution_mode, 'provider_backed') AS execution_mode,
		        COALESCE(ae.completion_mode, '') AS completion_mode,
		        COALESCE(ae.provider_timeout_ms, 0) AS provider_timeout_ms,
		        COALESCE(ae.session_ref, '') AS session_ref,
		        COALESCE(ae.provider_session_id, '') AS provider_session_id,
		        COALESCE(ae.active_provider_invocation_id, '') AS active_provider_invocation_id,
		        COALESCE(ae.lease_owner, '') AS lease_owner,
		        COALESCE(ae.lease_version, 0) AS lease_version
		 FROM node_runs nr
		 JOIN canonical_attempts latest ON latest.node_run_id = nr.node_run_id AND latest.canonical_rank = 1
		 LEFT JOIN attempt_executions ae ON ae.attempt_id = nr.run_id || ':' || nr.node_id || ':' || nr.attempt
		 LEFT JOIN latest_node_transitions lnt ON lnt.node_run_id = nr.node_run_id AND lnt.rn = 1
		 WHERE nr.run_id = ?`,
		runID,
		runID,
		runID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attempts := make(map[string]nodeAttempt)
	for rows.Next() {
		var attempt nodeAttempt
		var executionMode, completionMode string
		var hasExecutionRecord int
		if err := rows.Scan(&attempt.NodeRunID, &attempt.NodeID, &attempt.Attempt, &attempt.Wave, &attempt.NodeStatus, &attempt.TransitionReasonCode, &attempt.TransitionAuthority, &attempt.RequiredRole, &attempt.AgentID, &attempt.StartedAt, &hasExecutionRecord, &attempt.DurableState, &executionMode, &completionMode, &attempt.ProviderTimeoutMillis, &attempt.SessionRef, &attempt.ProviderSessionID, &attempt.ProviderInvocationID, &attempt.LeaseOwner, &attempt.LeaseVersion); err != nil {
			return nil, err
		}
		attempt.HasExecutionRecord = hasExecutionRecord != 0
		attempt.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
		attempt.CompletionMode = executionstore.ProviderCompletionMode(completionMode)
		attempts[attempt.NodeID] = attempt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return attempts, nil
}

func (o *orchestrationLoop) nextEventSeq(ctx context.Context, attemptID string) int {
	var seq int
	err := o.db.QueryRowContext(ctx,
		`SELECT MAX(seq) FROM (
			SELECT COALESCE(MAX(seq), 0) AS seq FROM events WHERE attempt_id = ?
			UNION ALL
			SELECT COALESCE(MAX(seq), 0) AS seq FROM attempt_event_log WHERE attempt_id = ?
		)`, attemptID, attemptID,
	).Scan(&seq)
	if err != nil {
		return 1
	}
	return seq + 1
}

func (o *orchestrationLoop) emitCommunicationMessage(ctx context.Context, req communicationMessageRequest) error {
	if o == nil || o.mailbox == nil {
		return nil
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.FromAgentID) == "" || strings.TrimSpace(req.ToAgentID) == "" {
		return nil
	}

	payload := map[string]any{
		"message_source":               "communication",
		"communication_type":           strings.TrimSpace(req.CommunicationType),
		"run_id":                       strings.TrimSpace(req.RunID),
		"node_id":                      strings.TrimSpace(req.NodeID),
		"node_run_id":                  strings.TrimSpace(req.NodeRunID),
		"attempt_id":                   strings.TrimSpace(req.AttemptID),
		"from_agent_id":                strings.TrimSpace(req.FromAgentID),
		"to_agent_id":                  strings.TrimSpace(req.ToAgentID),
		"reason_code":                  strings.TrimSpace(req.ReasonCode),
		"reason_text":                  strings.TrimSpace(req.ReasonText),
		"authority_source":             strings.TrimSpace(req.AuthoritySource),
		"delegator_instruction_scoped": strings.TrimSpace(req.DelegatorScoped),
		"instruction_full":             req.InstructionFull,
		"response_full":                req.ResponseFull,
		"response_raw":                 req.ResponseRaw,
	}
	if req.InstructionRef != nil {
		delete(payload, "instruction_full")
		payload["instruction_full_ref"] = map[string]any{"content_id": req.InstructionRef.ContentID, "sha256": req.InstructionRef.SHA256, "size_bytes": req.InstructionRef.SizeBytes}
	}
	if req.ResponseRef != nil {
		delete(payload, "response_full")
		delete(payload, "response_raw")
		payload["response_full_ref"] = req.ResponseRef
		payload["response_preview"] = req.ResponseFull
	}
	senderRoleID := o.roleIDForAgent(ctx, req.FromAgentID)
	if strings.TrimSpace(senderRoleID) == "" {
		senderRoleID = "coordinator"
	}
	mailboxTemplate := o.roleComm.MailboxTemplateForRole(senderRoleID)
	body := rolecomm.RenderTemplate(mailboxTemplate.BodyTemplate, map[string]string{
		"RunID":      strings.TrimSpace(req.RunID),
		"EventType":  firstNonEmptyString(strings.TrimSpace(req.CommunicationType), "communication.recorded"),
		"ReasonText": firstNonEmptyString(strings.TrimSpace(req.ReasonText), fmt.Sprintf("communication type=%s node=%s", strings.TrimSpace(req.CommunicationType), strings.TrimSpace(req.NodeID))),
	}, "Communication record updated.")
	payload["body_markdown"] = body

	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	messageID := strings.TrimSpace(req.MessageID)
	if messageID == "" {
		messageID = fmt.Sprintf("msg_comm_%s_%s", sanitizeFileToken(req.RunID), sanitizeFileToken(firstNonEmptyString(req.AttemptID, req.CommunicationType)))
	}
	_, err = o.mailbox.SendMessage(ctx, state.SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       strings.TrimSpace(req.RunID),
		FromAgentID: strings.TrimSpace(req.FromAgentID),
		ToAgentID:   strings.TrimSpace(req.ToAgentID),
		ThreadID:    firstNonEmptyString(strings.TrimSpace(req.ThreadID), fmt.Sprintf("thread_%s_communication", sanitizeFileToken(req.RunID))),
		MessageType: state.MailboxMessageTypeStatus,
		Priority:    state.MailboxPriorityNormal,
		PayloadJSON: string(raw),
		CreatedAt:   strings.TrimSpace(req.OccurredAt),
	})
	return err
}

func cloneDurationMap(input map[string]time.Duration) map[string]time.Duration {
	if len(input) == 0 {
		return map[string]time.Duration{}
	}
	copyMap := make(map[string]time.Duration, len(input))
	for key, value := range input {
		copyMap[key] = value
	}
	return copyMap
}

func composeAttemptID(runID, nodeID string, attempt int) string {
	return fmt.Sprintf("%s:%s:%d", runID, nodeID, attempt)
}
