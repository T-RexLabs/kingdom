package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
	"kingdom/internal/workspace"

	"gopkg.in/yaml.v3"
)

func (s *Server) handleCreateDecree(w http.ResponseWriter, r *http.Request) {
	var req gen.CreateDecreeRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	createdAt := s.now().UTC().Format(time.RFC3339)
	decreeID := s.idGenerator("decree")

	metadata := map[string]any{}
	if req.Metadata != nil {
		for key, value := range *req.Metadata {
			metadata[key] = value
		}
	}

	directSnapshotID := ""
	if req.ExternalSourceSnapshotId != nil {
		directSnapshotID = strings.TrimSpace(*req.ExternalSourceSnapshotId)
	}
	var planningLineage *state.DecreePlanningLineage
	if req.PlanningLineage != nil {
		planningLineage = &state.DecreePlanningLineage{
			PlanningSessionID: req.PlanningLineage.PlanningSessionId,
			PlanningDraftID:   req.PlanningLineage.PlanningDraftId,
		}
	}
	err := s.externalStore.CreateDecree(r.Context(), state.CreateDecreeRequest{
		DecreeID: decreeID, Title: req.DecreeTitle, Text: req.DecreeText,
		Metadata: metadata, CreatedAt: createdAt, DirectSnapshotID: directSnapshotID,
		PlanningLineage: planningLineage, NewLinkID: func() string { return s.idGenerator("external_link") },
		ActorID: authenticatedActorID(r.Context()),
	})
	if err != nil {
		switch {
		case errors.Is(err, state.ErrDecreeLineageInvalid):
			writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), "api.create_decree", nil)
			return
		case errors.Is(err, state.ErrDecreeProvenanceConflict):
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, err.Error(), "api.create_decree", nil)
			return
		case errors.Is(err, state.ErrDecreeProvenanceNotFound), errors.Is(err, state.ErrExternalNotFound):
			writeExternalError(w, err, "api.create_decree")
			return
		}
		s.writeInternalError(w, "create decree failed")
		return
	}

	writeJSON(w, http.StatusCreated, gen.CreateDecreeResult{
		DecreeId:  decreeID,
		CreatedAt: mustParseRFC3339(createdAt),
	})
}

func (s *Server) handlePublishWorkflow(w http.ResponseWriter, r *http.Request) {
	var req gen.PublishWorkflowRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	publishedAt := s.now().UTC().Format(time.RFC3339)
	workflowDefinitionID := s.idGenerator("workflow")

	rawDefinition, err := json.Marshal(req.DefinitionJson)
	if err != nil {
		s.writeBadRequest(w, "definition_json must be a valid object")
		return
	}

	compiled, err := s.compileWorkflowDefinition(req.WorkflowId, rawDefinition)
	if err != nil {
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), "api.publish_workflow", nil)
		return
	}

	_, err = s.db.ExecContext(
		r.Context(),
		`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES (?, ?, ?, ?, ?)`,
		workflowDefinitionID,
		req.WorkflowId,
		req.Version,
		string(compiled.CanonicalJSON),
		publishedAt,
	)
	if err != nil {
		if isUniqueConstraintError(err) {
			var existingDefinition string
			if lookupErr := s.db.QueryRowContext(r.Context(),
				`SELECT definition_json FROM workflow_definitions WHERE workflow_name = ? AND workflow_version = ?`,
				req.WorkflowId,
				req.Version,
			).Scan(&existingDefinition); lookupErr != nil {
				s.writeInternalError(w, "read existing workflow version failed")
				return
			}
			message := "workflow version already exists with a different definition"
			if existingDefinition == string(compiled.CanonicalJSON) {
				message = "workflow version already exists"
			}
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, message, "api.publish_workflow", nil)
			return
		}
		s.writeInternalError(w, "publish workflow failed")
		return
	}

	if err := s.persistWorkflowGraph(r.Context(), workflowDefinitionID, compiled.CanonicalJSON, publishedAt); err != nil {
		if isForeignKeyError(err) {
			writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "workflow definition references unknown role", "api.publish_workflow", nil)
			return
		}
		s.writeInternalError(w, "persist workflow graph failed")
		return
	}

	writeJSON(w, http.StatusCreated, gen.PublishWorkflowResult{
		WorkflowId:  req.WorkflowId,
		Version:     req.Version,
		PublishedAt: mustParseRFC3339(publishedAt),
	})
}

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req gen.CreateRunRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	workflowID := workflow.BuiltInWorkflowID(req.SkipReviewsAndValidations != nil && *req.SkipReviewsAndValidations)

	var workflowDefinitionID string
	var definitionJSON string
	err := s.db.QueryRowContext(
		r.Context(),
		`SELECT workflow_definition_id, definition_json FROM workflow_definitions WHERE workflow_name = ? ORDER BY created_at DESC LIMIT 1`,
		workflowID,
	).Scan(&workflowDefinitionID, &definitionJSON)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "workflow not found", "api.create_run", map[string]any{"workflow_id": workflowID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "lookup workflow failed")
		return
	}

	if !s.decreeExists(r.Context(), req.DecreeId) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "decree not found", "api.create_run", map[string]any{"decree_id": req.DecreeId})
		return
	}

	now := s.now().UTC().Format(time.RFC3339)
	runID := s.idGenerator("run")
	publishMode := workspace.NormalizePublishMode(string(req.PublishMode))
	if publishMode == workspace.PublishModeGitWorktree {
		topology := workspace.ResolveGitProjectTopology(strings.TrimSpace(s.projectRoot), nil)
		if !topology.WorktreeAvailable {
			s.writeBadRequest(w, topology.FailureReason)
			return
		}
	}
	var decreeTitle string
	if err := s.db.QueryRowContext(r.Context(), `SELECT decree_title FROM decrees WHERE decree_id = ?`, req.DecreeId).Scan(&decreeTitle); err != nil {
		s.writeInternalError(w, "lookup decree title failed")
		return
	}
	maxExecutionReviewCycles := 0
	maxValidationReworkCycles := 0
	runUntilAllApprove := false
	autonomousApprovals := s.autonomousApprovalsDefault
	parsedDefinition, parseErr := workflow.ParseDefinitionJSON([]byte(definitionJSON))
	if parseErr != nil {
		s.writeInternalError(w, "decode workflow definition failed")
		return
	}
	resolvedLoopSettings, loopErr := resolveCreateRunLoopSettings(parsedDefinition, req)
	if loopErr != nil {
		s.writeBadRequest(w, loopErr.Error())
		return
	}
	maxExecutionReviewCycles = resolvedLoopSettings.MaxExecutionReviewCycles
	maxValidationReworkCycles = resolvedLoopSettings.MaxValidationReworkCycles
	runUntilAllApprove = resolvedLoopSettings.RunUntilAllApprove
	if req.AutonomousApprovals != nil {
		autonomousApprovals = *req.AutonomousApprovals
	}

	normalizedModelSelection, modelSelectionJSON, modelErr := s.normalizeRunModelSelection(r.Context(), &req.ModelSelection)
	if modelErr != nil {
		if r.Context().Err() != nil && (errors.Is(modelErr, context.Canceled) || errors.Is(modelErr, context.DeadlineExceeded)) {
			return
		}
		if isProviderModelCatalogUnavailable(modelErr) {
			writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeInternalError, "provider model catalog unavailable", "api.create_run", nil)
			return
		}
		s.writeBadRequest(w, modelErr.Error())
		return
	}
	if modelErr := s.validateRequiredRunModelSelection(normalizedModelSelection); modelErr != nil {
		s.writeBadRequest(w, modelErr.Error())
		return
	}

	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		s.writeInternalError(w, "create run failed")
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.ExecContext(
		r.Context(),
		`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, autonomous_approvals, model_selection_json, publish_mode, run_until_all_approve, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at)
		 VALUES (?, NULL, ?, ?, 'pending', 'draft', 0, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		runID,
		req.DecreeId,
		workflowDefinitionID,
		boolToInt(autonomousApprovals),
		modelSelectionJSON,
		publishMode,
		boolToInt(runUntilAllApprove),
		maxExecutionReviewCycles,
		maxValidationReworkCycles,
		now,
		now,
	)
	if err != nil {
		s.writeInternalError(w, "create run failed")
		return
	}

	payloadRaw, err := json.Marshal(map[string]any{
		"run_id":        runID,
		"status":        "pending",
		"display_state": string(gen.RunDisplayStatePending),
		"plan_state":    "draft",
		"created_at":    now,
		"updated_at":    now,
	})
	if err != nil {
		s.writeInternalError(w, "encode run collection payload failed")
		return
	}
	if err := state.AppendRunCollectionEventTx(r.Context(), tx, state.RunCollectionEventAppendRequest{
		EventID:     s.idGenerator("collection_evt"),
		RunID:       runID,
		EventType:   state.RunCollectionEventTypeCreated,
		PayloadJSON: string(payloadRaw),
		CreatedAt:   now,
	}); err != nil {
		s.writeInternalError(w, "create run failed")
		return
	}
	if err := tx.Commit(); err != nil {
		s.writeInternalError(w, "create run failed")
		return
	}
	committed = true

	writeJSON(w, http.StatusCreated, gen.RunSummary{
		RunId:                     runID,
		WorkflowId:                workflowID,
		DecreeId:                  req.DecreeId,
		DecreeTitle:               decreeTitle,
		AutonomousApprovals:       &autonomousApprovals,
		Status:                    "pending",
		DisplayState:              gen.RunDisplayStatePending,
		PlanState:                 "draft",
		CreatedAt:                 mustParseRFC3339(now),
		LastActivityAt:            mustParseRFC3339(now),
		UpdatedAt:                 mustParseRFC3339(now),
		RunUntilAllApprove:        &runUntilAllApprove,
		MaxExecutionReviewCycles:  &maxExecutionReviewCycles,
		MaxValidationReworkCycles: &maxValidationReworkCycles,
		PublishMode:               refRunPublishMode(gen.RunPublishMode(publishMode)),
		ModelSelection:            normalizedModelSelection,
	})
}

func resolveCreateRunLoopSettings(definition workflow.Definition, req gen.CreateRunRequest) (workflow.LoopSettings, error) {
	if req.MaxExecutionReviewCycles != nil {
		if *req.MaxExecutionReviewCycles < 0 {
			return workflow.LoopSettings{}, fmt.Errorf("max_execution_review_cycles must be greater than or equal to zero when provided")
		}
	}
	if req.MaxValidationReworkCycles != nil {
		if *req.MaxValidationReworkCycles < 0 {
			return workflow.LoopSettings{}, fmt.Errorf("max_validation_rework_cycles must be greater than or equal to zero when provided")
		}
	}
	return workflow.ResolveRunLoopSettings(definition, workflow.LoopSettingsOverrides{
		RunUntilAllApprove:        req.RunUntilAllApprove,
		MaxExecutionReviewCycles:  req.MaxExecutionReviewCycles,
		MaxValidationReworkCycles: req.MaxValidationReworkCycles,
	}), nil
}

func refRunPublishMode(mode gen.RunPublishMode) *gen.RunPublishMode {
	return &mode
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Server) persistWorkflowGraph(ctx context.Context, workflowDefinitionID string, rawDefinition []byte, createdAt string) error {
	var definition workflow.Definition
	if err := json.Unmarshal(rawDefinition, &definition); err != nil {
		return fmt.Errorf("decode workflow definition graph: %w", err)
	}

	for _, node := range definition.Nodes {
		if strings.TrimSpace(node.NodeID) == "" {
			continue
		}
		if strings.TrimSpace(node.RequiredRole) == "" {
			continue
		}

		_, err := s.db.ExecContext(
			ctx,
			`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			s.idGenerator("workflow_node"),
			workflowDefinitionID,
			node.NodeID,
			node.RequiredRole,
			node.NodeClass,
			createdAt,
		)
		if err != nil {
			return err
		}
	}

	for _, edge := range definition.Edges {
		onCondition := strings.TrimSpace(edge.On)
		if onCondition == "" {
			onCondition = "success"
		}

		_, err := s.db.ExecContext(
			ctx,
			`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			s.idGenerator("workflow_edge"),
			workflowDefinitionID,
			edge.From,
			edge.To,
			onCondition,
			createdAt,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) decreeExists(ctx context.Context, decreeID string) bool {
	var marker int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM decrees WHERE decree_id = ? LIMIT 1`, decreeID).Scan(&marker)
	return err == nil
}

func (s *Server) normalizeRunModelSelection(ctx context.Context, selection *gen.RunModelSelection) (*gen.RunModelSelection, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	canonicalRoles := make(map[string]struct{}, len(state.CanonicalRoleIDs()))
	for _, roleID := range state.CanonicalRoleIDs() {
		canonicalRoles[strings.TrimSpace(roleID)] = struct{}{}
	}
	if selection == nil {
		return nil, "{}", nil
	}
	items, err := s.providerModelCatalog.List(ctx, false)
	if err != nil {
		return nil, "", err
	}
	modelLookup := make(map[string]gen.ProviderModel, len(items))
	for _, item := range items {
		id := strings.TrimSpace(item.Id)
		if id == "" {
			continue
		}
		modelLookup[id] = item
	}
	normalizeEntry := func(path string, entry *gen.RunModelSelectionEntry) (*gen.RunModelSelectionEntry, error) {
		if entry == nil {
			return nil, nil
		}
		model := strings.TrimSpace(entry.Model)
		if model == "" {
			return nil, fmt.Errorf("%s.model must not be empty", path)
		}
		catalogModel, ok := modelLookup[model]
		if !ok {
			return nil, &runModelSelectionValidationError{message: fmt.Sprintf("%s.model %q is not available for this account", path, model)}
		}
		variant := ""
		if entry.ThinkingEffort != nil {
			variant = strings.TrimSpace(*entry.ThinkingEffort)
		}
		options := make(map[string]struct{}, len(catalogModel.ThinkingEffortOptions))
		for _, option := range catalogModel.ThinkingEffortOptions {
			value := strings.TrimSpace(option.Value)
			if value != "" {
				options[value] = struct{}{}
			}
		}
		if len(options) == 0 {
			if variant != "" {
				return nil, &runModelSelectionValidationError{message: fmt.Sprintf("%s.thinking_effort %q is not supported for model %q", path, variant, model)}
			}
			return &gen.RunModelSelectionEntry{Model: model}, nil
		}
		if variant == "" && catalogModel.ThinkingEffortDefault != nil {
			variant = strings.TrimSpace(*catalogModel.ThinkingEffortDefault)
		}
		if variant == "" {
			for option := range options {
				variant = option
				break
			}
		}
		if _, ok := options[variant]; !ok {
			return nil, &runModelSelectionValidationError{message: fmt.Sprintf("%s.thinking_effort %q is not available for model %q", path, variant, model)}
		}
		return &gen.RunModelSelectionEntry{Model: model, ThinkingEffort: &variant}, nil
	}
	normalized := &gen.RunModelSelection{}
	global, err := normalizeEntry("model_selection.global", &selection.Global)
	if err != nil {
		return nil, "", err
	}
	if global != nil {
		normalized.Global = *global
	}
	if selection.RoleOverrides != nil {
		resolved := make(map[string]gen.RunModelSelectionEntry, len(*selection.RoleOverrides))
		for rawRoleID, entry := range *selection.RoleOverrides {
			roleID := strings.TrimSpace(rawRoleID)
			if roleID == "" {
				return nil, "", fmt.Errorf("model_selection.role_overrides contains an empty role id")
			}
			if _, ok := canonicalRoles[roleID]; !ok {
				return nil, "", fmt.Errorf("model_selection.role_overrides role %q is not recognized", roleID)
			}
			normalizedEntry, entryErr := normalizeEntry("model_selection.role_overrides."+roleID, &entry)
			if entryErr != nil {
				return nil, "", entryErr
			}
			if normalizedEntry != nil {
				resolved[roleID] = *normalizedEntry
			}
		}
		if len(resolved) > 0 {
			normalized.RoleOverrides = &resolved
		}
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return nil, "", fmt.Errorf("encode model_selection: %w", err)
	}
	return normalized, string(raw), nil
}

func decodeRunModelSelection(raw string) (*gen.RunModelSelection, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var decoded gen.RunModelSelection
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return nil, fmt.Errorf("decode run model selection: %w", err)
	}
	if strings.TrimSpace(decoded.Global.Model) == "" && (decoded.RoleOverrides == nil || len(*decoded.RoleOverrides) == 0) {
		return nil, nil
	}
	return &decoded, nil
}

type runModelSelectionValidationError struct {
	message string
}

func (e *runModelSelectionValidationError) Error() string {
	return e.message
}

func isRunModelSelectionValidationError(err error) bool {
	var validationErr *runModelSelectionValidationError
	return errors.As(err, &validationErr)
}

func (s *Server) validateRequiredRunModelSelection(selection *gen.RunModelSelection) error {
	if selection == nil || strings.TrimSpace(selection.Global.Model) == "" {
		return &runModelSelectionValidationError{message: "model_selection.global.model is required"}
	}
	return nil
}

func (s *Server) compileWorkflowDefinition(expectedWorkflowID string, rawDefinition []byte) (*workflow.CompileResult, error) {
	definition, err := workflow.ParseDefinitionJSON(rawDefinition)
	if err != nil {
		return nil, fmt.Errorf("decode workflow definition_json: %w", err)
	}

	if strings.TrimSpace(definition.WorkflowID) == "" {
		return nil, fmt.Errorf("workflow definition_json.workflow_id is required")
	}
	if definition.WorkflowID != expectedWorkflowID {
		return nil, fmt.Errorf("workflow_id mismatch between request and definition_json")
	}

	rawYAML, err := yaml.Marshal(definition)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow definition yaml: %w", err)
	}

	result, err := s.workflowCompiler.CompileYAML(rawYAML)
	if err != nil {
		return nil, err
	}
	return result, nil
}
