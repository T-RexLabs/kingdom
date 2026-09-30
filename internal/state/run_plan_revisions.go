package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/workflow"
	"kingdom/internal/nodeattempt"
)

type RunPlanRevision struct {
	PlanRevisionSeq int64
	PlanRevisionID  string
	RunID           string
	ContentMarkdown string
	Components      []RunPlanRevisionComponent
	CreatedAt       string
}

type RunPlanRevisionComponent struct {
	NodeID           string                              `json:"node_id"`
	PlanningOutputID string                              `json:"planning_output_id"`
	NodeRunID        string                              `json:"node_run_id"`
	AttemptID        string                              `json:"attempt_id"`
	AgentID          string                              `json:"agent_id"`
	CreatedAt        string                              `json:"created_at"`
	ContentMarkdown  string                              `json:"content_markdown"`
	ContentRef       *providerexecution.ContentReference `json:"content_ref,omitempty"`
}

type CreateRunPlanRevisionRequest struct {
	PlanRevisionID  string
	RunID           string
	ContentMarkdown string
	Components      []RunPlanRevisionComponent
	CreatedAt       string
}

type RunPlanRevisionStore struct {
	db *sql.DB
}

func NewRunPlanRevisionStore(db *sql.DB) *RunPlanRevisionStore {
	return &RunPlanRevisionStore{db: db}
}

func (s *RunPlanRevisionStore) LatestRunPlanRevision(ctx context.Context, runID string) (RunPlanRevision, bool, error) {
	items, err := s.ListRunPlanRevisions(ctx, runID, 1)
	if err != nil {
		return RunPlanRevision{}, false, err
	}
	if len(items) == 0 {
		return RunPlanRevision{}, false, nil
	}
	return items[0], true, nil
}

func (s *RunPlanRevisionStore) PreviousRunPlanRevision(ctx context.Context, runID, excludePlanRevisionID string) (RunPlanRevision, bool, error) {
	if s == nil || s.db == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision store is required")
	}
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunPlanRevision{}, false, fmt.Errorf("run id is required")
	}
	trimmedExcludedID := strings.TrimSpace(excludePlanRevisionID)
	if trimmedExcludedID == "" {
		return RunPlanRevision{}, false, fmt.Errorf("excluded plan revision id is required")
	}

	var revision RunPlanRevision
	err := scanRunPlanRevision(s.db.QueryRowContext(ctx,
		`SELECT plan_revision_seq, plan_revision_id, run_id, content_markdown, components_json, created_at
		 FROM run_plan_revisions
		 WHERE run_id = ?
		   AND plan_revision_id <> ?
		 ORDER BY plan_revision_seq DESC
		 LIMIT 1`,
		trimmedRunID,
		trimmedExcludedID,
	), &revision)
	if err == sql.ErrNoRows {
		return RunPlanRevision{}, false, nil
	}
	if err != nil {
		return RunPlanRevision{}, false, fmt.Errorf("load previous run plan revision: %w", err)
	}
	return revision, true, nil
}

func (s *RunPlanRevisionStore) RunPlanRevision(ctx context.Context, planRevisionID string) (RunPlanRevision, bool, error) {
	if s == nil || s.db == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision store is required")
	}
	trimmedPlanRevisionID := strings.TrimSpace(planRevisionID)
	if trimmedPlanRevisionID == "" {
		return RunPlanRevision{}, false, fmt.Errorf("plan revision id is required")
	}
	var revision RunPlanRevision
	if err := scanRunPlanRevision(s.db.QueryRowContext(ctx,
		`SELECT plan_revision_seq, plan_revision_id, run_id, content_markdown, components_json, created_at
		 FROM run_plan_revisions
		 WHERE plan_revision_id = ?
		 LIMIT 1`,
		trimmedPlanRevisionID,
	), &revision); err != nil {
		if err == sql.ErrNoRows {
			return RunPlanRevision{}, false, nil
		}
		return RunPlanRevision{}, false, fmt.Errorf("load run plan revision %q: %w", trimmedPlanRevisionID, err)
	}
	return revision, true, nil
}

func (s *RunPlanRevisionStore) ListRunPlanRevisions(ctx context.Context, runID string, limit int) ([]RunPlanRevision, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if limit <= 0 {
		limit = 1
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT plan_revision_seq, plan_revision_id, run_id, content_markdown, components_json, created_at
		 FROM run_plan_revisions
		 WHERE run_id = ?
		 ORDER BY plan_revision_seq DESC
		 LIMIT ?`,
		trimmedRunID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list run plan revisions: %w", err)
	}
	defer rows.Close()

	items := make([]RunPlanRevision, 0, limit)
	for rows.Next() {
		var item RunPlanRevision
		if err := scanRunPlanRevision(rows, &item); err != nil {
			return nil, fmt.Errorf("scan run plan revision: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run plan revisions: %w", err)
	}
	return items, nil
}

func (s *RunPlanRevisionStore) CurrentRunPlanRevision(ctx context.Context, runID string) (RunPlanRevision, bool, error) {
	if s == nil || s.db == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision store is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RunPlanRevision{}, false, fmt.Errorf("begin current run plan revision transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return s.currentRunPlanRevisionTx(ctx, tx, runID)
}

func (s *RunPlanRevisionStore) RefreshCurrentRunPlanRevision(ctx context.Context, runID string) (RunPlanRevision, bool, error) {
	if s == nil || s.db == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision store is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunPlanRevision{}, false, fmt.Errorf("begin run plan revision refresh transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	revision, ok, err := s.refreshCurrentRunPlanRevisionTx(ctx, tx, runID)
	if err != nil {
		return RunPlanRevision{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return RunPlanRevision{}, false, fmt.Errorf("commit run plan revision refresh transaction: %w", err)
	}
	committed = true
	return revision, ok, nil
}

func (s *RunPlanRevisionStore) currentRunPlanRevisionTx(ctx context.Context, tx *sql.Tx, runID string) (RunPlanRevision, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if tx == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision transaction is required")
	}
	if trimmedRunID == "" {
		return RunPlanRevision{}, false, fmt.Errorf("run id is required")
	}
	candidate, ok, err := buildCurrentRunPlanRevisionTx(ctx, tx, trimmedRunID)
	if err != nil || !ok {
		return RunPlanRevision{}, ok, err
	}
	return runPlanRevisionFromCreateRequest(candidate), true, nil
}

func (s *RunPlanRevisionStore) refreshCurrentRunPlanRevisionTx(ctx context.Context, tx *sql.Tx, runID string) (RunPlanRevision, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if tx == nil {
		return RunPlanRevision{}, false, fmt.Errorf("run plan revision transaction is required")
	}
	if trimmedRunID == "" {
		return RunPlanRevision{}, false, fmt.Errorf("run id is required")
	}
	candidate, ok, err := buildCurrentRunPlanRevisionTx(ctx, tx, trimmedRunID)
	if err != nil || !ok {
		return RunPlanRevision{}, ok, err
	}
	revision, err := upsertRunPlanRevisionTx(ctx, tx, candidate)
	if err != nil {
		return RunPlanRevision{}, false, err
	}
	return revision, true, nil
}

func buildCurrentRunPlanRevisionTx(ctx context.Context, tx *sql.Tx, runID string) (CreateRunPlanRevisionRequest, bool, error) {
	nodeIDs, err := planDraftingNodeIDsTx(ctx, tx, runID)
	if err != nil {
		return CreateRunPlanRevisionRequest{}, false, err
	}
	if len(nodeIDs) == 0 {
		return CreateRunPlanRevisionRequest{}, false, nil
	}

	components := make([]RunPlanRevisionComponent, 0, len(nodeIDs))
	latestCreatedAt := ""
	componentIDs := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		latestNodeRunID, status, err := latestNodeRunForPlanRevisionTx(ctx, tx, runID, nodeID)
		if err != nil {
			return CreateRunPlanRevisionRequest{}, false, err
		}
		if latestNodeRunID == "" || status != "succeeded" {
			return CreateRunPlanRevisionRequest{}, false, nil
		}

		output, ok, err := latestRunPlanningOutputForNodeTx(ctx, tx, runID, nodeID, RunPlanningOutputKindPlanRevision)
		if err != nil {
			return CreateRunPlanRevisionRequest{}, false, err
		}
		if !ok || strings.TrimSpace(output.NodeRunID) != latestNodeRunID {
			return CreateRunPlanRevisionRequest{}, false, nil
		}

		components = append(components, RunPlanRevisionComponent{
			NodeID:           output.NodeID,
			PlanningOutputID: output.PlanningOutputID,
			NodeRunID:        output.NodeRunID,
			AttemptID:        output.AttemptID,
			AgentID:          output.AgentID,
			CreatedAt:        output.CreatedAt,
			ContentMarkdown:  output.ContentMarkdown,
			ContentRef:       output.ContentRef,
		})
		componentIDs = append(componentIDs, output.PlanningOutputID)
		if latestCreatedAt == "" || strings.TrimSpace(output.CreatedAt) > latestCreatedAt {
			latestCreatedAt = strings.TrimSpace(output.CreatedAt)
		}
	}
	if len(components) == 0 {
		return CreateRunPlanRevisionRequest{}, false, nil
	}

	return CreateRunPlanRevisionRequest{
		PlanRevisionID:  runPlanRevisionID(runID, componentIDs),
		RunID:           runID,
		ContentMarkdown: composeRunPlanRevisionContent(components),
		Components:      components,
		CreatedAt:       latestCreatedAt,
	}, true, nil
}

func upsertRunPlanRevisionTx(ctx context.Context, tx *sql.Tx, req CreateRunPlanRevisionRequest) (RunPlanRevision, error) {
	if err := validateRunPlanRevisionCreate(req); err != nil {
		return RunPlanRevision{}, err
	}
	candidate := runPlanRevisionFromCreateRequest(req)
	existing, ok, err := loadRunPlanRevisionRowTx(ctx, tx, candidate.PlanRevisionID)
	if err != nil {
		return RunPlanRevision{}, err
	}
	if ok {
		if !sameRunPlanRevisionProjection(existing, candidate) {
			return RunPlanRevision{}, fmt.Errorf("run plan revision %q drifted from canonical planning outputs", candidate.PlanRevisionID)
		}
		return existing, nil
	}
	componentsJSON, err := json.Marshal(req.Components)
	if err != nil {
		return RunPlanRevision{}, fmt.Errorf("encode run plan revision components: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO run_plan_revisions (plan_revision_id, run_id, content_markdown, components_json, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		strings.TrimSpace(req.PlanRevisionID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.ContentMarkdown),
		string(componentsJSON),
		strings.TrimSpace(req.CreatedAt),
	); err != nil {
		if IsUniqueViolation(err) {
			existing, ok, loadErr := loadRunPlanRevisionRowTx(ctx, tx, candidate.PlanRevisionID)
			if loadErr != nil {
				return RunPlanRevision{}, loadErr
			}
			if !ok {
				return RunPlanRevision{}, fmt.Errorf("run plan revision %q conflicted without a persisted row", candidate.PlanRevisionID)
			}
			if !sameRunPlanRevisionProjection(existing, candidate) {
				return RunPlanRevision{}, fmt.Errorf("run plan revision %q drifted from canonical planning outputs", candidate.PlanRevisionID)
			}
			return existing, nil
		}
		return RunPlanRevision{}, fmt.Errorf("insert run plan revision: %w", err)
	}
	return loadRunPlanRevisionTx(ctx, tx, strings.TrimSpace(req.PlanRevisionID))
}

func loadRunPlanRevisionTx(ctx context.Context, tx *sql.Tx, planRevisionID string) (RunPlanRevision, error) {
	revision, ok, err := loadRunPlanRevisionRowTx(ctx, tx, planRevisionID)
	if err != nil {
		return RunPlanRevision{}, err
	}
	if !ok {
		return RunPlanRevision{}, fmt.Errorf("run plan revision %q not found", strings.TrimSpace(planRevisionID))
	}
	return revision, nil
}

func loadRunPlanRevisionRowTx(ctx context.Context, tx *sql.Tx, planRevisionID string) (RunPlanRevision, bool, error) {
	var revision RunPlanRevision
	if err := scanRunPlanRevision(tx.QueryRowContext(ctx,
		`SELECT plan_revision_seq, plan_revision_id, run_id, content_markdown, components_json, created_at
		 FROM run_plan_revisions
		 WHERE plan_revision_id = ?
		 LIMIT 1`,
		strings.TrimSpace(planRevisionID),
	), &revision); err != nil {
		if err == sql.ErrNoRows {
			return RunPlanRevision{}, false, nil
		}
		return RunPlanRevision{}, false, fmt.Errorf("load run plan revision %q: %w", strings.TrimSpace(planRevisionID), err)
	}
	return revision, true, nil
}

func validateRunPlanRevisionCreate(req CreateRunPlanRevisionRequest) error {
	if referencedPlanComponents(req.Components) && req.ContentMarkdown != "" {
		return fmt.Errorf("referenced plan revision must not repeat composed content inline")
	}
	if strings.TrimSpace(req.PlanRevisionID) == "" {
		return fmt.Errorf("plan revision id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.ContentMarkdown) == "" && !referencedPlanComponents(req.Components) {
		return fmt.Errorf("content markdown is required")
	}
	if len(req.Components) == 0 {
		return fmt.Errorf("at least one run plan revision component is required")
	}
	for idx, component := range req.Components {
		if strings.TrimSpace(component.NodeID) == "" {
			return fmt.Errorf("run plan revision component %d node id is required", idx)
		}
		if strings.TrimSpace(component.PlanningOutputID) == "" {
			return fmt.Errorf("run plan revision component %d planning output id is required", idx)
		}
		if strings.TrimSpace(component.NodeRunID) == "" {
			return fmt.Errorf("run plan revision component %d node run id is required", idx)
		}
		if strings.TrimSpace(component.AttemptID) == "" {
			return fmt.Errorf("run plan revision component %d attempt id is required", idx)
		}
		if strings.TrimSpace(component.AgentID) == "" {
			return fmt.Errorf("run plan revision component %d agent id is required", idx)
		}
		if strings.TrimSpace(component.CreatedAt) == "" {
			return fmt.Errorf("run plan revision component %d created_at is required", idx)
		}
		if component.ContentRef != nil {
			if err := component.ContentRef.Validate(component.ContentRef.InvocationID); err != nil {
				return err
			}
			if component.ContentMarkdown != "" || component.ContentRef.SizeBytes <= 0 {
				return fmt.Errorf("plan component content authority is invalid")
			}
		} else if strings.TrimSpace(component.ContentMarkdown) == "" {
			return fmt.Errorf("run plan revision component %d content markdown is required", idx)
		}
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("created_at is required")
	}
	return nil
}

func scanRunPlanRevision(scanner interface{ Scan(dest ...any) error }, target *RunPlanRevision) error {
	if target == nil {
		return fmt.Errorf("run plan revision target is required")
	}
	var componentsJSON string
	if err := scanner.Scan(
		&target.PlanRevisionSeq,
		&target.PlanRevisionID,
		&target.RunID,
		&target.ContentMarkdown,
		&componentsJSON,
		&target.CreatedAt,
	); err != nil {
		return err
	}
	target.Components = nil
	trimmedComponentsJSON := strings.TrimSpace(componentsJSON)
	if trimmedComponentsJSON == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(trimmedComponentsJSON), &target.Components); err != nil {
		return fmt.Errorf("decode run plan revision components: %w", err)
	}
	return nil
}

func latestNodeRunForPlanRevisionTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, string, error) {
	var nodeRunID string
	var status string
	query := fmt.Sprintf(`SELECT node_run_id, node_status
		 FROM node_runs
		 WHERE run_id = ? AND node_id = ?
		 ORDER BY %s
		 LIMIT 1`, nodeattempt.CanonicalNodeAttemptOrderSQL("node_runs"))
	err := tx.QueryRowContext(ctx, query, runID, nodeID).Scan(&nodeRunID, &status)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("load latest plan node run run=%q node=%q: %w", runID, nodeID, err)
	}
	return strings.TrimSpace(nodeRunID), strings.TrimSpace(status), nil
}

func planDraftingNodeIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	var definitionJSON string
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(wd.definition_json, '')
		 FROM workflow_runs wr
		 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
		 WHERE wr.run_id = ?`,
		runID,
	).Scan(&definitionJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("load workflow definition for run %q: %w", runID, err)
	}
	trimmedDefinitionJSON := strings.TrimSpace(definitionJSON)
	if trimmedDefinitionJSON != "" {
		definition, parseErr := workflow.ParseDefinitionJSON([]byte(trimmedDefinitionJSON))
		if parseErr == nil {
			nodeIDs := make([]string, 0)
			for _, node := range definition.Nodes {
				if strings.TrimSpace(node.NodeClass) != workflow.NodeClassPlanDrafting {
					continue
				}
				trimmedNodeID := strings.TrimSpace(node.NodeID)
				if trimmedNodeID == "" {
					continue
				}
				nodeIDs = append(nodeIDs, trimmedNodeID)
			}
			if len(nodeIDs) > 0 {
				return nodeIDs, nil
			}
		}
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT wn.node_id
		 FROM workflow_runs wr
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
		 WHERE wr.run_id = ?
		   AND wn.node_class = 'plan_drafting'
		 ORDER BY wn.created_at ASC, wn.node_id ASC`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list plan drafting nodes for run %q: %w", runID, err)
	}
	defer rows.Close()

	nodeIDs := make([]string, 0)
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scan plan drafting node for run %q: %w", runID, err)
		}
		trimmedNodeID := strings.TrimSpace(nodeID)
		if trimmedNodeID == "" {
			continue
		}
		nodeIDs = append(nodeIDs, trimmedNodeID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate plan drafting nodes for run %q: %w", runID, err)
	}
	return nodeIDs, nil
}

func latestRunPlanningOutputForNodeTx(ctx context.Context, tx *sql.Tx, runID, nodeID string, kind RunPlanningOutputKind) (RunPlanningOutput, bool, error) {
	var item RunPlanningOutput
	err := scanRunPlanningOutput(tx.QueryRowContext(ctx,
		`SELECT planning_output_seq, planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at,content_ref_json
		 FROM run_planning_outputs
		 WHERE run_id = ? AND node_id = ? AND kind = ?
		 ORDER BY planning_output_seq DESC
		 LIMIT 1`,
		runID,
		nodeID,
		string(kind),
	), &item)
	if err == sql.ErrNoRows {
		return RunPlanningOutput{}, false, nil
	}
	if err != nil {
		return RunPlanningOutput{}, false, fmt.Errorf("load latest run planning output run=%q node=%q kind=%q: %w", runID, nodeID, kind, err)
	}
	return item, true, nil
}

func composeRunPlanRevisionContent(components []RunPlanRevisionComponent) string {
	if referencedPlanComponents(components) {
		return ""
	}
	if len(components) == 0 {
		return ""
	}
	if len(components) == 1 {
		return strings.TrimSpace(components[0].ContentMarkdown)
	}
	sections := make([]string, 0, len(components))
	for _, component := range components {
		sections = append(sections, fmt.Sprintf("## %s\n\n%s", strings.TrimSpace(component.NodeID), strings.TrimSpace(component.ContentMarkdown)))
	}
	return strings.Join(sections, "\n\n")
}

func runPlanRevisionFromCreateRequest(req CreateRunPlanRevisionRequest) RunPlanRevision {
	components := append([]RunPlanRevisionComponent(nil), req.Components...)
	return RunPlanRevision{
		PlanRevisionID:  strings.TrimSpace(req.PlanRevisionID),
		RunID:           strings.TrimSpace(req.RunID),
		ContentMarkdown: strings.TrimSpace(req.ContentMarkdown),
		Components:      components,
		CreatedAt:       strings.TrimSpace(req.CreatedAt),
	}
}

func sameRunPlanRevisionProjection(existing, candidate RunPlanRevision) bool {
	return strings.TrimSpace(existing.PlanRevisionID) == strings.TrimSpace(candidate.PlanRevisionID) &&
		strings.TrimSpace(existing.RunID) == strings.TrimSpace(candidate.RunID) &&
		strings.TrimSpace(existing.ContentMarkdown) == strings.TrimSpace(candidate.ContentMarkdown) &&
		strings.TrimSpace(existing.CreatedAt) == strings.TrimSpace(candidate.CreatedAt) &&
		slices.EqualFunc(existing.Components, candidate.Components, func(a, b RunPlanRevisionComponent) bool {
			return a.NodeID == b.NodeID && a.PlanningOutputID == b.PlanningOutputID && a.NodeRunID == b.NodeRunID && a.AttemptID == b.AttemptID && a.AgentID == b.AgentID && a.CreatedAt == b.CreatedAt && a.ContentMarkdown == b.ContentMarkdown && equalPlanningContentRef(a.ContentRef, b.ContentRef)
		})
}

func referencedPlanComponents(components []RunPlanRevisionComponent) bool {
	for _, component := range components {
		if component.ContentRef != nil {
			return true
		}
	}
	return false
}

func (s *RunPlanRevisionStore) CopyContent(ctx context.Context, revision RunPlanRevision, dst io.Writer) error {
	if !referencedPlanComponents(revision.Components) {
		_, err := io.CopyBuffer(dst, struct{ io.Reader }{strings.NewReader(revision.ContentMarkdown)}, make([]byte, 64*1024))
		return err
	}
	if revision.ContentMarkdown != "" {
		return fmt.Errorf("run plan revision has conflicting content authorities")
	}
	for index, component := range revision.Components {
		if index > 0 {
			if _, err := io.WriteString(dst, "\n\n"); err != nil {
				return err
			}
		}
		if len(revision.Components) > 1 {
			if _, err := fmt.Fprintf(dst, "## %s\n\n", strings.TrimSpace(component.NodeID)); err != nil {
				return err
			}
		}
		reader, err := NewRunPlanningOutputStore(s.db).OpenContent(ctx, RunPlanningOutput{ContentMarkdown: component.ContentMarkdown, ContentRef: component.ContentRef})
		if err != nil {
			return err
		}
		_, copyErr := io.CopyBuffer(dst, struct{ io.Reader }{reader}, make([]byte, 64*1024))
		closeErr := reader.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (s *RunPlanRevisionStore) OpenContent(ctx context.Context, revision RunPlanRevision) (io.ReadCloser, error) {
	return providerexecution.OpenContentCopy(ctx, func(ctx context.Context, dst io.Writer) error { return s.CopyContent(ctx, revision, dst) }), nil
}

func runPlanRevisionID(runID string, componentIDs []string) string {
	payload := strings.TrimSpace(runID) + "\n" + strings.Join(componentIDs, "\n")
	sum := sha256.Sum256([]byte(payload))
	return "planrev_" + hex.EncodeToString(sum[:12])
}
