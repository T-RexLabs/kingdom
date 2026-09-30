package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
)

type RunPlanningOutputKind string

const (
	RunPlanningOutputKindAdvisoryGuidance RunPlanningOutputKind = "advisory_guidance"
	RunPlanningOutputKindPlanRevision     RunPlanningOutputKind = "plan_revision"
)

type RunPlanningOutput struct {
	PlanningOutputID  string
	PlanningOutputSeq int64
	RunID             string
	NodeID            string
	NodeRunID         string
	AttemptID         string
	AgentID           string
	SourceMessageID   string
	Kind              RunPlanningOutputKind
	ContentMarkdown   string
	ContentRef        *providerexecution.ContentReference
	CreatedAt         string
}

type CreateRunPlanningOutputRequest struct {
	PlanningOutputID string
	RunID            string
	NodeID           string
	NodeRunID        string
	AttemptID        string
	AgentID          string
	SourceMessageID  string
	Kind             RunPlanningOutputKind
	ContentMarkdown  string
	ContentRef       *providerexecution.ContentReference
	CreatedAt        string
}

type RunPlanningOutputStore struct {
	db *sql.DB
}

func NewRunPlanningOutputStore(db *sql.DB) *RunPlanningOutputStore {
	return &RunPlanningOutputStore{db: db}
}

func (s *RunPlanningOutputStore) LatestRunPlanningOutput(ctx context.Context, runID string, kind RunPlanningOutputKind) (RunPlanningOutput, bool, error) {
	items, err := s.ListRunPlanningOutputs(ctx, runID, kind, "", 1)
	if err != nil {
		return RunPlanningOutput{}, false, err
	}
	if len(items) == 0 {
		return RunPlanningOutput{}, false, nil
	}
	return items[0], true, nil
}

func (s *RunPlanningOutputStore) ListRunPlanningOutputs(ctx context.Context, runID string, kind RunPlanningOutputKind, agentID string, limit int) ([]RunPlanningOutput, error) {
	trimmedRunID := strings.TrimSpace(runID)
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if !isSupportedRunPlanningOutputKind(kind) {
		return nil, fmt.Errorf("run planning output kind is required")
	}
	if limit <= 0 {
		limit = 1
	}

	query := `SELECT planning_output_seq, planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at, content_ref_json
		FROM run_planning_outputs
		WHERE run_id = ?
		  AND kind = ?`
	args := []any{trimmedRunID, string(kind)}
	if trimmedAgentID != "" {
		query += `
		  AND agent_id = ?`
		args = append(args, trimmedAgentID)
	}
	query += `
		ORDER BY planning_output_seq DESC
		LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list run planning outputs: %w", err)
	}
	defer rows.Close()

	items := make([]RunPlanningOutput, 0, limit)
	for rows.Next() {
		var item RunPlanningOutput
		if err := scanRunPlanningOutput(rows, &item); err != nil {
			return nil, fmt.Errorf("scan run planning output: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run planning outputs: %w", err)
	}
	return items, nil
}

func insertRunPlanningOutput(ctx context.Context, exec sqlExecutor, req CreateRunPlanningOutputRequest) (RunPlanningOutput, error) {
	if err := validateRunPlanningOutputCreate(req); err != nil {
		return RunPlanningOutput{}, err
	}
	row := RunPlanningOutput{
		PlanningOutputID: strings.TrimSpace(req.PlanningOutputID),
		RunID:            strings.TrimSpace(req.RunID),
		NodeID:           strings.TrimSpace(req.NodeID),
		NodeRunID:        strings.TrimSpace(req.NodeRunID),
		AttemptID:        strings.TrimSpace(req.AttemptID),
		AgentID:          strings.TrimSpace(req.AgentID),
		SourceMessageID:  strings.TrimSpace(req.SourceMessageID),
		Kind:             req.Kind,
		ContentMarkdown:  strings.TrimSpace(req.ContentMarkdown),
		ContentRef:       req.ContentRef,
		CreatedAt:        strings.TrimSpace(req.CreatedAt),
	}
	refJSON := ""
	if row.ContentRef != nil {
		raw, _ := json.Marshal(row.ContentRef)
		refJSON = string(raw)
	}
	result, err := exec.ExecContext(ctx,
		`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at,content_ref_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PlanningOutputID,
		row.RunID,
		row.NodeID,
		row.NodeRunID,
		row.AttemptID,
		row.AgentID,
		row.SourceMessageID,
		string(row.Kind),
		row.ContentMarkdown,
		row.CreatedAt,
		refJSON,
	)
	if err != nil {
		return RunPlanningOutput{}, fmt.Errorf("insert run planning output: %w", err)
	}
	seq, err := result.LastInsertId()
	if err == nil {
		row.PlanningOutputSeq = seq
	}
	return row, nil
}

func persistRunPlanningOutputTx(ctx context.Context, tx *sql.Tx, db *sql.DB, req CreateRunPlanningOutputRequest) error {
	if tx == nil {
		return fmt.Errorf("run planning output transaction is required")
	}
	if db == nil {
		return fmt.Errorf("run planning output database is required")
	}
	if err := validateRunPlanningOutputLineageTx(ctx, tx, req); err != nil {
		return err
	}

	output, err := insertRunPlanningOutput(ctx, tx, req)
	if err != nil {
		if !IsUniqueViolation(err) {
			return err
		}
		existing, found, loadErr := loadRunPlanningOutputByIDTx(ctx, tx, req.PlanningOutputID)
		if loadErr != nil {
			return loadErr
		}
		if !found {
			return err
		}
		if !runPlanningOutputMatchesRequest(existing, req) {
			return fmt.Errorf("planning output %q already exists with different content", strings.TrimSpace(req.PlanningOutputID))
		}
		output = existing
	}

	if output.Kind != RunPlanningOutputKindPlanRevision {
		return nil
	}
	_, _, err = NewRunPlanRevisionStore(db).refreshCurrentRunPlanRevisionTx(ctx, tx, output.RunID)
	return err
}

func loadRunPlanningOutputByIDTx(ctx context.Context, tx *sql.Tx, planningOutputID string) (RunPlanningOutput, bool, error) {
	if tx == nil {
		return RunPlanningOutput{}, false, fmt.Errorf("run planning output transaction is required")
	}
	var item RunPlanningOutput
	err := scanRunPlanningOutput(tx.QueryRowContext(ctx,
		`SELECT planning_output_seq, planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at,content_ref_json
		 FROM run_planning_outputs
		 WHERE planning_output_id = ?`,
		strings.TrimSpace(planningOutputID),
	), &item)
	if err == sql.ErrNoRows {
		return RunPlanningOutput{}, false, nil
	}
	if err != nil {
		return RunPlanningOutput{}, false, fmt.Errorf("load run planning output %q: %w", strings.TrimSpace(planningOutputID), err)
	}
	return item, true, nil
}

func runPlanningOutputMatchesRequest(output RunPlanningOutput, req CreateRunPlanningOutputRequest) bool {
	return strings.TrimSpace(output.PlanningOutputID) == strings.TrimSpace(req.PlanningOutputID) &&
		strings.TrimSpace(output.RunID) == strings.TrimSpace(req.RunID) &&
		strings.TrimSpace(output.NodeID) == strings.TrimSpace(req.NodeID) &&
		strings.TrimSpace(output.NodeRunID) == strings.TrimSpace(req.NodeRunID) &&
		strings.TrimSpace(output.AttemptID) == strings.TrimSpace(req.AttemptID) &&
		strings.TrimSpace(output.AgentID) == strings.TrimSpace(req.AgentID) &&
		strings.TrimSpace(output.SourceMessageID) == strings.TrimSpace(req.SourceMessageID) &&
		output.Kind == req.Kind &&
		strings.TrimSpace(output.ContentMarkdown) == strings.TrimSpace(req.ContentMarkdown) &&
		equalPlanningContentRef(output.ContentRef, req.ContentRef) &&
		strings.TrimSpace(output.CreatedAt) == strings.TrimSpace(req.CreatedAt)
}

func validateRunPlanningOutputLineageTx(ctx context.Context, tx *sql.Tx, req CreateRunPlanningOutputRequest) error {
	if tx == nil {
		return fmt.Errorf("run planning output transaction is required")
	}
	if err := validateRunPlanningOutputCreate(req); err != nil {
		return err
	}

	var persistedRunID string
	var persistedNodeID string
	var persistedAgentID string
	var nodeClass string
	err := tx.QueryRowContext(ctx,
		`SELECT nr.run_id, nr.node_id, nr.agent_id, wn.node_class
		 FROM node_runs nr
		 JOIN workflow_runs wr ON wr.run_id = nr.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE nr.node_run_id = ?
		 LIMIT 1`,
		strings.TrimSpace(req.NodeRunID),
	).Scan(&persistedRunID, &persistedNodeID, &persistedAgentID, &nodeClass)
	if err == sql.ErrNoRows {
		return fmt.Errorf("planning output node run %q not found", strings.TrimSpace(req.NodeRunID))
	}
	if err != nil {
		return fmt.Errorf("load planning output lineage for node run %q: %w", strings.TrimSpace(req.NodeRunID), err)
	}
	if strings.TrimSpace(req.RunID) != strings.TrimSpace(persistedRunID) {
		return fmt.Errorf("planning output run id must match node run lineage")
	}
	if strings.TrimSpace(req.NodeID) != strings.TrimSpace(persistedNodeID) {
		return fmt.Errorf("planning output node id must match node run lineage")
	}
	if strings.TrimSpace(req.AgentID) != strings.TrimSpace(persistedAgentID) {
		return fmt.Errorf("planning output agent id must match node run lineage")
	}
	var persistedAttemptID string
	err = tx.QueryRowContext(ctx,
		`SELECT attempt_id
		 FROM attempt_executions
		 WHERE attempt_id = ?
		   AND run_id = ?
		   AND node_run_id = ?
		 LIMIT 1`,
		strings.TrimSpace(req.AttemptID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeRunID),
	).Scan(&persistedAttemptID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("planning output attempt id must match node run lineage")
	}
	if err != nil {
		return fmt.Errorf("load planning output attempt lineage for node run %q: %w", strings.TrimSpace(req.NodeRunID), err)
	}

	expectedNodeClass := ""
	switch req.Kind {
	case RunPlanningOutputKindAdvisoryGuidance:
		expectedNodeClass = "advisory_synthesis"
	case RunPlanningOutputKindPlanRevision:
		expectedNodeClass = "plan_drafting"
	}
	if expectedNodeClass != "" && strings.TrimSpace(nodeClass) != expectedNodeClass {
		return fmt.Errorf("planning output kind %q requires node class %q", req.Kind, expectedNodeClass)
	}
	if req.ContentRef != nil {
		var raw string
		if err := tx.QueryRowContext(ctx, `SELECT pcs.final_text_ref_json FROM attempt_executions ae
		 JOIN provider_completion_settlements pcs ON pcs.settlement_id=ae.canonical_settlement_id
		 WHERE ae.attempt_id=? AND ae.run_id=? AND ae.node_run_id=? AND ae.state='committed_success' AND pcs.outcome='success'`, req.AttemptID, req.RunID, req.NodeRunID).Scan(&raw); err != nil {
			return fmt.Errorf("load canonical planning content: %w", err)
		}
		var ref providerexecution.ContentReference
		if err := json.Unmarshal([]byte(raw), &ref); err != nil {
			return err
		}
		if ref != *req.ContentRef {
			return fmt.Errorf("planning content does not match canonical settlement")
		}
	}

	trimmedSourceMessageID := strings.TrimSpace(req.SourceMessageID)
	if trimmedSourceMessageID == "" {
		return nil
	}
	var sourceMessageRunID string
	err = tx.QueryRowContext(ctx,
		`SELECT run_id
		 FROM mailbox_messages
		 WHERE message_id = ?
		 LIMIT 1`,
		trimmedSourceMessageID,
	).Scan(&sourceMessageRunID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("planning output source message %q not found", trimmedSourceMessageID)
	}
	if err != nil {
		return fmt.Errorf("load planning output source message %q: %w", trimmedSourceMessageID, err)
	}
	if strings.TrimSpace(sourceMessageRunID) != strings.TrimSpace(req.RunID) {
		return fmt.Errorf("planning output source message must belong to the same run")
	}
	return nil
}

func validateRunPlanningOutputCreate(req CreateRunPlanningOutputRequest) error {
	if strings.TrimSpace(req.PlanningOutputID) == "" {
		return fmt.Errorf("planning output id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.NodeID) == "" {
		return fmt.Errorf("node id is required")
	}
	if strings.TrimSpace(req.NodeRunID) == "" {
		return fmt.Errorf("node run id is required")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("attempt id is required")
	}
	if strings.TrimSpace(req.AgentID) == "" {
		return fmt.Errorf("agent id is required")
	}
	if !isSupportedRunPlanningOutputKind(req.Kind) {
		return fmt.Errorf("unsupported run planning output kind %q", req.Kind)
	}
	if req.ContentRef != nil {
		if err := req.ContentRef.Validate(req.ContentRef.InvocationID); err != nil {
			return err
		}
		if req.ContentMarkdown != "" || req.ContentRef.SizeBytes <= 0 {
			return fmt.Errorf("referenced planning output must have nonempty canonical content and no inline duplicate")
		}
	} else if strings.TrimSpace(req.ContentMarkdown) == "" {
		return fmt.Errorf("content markdown is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("created_at is required")
	}
	return nil
}

func isSupportedRunPlanningOutputKind(kind RunPlanningOutputKind) bool {
	switch kind {
	case RunPlanningOutputKindAdvisoryGuidance, RunPlanningOutputKindPlanRevision:
		return true
	default:
		return false
	}
}

func scanRunPlanningOutput(scanner interface{ Scan(dest ...any) error }, target *RunPlanningOutput) error {
	if target == nil {
		return fmt.Errorf("run planning output target is required")
	}
	var kind string
	var refJSON string
	if err := scanner.Scan(
		&target.PlanningOutputSeq,
		&target.PlanningOutputID,
		&target.RunID,
		&target.NodeID,
		&target.NodeRunID,
		&target.AttemptID,
		&target.AgentID,
		&target.SourceMessageID,
		&kind,
		&target.ContentMarkdown,
		&target.CreatedAt,
		&refJSON,
	); err != nil {
		return err
	}
	target.Kind = RunPlanningOutputKind(strings.TrimSpace(kind))
	if refJSON != "" {
		var ref providerexecution.ContentReference
		if err := json.Unmarshal([]byte(refJSON), &ref); err != nil {
			return err
		}
		if err := ref.Validate(ref.InvocationID); err != nil {
			return err
		}
		if target.ContentMarkdown != "" {
			return fmt.Errorf("planning content has conflicting authorities")
		}
		target.ContentRef = &ref
	}
	return nil
}

func equalPlanningContentRef(a, b *providerexecution.ContentReference) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (s *RunPlanningOutputStore) OpenContent(ctx context.Context, output RunPlanningOutput) (io.ReadCloser, error) {
	if output.ContentRef == nil {
		return io.NopCloser(strings.NewReader(strings.TrimSpace(output.ContentMarkdown))), nil
	}
	return executionstore.New(s.db).OpenProviderContent(ctx, *output.ContentRef)
}
