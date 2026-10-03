package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *PlanningStore) CreateDraft(ctx context.Context, req CreatePlanningDraftRequest) (PlanningDraft, error) {
	if err := validatePlanningDraftCreate(req); err != nil {
		return PlanningDraft{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningDraft{}, err
	}
	defer tx.Rollback()
	draft, err := insertPlanningDraft(ctx, tx, req)
	if err != nil {
		return PlanningDraft{}, err
	}
	if err := tx.Commit(); err != nil {
		return PlanningDraft{}, err
	}
	return draft, nil
}

func insertPlanningDraft(ctx context.Context, exec sqlExecutor, req CreatePlanningDraftRequest) (PlanningDraft, error) {
	if err := validatePlanningTextContent(ctx, req.TextSource, true); err != nil {
		return PlanningDraft{}, err
	}
	ref, err := persistPlanningText(ctx, exec, req.PlanningSessionID, PlanningDraftContentID(req.PlanningDraftID), req.TextSource, req.CreatedAt)
	if err != nil {
		return PlanningDraft{}, err
	}
	row := PlanningDraft{
		PlanningDraftID:   strings.TrimSpace(req.PlanningDraftID),
		PlanningSessionID: strings.TrimSpace(req.PlanningSessionID),
		WorkOrderTitle:    strings.TrimSpace(req.WorkOrderTitle),
		WorkOrderText:     req.WorkOrderText,
		SummaryJSON:       strings.TrimSpace(req.SummaryJSON),
		CreatedAt:         strings.TrimSpace(req.CreatedAt),
	}
	_, err = exec.ExecContext(ctx,
		`INSERT INTO planning_drafts (planning_draft_id, planning_session_id, work_order_title, work_order_text, summary_json, created_at,work_order_text_content_id)
		 VALUES (?, ?, ?, ?, ?, ?,?)`,
		row.PlanningDraftID,
		row.PlanningSessionID,
		row.WorkOrderTitle,
		row.WorkOrderText,
		row.SummaryJSON,
		row.CreatedAt,
		planningContentID(ref),
	)
	if err != nil {
		return PlanningDraft{}, fmt.Errorf("insert planning draft: %w", err)
	}
	return row, nil
}

func (s *PlanningStore) GetDraft(ctx context.Context, planningDraftID string) (PlanningDraft, bool, error) {
	var row PlanningDraft
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_draft_id, planning_session_id, work_order_title, work_order_text, summary_json, created_at
		 FROM planning_drafts
		 WHERE planning_draft_id = ?`,
		strings.TrimSpace(planningDraftID),
	).Scan(
		&row.PlanningDraftID,
		&row.PlanningSessionID,
		&row.WorkOrderTitle,
		&row.WorkOrderText,
		&row.SummaryJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningDraft{}, false, nil
	}
	if err != nil {
		return PlanningDraft{}, false, fmt.Errorf("get planning draft: %w", err)
	}
	if err := s.hydrateDraftText(ctx, &row); err != nil {
		return PlanningDraft{}, false, err
	}
	return row, true, nil
}

func (s *PlanningStore) GetCurrentDraft(ctx context.Context, planningSessionID string) (PlanningDraft, bool, error) {
	session, ok, err := s.GetSession(ctx, planningSessionID)
	if err != nil {
		return PlanningDraft{}, false, err
	}
	if !ok || session.CurrentDraftID == nil || strings.TrimSpace(*session.CurrentDraftID) == "" {
		return PlanningDraft{}, false, nil
	}
	return s.GetDraft(ctx, *session.CurrentDraftID)
}

func (s *PlanningStore) GetLatestDraft(ctx context.Context, planningSessionID string) (PlanningDraft, bool, error) {
	var row PlanningDraft
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_draft_id, planning_session_id, work_order_title, work_order_text, summary_json, created_at
		 FROM planning_drafts
		 WHERE planning_session_id = ?
		 ORDER BY created_at DESC, planning_draft_id DESC
		 LIMIT 1`,
		strings.TrimSpace(planningSessionID),
	).Scan(
		&row.PlanningDraftID,
		&row.PlanningSessionID,
		&row.WorkOrderTitle,
		&row.WorkOrderText,
		&row.SummaryJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningDraft{}, false, nil
	}
	if err != nil {
		return PlanningDraft{}, false, fmt.Errorf("get latest planning draft: %w", err)
	}
	if err := s.hydrateDraftText(ctx, &row); err != nil {
		return PlanningDraft{}, false, err
	}
	return row, true, nil
}

func (s *PlanningStore) hydrateDraftText(ctx context.Context, draft *PlanningDraft) error {
	var id sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT work_order_text_content_id FROM planning_drafts WHERE planning_draft_id=?`, draft.PlanningDraftID).Scan(&id); err != nil {
		return err
	}
	if !id.Valid {
		return nil
	}
	source, err := s.planningTextSource(ctx, id.String)
	if err != nil {
		return err
	}
	draft.WorkOrderText, err = readPlanningText(ctx, source)
	return err
}
