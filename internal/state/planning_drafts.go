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
	return insertPlanningDraft(ctx, s.db, req)
}

func insertPlanningDraft(ctx context.Context, exec sqlExecutor, req CreatePlanningDraftRequest) (PlanningDraft, error) {
	row := PlanningDraft{
		PlanningDraftID:   strings.TrimSpace(req.PlanningDraftID),
		PlanningSessionID: strings.TrimSpace(req.PlanningSessionID),
		DecreeTitle:       strings.TrimSpace(req.DecreeTitle),
		DecreeText:        req.DecreeText,
		SummaryJSON:       strings.TrimSpace(req.SummaryJSON),
		CreatedAt:         strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO planning_drafts (planning_draft_id, planning_session_id, decree_title, decree_text, summary_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		row.PlanningDraftID,
		row.PlanningSessionID,
		row.DecreeTitle,
		row.DecreeText,
		row.SummaryJSON,
		row.CreatedAt,
	)
	if err != nil {
		return PlanningDraft{}, fmt.Errorf("insert planning draft: %w", err)
	}
	return row, nil
}

func (s *PlanningStore) GetDraft(ctx context.Context, planningDraftID string) (PlanningDraft, bool, error) {
	var row PlanningDraft
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_draft_id, planning_session_id, decree_title, decree_text, summary_json, created_at
		 FROM planning_drafts
		 WHERE planning_draft_id = ?`,
		strings.TrimSpace(planningDraftID),
	).Scan(
		&row.PlanningDraftID,
		&row.PlanningSessionID,
		&row.DecreeTitle,
		&row.DecreeText,
		&row.SummaryJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningDraft{}, false, nil
	}
	if err != nil {
		return PlanningDraft{}, false, fmt.Errorf("get planning draft: %w", err)
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
		`SELECT planning_draft_id, planning_session_id, decree_title, decree_text, summary_json, created_at
		 FROM planning_drafts
		 WHERE planning_session_id = ?
		 ORDER BY created_at DESC, planning_draft_id DESC
		 LIMIT 1`,
		strings.TrimSpace(planningSessionID),
	).Scan(
		&row.PlanningDraftID,
		&row.PlanningSessionID,
		&row.DecreeTitle,
		&row.DecreeText,
		&row.SummaryJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningDraft{}, false, nil
	}
	if err != nil {
		return PlanningDraft{}, false, fmt.Errorf("get latest planning draft: %w", err)
	}
	return row, true, nil
}
