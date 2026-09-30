package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type RunPlanReviewDecision string

const (
	RunPlanReviewDecisionApproved    RunPlanReviewDecision = "approved"
	RunPlanReviewDecisionNeedsRework RunPlanReviewDecision = "needs_rework"
)

type RunPlanReview struct {
	PlanReviewID           string
	RunID                  string
	ReviewedPlanRevisionID string
	Decision               RunPlanReviewDecision
	FeedbackMarkdown       string
	ActorID                string
	RoleID                 string
	QueuedAdvisoryAttempt  *int
	QueuedPlanAttempt      *int
	CreatedAt              string
}

type CreateRunPlanReviewRequest struct {
	PlanReviewID           string
	RunID                  string
	ReviewedPlanRevisionID string
	Decision               RunPlanReviewDecision
	FeedbackMarkdown       string
	ActorID                string
	RoleID                 string
	QueuedAdvisoryAttempt  *int
	QueuedPlanAttempt      *int
	CreatedAt              string
}

type RunPlanReviewStore struct {
	db *sql.DB
}

func NewRunPlanReviewStore(db *sql.DB) *RunPlanReviewStore {
	return &RunPlanReviewStore{db: db}
}

func (s *RunPlanReviewStore) ListRunPlanReviews(ctx context.Context, runID string, limit int) ([]RunPlanReview, error) {
	return s.ListRunPlanReviewsPage(ctx, runID, limit, "", "")
}

func (s *RunPlanReviewStore) ListRunPlanReviewsPage(ctx context.Context, runID string, limit int, beforeCreatedAt, beforeID string) ([]RunPlanReview, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	query := `SELECT plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at
		 FROM run_plan_reviews
		 WHERE run_id = ?`
	args := []any{trimmedRunID}
	if beforeID != "" {
		query += ` AND (created_at, plan_review_id) < (?, ?)`
		args = append(args, beforeCreatedAt, beforeID)
	}
	query += ` ORDER BY created_at DESC, plan_review_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list run plan reviews: %w", err)
	}
	defer rows.Close()

	items := make([]RunPlanReview, 0, limit)
	for rows.Next() {
		var row RunPlanReview
		if err := scanRunPlanReview(rows, &row); err != nil {
			return nil, fmt.Errorf("scan run plan review: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run plan reviews: %w", err)
	}
	return items, nil
}

func (s *RunPlanReviewStore) LatestApprovedRunPlanReview(ctx context.Context, runID string) (RunPlanReview, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunPlanReview{}, false, fmt.Errorf("run id is required")
	}

	var row RunPlanReview
	err := scanRunPlanReview(s.db.QueryRowContext(ctx,
		`SELECT plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at
		 FROM run_plan_reviews
		 WHERE run_id = ?
		   AND decision = ?
		 ORDER BY created_at DESC, plan_review_id DESC
		 LIMIT 1`,
		trimmedRunID,
		RunPlanReviewDecisionApproved,
	), &row)
	if err == sql.ErrNoRows {
		return RunPlanReview{}, false, nil
	}
	if err != nil {
		return RunPlanReview{}, false, fmt.Errorf("load latest approved run plan review: %w", err)
	}
	return row, true, nil
}

func (s *RunPlanReviewStore) GetNeedsReworkForQueuedAdvisoryAttempt(ctx context.Context, runID string, attempt int) (RunPlanReview, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunPlanReview{}, false, fmt.Errorf("run id is required")
	}
	if attempt < 1 {
		return RunPlanReview{}, false, fmt.Errorf("attempt must be >= 1")
	}

	var row RunPlanReview
	err := scanRunPlanReview(s.db.QueryRowContext(ctx,
		`SELECT rpr.plan_review_id, rpr.run_id, rpr.reviewed_plan_revision_id, rpr.decision, rpr.feedback_markdown, rpr.actor_id, rpr.role_id, rpr.queued_advisory_attempt, rpr.queued_plan_attempt, rpr.created_at
		 FROM run_plan_reviews rpr
		 JOIN workflow_runs wr ON wr.run_id = rpr.run_id
		 WHERE rpr.run_id = ?
		   AND rpr.decision = ?
		   AND rpr.queued_advisory_attempt = ?
		   AND COALESCE(wr.autonomous_approvals, 1) = 0
		 ORDER BY rpr.created_at DESC, rpr.plan_review_id DESC
		 LIMIT 1`,
		trimmedRunID,
		RunPlanReviewDecisionNeedsRework,
		attempt,
	), &row)
	if err == sql.ErrNoRows {
		return RunPlanReview{}, false, nil
	}
	if err != nil {
		return RunPlanReview{}, false, fmt.Errorf("get run plan review for advisory attempt: %w", err)
	}
	return row, true, nil
}

func (s *RunPlanReviewStore) GetNeedsReworkForQueuedPlanAttempt(ctx context.Context, runID string, attempt int) (RunPlanReview, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunPlanReview{}, false, fmt.Errorf("run id is required")
	}
	if attempt < 1 {
		return RunPlanReview{}, false, fmt.Errorf("attempt must be >= 1")
	}

	var row RunPlanReview
	err := scanRunPlanReview(s.db.QueryRowContext(ctx,
		`SELECT rpr.plan_review_id, rpr.run_id, rpr.reviewed_plan_revision_id, rpr.decision, rpr.feedback_markdown, rpr.actor_id, rpr.role_id, rpr.queued_advisory_attempt, rpr.queued_plan_attempt, rpr.created_at
		 FROM run_plan_reviews rpr
		 JOIN workflow_runs wr ON wr.run_id = rpr.run_id
		 WHERE rpr.run_id = ?
		   AND rpr.decision = ?
		   AND rpr.queued_plan_attempt = ?
		   AND COALESCE(wr.autonomous_approvals, 1) = 0
		 ORDER BY rpr.created_at DESC, rpr.plan_review_id DESC
		 LIMIT 1`,
		trimmedRunID,
		RunPlanReviewDecisionNeedsRework,
		attempt,
	), &row)
	if err == sql.ErrNoRows {
		return RunPlanReview{}, false, nil
	}
	if err != nil {
		return RunPlanReview{}, false, fmt.Errorf("get run plan review for plan attempt: %w", err)
	}
	return row, true, nil
}

func insertRunPlanReview(ctx context.Context, exec sqlExecutor, req CreateRunPlanReviewRequest) (RunPlanReview, error) {
	if err := validateRunPlanReviewCreate(req); err != nil {
		return RunPlanReview{}, err
	}
	row := RunPlanReview{
		PlanReviewID:           strings.TrimSpace(req.PlanReviewID),
		RunID:                  strings.TrimSpace(req.RunID),
		ReviewedPlanRevisionID: strings.TrimSpace(req.ReviewedPlanRevisionID),
		Decision:               req.Decision,
		FeedbackMarkdown:       strings.TrimSpace(req.FeedbackMarkdown),
		ActorID:                strings.TrimSpace(req.ActorID),
		RoleID:                 strings.TrimSpace(req.RoleID),
		QueuedAdvisoryAttempt:  intPtrValue(req.QueuedAdvisoryAttempt),
		QueuedPlanAttempt:      intPtrValue(req.QueuedPlanAttempt),
		CreatedAt:              strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.PlanReviewID,
		row.RunID,
		row.ReviewedPlanRevisionID,
		row.Decision,
		row.FeedbackMarkdown,
		row.ActorID,
		row.RoleID,
		nullableIntValue(row.QueuedAdvisoryAttempt),
		nullableIntValue(row.QueuedPlanAttempt),
		row.CreatedAt,
	)
	if err != nil {
		return RunPlanReview{}, fmt.Errorf("insert run plan review: %w", err)
	}
	return row, nil
}

func validateRunPlanReviewCreate(req CreateRunPlanReviewRequest) error {
	if strings.TrimSpace(req.PlanReviewID) == "" {
		return fmt.Errorf("plan review id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.ReviewedPlanRevisionID) == "" {
		return fmt.Errorf("reviewed plan revision id is required")
	}
	if req.Decision != RunPlanReviewDecisionApproved && req.Decision != RunPlanReviewDecisionNeedsRework {
		return fmt.Errorf("invalid run plan review decision: %q", req.Decision)
	}
	if req.Decision == RunPlanReviewDecisionNeedsRework && strings.TrimSpace(req.FeedbackMarkdown) == "" {
		return fmt.Errorf("feedback markdown is required for plan rework")
	}
	if strings.TrimSpace(req.ActorID) == "" {
		return fmt.Errorf("actor id is required")
	}
	if strings.TrimSpace(req.RoleID) == "" {
		return fmt.Errorf("role id is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("created_at is required")
	}
	if req.QueuedAdvisoryAttempt != nil && *req.QueuedAdvisoryAttempt < 1 {
		return fmt.Errorf("queued advisory attempt must be >= 1")
	}
	if req.QueuedPlanAttempt != nil && *req.QueuedPlanAttempt < 1 {
		return fmt.Errorf("queued plan attempt must be >= 1")
	}
	return nil
}

func scanRunPlanReview(scanner interface{ Scan(dest ...any) error }, target *RunPlanReview) error {
	if target == nil {
		return fmt.Errorf("run plan review target is required")
	}
	var queuedAdvisory sql.NullInt64
	var queuedPlan sql.NullInt64
	err := scanner.Scan(
		&target.PlanReviewID,
		&target.RunID,
		&target.ReviewedPlanRevisionID,
		&target.Decision,
		&target.FeedbackMarkdown,
		&target.ActorID,
		&target.RoleID,
		&queuedAdvisory,
		&queuedPlan,
		&target.CreatedAt,
	)
	if err != nil {
		return err
	}
	if queuedAdvisory.Valid {
		value := int(queuedAdvisory.Int64)
		target.QueuedAdvisoryAttempt = &value
	} else {
		target.QueuedAdvisoryAttempt = nil
	}
	if queuedPlan.Valid {
		value := int(queuedPlan.Int64)
		target.QueuedPlanAttempt = &value
	} else {
		target.QueuedPlanAttempt = nil
	}
	return nil
}

func nullableIntValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}
