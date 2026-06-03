package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type RunFinalReviewDecision string

const (
	RunFinalReviewDecisionConfirmed   RunFinalReviewDecision = "confirmed"
	RunFinalReviewDecisionNeedsRework RunFinalReviewDecision = "needs_rework"
)

type RunFinalReview struct {
	FinalReviewID     string
	RunID             string
	Decision          RunFinalReviewDecision
	FeedbackMarkdown  string
	ActorID           string
	RoleID            string
	QueuedPlanAttempt *int
	CreatedAt         string
}

type CreateRunFinalReviewRequest struct {
	FinalReviewID     string
	RunID             string
	Decision          RunFinalReviewDecision
	FeedbackMarkdown  string
	ActorID           string
	RoleID            string
	QueuedPlanAttempt *int
	CreatedAt         string
}

type RunFinalReviewStore struct {
	db *sql.DB
}

func NewRunFinalReviewStore(db *sql.DB) *RunFinalReviewStore {
	return &RunFinalReviewStore{db: db}
}

func (s *RunFinalReviewStore) ListRunFinalReviews(ctx context.Context, runID string, limit int) ([]RunFinalReview, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at
		 FROM run_final_reviews
		 WHERE run_id = ?
		 ORDER BY created_at DESC, final_review_id DESC
		 LIMIT ?`,
		trimmedRunID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list run final reviews: %w", err)
	}
	defer rows.Close()

	items := make([]RunFinalReview, 0, limit)
	for rows.Next() {
		var row RunFinalReview
		if err := scanRunFinalReview(rows, &row); err != nil {
			return nil, fmt.Errorf("scan run final review: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run final reviews: %w", err)
	}
	return items, nil
}

func (s *RunFinalReviewStore) GetNeedsReworkForQueuedPlanAttempt(ctx context.Context, runID string, attempt int) (RunFinalReview, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunFinalReview{}, false, fmt.Errorf("run id is required")
	}
	if attempt < 1 {
		return RunFinalReview{}, false, fmt.Errorf("attempt must be >= 1")
	}

	var row RunFinalReview
	err := scanRunFinalReview(s.db.QueryRowContext(ctx,
		`SELECT rfr.final_review_id, rfr.run_id, rfr.decision, rfr.feedback_markdown, rfr.actor_id, rfr.role_id, rfr.queued_plan_attempt, rfr.created_at
		 FROM run_final_reviews rfr
		 JOIN workflow_runs wr ON wr.run_id = rfr.run_id
		 WHERE rfr.run_id = ?
		   AND rfr.decision = ?
		   AND rfr.queued_plan_attempt = ?
		   AND COALESCE(wr.autonomous_approvals, 1) = 0
		 ORDER BY rfr.created_at DESC, rfr.final_review_id DESC
		 LIMIT 1`,
		trimmedRunID,
		RunFinalReviewDecisionNeedsRework,
		attempt,
	), &row)
	if err == sql.ErrNoRows {
		return RunFinalReview{}, false, nil
	}
	if err != nil {
		return RunFinalReview{}, false, fmt.Errorf("get run final review for queued plan attempt: %w", err)
	}
	return row, true, nil
}

func insertRunFinalReview(ctx context.Context, exec sqlExecutor, req CreateRunFinalReviewRequest) (RunFinalReview, error) {
	if err := validateRunFinalReviewCreate(req); err != nil {
		return RunFinalReview{}, err
	}
	row := RunFinalReview{
		FinalReviewID:     strings.TrimSpace(req.FinalReviewID),
		RunID:             strings.TrimSpace(req.RunID),
		Decision:          req.Decision,
		FeedbackMarkdown:  strings.TrimSpace(req.FeedbackMarkdown),
		ActorID:           strings.TrimSpace(req.ActorID),
		RoleID:            strings.TrimSpace(req.RoleID),
		QueuedPlanAttempt: intPtrValue(req.QueuedPlanAttempt),
		CreatedAt:         strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
		`INSERT INTO run_final_reviews (final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		row.FinalReviewID,
		row.RunID,
		row.Decision,
		row.FeedbackMarkdown,
		row.ActorID,
		row.RoleID,
		nullableIntValue(row.QueuedPlanAttempt),
		row.CreatedAt,
	)
	if err != nil {
		return RunFinalReview{}, fmt.Errorf("insert run final review: %w", err)
	}
	return row, nil
}

func validateRunFinalReviewCreate(req CreateRunFinalReviewRequest) error {
	if strings.TrimSpace(req.FinalReviewID) == "" {
		return fmt.Errorf("final review id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if req.Decision != RunFinalReviewDecisionConfirmed && req.Decision != RunFinalReviewDecisionNeedsRework {
		return fmt.Errorf("invalid run final review decision: %q", req.Decision)
	}
	if req.Decision == RunFinalReviewDecisionNeedsRework && strings.TrimSpace(req.FeedbackMarkdown) == "" {
		return fmt.Errorf("feedback markdown is required for final rework")
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
	if req.QueuedPlanAttempt != nil && *req.QueuedPlanAttempt < 1 {
		return fmt.Errorf("queued plan attempt must be >= 1")
	}
	return nil
}

func scanRunFinalReview(scanner interface{ Scan(dest ...any) error }, target *RunFinalReview) error {
	if target == nil {
		return fmt.Errorf("run final review target is required")
	}
	var queuedPlan sql.NullInt64
	err := scanner.Scan(
		&target.FinalReviewID,
		&target.RunID,
		&target.Decision,
		&target.FeedbackMarkdown,
		&target.ActorID,
		&target.RoleID,
		&queuedPlan,
		&target.CreatedAt,
	)
	if err != nil {
		return err
	}
	if queuedPlan.Valid {
		value := int(queuedPlan.Int64)
		target.QueuedPlanAttempt = &value
	} else {
		target.QueuedPlanAttempt = nil
	}
	return nil
}
