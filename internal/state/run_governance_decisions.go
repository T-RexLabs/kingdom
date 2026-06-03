package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	RunGovernanceDecisionStagePlan  = "plan"
	RunGovernanceDecisionStageFinal = "final"
)

type RunGovernanceDecision struct {
	DecisionID       string
	Stage            string
	Decision         string
	FeedbackMarkdown string
	CreatedAt        string
}

type RunGovernanceDecisionCursor struct {
	CreatedAt  string
	Stage      string
	DecisionID string
}

type RunGovernanceDecisionPage struct {
	RunID string
	Items []RunGovernanceDecision
	Next  *RunGovernanceDecisionCursor
}

type RunGovernanceDecisionStore struct {
	db *sql.DB
}

func NewRunGovernanceDecisionStore(db *sql.DB) *RunGovernanceDecisionStore {
	return &RunGovernanceDecisionStore{db: db}
}

func (s *RunGovernanceDecisionStore) ListRunGovernanceDecisionsPage(ctx context.Context, runID string, limit int, after *RunGovernanceDecisionCursor) (RunGovernanceDecisionPage, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return RunGovernanceDecisionPage{}, fmt.Errorf("run id is required")
	}
	if limit <= 0 {
		limit = 50
	}

	page := RunGovernanceDecisionPage{RunID: trimmedRunID, Items: []RunGovernanceDecision{}}
	args := []any{trimmedRunID, trimmedRunID}
	query := `
SELECT decision_id, stage, decision, feedback_markdown, created_at
FROM (
	SELECT plan_review_id AS decision_id,
	       'plan' AS stage,
	       decision,
	       feedback_markdown,
	       created_at,
	       0 AS stage_rank
	FROM run_plan_reviews
	WHERE run_id = ?
	UNION ALL
	SELECT final_review_id AS decision_id,
	       'final' AS stage,
	       decision,
	       feedback_markdown,
	       created_at,
	       1 AS stage_rank
	FROM run_final_reviews
	WHERE run_id = ?
)
WHERE 1 = 1`
	if after != nil {
		stageRank, err := runGovernanceDecisionStageRank(after.Stage)
		if err != nil {
			return RunGovernanceDecisionPage{}, err
		}
		query += `
	AND (
		created_at < ?
		OR (created_at = ? AND stage_rank < ?)
		OR (created_at = ? AND stage_rank = ? AND decision_id < ?)
	)`
		args = append(args,
			strings.TrimSpace(after.CreatedAt),
			strings.TrimSpace(after.CreatedAt),
			stageRank,
			strings.TrimSpace(after.CreatedAt),
			stageRank,
			strings.TrimSpace(after.DecisionID),
		)
	}
	query += `
ORDER BY created_at DESC, stage_rank DESC, decision_id DESC
LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return RunGovernanceDecisionPage{}, fmt.Errorf("list run governance decisions page: %w", err)
	}
	defer rows.Close()

	items := make([]RunGovernanceDecision, 0, limit+1)
	for rows.Next() {
		var row RunGovernanceDecision
		if err := rows.Scan(&row.DecisionID, &row.Stage, &row.Decision, &row.FeedbackMarkdown, &row.CreatedAt); err != nil {
			return RunGovernanceDecisionPage{}, fmt.Errorf("scan run governance decision: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return RunGovernanceDecisionPage{}, fmt.Errorf("iterate run governance decisions: %w", err)
	}
	if len(items) > limit {
		trimmed := items[:limit]
		last := trimmed[len(trimmed)-1]
		page.Items = trimmed
		page.Next = &RunGovernanceDecisionCursor{CreatedAt: last.CreatedAt, Stage: last.Stage, DecisionID: last.DecisionID}
		return page, nil
	}
	page.Items = items
	return page, nil
}

func runGovernanceDecisionStageRank(stage string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(stage)) {
	case RunGovernanceDecisionStagePlan:
		return 0, nil
	case RunGovernanceDecisionStageFinal:
		return 1, nil
	default:
		return 0, fmt.Errorf("invalid governance decision stage %q", stage)
	}
}
