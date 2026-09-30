package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	coreworkflow "kingdom/internal/core/workflow"
)

type tokenUsageQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type TokenUsageRecord struct {
	UsageID              string `json:"usage_id"`
	RunID                string `json:"run_id"`
	NodeRunID            string `json:"node_run_id"`
	AttemptID            string `json:"attempt_id"`
	ProviderInvocationID string `json:"provider_invocation_id,omitempty"`
	RequiredRole         string `json:"required_role"`
	Model                string `json:"model"`
	Variant              string `json:"variant"`
	InputTokens          int64  `json:"input_tokens"`
	OutputTokens         int64  `json:"output_tokens"`
	TotalTokens          int64  `json:"total_tokens"`
	ReasoningTokens      int64  `json:"reasoning_tokens"`
	CacheReadTokens      int64  `json:"cache_read_tokens"`
	CacheWriteTokens     int64  `json:"cache_write_tokens"`
	CreatedAt            string `json:"created_at"`
}

type InsertTokenUsageRequest struct {
	RunID                string
	NodeRunID            string
	AttemptID            string
	ProviderInvocationID string
	RequiredRole         string
	Model                string
	Variant              string
	InputTokens          int64
	OutputTokens         int64
	TotalTokens          int64
	ReasoningTokens      int64
	CacheReadTokens      int64
	CacheWriteTokens     int64
	CreatedAt            string
}

type TokenUsageSummary struct {
	TotalTokens      int64 `json:"total_tokens"`
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
	RecordCount      int   `json:"record_count"`
}

type TokenUsageByRole struct {
	RequiredRole    string              `json:"required_role"`
	Model           string              `json:"model"`
	Variant         string              `json:"variant"`
	MixedModel      bool                `json:"mixed_model"`
	MixedVariant    bool                `json:"mixed_variant"`
	TotalTokens     int64               `json:"total_tokens"`
	InputTokens     int64               `json:"input_tokens"`
	OutputTokens    int64               `json:"output_tokens"`
	ReasoningTokens int64               `json:"reasoning_tokens"`
	RecordCount     int                 `json:"record_count"`
	ByModel         []TokenUsageByModel `json:"by_model"`
}

type TokenUsageByModel struct {
	Model           string `json:"model"`
	Variant         string `json:"variant"`
	TotalTokens     int64  `json:"total_tokens"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
	RecordCount     int    `json:"record_count"`
	RunCount        int    `json:"run_count"`
}

type TokenUsageByRun struct {
	RunID          string `json:"run_id"`
	WorkOrderTitle string `json:"work_order_title"`
	CreatedAt      string `json:"created_at"`
	TotalTokens    int64  `json:"total_tokens"`
	InputTokens    int64  `json:"input_tokens"`
	OutputTokens   int64  `json:"output_tokens"`
	RecordCount    int    `json:"record_count"`
}

type TokenUsageStore struct {
	db tokenUsageQuerier
}

const derivedTokenSumExpr = "COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) + COALESCE(SUM(reasoning_tokens), 0)"

func NewTokenUsageStore(db *sql.DB) *TokenUsageStore {
	return &TokenUsageStore{db: db}
}

func (s *TokenUsageStore) WithQuerier(q tokenUsageQuerier) *TokenUsageStore {
	if q == nil {
		return s
	}
	return &TokenUsageStore{db: q}
}

func (s *TokenUsageStore) Insert(ctx context.Context, req InsertTokenUsageRequest) (TokenUsageRecord, error) {
	usageID, err := newTokenUsageID()
	if err != nil {
		return TokenUsageRecord{}, fmt.Errorf("generate token usage id: %w", err)
	}

	record := TokenUsageRecord{
		UsageID:              usageID,
		RunID:                req.RunID,
		NodeRunID:            req.NodeRunID,
		AttemptID:            req.AttemptID,
		ProviderInvocationID: strings.TrimSpace(req.ProviderInvocationID),
		RequiredRole:         req.RequiredRole,
		Model:                req.Model,
		Variant:              req.Variant,
		InputTokens:          req.InputTokens,
		OutputTokens:         req.OutputTokens,
		TotalTokens:          req.TotalTokens,
		ReasoningTokens:      req.ReasoningTokens,
		CacheReadTokens:      req.CacheReadTokens,
		CacheWriteTokens:     req.CacheWriteTokens,
		CreatedAt:            req.CreatedAt,
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, provider_invocation_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at)
		 VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT DO NOTHING`,
		record.UsageID,
		record.RunID,
		record.NodeRunID,
		record.AttemptID,
		record.ProviderInvocationID,
		record.RequiredRole,
		record.Model,
		record.Variant,
		record.InputTokens,
		record.OutputTokens,
		record.TotalTokens,
		record.ReasoningTokens,
		record.CacheReadTokens,
		record.CacheWriteTokens,
		record.CreatedAt,
	)
	if err != nil {
		return TokenUsageRecord{}, fmt.Errorf("insert token usage: %w", err)
	}

	return record, nil
}

func (s *TokenUsageStore) SumForRun(ctx context.Context, runID string) (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT SUM(total_tokens) FROM token_usage WHERE run_id = ?`,
		runID,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("sum token usage for run %q: %w", runID, err)
	}
	if !total.Valid {
		return 0, nil
	}
	return total.Int64, nil
}

func (s *TokenUsageStore) SummaryByTimeWindow(ctx context.Context, sinceRFC3339 string) (TokenUsageSummary, error) {
	return s.summaryByFilter(ctx, "created_at >= ?", sinceRFC3339)
}

func (s *TokenUsageStore) SummaryByRun(ctx context.Context, runID string) (TokenUsageSummary, error) {
	return s.summaryByFilter(ctx, "run_id = ?", runID)
}

func (s *TokenUsageStore) SummaryByTimeWindowExecutionAnalytics(ctx context.Context, sinceRFC3339 string) (TokenUsageSummary, error) {
	return s.executionAnalyticsSummaryByFilter(ctx, "tu.created_at >= ?", sinceRFC3339)
}

func (s *TokenUsageStore) SummaryByRunExecutionAnalytics(ctx context.Context, runID string) (TokenUsageSummary, error) {
	return s.executionAnalyticsSummaryByFilter(ctx, "tu.run_id = ?", runID)
}

func (s *TokenUsageStore) summaryByFilter(ctx context.Context, whereClause string, arg any) (TokenUsageSummary, error) {
	var summary TokenUsageSummary
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COALESCE(SUM(total_tokens), 0),
		        COALESCE(SUM(input_tokens), 0),
		        COALESCE(SUM(output_tokens), 0),
		        COALESCE(SUM(reasoning_tokens), 0),
		        COALESCE(SUM(cache_read_tokens), 0),
		        COALESCE(SUM(cache_write_tokens), 0),
		        COUNT(1)
		 FROM token_usage
		 WHERE %s`, whereClause),
		arg,
	).Scan(
		&summary.TotalTokens,
		&summary.InputTokens,
		&summary.OutputTokens,
		&summary.ReasoningTokens,
		&summary.CacheReadTokens,
		&summary.CacheWriteTokens,
		&summary.RecordCount,
	)
	if err != nil {
		return TokenUsageSummary{}, fmt.Errorf("query token usage summary: %w", err)
	}
	return summary, nil
}

func (s *TokenUsageStore) ByRole(ctx context.Context, sinceRFC3339 string) ([]TokenUsageByRole, error) {
	return s.byRoleFilter(ctx, "created_at >= ?", sinceRFC3339)
}

func (s *TokenUsageStore) ByRoleForRun(ctx context.Context, runID string) ([]TokenUsageByRole, error) {
	return s.byRoleFilter(ctx, "run_id = ?", runID)
}

func (s *TokenUsageStore) ByRoleExecutionAnalytics(ctx context.Context, sinceRFC3339 string) ([]TokenUsageByRole, error) {
	return s.executionAnalyticsByRoleFilter(ctx, "tu.created_at >= ?", sinceRFC3339)
}

func (s *TokenUsageStore) ByRoleForRunExecutionAnalytics(ctx context.Context, runID string) ([]TokenUsageByRole, error) {
	return s.executionAnalyticsByRoleFilter(ctx, "tu.run_id = ?", runID)
}

func (s *TokenUsageStore) byRoleFilter(ctx context.Context, whereClause string, arg any) ([]TokenUsageByRole, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT required_role,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(model), '')) = 1 THEN MAX(NULLIF(TRIM(model), '')) ELSE '' END AS model,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(variant), '')) = 1 THEN MAX(NULLIF(TRIM(variant), '')) ELSE '' END AS variant,
		        COUNT(DISTINCT NULLIF(TRIM(model), '')) > 1 AS mixed_model,
		        COUNT(DISTINCT NULLIF(TRIM(variant), '')) > 1 AS mixed_variant,
		        COALESCE(SUM(total_tokens), 0),
		        COALESCE(SUM(input_tokens), 0),
		        COALESCE(SUM(output_tokens), 0),
		        COALESCE(SUM(reasoning_tokens), 0),
		        COUNT(1)
		 FROM token_usage
		 WHERE %s
		 GROUP BY required_role
		 ORDER BY %s DESC, required_role ASC`, whereClause, derivedTokenSumExpr),
		arg,
	)
	if err != nil {
		return nil, fmt.Errorf("query token usage by role: %w", err)
	}
	defer rows.Close()

	results := make([]TokenUsageByRole, 0)
	for rows.Next() {
		var r TokenUsageByRole
		if err := rows.Scan(&r.RequiredRole, &r.Model, &r.Variant, &r.MixedModel, &r.MixedVariant, &r.TotalTokens, &r.InputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.RecordCount); err != nil {
			return nil, fmt.Errorf("scan token usage by role row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate token usage by role rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close token usage by role rows: %w", err)
	}

	for i := range results {
		breakdown, breakdownErr := s.byModelForRoleFilter(ctx, whereClause, arg, results[i].RequiredRole)
		if breakdownErr != nil {
			return nil, breakdownErr
		}
		results[i].ByModel = breakdown
	}

	return results, nil
}

func (s *TokenUsageStore) byModelForRoleFilter(ctx context.Context, whereClause string, arg any, role string) ([]TokenUsageByModel, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(model), ''), '') AS model,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(variant), '')) = 1 THEN COALESCE(MAX(NULLIF(TRIM(variant), '')), '') ELSE '' END AS variant,
		        COALESCE(SUM(total_tokens), 0),
		        COALESCE(SUM(input_tokens), 0),
		        COALESCE(SUM(output_tokens), 0),
		        COALESCE(SUM(reasoning_tokens), 0),
		        COUNT(1),
		        COUNT(DISTINCT run_id)
		 FROM token_usage
		 WHERE %s AND required_role = ?
		 GROUP BY COALESCE(NULLIF(TRIM(model), ''), '')
		 ORDER BY %s DESC, MAX(NULLIF(TRIM(model), '')) ASC`, whereClause, derivedTokenSumExpr),
		arg,
		role,
	)
	if err != nil {
		return nil, fmt.Errorf("query token usage by role model breakdown: %w", err)
	}
	defer rows.Close()

	results := make([]TokenUsageByModel, 0)
	for rows.Next() {
		var row TokenUsageByModel
		if err := rows.Scan(&row.Model, &row.Variant, &row.TotalTokens, &row.InputTokens, &row.OutputTokens, &row.ReasoningTokens, &row.RecordCount, &row.RunCount); err != nil {
			return nil, fmt.Errorf("scan token usage by role model breakdown row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate token usage by role model breakdown rows: %w", err)
	}

	return results, nil
}

func (s *TokenUsageStore) TopRunsByTokens(ctx context.Context, sinceRFC3339 string, limit int) ([]TokenUsageByRun, error) {
	if limit <= 0 {
		limit = 10
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT tu.run_id,
		        COALESCE(d.work_order_title, ''),
		        COALESCE(wr.created_at, ''),
		        COALESCE(SUM(tu.total_tokens), 0),
		        COALESCE(SUM(tu.input_tokens), 0),
		        COALESCE(SUM(tu.output_tokens), 0),
		        COUNT(1)
		 FROM token_usage tu
		 LEFT JOIN workflow_runs wr ON wr.run_id = tu.run_id
		 LEFT JOIN work_orders d ON d.work_order_id = wr.work_order_id
		 WHERE tu.created_at >= ?
		 GROUP BY tu.run_id
		 ORDER BY SUM(tu.total_tokens) DESC
		 LIMIT ?`,
		sinceRFC3339,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query top runs by tokens: %w", err)
	}
	defer rows.Close()

	results := make([]TokenUsageByRun, 0)
	for rows.Next() {
		var r TokenUsageByRun
		if err := rows.Scan(&r.RunID, &r.WorkOrderTitle, &r.CreatedAt, &r.TotalTokens, &r.InputTokens, &r.OutputTokens, &r.RecordCount); err != nil {
			return nil, fmt.Errorf("scan top runs by tokens row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate top runs by tokens rows: %w", err)
	}

	return results, nil
}

func (s *TokenUsageStore) executionAnalyticsSummaryByFilter(ctx context.Context, whereClause string, arg any) (TokenUsageSummary, error) {
	var summary TokenUsageSummary
	nodeClassFilter, nodeClassArgs := executionAnalyticsTokenNodeClassFilterSQL("wn")
	args := make([]any, 0, 1+len(nodeClassArgs))
	args = append(args, arg)
	args = append(args, nodeClassArgs...)
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COALESCE(SUM(tu.total_tokens), 0),
		        COALESCE(SUM(tu.input_tokens), 0),
		        COALESCE(SUM(tu.output_tokens), 0),
		        COALESCE(SUM(tu.reasoning_tokens), 0),
		        COALESCE(SUM(tu.cache_read_tokens), 0),
		        COALESCE(SUM(tu.cache_write_tokens), 0),
		        COUNT(1)
		 FROM token_usage tu
		 JOIN node_runs nr ON nr.run_id = tu.run_id AND nr.node_run_id = tu.node_run_id
		 JOIN workflow_runs wr ON wr.run_id = tu.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE %s
		   AND %s`, whereClause, nodeClassFilter),
		args...,
	).Scan(
		&summary.TotalTokens,
		&summary.InputTokens,
		&summary.OutputTokens,
		&summary.ReasoningTokens,
		&summary.CacheReadTokens,
		&summary.CacheWriteTokens,
		&summary.RecordCount,
	)
	if err != nil {
		return TokenUsageSummary{}, fmt.Errorf("query execution analytics token usage summary: %w", err)
	}
	return summary, nil
}

func (s *TokenUsageStore) executionAnalyticsByRoleFilter(ctx context.Context, whereClause string, arg any) ([]TokenUsageByRole, error) {
	nodeClassFilter, nodeClassArgs := executionAnalyticsTokenNodeClassFilterSQL("wn")
	args := make([]any, 0, 1+len(nodeClassArgs))
	args = append(args, arg)
	args = append(args, nodeClassArgs...)
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT tu.required_role,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(tu.model), '')) = 1 THEN MAX(NULLIF(TRIM(tu.model), '')) ELSE '' END AS model,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(tu.variant), '')) = 1 THEN MAX(NULLIF(TRIM(tu.variant), '')) ELSE '' END AS variant,
		        COUNT(DISTINCT NULLIF(TRIM(tu.model), '')) > 1 AS mixed_model,
		        COUNT(DISTINCT NULLIF(TRIM(tu.variant), '')) > 1 AS mixed_variant,
		        COALESCE(SUM(tu.total_tokens), 0),
		        COALESCE(SUM(tu.input_tokens), 0),
		        COALESCE(SUM(tu.output_tokens), 0),
		        COALESCE(SUM(tu.reasoning_tokens), 0),
		        COUNT(1)
		 FROM token_usage tu
		 JOIN node_runs nr ON nr.run_id = tu.run_id AND nr.node_run_id = tu.node_run_id
		 JOIN workflow_runs wr ON wr.run_id = tu.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE %s
		   AND %s
		 GROUP BY tu.required_role
		 ORDER BY COALESCE(SUM(tu.input_tokens), 0) + COALESCE(SUM(tu.output_tokens), 0) + COALESCE(SUM(tu.reasoning_tokens), 0) DESC, tu.required_role ASC`, whereClause, nodeClassFilter),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution analytics token usage by role: %w", err)
	}
	defer rows.Close()

	results := make([]TokenUsageByRole, 0)
	for rows.Next() {
		var r TokenUsageByRole
		if err := rows.Scan(&r.RequiredRole, &r.Model, &r.Variant, &r.MixedModel, &r.MixedVariant, &r.TotalTokens, &r.InputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.RecordCount); err != nil {
			return nil, fmt.Errorf("scan execution analytics token usage by role row: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution analytics token usage by role rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close execution analytics token usage by role rows: %w", err)
	}

	for i := range results {
		breakdown, breakdownErr := s.executionAnalyticsByModelForRoleFilter(ctx, whereClause, arg, results[i].RequiredRole)
		if breakdownErr != nil {
			return nil, breakdownErr
		}
		results[i].ByModel = breakdown
	}

	return results, nil
}

func (s *TokenUsageStore) executionAnalyticsByModelForRoleFilter(ctx context.Context, whereClause string, arg any, role string) ([]TokenUsageByModel, error) {
	nodeClassFilter, nodeClassArgs := executionAnalyticsTokenNodeClassFilterSQL("wn")
	args := make([]any, 0, 2+len(nodeClassArgs))
	args = append(args, arg)
	args = append(args, role)
	args = append(args, nodeClassArgs...)
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(tu.model), ''), '') AS model,
		        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(tu.variant), '')) = 1 THEN COALESCE(MAX(NULLIF(TRIM(tu.variant), '')), '') ELSE '' END AS variant,
		        COALESCE(SUM(tu.total_tokens), 0),
		        COALESCE(SUM(tu.input_tokens), 0),
		        COALESCE(SUM(tu.output_tokens), 0),
		        COALESCE(SUM(tu.reasoning_tokens), 0),
		        COUNT(1),
		        COUNT(DISTINCT tu.run_id)
		 FROM token_usage tu
		 JOIN node_runs nr ON nr.run_id = tu.run_id AND nr.node_run_id = tu.node_run_id
		 JOIN workflow_runs wr ON wr.run_id = tu.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE %s
		   AND tu.required_role = ?
		   AND %s
		 GROUP BY COALESCE(NULLIF(TRIM(tu.model), ''), '')
		 ORDER BY COALESCE(SUM(tu.input_tokens), 0) + COALESCE(SUM(tu.output_tokens), 0) + COALESCE(SUM(tu.reasoning_tokens), 0) DESC, MAX(NULLIF(TRIM(tu.model), '')) ASC`, whereClause, nodeClassFilter),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution analytics token usage by role model breakdown: %w", err)
	}
	defer rows.Close()

	results := make([]TokenUsageByModel, 0)
	for rows.Next() {
		var row TokenUsageByModel
		if err := rows.Scan(&row.Model, &row.Variant, &row.TotalTokens, &row.InputTokens, &row.OutputTokens, &row.ReasoningTokens, &row.RecordCount, &row.RunCount); err != nil {
			return nil, fmt.Errorf("scan execution analytics token usage by role model breakdown row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution analytics token usage by role model breakdown rows: %w", err)
	}

	return results, nil
}

func executionAnalyticsTokenNodeClassFilterSQL(alias string) (string, []any) {
	trimmedAlias := strings.TrimSpace(alias)
	if trimmedAlias == "" {
		trimmedAlias = "wn"
	}
	classes := coreworkflow.ExecutionAnalyticsIncludedNodeClasses()
	placeholders := make([]string, 0, len(classes))
	args := make([]any, 0, len(classes))
	for _, nodeClass := range classes {
		placeholders = append(placeholders, "?")
		args = append(args, nodeClass)
	}
	return fmt.Sprintf("%s.node_class IN (%s)", trimmedAlias, strings.Join(placeholders, ", ")), args
}

func newTokenUsageID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "usage_" + hex.EncodeToString(b), nil
}
