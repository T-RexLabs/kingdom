package state

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	coreworkflow "kingdom/internal/core/workflow"
)

type reviewWaveReadQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type ReviewWaveOutcome struct {
	Settled        bool
	HasRework      bool
	HasHardFailure bool
}

func ReviewWaveOutcomeForWave(ctx context.Context, q reviewWaveReadQuerier, runID string, wave int) (ReviewWaveOutcome, error) {
	result := ReviewWaveOutcome{}
	if q == nil {
		return result, fmt.Errorf("review wave querier is required")
	}
	if wave <= 0 {
		return result, nil
	}

	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return result, fmt.Errorf("run_id is required")
	}

	var total int
	var reworkFailures int
	var failedHardFailures int
	var abnormalTerminals int
	var settledCount int
	err := q.QueryRowContext(ctx, `
WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.node_status,
	       ranked.node_run_id
	FROM (
		SELECT nr.node_id,
		       nr.node_status,
		       nr.node_run_id,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		JOIN workflow_runs wr ON wr.run_id = nr.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE nr.run_id = ?
		  AND wn.node_class = ?
		  AND nr.wave = ?
	) ranked
	WHERE ranked.rn = 1
)
SELECT COUNT(1) AS total,
	   COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') = 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS rework_failures,
	   COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') != 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS failed_hard_failures,
	   COALESCE(SUM(CASE WHEN lwa.node_status IN ('cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS abnormal_terminal_count,
	   COALESCE(SUM(CASE WHEN lwa.node_status IN ('succeeded', 'failed', 'cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS settled_count
FROM latest_wave_attempts lwa
LEFT JOIN attempt_executions ae ON ae.node_run_id = lwa.node_run_id AND ae.run_id = ?`, trimmedRunID, coreworkflow.NodeClassExecutionReview, wave, trimmedRunID).Scan(
		&total,
		&reworkFailures,
		&failedHardFailures,
		&abnormalTerminals,
		&settledCount,
	)
	if err != nil {
		return result, fmt.Errorf("load review wave outcome run=%q wave=%d: %w", trimmedRunID, wave, err)
	}

	if total == 0 {
		result.Settled = true
		return result, nil
	}
	result.Settled = settledCount == total
	result.HasRework = reworkFailures > 0
	result.HasHardFailure = failedHardFailures > 0 || abnormalTerminals > 0
	return result, nil
}

func CountReviewReworkWaves(ctx context.Context, q reviewWaveReadQuerier, runID string) (int, error) {
	return countReviewReworkWaves(ctx, q, runID, 0)
}

func CountReviewReworkWavesBeforeWave(ctx context.Context, q reviewWaveReadQuerier, runID string, wave int) (int, error) {
	if wave <= 0 {
		return 0, nil
	}
	return countReviewReworkWaves(ctx, q, runID, wave)
}

func ReviewWaveOutcomeForNodeIDsAndWave(ctx context.Context, q reviewWaveReadQuerier, runID string, nodeIDs []string, wave int) (ReviewWaveOutcome, error) {
	return reviewWaveOutcomeForNodes(ctx, q, runID, normalizeWaveNodeIDs(nodeIDs), wave)
}

func CountReviewReworkWavesForNodeIDs(ctx context.Context, q reviewWaveReadQuerier, runID string, nodeIDs []string) (int, error) {
	return countReviewReworkWavesForNodes(ctx, q, runID, normalizeWaveNodeIDs(nodeIDs), 0)
}

func CountReviewReworkWavesBeforeWaveForNodeIDs(ctx context.Context, q reviewWaveReadQuerier, runID string, nodeIDs []string, wave int) (int, error) {
	if wave <= 0 {
		return 0, nil
	}
	return countReviewReworkWavesForNodes(ctx, q, runID, normalizeWaveNodeIDs(nodeIDs), wave)
}

func countReviewReworkWaves(ctx context.Context, q reviewWaveReadQuerier, runID string, beforeWave int) (int, error) {
	if q == nil {
		return 0, fmt.Errorf("review wave querier is required")
	}
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return 0, fmt.Errorf("run_id is required")
	}

	query := `
	WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.wave,
	       ranked.node_status,
	       ranked.node_run_id
	FROM (
		SELECT nr.node_id,
		       nr.wave,
		       nr.node_status,
		       nr.node_run_id,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		JOIN workflow_runs wr ON wr.run_id = nr.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE nr.run_id = ?
		  AND wn.node_class = ?`
	args := []any{trimmedRunID, coreworkflow.NodeClassExecutionReview}
	if beforeWave > 0 {
		query += `
		  AND nr.wave < ?`
		args = append(args, beforeWave)
	}
	query += `
	) ranked
	WHERE ranked.rn = 1
), review_wave_summary AS (
	SELECT lwa.wave,
	       COUNT(1) AS total,
	       COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') = 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS rework_failures,
	       COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') != 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS failed_hard_failures,
	       COALESCE(SUM(CASE WHEN lwa.node_status IN ('cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS abnormal_terminal_count,
	       COALESCE(SUM(CASE WHEN lwa.node_status IN ('succeeded', 'failed', 'cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS settled_count
	FROM latest_wave_attempts lwa
	LEFT JOIN attempt_executions ae ON ae.node_run_id = lwa.node_run_id AND ae.run_id = ?
	GROUP BY lwa.wave
)
SELECT COUNT(1)
FROM review_wave_summary
WHERE rework_failures > 0
	AND failed_hard_failures = 0
	AND abnormal_terminal_count = 0
	AND settled_count = total`
	args = append(args, trimmedRunID)

	var count int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		if beforeWave > 0 {
			return 0, fmt.Errorf("count review rework waves before wave run=%q wave=%d: %w", trimmedRunID, beforeWave, err)
		}
		return 0, fmt.Errorf("count review rework waves run=%q: %w", trimmedRunID, err)
	}
	return count, nil
}

func reviewWaveOutcomeForNodes(ctx context.Context, q reviewWaveReadQuerier, runID string, nodeIDs []string, wave int) (ReviewWaveOutcome, error) {
	result := ReviewWaveOutcome{}
	if q == nil {
		return result, fmt.Errorf("review wave querier is required")
	}
	if wave <= 0 || len(nodeIDs) == 0 {
		return result, nil
	}
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return result, fmt.Errorf("run_id is required")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")
	args := make([]any, 0, len(nodeIDs)+2)
	args = append(args, trimmedRunID, wave)
	for _, nodeID := range nodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`
WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.node_status,
	       ranked.node_run_id
	FROM (
		SELECT nr.node_id,
		       nr.node_status,
		       nr.node_run_id,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		WHERE nr.run_id = ?
		  AND nr.wave = ?
		  AND nr.node_id IN (%s)
	) ranked
	WHERE ranked.rn = 1
)
SELECT COUNT(1) AS total,
	   COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') = 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS rework_failures,
	   COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') != 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS failed_hard_failures,
	   COALESCE(SUM(CASE WHEN lwa.node_status IN ('cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS abnormal_terminal_count,
	   COALESCE(SUM(CASE WHEN lwa.node_status IN ('succeeded', 'failed', 'cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS settled_count
FROM latest_wave_attempts lwa
LEFT JOIN attempt_executions ae ON ae.node_run_id = lwa.node_run_id AND ae.run_id = ?`, placeholders)
	args = append(args, trimmedRunID)
	var total int
	var reworkFailures int
	var failedHardFailures int
	var abnormalTerminals int
	var settledCount int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&total, &reworkFailures, &failedHardFailures, &abnormalTerminals, &settledCount); err != nil {
		return result, fmt.Errorf("load review wave outcome for nodes run=%q wave=%d: %w", trimmedRunID, wave, err)
	}
	if total == 0 {
		result.Settled = true
		return result, nil
	}
	result.Settled = settledCount == total
	result.HasRework = reworkFailures > 0
	result.HasHardFailure = failedHardFailures > 0 || abnormalTerminals > 0
	return result, nil
}

func countReviewReworkWavesForNodes(ctx context.Context, q reviewWaveReadQuerier, runID string, nodeIDs []string, beforeWave int) (int, error) {
	if q == nil {
		return 0, fmt.Errorf("review wave querier is required")
	}
	if len(nodeIDs) == 0 {
		return 0, nil
	}
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return 0, fmt.Errorf("run_id is required")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")
	query := fmt.Sprintf(`
	WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.wave,
	       ranked.node_status,
	       ranked.node_run_id
	FROM (
		SELECT nr.node_id,
		       nr.wave,
		       nr.node_status,
		       nr.node_run_id,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		WHERE nr.run_id = ?
		  AND nr.node_id IN (%s)`, placeholders)
	args := make([]any, 0, len(nodeIDs)+2)
	args = append(args, trimmedRunID)
	for _, nodeID := range nodeIDs {
		args = append(args, nodeID)
	}
	if beforeWave > 0 {
		query += `
		  AND nr.wave < ?`
		args = append(args, beforeWave)
	}
	query += `
	) ranked
	WHERE ranked.rn = 1
), review_wave_summary AS (
	SELECT lwa.wave,
	       COUNT(1) AS total,
	       COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') = 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS rework_failures,
	       COALESCE(SUM(CASE WHEN lwa.node_status = 'failed' AND COALESCE(ae.failure_code, '') != 'execution_review_rework' THEN 1 ELSE 0 END), 0) AS failed_hard_failures,
	       COALESCE(SUM(CASE WHEN lwa.node_status IN ('cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS abnormal_terminal_count,
	       COALESCE(SUM(CASE WHEN lwa.node_status IN ('succeeded', 'failed', 'cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS settled_count
	FROM latest_wave_attempts lwa
	LEFT JOIN attempt_executions ae ON ae.node_run_id = lwa.node_run_id AND ae.run_id = ?
	GROUP BY lwa.wave
)
SELECT COUNT(1)
FROM review_wave_summary
WHERE rework_failures > 0
	AND failed_hard_failures = 0
	AND abnormal_terminal_count = 0
	AND settled_count = total`
	args = append(args, trimmedRunID)
	var count int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		if beforeWave > 0 {
			return 0, fmt.Errorf("count review rework waves before wave for nodes run=%q wave=%d: %w", trimmedRunID, beforeWave, err)
		}
		return 0, fmt.Errorf("count review rework waves for nodes run=%q: %w", trimmedRunID, err)
	}
	return count, nil
}

func normalizeWaveNodeIDs(nodeIDs []string) []string {
	if len(nodeIDs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(nodeIDs))
	trimmed := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		candidate := strings.TrimSpace(nodeID)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		trimmed = append(trimmed, candidate)
	}
	sort.Strings(trimmed)
	return trimmed
}
