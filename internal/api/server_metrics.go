package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	coreworkflow "kingdom/internal/core/workflow"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

type tokenSummaryPayload struct {
	ProviderTotalTokens int64 `json:"provider_total_tokens"`
	DerivedTotalTokens  int64 `json:"derived_total_tokens"`
	Input               int64 `json:"input"`
	Output              int64 `json:"output"`
	Reasoning           int64 `json:"reasoning"`
	CacheRead           int64 `json:"cache_read"`
	CacheWrite          int64 `json:"cache_write"`
	RecordCount         int   `json:"record_count"`
}

type tokenByRolePayload struct {
	RequiredRole        string                `json:"required_role"`
	Model               string                `json:"model,omitempty"`
	ThinkingEffort      string                `json:"thinking_effort,omitempty"`
	MixedModel          bool                  `json:"mixed_model,omitempty"`
	MixedThinkingEffort bool                  `json:"mixed_thinking_effort,omitempty"`
	ProviderTotalTokens int64                 `json:"provider_total_tokens"`
	DerivedTotalTokens  int64                 `json:"derived_total_tokens"`
	InputTokens         int64                 `json:"input_tokens"`
	OutputTokens        int64                 `json:"output_tokens"`
	ReasoningTokens     int64                 `json:"reasoning_tokens"`
	RecordCount         int                   `json:"record_count"`
	ByModel             []tokenByModelPayload `json:"by_model"`
}

type tokenByModelPayload struct {
	ProviderId          string `json:"provider_id,omitempty"`
	ProviderName        string `json:"provider_name,omitempty"`
	Model               string `json:"model,omitempty"`
	ModelName           string `json:"model_name,omitempty"`
	ThinkingEffort      string `json:"thinking_effort,omitempty"`
	ProviderTotalTokens int64  `json:"provider_total_tokens"`
	DerivedTotalTokens  int64  `json:"derived_total_tokens"`
	InputTokens         int64  `json:"input_tokens"`
	OutputTokens        int64  `json:"output_tokens"`
	ReasoningTokens     int64  `json:"reasoning_tokens"`
	RecordCount         int    `json:"record_count"`
	RunCount            int    `json:"run_count"`
}

type tokenModelMetadata struct {
	providerID   string
	providerName string
	modelName    string
}

func buildTokenSummaryPayload(summary state.TokenUsageSummary) tokenSummaryPayload {
	return tokenSummaryPayload{
		ProviderTotalTokens: summary.TotalTokens,
		DerivedTotalTokens:  summary.InputTokens + summary.OutputTokens + summary.ReasoningTokens,
		Input:               summary.InputTokens,
		Output:              summary.OutputTokens,
		Reasoning:           summary.ReasoningTokens,
		CacheRead:           summary.CacheReadTokens,
		CacheWrite:          summary.CacheWriteTokens,
		RecordCount:         summary.RecordCount,
	}
}

func buildTokenByRolePayload(rows []state.TokenUsageByRole, metadataByModel map[string]tokenModelMetadata) []tokenByRolePayload {
	result := make([]tokenByRolePayload, 0, len(rows))
	for _, row := range rows {
		result = append(result, tokenByRolePayload{
			RequiredRole:        row.RequiredRole,
			Model:               strings.TrimSpace(row.Model),
			ThinkingEffort:      strings.TrimSpace(row.Variant),
			MixedModel:          row.MixedModel,
			MixedThinkingEffort: row.MixedVariant,
			ProviderTotalTokens: row.TotalTokens,
			DerivedTotalTokens:  row.InputTokens + row.OutputTokens + row.ReasoningTokens,
			InputTokens:         row.InputTokens,
			OutputTokens:        row.OutputTokens,
			ReasoningTokens:     row.ReasoningTokens,
			RecordCount:         row.RecordCount,
			ByModel:             buildTokenByModelPayload(row.ByModel, metadataByModel),
		})
	}
	return result
}

func buildTokenByModelPayload(rows []state.TokenUsageByModel, metadataByModel map[string]tokenModelMetadata) []tokenByModelPayload {
	result := make([]tokenByModelPayload, 0, len(rows))
	for _, row := range rows {
		providerID, modelName := splitProviderQualifiedModel(row.Model)
		providerName := providerDisplayName(providerID)
		if metadata, ok := metadataByModel[strings.TrimSpace(row.Model)]; ok {
			if trimmed := strings.TrimSpace(metadata.providerID); trimmed != "" {
				providerID = trimmed
			}
			if trimmed := strings.TrimSpace(metadata.providerName); trimmed != "" {
				providerName = trimmed
			}
			if trimmed := strings.TrimSpace(metadata.modelName); trimmed != "" {
				modelName = trimmed
			}
		}
		result = append(result, tokenByModelPayload{
			ProviderId:          providerID,
			ProviderName:        providerName,
			Model:               strings.TrimSpace(row.Model),
			ModelName:           modelDisplayName(row.Model, modelName),
			ThinkingEffort:      strings.TrimSpace(row.Variant),
			ProviderTotalTokens: row.TotalTokens,
			DerivedTotalTokens:  row.InputTokens + row.OutputTokens + row.ReasoningTokens,
			InputTokens:         row.InputTokens,
			OutputTokens:        row.OutputTokens,
			ReasoningTokens:     row.ReasoningTokens,
			RecordCount:         row.RecordCount,
			RunCount:            row.RunCount,
		})
	}
	return result
}

func buildTokenModelMetadataLookup(rows []gen.ProviderModel) map[string]tokenModelMetadata {
	lookup := make(map[string]tokenModelMetadata, len(rows))
	for _, row := range rows {
		modelID := strings.TrimSpace(row.Id)
		if modelID == "" {
			continue
		}
		lookup[modelID] = tokenModelMetadata{
			providerID:   strings.TrimSpace(row.ProviderId),
			providerName: strings.TrimSpace(row.ProviderName),
			modelName:    strings.TrimSpace(row.ModelName),
		}
	}
	return lookup
}

func splitProviderQualifiedModel(raw string) (string, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", ""
	}
	providerID, modelName, ok := strings.Cut(trimmed, "/")
	if !ok {
		return "", trimmed
	}
	return strings.TrimSpace(providerID), strings.TrimSpace(modelName)
}

func providerDisplayName(providerID string) string {
	trimmed := strings.TrimSpace(providerID)
	if trimmed == "" {
		return ""
	}
	known := map[string]string{
		"github-copilot": "GitHub Copilot",
		"github-models":  "GitHub Models",
		"openai":         "OpenAI",
		"opencode":       "OpenCode",
	}
	if label, ok := known[strings.ToLower(trimmed)]; ok {
		return label
	}
	replacer := strings.NewReplacer("-", " ", "_", " ")
	words := strings.Fields(replacer.Replace(trimmed))
	for i := range words {
		w := strings.ToLower(words[i])
		if w == "ai" {
			words[i] = "AI"
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	if len(words) == 0 {
		return ""
	}
	return strings.Join(words, " ")
}

func modelDisplayName(raw string, modelName string) string {
	if trimmed := strings.TrimSpace(modelName); trimmed != "" {
		return trimmed
	}
	_, fallback := splitProviderQualifiedModel(raw)
	return fallback
}

type sqlReadQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type runtimeMetricsPayload struct {
	Status            string                          `json:"status"`
	RunningRuns       int                             `json:"running_runs"`
	RunningNodes      int                             `json:"running_nodes"`
	StaleRunningNodes int                             `json:"stale_running_nodes"`
	LatestEventAt     string                          `json:"latest_event_at,omitempty"`
	Planner           *planning.PlannerRuntimeMetrics `json:"planner,omitempty"`
}

type dashboardTokenSummaryPayload struct {
	DerivedTotalTokens int64 `json:"derived_total_tokens"`
	Input              int64 `json:"input"`
	Output             int64 `json:"output"`
	Reasoning          int64 `json:"reasoning"`
}

type dashboardTrendPointPayload struct {
	BucketStartAt string `json:"bucket_start_at"`
	RunCount      int    `json:"run_count"`
}

type dashboardSnapshotPayload struct {
	SnapshotAt      string                       `json:"snapshot_at"`
	WindowMinutes   int                          `json:"window_minutes"`
	AllTimeRunCount int                          `json:"all_time_run_count"`
	Runtime         runtimeMetricsPayload        `json:"runtime"`
	Overview        analyticsOverviewPayload     `json:"overview"`
	Tokens          dashboardTokenSummaryPayload `json:"tokens"`
	RecentRuns      []gen.RunSummary             `json:"recent_runs"`
	RunTrend        []dashboardTrendPointPayload `json:"run_trend"`
}

type analyticsOverviewPayload struct {
	TotalRuns            int     `json:"total_runs"`
	SucceededRuns        int     `json:"succeeded_runs"`
	FailedRuns           int     `json:"failed_runs"`
	CancelledRuns        int     `json:"cancelled_runs"`
	AvgDurationSeconds   float64 `json:"avg_duration_seconds"`
	SuccessRate          float64 `json:"success_rate"`
	ReviewReworkRate     float64 `json:"review_rework_rate"`
	ValidationReworkRate float64 `json:"validation_rework_rate"`
}

type analyticsOutcomeBucketPayload struct {
	Bucket    string `json:"bucket"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Cancelled int    `json:"cancelled"`
}

type analyticsRoleDurationPayload struct {
	Role               string  `json:"role"`
	AvgDurationSeconds float64 `json:"avg_duration_seconds"`
}

type analyticsRunAnalyticsPayload struct {
	BucketType     string                          `json:"bucket_type"`
	OutcomeSeries  []analyticsOutcomeBucketPayload `json:"outcome_series"`
	DurationByRole []analyticsRoleDurationPayload  `json:"duration_by_role"`
	RecentRuns     []gen.RunSummary                `json:"recent_runs"`
}

type analyticsTokenSummaryPayload struct {
	ProviderTotalTokens int64                `json:"provider_total_tokens"`
	DerivedTotalTokens  int64                `json:"derived_total_tokens"`
	Input               int64                `json:"input"`
	Output              int64                `json:"output"`
	Reasoning           int64                `json:"reasoning"`
	CacheRead           int64                `json:"cache_read"`
	CacheWrite          int64                `json:"cache_write"`
	ByRole              []tokenByRolePayload `json:"by_role"`
}

type analyticsArtifactSummaryPayload struct {
	TotalCount     int   `json:"total_count"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
}

type analyticsNodeSummaryPayload struct {
	TotalExecuted int `json:"total_executed"`
}

type analyticsSelectedRunPayload struct {
	RunID          string `json:"run_id"`
	WorkOrderTitle string `json:"work_order_title"`
	Status         string `json:"status"`
	CreatedAt      string `json:"created_at"`
}

type analyticsExecutionDetailPayload struct {
	SelectedRun *analyticsSelectedRunPayload    `json:"selected_run,omitempty"`
	Tokens      analyticsTokenSummaryPayload    `json:"tokens"`
	Artifacts   analyticsArtifactSummaryPayload `json:"artifacts"`
	Nodes       analyticsNodeSummaryPayload     `json:"nodes"`
}

type analyticsSnapshotPayload struct {
	WindowStart     string                       `json:"window_start"`
	WindowEnd       string                       `json:"window_end"`
	SnapshotAt      string                       `json:"snapshot_at"`
	WindowMinutes   int                          `json:"window_minutes"`
	AllTimeRunCount int                          `json:"all_time_run_count"`
	Overview        analyticsOverviewPayload     `json:"overview"`
	RunAnalytics    analyticsRunAnalyticsPayload `json:"run_analytics"`
}

type analyticsExecutionDetailSnapshotPayload struct {
	WindowStart     string                          `json:"window_start"`
	WindowEnd       string                          `json:"window_end"`
	SnapshotAt      string                          `json:"snapshot_at"`
	WindowMinutes   int                             `json:"window_minutes"`
	ExecutionDetail analyticsExecutionDetailPayload `json:"execution_detail"`
}

var errAnalyticsSelectedRunNotFound = errors.New("realm analytics selected run not found")

func (s *Server) handleGetMetricsRuntime(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_metrics_runtime"); !ok {
		return
	}

	snapshotAt := s.now().UTC()
	runtimeMetrics, err := s.buildRuntimeMetrics(r.Context(), s.db, snapshotAt)
	if err != nil {
		s.writeInternalError(w, "fetch runtime metrics failed")
		return
	}
	plannerMetrics, err := s.loadPlannerRuntimeMetrics(r.Context())
	if err != nil {
		s.writeInternalError(w, "fetch runtime metrics failed")
		return
	}
	runtimeMetrics.Planner = plannerMetrics

	writeJSON(w, http.StatusOK, runtimeMetrics)
}

func (s *Server) handleGetDashboardSnapshot(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_dashboard_snapshot"); !ok {
		return
	}

	snapshot, err := s.buildDashboardSnapshot(r.Context())
	if err != nil {
		s.writeInternalError(w, "fetch dashboard snapshot failed")
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleGetAnalyticsSnapshot(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_realm_analytics_snapshot"); !ok {
		return
	}
	snapshotAt := s.now().UTC()
	window, ok := s.parseAnalyticsWindow(w, r, snapshotAt)
	if !ok {
		return
	}

	snapshot, err := s.buildAnalyticsSnapshot(r.Context(), snapshotAt, window)
	if err != nil {
		s.writeInternalError(w, "fetch realm analytics snapshot failed")
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleGetAnalyticsExecutionDetailSnapshot(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_realm_analytics_execution_detail_snapshot"); !ok {
		return
	}
	snapshotAt := s.now().UTC()
	window, ok := s.parseAnalyticsWindow(w, r, snapshotAt)
	if !ok {
		return
	}
	selectedRunID := strings.TrimSpace(r.URL.Query().Get("run_id"))

	snapshot, err := s.buildAnalyticsExecutionDetailSnapshot(r.Context(), snapshotAt, window, selectedRunID)
	if err != nil {
		if errors.Is(err, errAnalyticsSelectedRunNotFound) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_realm_analytics_execution_detail_snapshot", map[string]any{"run_id": selectedRunID})
			return
		}
		s.writeInternalError(w, "fetch realm analytics execution detail snapshot failed")
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) buildDashboardSnapshot(ctx context.Context) (dashboardSnapshotPayload, error) {
	snapshotAt := s.now().UTC()
	windowStartAt := snapshotAt.Add(-24 * time.Hour)
	windowStart := windowStartAt.Format(time.RFC3339)
	plannerMetrics, err := s.loadPlannerRuntimeMetrics(ctx)
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return dashboardSnapshotPayload{}, fmt.Errorf("begin dashboard snapshot tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	runtimeMetrics, err := s.buildRuntimeMetrics(ctx, tx, snapshotAt)
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	runtimeMetrics.Planner = plannerMetrics
	allTimeRunCount, err := loadAllTimeRunCount(ctx, tx)
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	tokenSummary, err := snapshotScopedTokenStore(s.tokenUsageStore, tx).SummaryByTimeWindowExecutionAnalytics(ctx, windowStart, snapshotAt.Format(time.RFC3339Nano))
	if err != nil {
		return dashboardSnapshotPayload{}, fmt.Errorf("query dashboard token summary: %w", err)
	}
	overview, err := loadAnalyticsOverview(ctx, tx, windowStart, snapshotAt.Format(time.RFC3339Nano))
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	recentRuns, err := loadDashboardRecentRuns(ctx, tx, windowStart, 10)
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	runTrend, err := loadDashboardTrend(ctx, tx, windowStartAt, snapshotAt)
	if err != nil {
		return dashboardSnapshotPayload{}, err
	}
	if err := tx.Commit(); err != nil {
		return dashboardSnapshotPayload{}, fmt.Errorf("commit dashboard snapshot tx: %w", err)
	}
	windowTokenSummary := buildTokenSummaryPayload(tokenSummary)

	return dashboardSnapshotPayload{
		SnapshotAt:      snapshotAt.Format(time.RFC3339),
		WindowMinutes:   1440,
		AllTimeRunCount: allTimeRunCount,
		Runtime:         runtimeMetrics,
		Overview:        overview,
		Tokens: dashboardTokenSummaryPayload{
			DerivedTotalTokens: windowTokenSummary.DerivedTotalTokens,
			Input:              windowTokenSummary.Input,
			Output:             windowTokenSummary.Output,
			Reasoning:          windowTokenSummary.Reasoning,
		},
		RecentRuns: recentRuns,
		RunTrend:   runTrend,
	}, nil
}

func (s *Server) buildAnalyticsSnapshot(ctx context.Context, snapshotAt time.Time, window analyticsWindow) (analyticsSnapshotPayload, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return analyticsSnapshotPayload{}, fmt.Errorf("begin realm analytics snapshot tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	allTimeRunCount, err := loadAllTimeRunCount(ctx, tx)
	if err != nil {
		return analyticsSnapshotPayload{}, err
	}
	overview, err := loadAnalyticsOverview(ctx, tx, window.Start, window.End)
	if err != nil {
		return analyticsSnapshotPayload{}, err
	}
	runAnalytics, err := loadAnalyticsRunAnalytics(ctx, tx, window.Start, window.End, window.Minutes)
	if err != nil {
		return analyticsSnapshotPayload{}, err
	}
	if err := tx.Commit(); err != nil {
		return analyticsSnapshotPayload{}, fmt.Errorf("commit realm analytics snapshot tx: %w", err)
	}

	return analyticsSnapshotPayload{
		SnapshotAt:      snapshotAt.Format(time.RFC3339),
		WindowMinutes:   window.Minutes,
		WindowStart:     window.Start,
		WindowEnd:       window.End,
		AllTimeRunCount: allTimeRunCount,
		Overview:        overview,
		RunAnalytics:    runAnalytics,
	}, nil
}

func (s *Server) buildAnalyticsExecutionDetailSnapshot(ctx context.Context, snapshotAt time.Time, window analyticsWindow, selectedRunID string) (analyticsExecutionDetailSnapshotPayload, error) {
	metadataByModel := s.loadTokenModelMetadata(ctx)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return analyticsExecutionDetailSnapshotPayload{}, fmt.Errorf("begin realm analytics execution detail tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	tokenStore := snapshotScopedTokenStore(s.tokenUsageStore, tx)

	executionDetail, err := s.loadAnalyticsExecutionDetail(ctx, tx, tokenStore, metadataByModel, window.Start, window.End, selectedRunID)
	if err != nil {
		return analyticsExecutionDetailSnapshotPayload{}, err
	}
	if err := tx.Commit(); err != nil {
		return analyticsExecutionDetailSnapshotPayload{}, fmt.Errorf("commit realm analytics execution detail tx: %w", err)
	}

	return analyticsExecutionDetailSnapshotPayload{
		SnapshotAt:      snapshotAt.Format(time.RFC3339),
		WindowMinutes:   window.Minutes,
		WindowStart:     window.Start,
		WindowEnd:       window.End,
		ExecutionDetail: executionDetail,
	}, nil
}

type analyticsWindow struct {
	Start   string
	End     string
	Minutes int
}

func (s *Server) parseAnalyticsWindow(w http.ResponseWriter, r *http.Request, snapshotAt time.Time) (analyticsWindow, bool) {
	query := r.URL.Query()
	if query.Has("start_at") || query.Has("end_at") {
		if query.Has("window_minutes") || len(query["start_at"]) != 1 || len(query["end_at"]) != 1 {
			s.writeBadRequest(w, "start_at and end_at must be supplied together without window_minutes")
			return analyticsWindow{}, false
		}
		start, startErr := time.Parse(time.RFC3339Nano, query.Get("start_at"))
		end, endErr := time.Parse(time.RFC3339Nano, query.Get("end_at"))
		if startErr != nil || endErr != nil {
			s.writeBadRequest(w, "start_at and end_at must be RFC3339 timestamps with a time zone")
			return analyticsWindow{}, false
		}
		if !end.After(start) {
			s.writeBadRequest(w, "end_at must be after start_at")
			return analyticsWindow{}, false
		}
		// Unix seconds preserve long calendar intervals that overflow time.Duration.
		seconds := end.Unix() - start.Unix()
		if end.Nanosecond() > start.Nanosecond() {
			seconds++
		}
		return analyticsWindow{Start: start.UTC().Format(time.RFC3339Nano), End: end.UTC().Format(time.RFC3339Nano), Minutes: int((seconds + 59) / 60)}, true
	}
	minutes := 1440
	if rawMinutes := strings.TrimSpace(query.Get("window_minutes")); rawMinutes != "" {
		parsed, err := strconv.Atoi(rawMinutes)
		if err != nil || parsed < 1 || parsed > 525600 || len(query["window_minutes"]) != 1 {
			s.writeBadRequest(w, "window_minutes must be an integer between 1 and 525600")
			return analyticsWindow{}, false
		}
		minutes = parsed
	}
	return analyticsWindow{Start: snapshotAt.Add(-time.Duration(minutes) * time.Minute).Format(time.RFC3339Nano), End: snapshotAt.Format(time.RFC3339Nano), Minutes: minutes}, true
}

func (s *Server) buildRuntimeMetrics(ctx context.Context, q sqlReadQuerier, snapshotAt time.Time) (runtimeMetricsPayload, error) {
	runningRuns, err := scalarInt(ctx, q, `SELECT COUNT(1) FROM workflow_runs WHERE run_status = 'running'`)
	if err != nil {
		return runtimeMetricsPayload{}, fmt.Errorf("query running runs: %w", err)
	}
	runningNodes, err := scalarInt(ctx, q, `SELECT COUNT(1) FROM node_runs WHERE node_status IN ('starting', 'running')`)
	if err != nil {
		return runtimeMetricsPayload{}, fmt.Errorf("query running nodes: %w", err)
	}
	staleRunningNodes, err := state.CountStaleAttemptsForMetrics(ctx, q, snapshotAt, 15*time.Minute)
	if err != nil {
		return runtimeMetricsPayload{}, fmt.Errorf("query stale running nodes: %w", err)
	}
	var latestEventAt sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT MAX(created_at) FROM events`).Scan(&latestEventAt); err != nil {
		return runtimeMetricsPayload{}, fmt.Errorf("query latest event timestamp: %w", err)
	}

	runtimeMetrics := runtimeMetricsPayload{
		Status:            deriveRuntimeMetricsStatus(staleRunningNodes),
		RunningRuns:       runningRuns,
		RunningNodes:      runningNodes,
		StaleRunningNodes: staleRunningNodes,
	}
	if latestEventAt.Valid {
		runtimeMetrics.LatestEventAt = strings.TrimSpace(latestEventAt.String)
	}
	return runtimeMetrics, nil
}

func deriveRuntimeMetricsStatus(staleRunningNodes int) string {
	if staleRunningNodes > 0 {
		return "degraded"
	}
	return "ok"
}

func loadAllTimeRunCount(ctx context.Context, q sqlReadQuerier) (int, error) {
	count, err := scalarInt(ctx, q, `SELECT COUNT(1) FROM workflow_runs`)
	if err != nil {
		return 0, fmt.Errorf("query all-time run count: %w", err)
	}
	return count, nil
}

func (s *Server) loadPlannerRuntimeMetrics(ctx context.Context) (*planning.PlannerRuntimeMetrics, error) {
	if s.planningService == nil {
		return nil, nil
	}
	plannerMetrics, err := s.planningService.RuntimeMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("query planner runtime metrics: %w", err)
	}
	return &plannerMetrics, nil
}

func loadDashboardRecentRuns(ctx context.Context, q sqlReadQuerier, since string, limit int) ([]gen.RunSummary, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT wr.run_id,
		        wd.workflow_name,
		        wr.work_order_id,
		        COALESCE(NULLIF(d.work_order_title, ''), wr.run_id),
		        wr.run_status,
		        wr.plan_state,
		        wr.created_at,
		        wr.updated_at,
		        ra.last_activity_at,
		        COALESCE(wr.autonomous_approvals, 1), `+state.RunStartedAtSelectExpr("wr")+`, `+state.RunStartStateSelectExpr("wr")+`
		 FROM workflow_runs wr
		 JOIN run_activity ra ON ra.run_id = wr.run_id
		 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
		 LEFT JOIN work_orders d ON d.work_order_id = wr.work_order_id
		 WHERE wr.created_at >= ?
		 ORDER BY ra.last_activity_at DESC, wr.run_id DESC
		 LIMIT ?`,
		since,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query dashboard recent runs: %w", err)
	}
	defer rows.Close()

	items, err := scanRunSummaryRows(rows, limit)
	if err != nil {
		return nil, fmt.Errorf("scan dashboard recent runs: %w", err)
	}
	return items, nil
}

func scanRunSummaryRows(rows *sql.Rows, limit int) ([]gen.RunSummary, error) {
	items := make([]gen.RunSummary, 0, limit)
	for rows.Next() {
		var summary gen.RunSummary
		var createdAt string
		var updatedAt string
		var lastActivityAt string
		var startedAt sql.NullString
		var autonomousApprovalsInt int
		if err := rows.Scan(
			&summary.RunId,
			&summary.WorkflowId,
			&summary.WorkOrderId,
			&summary.WorkOrderTitle,
			&summary.Status,
			&summary.PlanState,
			&createdAt,
			&updatedAt,
			&lastActivityAt,
			&autonomousApprovalsInt,
			&startedAt,
			&summary.StartState,
		); err != nil {
			return nil, err
		}
		summary.CreatedAt = mustParseRFC3339(createdAt)
		summary.UpdatedAt = mustParseRFC3339(updatedAt)
		summary.LastActivityAt = mustParseRFC3339(lastActivityAt)
		if startedAt.Valid {
			parsed := mustParseRFC3339(startedAt.String)
			summary.StartedAt = &parsed
		}
		autonomousApprovals := autonomousApprovalsInt != 0
		summary.AutonomousApprovals = &autonomousApprovals
		summary.DisplayState = gen.RunDisplayState(state.DeriveRunDisplayState(string(summary.Status), string(summary.PlanState)))
		items = append(items, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func loadDashboardTrend(ctx context.Context, q sqlReadQuerier, windowStart time.Time, snapshotAt time.Time) ([]dashboardTrendPointPayload, error) {
	windowStart = windowStart.UTC()
	snapshotAt = snapshotAt.UTC()
	const bucketWidth = 2 * time.Hour
	const bucketCount = 12
	const bucketWidthSeconds = int(bucketWidth / time.Second)
	windowStartRFC3339 := windowStart.Format(time.RFC3339)
	snapshotAtRFC3339 := snapshotAt.Format(time.RFC3339)

	rows, err := q.QueryContext(ctx,
		`SELECT CASE
		          WHEN unixepoch(created_at) = unixepoch(?) THEN 11
		          ELSE CAST((unixepoch(created_at) - unixepoch(?)) / ? AS INTEGER)
		        END AS bucket_index,
		        COUNT(1)
		 FROM workflow_runs
		 WHERE created_at >= ?
		   AND created_at <= ?
		 GROUP BY bucket_index
		 ORDER BY bucket_index ASC`,
		snapshotAtRFC3339,
		windowStartRFC3339,
		bucketWidthSeconds,
		windowStartRFC3339,
		snapshotAtRFC3339,
	)
	if err != nil {
		return nil, fmt.Errorf("query dashboard trend: %w", err)
	}
	defer rows.Close()

	trend := make([]dashboardTrendPointPayload, bucketCount)
	for idx := range trend {
		bucketStart := windowStart.Add(time.Duration(idx) * bucketWidth)
		trend[idx] = dashboardTrendPointPayload{BucketStartAt: bucketStart.Format(time.RFC3339)}
	}

	for rows.Next() {
		var bucketIndex int
		var runCount int
		if err := rows.Scan(&bucketIndex, &runCount); err != nil {
			return nil, fmt.Errorf("scan dashboard trend row: %w", err)
		}
		if bucketIndex < 0 || bucketIndex >= len(trend) {
			continue
		}
		trend[bucketIndex].RunCount = runCount
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dashboard trend rows: %w", err)
	}
	return trend, nil
}

func loadAnalyticsOverview(ctx context.Context, q sqlReadQuerier, since, until string) (analyticsOverviewPayload, error) {
	var overview analyticsOverviewPayload
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(1),
		        COALESCE(SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN run_status = 'cancelled' THEN 1 ELSE 0 END), 0),
		        COALESCE(AVG(CASE WHEN run_status IN ('succeeded', 'failed', 'cancelled')
		            THEN (julianday(updated_at) - julianday(created_at)) * 86400.0 END), 0)
		 FROM workflow_runs
		 WHERE julianday(created_at) >= julianday(?) AND julianday(created_at) < julianday(?)`,
		since, until,
	).Scan(&overview.TotalRuns, &overview.SucceededRuns, &overview.FailedRuns, &overview.CancelledRuns, &overview.AvgDurationSeconds); err != nil {
		return analyticsOverviewPayload{}, fmt.Errorf("query analytics overview runs: %w", err)
	}
	terminalRuns := overview.SucceededRuns + overview.FailedRuns + overview.CancelledRuns
	if terminalRuns > 0 {
		overview.SuccessRate = float64(overview.SucceededRuns) / float64(terminalRuns) * 100.0
	}
	var runsWithRework int
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT wr.run_id)
		 FROM workflow_runs wr
		 JOIN node_runs nr ON nr.run_id = wr.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE julianday(wr.created_at) >= julianday(?) AND julianday(wr.created_at) < julianday(?)
		   AND wn.node_class = 'execution_review'
		   AND nr.node_status = 'failed'`,
		since, until,
	).Scan(&runsWithRework); err != nil {
		return analyticsOverviewPayload{}, fmt.Errorf("query analytics rework incidence: %w", err)
	}
	if overview.TotalRuns > 0 {
		overview.ReviewReworkRate = float64(runsWithRework) / float64(overview.TotalRuns) * 100.0
	}
	var runsWithValidationFailures int
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT nr.run_id)
		 FROM node_runs nr
		 JOIN workflow_runs wr ON wr.run_id = nr.run_id
		 JOIN workflow_nodes wn ON wn.node_id = nr.node_id AND wn.workflow_definition_id = wr.workflow_definition_id
		 WHERE wn.node_class = 'validation_gates'
		   AND nr.node_status = 'failed'
		   AND julianday(wr.created_at) >= julianday(?) AND julianday(wr.created_at) < julianday(?)`,
		since, until,
	).Scan(&runsWithValidationFailures); err != nil {
		return analyticsOverviewPayload{}, fmt.Errorf("query analytics validation incidence: %w", err)
	}
	if overview.TotalRuns > 0 {
		overview.ValidationReworkRate = float64(runsWithValidationFailures) / float64(overview.TotalRuns) * 100.0
	}
	return overview, nil
}

func loadAnalyticsRunAnalytics(ctx context.Context, q sqlReadQuerier, since, until string, windowMinutes int) (analyticsRunAnalyticsPayload, error) {
	bucketType := "day"
	bucketExpr := "strftime('%Y-%m-%d', created_at)"
	if windowMinutes <= 24*60 {
		bucketType = "hour"
		bucketExpr = "strftime('%Y-%m-%dT%H:00:00Z', created_at)"
	}
	outcomeRows, err := q.QueryContext(ctx, fmt.Sprintf(
		`SELECT %s AS bucket,
		        COALESCE(SUM(CASE WHEN run_status = 'succeeded' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END), 0),
		        COALESCE(SUM(CASE WHEN run_status = 'cancelled' THEN 1 ELSE 0 END), 0)
		 FROM workflow_runs
		 WHERE julianday(created_at) >= julianday(?) AND julianday(created_at) < julianday(?) AND run_status IN ('succeeded', 'failed', 'cancelled')
		 GROUP BY bucket
		 ORDER BY bucket ASC`,
		bucketExpr,
	), since, until)
	if err != nil {
		return analyticsRunAnalyticsPayload{}, fmt.Errorf("query analytics outcome series: %w", err)
	}
	defer outcomeRows.Close()
	outcomeSeries := make([]analyticsOutcomeBucketPayload, 0)
	for outcomeRows.Next() {
		var bucket analyticsOutcomeBucketPayload
		if err := outcomeRows.Scan(&bucket.Bucket, &bucket.Succeeded, &bucket.Failed, &bucket.Cancelled); err != nil {
			return analyticsRunAnalyticsPayload{}, fmt.Errorf("scan analytics outcome series row: %w", err)
		}
		outcomeSeries = append(outcomeSeries, bucket)
	}
	if err := outcomeRows.Err(); err != nil {
		return analyticsRunAnalyticsPayload{}, fmt.Errorf("iterate analytics outcome series rows: %w", err)
	}

	nodeClassFilter, nodeClassArgs := executionAnalyticsNodeClassFilterSQL("wn")
	roleArgs := make([]any, 0, 2+len(nodeClassArgs))
	roleArgs = append(roleArgs, since, until)
	roleArgs = append(roleArgs, nodeClassArgs...)
	roleRows, err := q.QueryContext(ctx,
		fmt.Sprintf(`SELECT nr.required_role,
		        AVG((julianday(nr.finished_at) - julianday(nr.started_at)) * 86400.0)
		 FROM node_runs nr
		 JOIN workflow_runs wr ON wr.run_id = nr.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE nr.started_at IS NOT NULL AND nr.finished_at IS NOT NULL
		   AND julianday(nr.started_at) >= julianday(?) AND julianday(nr.started_at) < julianday(?)
		   AND %s
		 GROUP BY nr.required_role
		 ORDER BY AVG((julianday(nr.finished_at) - julianday(nr.started_at)) * 86400.0) DESC`, nodeClassFilter),
		roleArgs...,
	)
	if err != nil {
		return analyticsRunAnalyticsPayload{}, fmt.Errorf("query analytics duration by role: %w", err)
	}
	defer roleRows.Close()
	durationByRole := make([]analyticsRoleDurationPayload, 0)
	for roleRows.Next() {
		var row analyticsRoleDurationPayload
		if err := roleRows.Scan(&row.Role, &row.AvgDurationSeconds); err != nil {
			return analyticsRunAnalyticsPayload{}, fmt.Errorf("scan analytics duration by role row: %w", err)
		}
		durationByRole = append(durationByRole, row)
	}
	if err := roleRows.Err(); err != nil {
		return analyticsRunAnalyticsPayload{}, fmt.Errorf("iterate analytics duration by role rows: %w", err)
	}

	recentRuns, err := loadAnalyticsRecentRuns(ctx, q, since, until, 20)
	if err != nil {
		return analyticsRunAnalyticsPayload{}, fmt.Errorf("query analytics recent runs: %w", err)
	}

	return analyticsRunAnalyticsPayload{
		BucketType:     bucketType,
		OutcomeSeries:  outcomeSeries,
		DurationByRole: durationByRole,
		RecentRuns:     recentRuns,
	}, nil
}

func loadAnalyticsRecentRuns(ctx context.Context, q sqlReadQuerier, since, until string, limit int) ([]gen.RunSummary, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT wr.run_id,
		        wd.workflow_name,
		        wr.work_order_id,
		        COALESCE(NULLIF(d.work_order_title, ''), wr.run_id),
		        wr.run_status,
		        wr.plan_state,
		        wr.created_at,
		        wr.updated_at,
		        ra.last_activity_at,
		        COALESCE(wr.autonomous_approvals, 1), `+state.RunStartedAtSelectExpr("wr")+`, `+state.RunStartStateSelectExpr("wr")+`
		 FROM workflow_runs wr
		 JOIN run_activity ra ON ra.run_id = wr.run_id
		 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
		 LEFT JOIN work_orders d ON d.work_order_id = wr.work_order_id
		 WHERE julianday(wr.created_at) >= julianday(?) AND julianday(wr.created_at) < julianday(?)
		   AND wr.run_status IN ('succeeded', 'failed', 'cancelled')
		 ORDER BY wr.updated_at DESC, wr.run_id DESC
		 LIMIT ?`,
		since, until,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRunSummaryRows(rows, limit)
}

func (s *Server) loadAnalyticsExecutionDetail(
	ctx context.Context,
	q sqlReadQuerier,
	tokenStore *state.TokenUsageStore,
	metadataByModel map[string]tokenModelMetadata,
	since, until string,
	selectedRunID string,
) (analyticsExecutionDetailPayload, error) {
	if selectedRunID != "" {
		selectedRun, err := loadAnalyticsSelectedRun(ctx, q, selectedRunID)
		if err != nil {
			return analyticsExecutionDetailPayload{}, err
		}
		tokenSummary, err := tokenStore.SummaryByRunExecutionAnalytics(ctx, selectedRunID)
		if err != nil {
			return analyticsExecutionDetailPayload{}, fmt.Errorf("query selected run token summary: %w", err)
		}
		byRole, err := tokenStore.ByRoleForRunExecutionAnalytics(ctx, selectedRunID)
		if err != nil {
			return analyticsExecutionDetailPayload{}, fmt.Errorf("query selected run token breakdown: %w", err)
		}
		artifacts, err := loadArtifactSummaryForRun(ctx, q, selectedRunID)
		if err != nil {
			return analyticsExecutionDetailPayload{}, err
		}
		nodes, err := loadExecutionNodeSummaryForRun(ctx, q, selectedRunID)
		if err != nil {
			return analyticsExecutionDetailPayload{}, err
		}
		payload := buildAnalyticsTokenSummaryPayload(tokenSummary, byRole, metadataByModel)
		return analyticsExecutionDetailPayload{
			SelectedRun: selectedRun,
			Tokens:      payload,
			Artifacts:   artifacts,
			Nodes:       nodes,
		}, nil
	}

	tokenSummary, err := tokenStore.SummaryByTimeWindowExecutionAnalytics(ctx, since, until)
	if err != nil {
		return analyticsExecutionDetailPayload{}, fmt.Errorf("query aggregate token summary: %w", err)
	}
	byRole, err := tokenStore.ByRoleExecutionAnalytics(ctx, since, until)
	if err != nil {
		return analyticsExecutionDetailPayload{}, fmt.Errorf("query aggregate token breakdown: %w", err)
	}
	artifacts, err := loadArtifactSummaryWindow(ctx, q, since, until)
	if err != nil {
		return analyticsExecutionDetailPayload{}, err
	}
	nodes, err := loadExecutionNodeSummaryWindow(ctx, q, since, until)
	if err != nil {
		return analyticsExecutionDetailPayload{}, err
	}
	return analyticsExecutionDetailPayload{
		Tokens:    buildAnalyticsTokenSummaryPayload(tokenSummary, byRole, metadataByModel),
		Artifacts: artifacts,
		Nodes:     nodes,
	}, nil
}

func buildAnalyticsTokenSummaryPayload(summary state.TokenUsageSummary, byRole []state.TokenUsageByRole, metadataByModel map[string]tokenModelMetadata) analyticsTokenSummaryPayload {
	tokenSummary := buildTokenSummaryPayload(summary)
	return analyticsTokenSummaryPayload{
		ProviderTotalTokens: tokenSummary.ProviderTotalTokens,
		DerivedTotalTokens:  tokenSummary.DerivedTotalTokens,
		Input:               tokenSummary.Input,
		Output:              tokenSummary.Output,
		Reasoning:           tokenSummary.Reasoning,
		CacheRead:           tokenSummary.CacheRead,
		CacheWrite:          tokenSummary.CacheWrite,
		ByRole:              buildTokenByRolePayload(byRole, metadataByModel),
	}
}

func loadAnalyticsSelectedRun(ctx context.Context, q sqlReadQuerier, runID string) (*analyticsSelectedRunPayload, error) {
	var selectedRun analyticsSelectedRunPayload
	err := q.QueryRowContext(ctx,
		`SELECT wr.run_id,
		        COALESCE(NULLIF(d.work_order_title, ''), wr.run_id),
		        wr.run_status,
		        wr.created_at
		 FROM workflow_runs wr
		 LEFT JOIN work_orders d ON d.work_order_id = wr.work_order_id
		 WHERE wr.run_id = ?`,
		runID,
	).Scan(&selectedRun.RunID, &selectedRun.WorkOrderTitle, &selectedRun.Status, &selectedRun.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errAnalyticsSelectedRunNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query selected run summary: %w", err)
	}
	return &selectedRun, nil
}

func loadArtifactSummaryWindow(ctx context.Context, q sqlReadQuerier, since, until string) (analyticsArtifactSummaryPayload, error) {
	return loadArtifactSummary(ctx, q, `julianday(created_at) >= julianday(?) AND julianday(created_at) < julianday(?)`, since, until)
}

func loadArtifactSummaryForRun(ctx context.Context, q sqlReadQuerier, runID string) (analyticsArtifactSummaryPayload, error) {
	return loadArtifactSummary(ctx, q, `run_id = ?`, runID)
}

func loadArtifactSummary(ctx context.Context, q sqlReadQuerier, predicate string, args ...any) (analyticsArtifactSummaryPayload, error) {
	var summary analyticsArtifactSummaryPayload
	err := q.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COALESCE(COUNT(1), 0), COALESCE(SUM(size_bytes), 0)
		 FROM artifacts
		 WHERE %s`, predicate),
		args...,
	).Scan(&summary.TotalCount, &summary.TotalSizeBytes)
	if err != nil {
		return analyticsArtifactSummaryPayload{}, fmt.Errorf("query artifact summary: %w", err)
	}
	return summary, nil
}

func loadExecutionNodeSummaryWindow(ctx context.Context, q sqlReadQuerier, since, until string) (analyticsNodeSummaryPayload, error) {
	return loadExecutionNodeSummary(ctx, q, `julianday(nr.started_at) >= julianday(?) AND julianday(nr.started_at) < julianday(?)`, since, until)
}

func loadExecutionNodeSummaryForRun(ctx context.Context, q sqlReadQuerier, runID string) (analyticsNodeSummaryPayload, error) {
	return loadExecutionNodeSummary(ctx, q, `nr.run_id = ? AND nr.started_at IS NOT NULL`, runID)
}

func loadExecutionNodeSummary(ctx context.Context, q sqlReadQuerier, predicate string, filterArgs ...any) (analyticsNodeSummaryPayload, error) {
	var summary analyticsNodeSummaryPayload
	nodeClassFilter, nodeClassArgs := executionAnalyticsNodeClassFilterSQL("wn")
	args := make([]any, 0, len(filterArgs)+len(nodeClassArgs))
	args = append(args, filterArgs...)
	args = append(args, nodeClassArgs...)
	err := q.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(1)
		 FROM node_runs nr
		 JOIN workflow_runs wr ON wr.run_id = nr.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE %s
		   AND %s`, predicate, nodeClassFilter),
		args...,
	).Scan(&summary.TotalExecuted)
	if err != nil {
		return analyticsNodeSummaryPayload{}, fmt.Errorf("query execution node summary: %w", err)
	}
	return summary, nil
}

func executionAnalyticsNodeClassFilterSQL(alias string) (string, []any) {
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

func snapshotScopedTokenStore(store *state.TokenUsageStore, q sqlReadQuerier) *state.TokenUsageStore {
	return store.WithQuerier(q)
}

func (s *Server) loadTokenModelMetadata(ctx context.Context) map[string]tokenModelMetadata {
	if s.providerModelCatalog == nil {
		return map[string]tokenModelMetadata{}
	}
	providerModels, err := s.providerModelCatalog.List(ctx, false)
	if err != nil {
		return map[string]tokenModelMetadata{}
	}
	return buildTokenModelMetadataLookup(providerModels)
}

func scalarInt(ctx context.Context, q sqlReadQuerier, query string, args ...any) (int, error) {
	var value int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}
