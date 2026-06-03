package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
)

const (
	DefaultMaxAttemptsPerRun = 60
	DefaultMaxTokensPerRun   = int64(1_500_000)
	DefaultMaxWallTimePerRun = 2 * time.Hour
)

type RunBudgetLimits struct {
	MaxAttemptsPerRun int
	MaxTokensPerRun   int64
	MaxWallTimePerRun time.Duration
}

type EnforceRunBudgetRequest struct {
	RunID string

	Limits RunBudgetLimits

	OccurredAt string

	ActorID         string
	RoleID          string
	AuthoritySource string
	CorrelationID   string
	CausationID     string
}

type RunBudgetObservation struct {
	AttemptsUsed int
	TokensUsed   int64
	WallTimeUsed time.Duration
}

type EnforceRunBudgetResult struct {
	RunID        string
	Exceeded     bool
	ExceededCap  string
	Limit        int64
	Observed     int64
	Observation  RunBudgetObservation
	EscalateTo   string
	OccurredAt   string
	RunStatusNow string
}

type BudgetExceededError struct {
	Code        errorcatalog.Code
	RunID       string
	ExceededCap string
	Limit       int64
	Observed    int64
}

func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf("%s: run %q exceeded %s budget (limit=%d observed=%d)", e.Code, e.RunID, e.ExceededCap, e.Limit, e.Observed)
}

type BudgetGuard struct {
	db              *sql.DB
	transition      *TransitionService
	tokenUsageStore *TokenUsageStore
	now             func() time.Time
}

func NewBudgetGuard(db *sql.DB, eventSchemas *schemas.Set) *BudgetGuard {
	return NewBudgetGuardWithRoleCommunicationPolicy(db, rolecomm.Policy{}, eventSchemas)
}

func NewBudgetGuardWithRoleCommunicationPolicy(db *sql.DB, policy rolecomm.Policy, eventSchemas *schemas.Set) *BudgetGuard {
	return &BudgetGuard{
		db:              db,
		transition:      NewTransitionService(db, eventSchemas).WithRoleCommunicationPolicy(policy),
		tokenUsageStore: NewTokenUsageStore(db),
		now:             time.Now,
	}
}

func (g *BudgetGuard) EnforceRunBudget(ctx context.Context, req EnforceRunBudgetRequest) (EnforceRunBudgetResult, error) {
	if err := validateEnforceRunBudgetRequest(req); err != nil {
		return EnforceRunBudgetResult{}, err
	}

	limits := normalizeRunBudgetLimits(req.Limits)
	nowTime, err := parseBudgetTimestamp(req.OccurredAt, g.now)
	if err != nil {
		return EnforceRunBudgetResult{}, err
	}

	runStatus, runCreatedAt, err := g.loadRunBudgetContext(ctx, req.RunID)
	if err != nil {
		return EnforceRunBudgetResult{}, err
	}

	observation, err := g.observeUsage(ctx, req.RunID, nowTime, runCreatedAt)
	if err != nil {
		return EnforceRunBudgetResult{}, err
	}

	result := EnforceRunBudgetResult{
		RunID: req.RunID,
		Observation: RunBudgetObservation{
			AttemptsUsed: observation.AttemptsUsed,
			TokensUsed:   observation.TokensUsed,
			WallTimeUsed: observation.WallTimeUsed,
		},
		OccurredAt:   nowTime.UTC().Format(time.RFC3339),
		EscalateTo:   "chancellor",
		RunStatusNow: runStatus,
	}

	if runStatus != "running" {
		return result, nil
	}

	if exceededCap, limit, observed, exceeded := detectBudgetBreach(observation, limits); exceeded {
		result.Exceeded = true
		result.ExceededCap = exceededCap
		result.Limit = limit
		result.Observed = observed

		if err := g.failRunForBudgetBreach(ctx, req, runStatus, limits, result); err != nil {
			return EnforceRunBudgetResult{}, err
		}
		result.RunStatusNow = "failed"

		return result, &BudgetExceededError{
			Code:        errorcatalog.CodeBudgetExceeded,
			RunID:       req.RunID,
			ExceededCap: exceededCap,
			Limit:       limit,
			Observed:    observed,
		}
	}

	return result, nil
}

type runBudgetUsage struct {
	AttemptsUsed int
	TokensUsed   int64
	WallTimeUsed time.Duration
}

func (g *BudgetGuard) observeUsage(ctx context.Context, runID string, nowTime time.Time, runCreatedAt time.Time) (runBudgetUsage, error) {
	attemptCount, err := g.countAttempts(ctx, runID)
	if err != nil {
		return runBudgetUsage{}, err
	}

	tokenTotal, err := g.tokenUsageStore.SumForRun(ctx, runID)
	if err != nil {
		return runBudgetUsage{}, fmt.Errorf("query token usage for run %q: %w", runID, err)
	}

	wall := nowTime.Sub(runCreatedAt)
	if wall < 0 {
		wall = 0
	}

	return runBudgetUsage{
		AttemptsUsed: attemptCount,
		TokensUsed:   tokenTotal,
		WallTimeUsed: wall,
	}, nil
}

func (g *BudgetGuard) loadRunBudgetContext(ctx context.Context, runID string) (string, time.Time, error) {
	var runStatus string
	var runCreatedAtRaw string
	err := g.db.QueryRowContext(ctx,
		`SELECT run_status, created_at FROM workflow_runs WHERE run_id = ?`,
		runID,
	).Scan(&runStatus, &runCreatedAtRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, &EntityNotFoundError{EntityType: EntityTypeRun, RunID: runID}
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("load run budget context for run %q: %w", runID, err)
	}

	runCreatedAt, err := time.Parse(time.RFC3339, runCreatedAtRaw)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("parse run created_at for run %q: %w", runID, err)
	}

	return runStatus, runCreatedAt.UTC(), nil
}

func (g *BudgetGuard) countAttempts(ctx context.Context, runID string) (int, error) {
	var attempts int
	err := g.db.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM node_runs WHERE run_id = ?`,
		runID,
	).Scan(&attempts)
	if err != nil {
		return 0, fmt.Errorf("count attempts for run %q: %w", runID, err)
	}
	return attempts, nil
}

func (g *BudgetGuard) failRunForBudgetBreach(ctx context.Context, req EnforceRunBudgetRequest, runStatus string, limits RunBudgetLimits, result EnforceRunBudgetResult) error {
	occurredAt := result.OccurredAt
	transitionSuffix := strings.ReplaceAll(result.ExceededCap, "_", "-")
	transitionID := fmt.Sprintf("tr_budget_%s_%s_%d", req.RunID, transitionSuffix, g.now().UTC().UnixNano())
	eventID := fmt.Sprintf("evt_budget_%s_%s_%d", req.RunID, transitionSuffix, g.now().UTC().UnixNano())
	attemptID := fmt.Sprintf("run:%s:budget_guard", req.RunID)

	payload := fmt.Sprintf(
		`{"run_id":%q,"exceeded_cap":%q,"limit":%d,"observed":%d,"attempts_used":%d,"tokens_used":%d,"wall_time_seconds":%d,"max_attempts_per_run":%d,"max_tokens_per_run":%d,"max_wall_time_seconds":%d,"target_role":"chancellor"}`,
		req.RunID,
		result.ExceededCap,
		result.Limit,
		result.Observed,
		result.Observation.AttemptsUsed,
		result.Observation.TokensUsed,
		int64(result.Observation.WallTimeUsed/time.Second),
		limits.MaxAttemptsPerRun,
		limits.MaxTokensPerRun,
		int64(limits.MaxWallTimePerRun/time.Second),
	)

	err := g.transition.TerminalizeRunAndCancelLiveNodes(ctx, RunTerminalizationRequest{
		Transition: TransitionRequest{
			TransitionID:        transitionID,
			EventID:             eventID,
			RunID:               req.RunID,
			NodeRunID:           sql.NullString{},
			Entity:              EntityTypeRun,
			FromState:           runStatus,
			ToState:             "failed",
			ActorID:             firstNonEmptyValue(req.ActorID, "budget_guard"),
			RoleID:              firstNonEmptyValue(req.RoleID, "king"),
			AuthoritySource:     firstNonEmptyValue(req.AuthoritySource, "system.budget_guard"),
			ReasonCode:          string(errorcatalog.CodeBudgetExceeded),
			ReasonText:          fmt.Sprintf("run exceeded %s budget", result.ExceededCap),
			CorrelationID:       firstNonEmptyValue(req.CorrelationID, "corr_budget_"+req.RunID),
			CausationID:         firstNonEmptyValue(req.CausationID, "caus_budget_"+req.RunID+"_"+result.ExceededCap),
			OccurredAt:          occurredAt,
			TransitionPrevHash:  "budget_transition_prev_hash",
			TransitionEntryHash: "budget_transition_entry_hash_" + transitionID,
			EventPrevHash:       "budget_event_prev_hash",
			EventEntryHash:      "budget_event_entry_hash_" + eventID,
			AttemptID:           attemptID,
			SchemaVersion:       "v1",
			EventType:           "escalation.raised",
			EventSeq:            1,
			PayloadJSON:         payload,
		},
	})
	if err != nil {
		return fmt.Errorf("fail run %q for budget breach: %w", req.RunID, err)
	}

	return nil
}

func parseBudgetTimestamp(raw string, nowFn func() time.Time) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return nowFn().UTC(), nil
	}

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse budget guard timestamp: %w", err)
	}
	return parsed.UTC(), nil
}

func normalizeRunBudgetLimits(limits RunBudgetLimits) RunBudgetLimits {
	if limits.MaxAttemptsPerRun <= 0 {
		limits.MaxAttemptsPerRun = DefaultMaxAttemptsPerRun
	}
	if limits.MaxTokensPerRun <= 0 {
		limits.MaxTokensPerRun = DefaultMaxTokensPerRun
	}
	if limits.MaxWallTimePerRun <= 0 {
		limits.MaxWallTimePerRun = DefaultMaxWallTimePerRun
	}
	return limits
}

func detectBudgetBreach(usage runBudgetUsage, limits RunBudgetLimits) (string, int64, int64, bool) {
	if usage.AttemptsUsed > limits.MaxAttemptsPerRun {
		return "attempts", int64(limits.MaxAttemptsPerRun), int64(usage.AttemptsUsed), true
	}
	if usage.TokensUsed > limits.MaxTokensPerRun {
		return "tokens", limits.MaxTokensPerRun, usage.TokensUsed, true
	}
	if usage.WallTimeUsed > limits.MaxWallTimePerRun {
		return "wall_time", int64(limits.MaxWallTimePerRun / time.Second), int64(usage.WallTimeUsed / time.Second), true
	}
	return "", 0, 0, false
}

func validateEnforceRunBudgetRequest(req EnforceRunBudgetRequest) error {
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	return nil
}

func firstNonEmptyValue(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
