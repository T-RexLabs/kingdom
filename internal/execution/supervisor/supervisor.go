package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/execution/store"
)

type Runner interface {
	ExecuteAttempt(ctx context.Context, attempt store.AttemptExecution) (RunnerResult, error)
}

type RunnerResult struct {
	TerminalState string
	Payload       map[string]any
	FailureCode   string
	FailureText   string
}

type Config struct {
	Store      *store.Store
	Runner     Runner
	LeaseOwner string
	LeaseTTL   time.Duration
	Now        func() time.Time
}

type Supervisor struct {
	store      *store.Store
	runner     Runner
	leaseOwner string
	leaseTTL   time.Duration
	now        func() time.Time
}

func New(cfg Config) (*Supervisor, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if cfg.Runner == nil {
		return nil, fmt.Errorf("runner is required")
	}
	owner := strings.TrimSpace(cfg.LeaseOwner)
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	return &Supervisor{
		store:      cfg.Store,
		runner:     cfg.Runner,
		leaseOwner: owner,
		leaseTTL:   leaseTTL,
		now:        now,
	}, nil
}

func (s *Supervisor) RunOnce(ctx context.Context) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("supervisor is nil")
	}
	queued, err := s.store.ListQueuedAttempts(ctx, 8)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, attempt := range queued {
		now := s.now().UTC()
		leaseUntil := now.Add(s.leaseTTL).Format(time.RFC3339)
		acquired, err := s.store.AcquireLease(ctx, store.AcquireLeaseRequest{
			AttemptID:    attempt.AttemptID,
			LeaseOwner:   s.leaseOwner,
			LeaseUntil:   leaseUntil,
			AcquiredAt:   now.Format(time.RFC3339),
			StateOnLease: "running",
		})
		if err != nil {
			return processed, err
		}
		if !acquired {
			continue
		}
		if err := s.executeAttempt(ctx, attempt, now); err != nil {
			return processed, err
		}
		processed++
	}

	return processed, nil
}

func (s *Supervisor) executeAttempt(ctx context.Context, attempt store.AttemptExecution, now time.Time) error {
	seq, err := s.store.NextAttemptSeq(ctx, attempt.AttemptID)
	if err != nil {
		return err
	}
	if err := s.store.AppendAttemptEvent(ctx, store.AppendAttemptEventRequest{
		AttemptEventID: fmt.Sprintf("ael_%s_started_%d", sanitizeToken(attempt.AttemptID), seq),
		AttemptID:      attempt.AttemptID,
		RunID:          attempt.RunID,
		NodeRunID:      attempt.NodeRunID,
		Seq:            seq,
		EventType:      "attempt.started",
		ReasonCode:     "orchestration_progress",
		PayloadJSON:    `{}`,
		CreatedAt:      now.Format(time.RFC3339),
	}); err != nil {
		return err
	}

	result, runErr := s.runner.ExecuteAttempt(ctx, attempt)
	terminalSeq, err := s.store.NextAttemptSeq(ctx, attempt.AttemptID)
	if err != nil {
		return err
	}

	if runErr != nil {
		payloadRaw, _ := json.Marshal(map[string]any{"error": runErr.Error()})
		return s.store.CommitTerminal(ctx, store.CommitTerminalRequest{
			OutboxID:      fmt.Sprintf("outbox_%s_%d", sanitizeToken(attempt.AttemptID), terminalSeq),
			AttemptID:     attempt.AttemptID,
			RunID:         attempt.RunID,
			NodeRunID:     attempt.NodeRunID,
			TerminalSeq:   terminalSeq,
			TerminalState: "committed_failure",
			PayloadJSON:   string(payloadRaw),
			CommittedAt:   s.now().UTC().Format(time.RFC3339),
			FailureCode:   "internal_error",
			FailureText:   runErr.Error(),
		})
	}

	terminalState := strings.TrimSpace(result.TerminalState)
	if terminalState == "" {
		terminalState = "committed_success"
	}
	payloadRaw, marshalErr := json.Marshal(result.Payload)
	if marshalErr != nil {
		return marshalErr
	}
	if len(payloadRaw) == 0 {
		payloadRaw = []byte(`{}`)
	}

	return s.store.CommitTerminal(ctx, store.CommitTerminalRequest{
		OutboxID:      fmt.Sprintf("outbox_%s_%d", sanitizeToken(attempt.AttemptID), terminalSeq),
		AttemptID:     attempt.AttemptID,
		RunID:         attempt.RunID,
		NodeRunID:     attempt.NodeRunID,
		TerminalSeq:   terminalSeq,
		TerminalState: terminalState,
		PayloadJSON:   string(payloadRaw),
		CommittedAt:   s.now().UTC().Format(time.RFC3339),
		FailureCode:   strings.TrimSpace(result.FailureCode),
		FailureText:   strings.TrimSpace(result.FailureText),
	})
}

func sanitizeToken(raw string) string {
	replacer := strings.NewReplacer(":", "_", "-", "_", "/", "_", " ", "_")
	return replacer.Replace(strings.TrimSpace(raw))
}
