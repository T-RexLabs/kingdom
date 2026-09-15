package planning

import (
	"context"
	"time"
)

const (
	planningRetryWakeBackoffBase = 100 * time.Millisecond
	planningRetryWakeBackoffMax  = 5 * time.Second
)

type retryWakeTimer interface {
	Stop() bool
	Chan() <-chan time.Time
}

type standardRetryWakeTimer struct{ timer *time.Timer }

func (t standardRetryWakeTimer) Stop() bool             { return t.timer.Stop() }
func (t standardRetryWakeTimer) Chan() <-chan time.Time { return t.timer.C }
func newStandardRetryWakeTimer(d time.Duration) retryWakeTimer {
	return standardRetryWakeTimer{timer: time.NewTimer(d)}
}

// schedulePlanningRetryWake maintains one bounded timer for the earliest
// persisted retry. A timer cannot authorize a launch: recovery still uses the
// durable due check and lease-fenced claim.
func (s *Service) schedulePlanningRetryWake(due time.Time) {
	s.schedulePlanningRetryWakeWithBackoff(due, false)
}

func (s *Service) schedulePlanningRetryWakeWithBackoff(due time.Time, backoffOverdueWake bool) {
	if s == nil || due.IsZero() || s.recoveryCtx == nil || s.recoveryCtx.Err() != nil {
		return
	}
	s.retryWakeMu.Lock()
	if !s.retryWakeDue.IsZero() && !due.Before(s.retryWakeDue) {
		s.retryWakeMu.Unlock()
		return
	}
	if s.retryWakeCancel != nil {
		close(s.retryWakeCancel)
		s.retryWakeCancel = nil
	}
	if s.retryWakeTimer != nil {
		s.retryWakeTimer.Stop()
	}
	now := nowTime(s.runner.deps.nowString())
	delay := due.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if delay == 0 && backoffOverdueWake {
		delay = planningRetryWakeBackoff(s.retryWakeFailures)
	}
	timer := s.retryWakeTimerFactory(delay)
	cancel := make(chan struct{})
	s.retryWakeTimer = timer
	s.retryWakeCancel = cancel
	s.retryWakeDue = due
	s.retryWakeWG.Add(1)
	s.retryWakeMu.Unlock()
	go func() {
		defer s.retryWakeWG.Done()
		select {
		case <-s.recoveryCtx.Done():
		case <-cancel:
		case <-timer.Chan():
			s.retryWakeMu.Lock()
			if s.retryWakeCancel != cancel {
				s.retryWakeMu.Unlock()
				return
			}
			s.retryWakeCancel = nil
			s.retryWakeTimer = nil
			s.retryWakeDue = time.Time{}
			s.retryWakeFailures++
			s.retryWakeMu.Unlock()
			if s.recoveryCtx.Err() != nil {
				return
			}
			if err := s.recoverOperationsAndSchedule(s.recoveryCtx, true); err != nil {
				s.schedulePlanningRetryWakeWithBackoff(due, true)
			}
		}
	}()
}

func planningRetryWakeBackoff(failures int) time.Duration {
	if failures <= 1 {
		return planningRetryWakeBackoffBase
	}
	delay := planningRetryWakeBackoffBase
	for attempt := 1; attempt < failures && delay < planningRetryWakeBackoffMax; attempt++ {
		delay *= 2
	}
	if delay > planningRetryWakeBackoffMax {
		return planningRetryWakeBackoffMax
	}
	return delay
}

func (s *Service) resetPlanningRetryWakeBackoff() {
	if s == nil {
		return
	}
	s.retryWakeMu.Lock()
	s.retryWakeFailures = 0
	s.retryWakeMu.Unlock()
}

func (s *Service) scheduleNextPlanningRetryWake(ctx context.Context, backoffOverdueWake bool) error {
	due, err := s.operations.deps.store.NextPendingPlanningRetryDue(ctx)
	if err != nil || due == nil {
		return err
	}
	s.schedulePlanningRetryWakeWithBackoff(*due, backoffOverdueWake)
	return nil
}

func (s *Service) stopPlanningRetryWake() {
	if s == nil {
		return
	}
	s.retryWakeMu.Lock()
	if s.retryWakeCancel != nil {
		close(s.retryWakeCancel)
		s.retryWakeCancel = nil
	}
	if s.retryWakeTimer != nil {
		s.retryWakeTimer.Stop()
		s.retryWakeTimer = nil
	}
	s.retryWakeDue = time.Time{}
	s.retryWakeFailures = 0
	s.retryWakeMu.Unlock()
}

func (s *Service) waitPlanningRetryWake(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.retryWakeWG.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
