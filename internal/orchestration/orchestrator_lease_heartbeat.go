package orchestration

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	executionstore "kingdom/internal/execution/store"
)

const (
	defaultProviderLeaseHeartbeatMaximum = 30 * time.Second
	providerLeaseRenewalTimeoutMaximum   = 5 * time.Second
)

type providerLeaseTicker interface {
	C() <-chan time.Time
	Stop()
}

type timeProviderLeaseTicker struct {
	ticker *time.Ticker
}

func newTimeProviderLeaseTicker(interval time.Duration) providerLeaseTicker {
	return &timeProviderLeaseTicker{ticker: time.NewTicker(interval)}
}

func (t *timeProviderLeaseTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *timeProviderLeaseTicker) Stop() {
	t.ticker.Stop()
}

type providerLeaseHeartbeatResult struct {
	leaseExpiresAt string
	authorityLost  bool
	err            error
}

type providerLeaseHeartbeat struct {
	stop context.CancelFunc
	done <-chan providerLeaseHeartbeatResult
}

func (h *providerLeaseHeartbeat) stopAndWait() providerLeaseHeartbeatResult {
	if h == nil {
		return providerLeaseHeartbeatResult{}
	}
	h.stop()
	return <-h.done
}

func (o *orchestrationLoop) startProviderLeaseHeartbeat(executionCtx context.Context, cancelExecution context.CancelFunc, job asyncExecutionResult) *providerLeaseHeartbeat {
	if o == nil || job.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(job.expectedLeaseOwner) == "" || job.expectedLeaseVersion <= 0 || job.providerLeaseDuration <= 0 {
		return nil
	}
	interval := o.providerExecutionLeaseHeartbeatEvery(job.providerLeaseDuration)
	if interval <= 0 {
		return nil
	}
	tickerFactory := o.newProviderLeaseTicker
	if tickerFactory == nil {
		tickerFactory = newTimeProviderLeaseTicker
	}
	heartbeatCtx, stop := context.WithCancel(firstNonNilContext(executionCtx))
	done := make(chan providerLeaseHeartbeatResult, 1)
	go func() {
		result := providerLeaseHeartbeatResult{leaseExpiresAt: strings.TrimSpace(job.expectedLeaseExpiresAt)}
		ticker := tickerFactory(interval)
		if ticker == nil {
			result.err = fmt.Errorf("provider lease ticker is required")
			o.recordProviderLeaseRenewalError(job, result.err)
			cancelExecution()
			done <- result
			return
		}
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				done <- result
				return
			case <-ticker.C():
				renewed, leaseExpiresAt, err := o.renewProviderExecutionLease(heartbeatCtx, job, result.leaseExpiresAt)
				if err != nil {
					result.err = err
					o.recordProviderLeaseRenewalError(job, err)
					cancelExecution()
					done <- result
					return
				}
				if !renewed {
					result.authorityLost = true
					cancelExecution()
					done <- result
					return
				}
				result.leaseExpiresAt = leaseExpiresAt
			}
		}
	}()
	return &providerLeaseHeartbeat{stop: stop, done: done}
}

func (o *orchestrationLoop) providerExecutionLeaseHeartbeatEvery(leaseDuration time.Duration) time.Duration {
	if leaseDuration <= 0 {
		return 0
	}
	maximum := leaseDuration / 3
	if maximum <= 0 {
		maximum = leaseDuration
	}
	configured := time.Duration(0)
	if o != nil {
		configured = o.providerLeaseHeartbeatInterval
	}
	if configured > 0 && configured < maximum {
		return configured
	}
	if maximum > defaultProviderLeaseHeartbeatMaximum {
		return defaultProviderLeaseHeartbeatMaximum
	}
	return maximum
}

func (o *orchestrationLoop) renewProviderExecutionLease(ctx context.Context, job asyncExecutionResult, previousLeaseExpiresAt string) (bool, string, error) {
	if o == nil || o.executionStore == nil {
		return false, "", fmt.Errorf("execution store is required for provider lease renewal")
	}
	renewedAt := o.now().UTC()
	leaseUntil := renewedAt.Add(job.providerLeaseDuration)
	if previous, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(previousLeaseExpiresAt)); err == nil && !leaseUntil.After(previous) {
		// SQLite's julianday comparison is millisecond-granular on supported builds.
		leaseUntil = previous.Add(time.Millisecond)
	}
	timeout := job.providerLeaseDuration / 3
	if timeout <= 0 || timeout > providerLeaseRenewalTimeoutMaximum {
		timeout = providerLeaseRenewalTimeoutMaximum
	}
	renewCtx, cancel := context.WithTimeout(context.WithoutCancel(firstNonNilContext(ctx)), timeout)
	defer cancel()
	renewed, err := o.executionStore.RenewProviderAttemptLease(renewCtx, executionstore.RenewProviderAttemptLeaseRequest{
		AttemptID:    job.attemptID,
		LeaseOwner:   job.expectedLeaseOwner,
		LeaseVersion: job.expectedLeaseVersion,
		LeaseUntil:   leaseUntil.Format(time.RFC3339Nano),
		RenewedAt:    renewedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return false, "", fmt.Errorf("renew provider execution lease for attempt %q: %w", strings.TrimSpace(job.attemptID), err)
	}
	return renewed, leaseUntil.Format(time.RFC3339Nano), nil
}

func (o *orchestrationLoop) recordProviderLeaseRenewalError(job asyncExecutionResult, err error) {
	if err == nil {
		return
	}
	wrapped := fmt.Errorf("provider lease heartbeat failed for attempt %q: %w", strings.TrimSpace(job.attemptID), err)
	slog.Warn("provider execution lease heartbeat failed", "attempt_id", strings.TrimSpace(job.attemptID), "error", err)
	if o != nil && o.asyncDrain != nil {
		o.asyncDrain.RecordError(wrapped)
	}
}
