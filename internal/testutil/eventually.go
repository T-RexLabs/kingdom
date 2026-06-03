package testutil

import (
	"fmt"
	"testing"
	"time"
)

const DefaultPollInterval = 10 * time.Millisecond

// Eventually retries condition until it succeeds or the timeout elapses.
func Eventually(t testing.TB, timeout, interval time.Duration, condition func() error) {
	t.Helper()

	if timeout <= 0 {
		t.Fatalf("Eventually requires timeout > 0, got %s", timeout)
	}
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := condition(); err == nil {
			return
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = fmt.Errorf("condition never succeeded")
			}
			t.Fatalf("condition not met within %s: %v", timeout, lastErr)
		}

		time.Sleep(interval)
	}
}

// Consistently verifies condition keeps succeeding for the full duration.
func Consistently(t testing.TB, duration, interval time.Duration, condition func() error) {
	t.Helper()

	if duration <= 0 {
		t.Fatalf("Consistently requires duration > 0, got %s", duration)
	}
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	deadline := time.Now().Add(duration)
	for {
		if err := condition(); err != nil {
			t.Fatalf("condition failed before %s elapsed: %v", duration, err)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(interval)
	}
}
