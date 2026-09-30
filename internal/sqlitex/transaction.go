// Package sqlitex provides SQLite-specific transaction retry support without
// coupling callers to a particular SQLite driver.
package sqlitex

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"strings"
	"time"
)

const (
	sqliteBusyCode   = 5
	sqliteLockedCode = 6
)

// Stats describes one complete transaction attempt sequence. Observe receives
// a final snapshot whether the transaction commits or terminates with an error.
type Stats struct {
	Attempts           int
	ContentionAttempts int
	ContentionDuration time.Duration
	TerminalError      error
}

// Options configures Transaction. Zero values select bounded production
// defaults. Random, Sleep, and Observe make timing and observability testable
// without tying this package to a metrics or logging implementation.
type Options struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Random      func() float64
	Sleep       func(context.Context, time.Duration) error
	Observe     func(Stats)
}

// IsBusy reports whether err represents SQLite SQLITE_BUSY or SQLITE_LOCKED.
// A typed driver code always takes precedence over a message fallback, so a
// wrapped typed non-busy error cannot be accidentally retried from its text.
func IsBusy(err error) bool {
	if err == nil {
		return false
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 0xff { // SQLite extended result codes retain the primary code.
		case sqliteBusyCode, sqliteLockedCode:
			return true
		default:
			return false
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "sqlite_busy") || strings.Contains(message, "sqlite_locked")
}

// Transaction runs begin, body, and commit as one replay-safe unit. The body
// may run more than once and therefore must contain only database effects that
// are safe to replay after a rolled-back transaction.
func Transaction(ctx context.Context, db *sql.DB, body func(context.Context, *sql.Tx) error, options Options) (Stats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return observeFailure(options, errors.New("database is required"))
	}
	if body == nil {
		return observeFailure(options, errors.New("transaction body is required"))
	}
	return Retry(ctx, func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err == nil {
			err = body(ctx, tx)
			if err == nil {
				err = tx.Commit()
			}
			if err != nil {
				_ = tx.Rollback()
			}
		}
		return err
	}, options)
}

// Retry retries a replay-safe operation on SQLite contention. Transaction is
// preferred for database writes because it owns the entire transaction
// lifecycle; Retry is for existing higher-level atomic operations.
func Retry(ctx context.Context, operation func() error, options Options) (Stats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stats := Stats{}
	finish := func(err error) (Stats, error) {
		stats.TerminalError = err
		if options.Observe != nil {
			options.Observe(stats)
		}
		return stats, err
	}
	if operation == nil {
		return finish(errors.New("retry operation is required"))
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 4
	}
	baseDelay := options.BaseDelay
	if baseDelay <= 0 {
		baseDelay = 25 * time.Millisecond
	}
	maxDelay := options.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 250 * time.Millisecond
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	random := options.Random
	if random == nil {
		random = rand.Float64
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepWithContext
	}
	var firstContentionAt time.Time
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return finish(err)
		}
		if attempt > 1 {
			if err := sleep(ctx, jitteredDelay(baseDelay, maxDelay, attempt-1, random)); err != nil {
				return finish(err)
			}
		}
		stats.Attempts++
		err := operation()
		if err == nil {
			return finish(nil)
		}
		if !IsBusy(err) {
			return finish(err)
		}
		stats.ContentionAttempts++
		if firstContentionAt.IsZero() {
			firstContentionAt = time.Now()
		}
		stats.ContentionDuration = time.Since(firstContentionAt)
		if attempt == maxAttempts {
			return finish(err)
		}
	}
	return finish(ctx.Err())
}

func observeFailure(options Options, err error) (Stats, error) {
	stats := Stats{TerminalError: err}
	if options.Observe != nil {
		options.Observe(stats)
	}
	return stats, err
}

func jitteredDelay(base, max time.Duration, retry int, random func() float64) time.Duration {
	delay := base
	for step := 1; step < retry && delay < max; step++ {
		delay *= 2
		if delay > max {
			delay = max
		}
	}
	fraction := random()
	if fraction < 0 {
		fraction = 0
	} else if fraction > 1 {
		fraction = 1
	}
	return delay/2 + time.Duration(float64(delay/2)*fraction)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
