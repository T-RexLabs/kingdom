package state

import (
	"errors"
	"strings"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const sqliteUniqueConstraintPrefix = "UNIQUE constraint failed:"

// IsUniqueViolation reports whether err is a SQLite UNIQUE or PRIMARY KEY
// constraint failure using the typed driver error instead of ad hoc message
// substring checks.
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		return false
	}
	code := serr.Code()
	return code == sqlite3.SQLITE_CONSTRAINT_UNIQUE ||
		code == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY ||
		code == sqlite3.SQLITE_CONSTRAINT
}

// IsUniqueViolationOn reports whether err is a SQLite unique-constraint
// failure for the exact target columns listed in targets. Targets should use
// the driver message format, e.g. "events.attempt_id".
func IsUniqueViolationOn(err error, targets ...string) bool {
	if !IsUniqueViolation(err) {
		return false
	}
	if len(targets) == 0 {
		return true
	}
	actual, ok := sqliteUniqueConstraintTargets(err)
	if !ok || len(actual) != len(targets) {
		return false
	}
	remaining := make(map[string]int, len(targets))
	for _, target := range targets {
		normalized := strings.TrimSpace(target)
		if normalized == "" {
			continue
		}
		remaining[normalized]++
	}
	if len(remaining) != len(actual) {
		return false
	}
	for _, target := range actual {
		count := remaining[target]
		if count == 0 {
			return false
		}
		if count == 1 {
			delete(remaining, target)
			continue
		}
		remaining[target] = count - 1
	}
	return len(remaining) == 0
}

func sqliteUniqueConstraintTargets(err error) ([]string, bool) {
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		return nil, false
	}
	message := serr.Error()
	idx := strings.Index(message, sqliteUniqueConstraintPrefix)
	if idx < 0 {
		return nil, false
	}
	remainder := strings.TrimSpace(message[idx+len(sqliteUniqueConstraintPrefix):])
	if suffixIdx := strings.LastIndex(remainder, " ("); suffixIdx >= 0 {
		remainder = strings.TrimSpace(remainder[:suffixIdx])
	}
	if remainder == "" {
		return nil, false
	}
	parts := strings.Split(remainder, ",")
	targets := make([]string, 0, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target != "" {
			targets = append(targets, target)
		}
	}
	if len(targets) == 0 {
		return nil, false
	}
	return targets, true
}
