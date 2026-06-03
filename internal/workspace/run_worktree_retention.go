package workspace

import (
	"fmt"
	"strings"
	"time"
)

const RunWorktreeRetentionNever = time.Duration(-1)

func ParseRunWorktreeRetention(raw string) (time.Duration, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return 0, fmt.Errorf("run worktree retention is required")
	}
	if trimmed == "never" {
		return RunWorktreeRetentionNever, nil
	}
	retention, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("run worktree retention must be a positive duration or 'never': %w", err)
	}
	if retention <= 0 {
		return 0, fmt.Errorf("run worktree retention must be a positive duration or 'never'")
	}
	return retention, nil
}

func NormalizeRunWorktreeRetention(retention time.Duration) time.Duration {
	if retention == RunWorktreeRetentionNever {
		return RunWorktreeRetentionNever
	}
	if retention > 0 {
		return retention
	}
	return DefaultRunWorktreeRetention
}

func RunWorktreeCleanupDisabled(retention time.Duration) bool {
	return retention == RunWorktreeRetentionNever
}
