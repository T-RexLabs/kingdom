package state

import "strings"

func shouldTrackAttemptProgressActivity(reasonCode string) bool {
	switch strings.TrimSpace(reasonCode) {
	case "runtime_observation", "runtime_completion_observation":
		return false
	default:
		return true
	}
}

func isSyntheticAttemptProgressReason(reasonCode string) bool {
	return !shouldTrackAttemptProgressActivity(reasonCode)
}
