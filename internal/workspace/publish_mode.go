package workspace

import "strings"

const (
	PublishModeDirect      = "direct"
	PublishModeGitWorktree = "git_worktree"
)

func NormalizePublishMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", PublishModeDirect:
		return PublishModeDirect
	case PublishModeGitWorktree:
		return PublishModeGitWorktree
	default:
		return PublishModeDirect
	}
}

func IsSupportedPublishMode(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case PublishModeDirect, PublishModeGitWorktree:
		return true
	default:
		return false
	}
}
