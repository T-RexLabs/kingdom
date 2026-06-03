package workspace

import (
	"path/filepath"
	"regexp"
	"strings"
)

var invalidSlugToken = regexp.MustCompile(`[^a-z0-9]+`)

func SlugifyPathComponent(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return ""
	}
	replaced := invalidSlugToken.ReplaceAllString(trimmed, "-")
	cleaned := strings.Trim(replaced, "-")
	if cleaned == "" {
		return ""
	}
	if len(cleaned) > 64 {
		return strings.Trim(cleaned[:64], "-")
	}
	return cleaned
}

func RunPathName(displayName, runID string) string {
	runToken := safeTokenOrFallback(runID, "run")
	displaySlug := SlugifyPathComponent(displayName)
	if displaySlug == "" {
		return runToken
	}
	return safeTokenOrFallback(displaySlug+"-"+runToken, runToken)
}

func ManagedRepoName(mainWorktreeRoot string) string {
	root := filepath.Clean(strings.TrimSpace(mainWorktreeRoot))
	base := strings.TrimSpace(filepath.Base(root))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "repository"
	}
	return safeTokenOrFallback(base, "repository")
}

func safeTokenOrFallback(raw, fallback string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback
	}
	cleaned := invalidWorkspaceToken.ReplaceAllString(trimmed, "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		return fallback
	}
	return cleaned
}
