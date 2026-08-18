package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

type installedBrowserExternalAdapter struct {
	versionFile string
}

func (a *installedBrowserExternalAdapter) Validate(context.Context, state.ExternalConnection) (external.ConnectionValidation, error) {
	if err := a.failure(); err != nil {
		return external.ConnectionValidation{}, err
	}
	return external.ConnectionValidation{InstallationAccountID: "10", InstallationAccountLogin: "octocat"}, nil
}

func (a *installedBrowserExternalAdapter) ListRepositories(context.Context, state.ExternalConnection, string, string) (external.RepositoryPage, error) {
	if err := a.failure(); err != nil {
		return external.RepositoryPage{}, err
	}
	return external.RepositoryPage{Items: []external.Repository{{ID: "200", Locator: "owner/private-fixture", CanonicalURL: "https://github.com/owner/private-fixture"}}}, nil
}

func (a *installedBrowserExternalAdapter) ListIssues(context.Context, state.ExternalConnection, string, string, string, string) (external.IssuePage, error) {
	if err := a.failure(); err != nil {
		return external.IssuePage{}, err
	}
	return external.IssuePage{Items: []external.IssueSummary{{NativeID: "100", Issue: a.issue(), CommentCount: 2}}, SearchResultCap: 1000}, nil
}

func (a *installedBrowserExternalAdapter) FetchIssue(context.Context, external.IssueRequest) (external.IssueResult, error) {
	if err := a.failure(); err != nil {
		return external.IssueResult{}, err
	}
	return external.IssueResult{RepositoryID: "200", NativeID: "100", Issue: a.issue(), Profile: state.ExternalImportProfile{}}, nil
}

func (*installedBrowserExternalAdapter) Evict(string) {}

func (a *installedBrowserExternalAdapter) failure() error {
	raw, err := os.ReadFile(a.versionFile)
	if err != nil {
		return nil
	}
	mode := strings.TrimSpace(strings.SplitN(string(raw), "\n", 2)[0])
	switch mode {
	case "FAIL:revoked":
		return &external.ProviderError{Class: external.FailureAuthentication}
	case "FAIL:unavailable":
		return &external.ProviderError{Class: external.FailureUnavailable}
	default:
		return nil
	}
}

func (a *installedBrowserExternalAdapter) issue() state.NormalizedExternalIssue {
	title, body := "Installed fixture issue", "Private deterministic issue body v1"
	if raw, err := os.ReadFile(a.versionFile); err == nil {
		parts := strings.SplitN(strings.TrimSpace(string(raw)), "\n", 2)
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			title = strings.TrimSpace(parts[0])
		}
		if len(parts) == 2 {
			body = strings.TrimSpace(parts[1])
		}
	}
	return state.NormalizedExternalIssue{
		Title: title, Body: body, State: "open", Repository: "owner/private-fixture", IssueNumber: 7,
		CanonicalURL: "https://github.com/owner/private-fixture/issues/7", CreatedAt: "2026-08-01T12:00:00Z", UpdatedAt: "2026-08-13T12:00:00Z",
	}
}

// TestInstalledBrowserFixtureServer is an opt-in, test-binary-only server for
// Playwright validation of the real installed-mode runtime and SPA. No fixture
// flag or alternate provider origin is compiled into the kd binary.
func TestInstalledBrowserFixtureServer(t *testing.T) {
	if os.Getenv("KINGDOM_INSTALLED_BROWSER_FIXTURE") != "1" {
		t.Skip("set KINGDOM_INSTALLED_BROWSER_FIXTURE=1 for installed browser validation")
	}
	root := strings.TrimSpace(os.Getenv("KINGDOM_FIXTURE_WORKSPACE"))
	port := strings.TrimSpace(os.Getenv("KINGDOM_FIXTURE_PORT"))
	versionFile := strings.TrimSpace(os.Getenv("KINGDOM_FIXTURE_ISSUE_FILE"))
	if root == "" || port == "" || versionFile == "" {
		t.Fatal("KINGDOM_FIXTURE_WORKSPACE, KINGDOM_FIXTURE_PORT, and KINGDOM_FIXTURE_ISSUE_FILE are required")
	}
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := runSummonWithDependencies(ctx, os.Stdout, os.Stderr, []string{"--config", configPath, "--port", port, "--ready-timeout", "60s"}, summonDependencies{externalAdapter: &installedBrowserExternalAdapter{versionFile: versionFile}}); err != nil && ctx.Err() == nil {
		t.Fatalf("fixture summon: %v", err)
	}
	fmt.Fprintln(os.Stdout, "installed browser fixture stopped")
}
