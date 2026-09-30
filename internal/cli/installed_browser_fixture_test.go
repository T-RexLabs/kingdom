package cli

import (
	"context"
	"fmt"
	"io"
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
	return external.ConnectionValidation{AppSlug: "kingdom-fixture", Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}}, nil
}

func (a *installedBrowserExternalAdapter) ListRepositories(context.Context, state.ExternalConnection, string, string) (external.RepositoryPage, error) {
	if err := a.failure(); err != nil {
		return external.RepositoryPage{}, err
	}
	return external.RepositoryPage{Items: []external.Repository{{ID: "200", Locator: "owner/private-fixture", CanonicalURL: "https://github.com/owner/private-fixture", AccountID: "10", InstallationID: "2"}}}, nil
}

func (a *installedBrowserExternalAdapter) ListIssues(context.Context, state.ExternalConnection, string, string, string, string) (external.IssuePage, error) {
	if err := a.failure(); err != nil {
		return external.IssuePage{}, err
	}
	return external.IssuePage{Items: []external.IssueSummary{{NativeID: "100", Issue: a.issue(), CommentCount: 2}}, SearchResultCap: 1000}, nil
}

func (a *installedBrowserExternalAdapter) FetchIssue(_ context.Context, req external.IssueRequest) (external.IssueResult, error) {
	if err := a.failure(); err != nil {
		return external.IssueResult{}, err
	}
	issue := a.issue()
	result := external.IssueResult{RepositoryID: "200", RepositoryInstallationID: "2", NativeID: "100", Issue: issue, Profile: req.Profile, ProviderCommentCount: 2}
	if req.Profile.IncludeComments {
		result.Comments = state.ExternalCommentList{
			{ProviderCommentID: "101", Author: "fixture-author", Body: "First deterministic fixture comment.", CanonicalURL: issue.CanonicalURL + "#issuecomment-101", CreatedAt: issue.CreatedAt, UpdatedAt: issue.UpdatedAt},
			{ProviderCommentID: "102", Author: "fixture-reviewer", Body: "Second deterministic fixture comment.", CanonicalURL: issue.CanonicalURL + "#issuecomment-102", CreatedAt: issue.CreatedAt, UpdatedAt: issue.UpdatedAt},
		}
		result.FetchedCommentCount = 2
		result.FetchedCommentPages = 1
	}
	return result, nil
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

func TestInstalledBrowserFixtureValidationSatisfiesCanonicalHealth(t *testing.T) {
	root := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", root}); err != nil {
		t.Fatal(err)
	}
	cfg, err := readInstallConfig(filepath.Join(root, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := openSQLiteDatabase(cfg.DBPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	store := state.NewExternalStore(db)
	at := "2026-09-30T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "fixture-connection", CredentialStrategy: "app_private_key", DisplayName: "Fixture",
		AppID: "1", ClientID: "Iv1", Installations: []state.ConnectionInstallation{{InstallationID: "2"}},
		CredentialHandle: "github-app:fixture-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "fixture-created",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "fixture-key", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &installedBrowserExternalAdapter{versionFile: filepath.Join(root, "issue.txt")}
	validation, err := adapter.Validate(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionStateAuditedVersionIdentity(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, validation.AppSlug, validation.Installations, "local-operator", "fixture-health", "connection.health_checked", connection.RowVersion); err != nil {
		t.Fatalf("fixture validation cannot publish canonical healthy state: %v", err)
	}
	stored, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AppSlug == "" || stored.HealthClass != "healthy" || stored.ValidatedGeneration != stored.CredentialGeneration || stored.Installations[0].AccountID != "10" {
		t.Fatalf("fixture health identity was not retained: %+v", stored)
	}
}

func TestInstalledBrowserFixtureHonorsCommentsProfile(t *testing.T) {
	adapter := &installedBrowserExternalAdapter{versionFile: filepath.Join(t.TempDir(), "issue.txt")}
	for _, include := range []bool{false, true} {
		t.Run(fmt.Sprint(include), func(t *testing.T) {
			profile := state.ExternalImportProfile{IncludeComments: include}
			result, err := adapter.FetchIssue(context.Background(), external.IssueRequest{RepositoryID: "200", IssueNumber: 7, Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			if result.Profile != profile || result.ProviderCommentCount != 2 || result.RepositoryInstallationID != "2" {
				t.Fatalf("fixture lost requested profile, comment count, or grant witness: %+v", result)
			}
			count := 0
			if result.Comments != nil {
				if err := result.Comments.Replay(context.Background(), func(comment state.ExternalComment) error {
					if comment.Body == "" || comment.ProviderCommentID == "" {
						t.Errorf("fixture returned incomplete comment: %+v", comment)
					}
					count++
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			want := 0
			if include {
				want = 2
			}
			if count != want || result.FetchedCommentCount != want {
				t.Fatalf("fixture captured %d comments (%d reported), want %d", count, result.FetchedCommentCount, want)
			}
		})
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
