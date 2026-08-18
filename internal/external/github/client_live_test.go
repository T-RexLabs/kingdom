package github

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

// TestLiveGitHubAppSmoke is deliberately excluded from deterministic gates. It
// requires a controlled operator-owned App and reads the private key from a
// file, never an environment value or command-line token.
func TestLiveGitHubAppSmoke(t *testing.T) {
	if os.Getenv("KINGDOM_GITHUB_LIVE_SMOKE") != "1" {
		t.Skip("set KINGDOM_GITHUB_LIVE_SMOKE=1 to run the controlled live provider smoke")
	}
	required := map[string]string{
		"KINGDOM_GITHUB_SMOKE_APP_ID":                    strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_APP_ID")),
		"KINGDOM_GITHUB_SMOKE_CLIENT_ID":                 strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_CLIENT_ID")),
		"KINGDOM_GITHUB_SMOKE_INSTALLATION_ID":           strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_INSTALLATION_ID")),
		"KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE":          strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE")),
		"KINGDOM_GITHUB_SMOKE_REPOSITORY_ID":             strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_REPOSITORY_ID")),
		"KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER":              strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER")),
		"KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID": strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID")),
		"KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER":  strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER")),
		"KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL":           strings.TrimSpace(os.Getenv("KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL")),
	}
	for name, value := range required {
		if value == "" {
			t.Skipf("controlled live GitHub App unavailable: %s is not set", name)
		}
	}
	issueNumber, err := strconv.Atoi(required["KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER"])
	if err != nil || issueNumber <= 0 {
		t.Fatalf("KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER must be a positive integer")
	}
	transferredIssueNumber, err := strconv.Atoi(required["KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER"])
	if err != nil || transferredIssueNumber <= 0 {
		t.Fatalf("KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER must be a positive integer")
	}
	if required["KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL"] != "1" {
		t.Fatalf("KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL must be 1; this controlled smoke permanently uninstalls the test App installation")
	}
	privateKey, err := os.ReadFile(required["KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE"])
	if err != nil {
		t.Fatalf("read private key file: %v", err)
	}
	secretsRoot := t.TempDir()
	keys, err := NewKeyStore(secretsRoot)
	if err != nil {
		t.Fatal(err)
	}
	const connectionID = "live-smoke"
	if _, err = keys.Provision(connectionID, privateKey); err != nil {
		t.Fatalf("provision temporary smoke key: %v", err)
	}
	client, err := NewClient(ClientConfig{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	connection := state.ExternalConnection{
		ConnectionID:         connectionID,
		Provider:             "github",
		AppID:                required["KINGDOM_GITHUB_SMOKE_APP_ID"],
		ClientID:             required["KINGDOM_GITHUB_SMOKE_CLIENT_ID"],
		InstallationID:       required["KINGDOM_GITHUB_SMOKE_INSTALLATION_ID"],
		CredentialHandle:     CredentialHandle(connectionID),
		CredentialGeneration: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err = client.Validate(ctx, connection); err != nil {
		t.Fatalf("validate controlled GitHub App: %v", err)
	}
	repositories, err := client.ListRepositories(ctx, connection, "", "")
	if err != nil {
		t.Fatalf("list installation repositories: %v", err)
	}
	foundRepository := false
	for _, repository := range repositories.Items {
		if repository.ID == required["KINGDOM_GITHUB_SMOKE_REPOSITORY_ID"] {
			foundRepository = true
			break
		}
	}
	if !foundRepository {
		t.Fatalf("controlled repository was not granted to the installation")
	}
	issues, err := client.ListIssues(ctx, connection, required["KINGDOM_GITHUB_SMOKE_REPOSITORY_ID"], "all", "", "")
	if err != nil {
		t.Fatalf("browse controlled issues: %v", err)
	}
	for _, issue := range issues.Items {
		if issue.Issue.IssueNumber == issueNumber && issue.NativeID == "" {
			t.Fatal("controlled issue omitted immutable provider identity")
		}
	}
	result, err := client.FetchIssue(ctx, external.IssueRequest{
		Connection:   connection,
		RepositoryID: required["KINGDOM_GITHUB_SMOKE_REPOSITORY_ID"],
		IssueNumber:  issueNumber,
		Profile:      state.ExternalImportProfile{IncludeComments: true},
	})
	if err != nil {
		t.Fatalf("fetch controlled issue and comments: %v", err)
	}
	if result.NativeID == "" || result.Issue.Repository == "" || result.Issue.CanonicalURL == "" {
		t.Fatalf("live normalized issue is incomplete (native_id_present=%t repository_present=%t url_present=%t number=%d)", result.NativeID != "", result.Issue.Repository != "", result.Issue.CanonicalURL != "", result.Issue.IssueNumber)
	}
	if !result.Transferred || result.RepositoryID != required["KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID"] || result.Issue.IssueNumber != transferredIssueNumber {
		t.Fatalf("live transfer was not resolved safely: transferred=%t repository_id=%q issue_number=%d", result.Transferred, result.RepositoryID, result.Issue.IssueNumber)
	}

	key, _, err := keys.Load(connectionID)
	if err != nil {
		t.Fatalf("reload temporary smoke key: %v", err)
	}
	jwt, err := appJWT(key, connection.AppID, time.Now())
	if err != nil {
		t.Fatalf("sign uninstall request: %v", err)
	}
	if _, err = client.doJSON(ctx, connection.ConnectionID, http.MethodDelete, "/app/installations/"+connection.InstallationID, jwt, nil, nil, ""); err != nil {
		t.Fatalf("uninstall controlled GitHub App installation: %v", err)
	}
	client.Evict(connection.ConnectionID)
	_, err = client.Validate(ctx, connection)
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Fatalf("uninstalled App validation error=%v, want revoked/authentication", err)
	}
	t.Logf("live GitHub smoke passed: repositories=%d issues_page=%d comments_retained=%d transfer=%s#%d revocation=confirmed", len(repositories.Items), len(issues.Items), len(result.Comments), result.Issue.Repository, result.Issue.IssueNumber)
}
