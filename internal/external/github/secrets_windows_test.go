//go:build windows

package github

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/securefs"
)

func TestWindowsSecretStoresCreateCurrentUserOnlyACLs(t *testing.T) {
	project := t.TempDir()
	if err := os.Mkdir(filepath.Join(project, ".kingdom"), 0o755); err != nil {
		t.Fatal(err)
	}
	secretsRoot := filepath.Join(project, ".kingdom", "secrets")
	keys, err := NewKeyStore(secretsRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Provision("connection-1", 1, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	tokens, err := NewUserTokenStore(secretsRoot)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := tokens.Store("connection-1", 1, UserTokens{
		AccessToken:      "access-token",
		RefreshToken:     "refresh-token",
		AccessExpiresAt:  now.Add(time.Hour),
		RefreshExpiresAt: now.Add(2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{
		secretsRoot,
		filepath.Join(secretsRoot, "github-apps"),
		filepath.Join(secretsRoot, "github-apps", "connection-1"),
		filepath.Join(secretsRoot, "github-apps", "connection-1", ".lifecycle.lock"),
		filepath.Join(secretsRoot, "github-apps", "connection-1", "private-key.1.pem"),
		filepath.Join(secretsRoot, "github-user-tokens"),
		filepath.Join(secretsRoot, "github-user-tokens", "connection-1"),
		filepath.Join(secretsRoot, "github-user-tokens", "connection-1", ".lifecycle.lock"),
		filepath.Join(secretsRoot, "github-user-tokens", "connection-1", "tokens.1.json"),
	} {
		file, err := os.Open(candidate)
		if err != nil {
			t.Fatal(err)
		}
		validateErr := securefs.ValidateCurrentUserOnly(file)
		closeErr := file.Close()
		if validateErr != nil {
			t.Fatalf("validate %q: %v", candidate, validateErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}
