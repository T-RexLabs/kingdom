package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/state"
)

func TestGitHubKeyProvisionRejectsTerminalConnectionsBeforeFilesystemMutation(t *testing.T) {
	for _, lifecycle := range []string{"removal_pending", "removed"} {
		t.Run(lifecycle, func(t *testing.T) {
			projectRoot := t.TempDir()
			if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
				t.Fatal(err)
			}
			kingdomHome := filepath.Join(projectRoot, ".kingdom")
			db, err := openSQLiteDatabase(filepath.Join(kingdomHome, "kingdom.db"), sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
			if err != nil {
				t.Fatal(err)
			}
			store := state.NewExternalStore(db)
			at := "2026-08-13T10:00:00Z"
			if _, err = store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "terminal-connection", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:terminal-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-create"}); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			health := lifecycle
			if err = store.UpdateConnectionState(context.Background(), "terminal-connection", lifecycle, health, "", at, false, "", ""); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}

			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			keyPath := filepath.Join(projectRoot, "private-key.pem")
			if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
				t.Fatal(err)
			}
			runtime := &Runtime{config: Config{CredentialFile: filepath.Join(kingdomHome, "secrets", "operator-token")}, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
			if err = runtime.handleGitHubKeyProvision([]string{"--connection-id", "terminal-connection", "--from", keyPath}, false); err == nil {
				t.Fatal("terminal connection accepted a private key")
			}
			installed := filepath.Join(kingdomHome, "secrets", "github-apps", "terminal-connection", "private-key.pem")
			if _, err = os.Stat(installed); !os.IsNotExist(err) {
				t.Fatalf("terminal connection left a credential behind: %v", err)
			}
		})
	}
}

func TestGitHubKeyProvisionRejectsUserTokenStrategyBeforeReadingPEM(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatal(err)
	}
	kingdomHome := filepath.Join(projectRoot, ".kingdom")
	db, err := openSQLiteDatabase(filepath.Join(kingdomHome, "kingdom.db"), sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewExternalStore(db)
	at := "2026-08-15T10:00:00Z"
	_, err = store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: "shared-connection", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", InstallationID: "2", CredentialHandle: "github-user:shared-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-shared"}, InstallationAccountID: "10", InstallationAccountLogin: "octocat"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{config: Config{CredentialFile: filepath.Join(kingdomHome, "secrets", "operator-token")}, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	err = runtime.handleGitHubKeyProvision([]string{"--connection-id", "shared-connection", "--from", filepath.Join(projectRoot, "does-not-exist.pem")}, false)
	if err == nil || !strings.Contains(err.Error(), "Reconnect") {
		t.Fatalf("strategy rejection error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(kingdomHome, "secrets", "github-apps", "shared-connection")); !os.IsNotExist(err) {
		t.Fatalf("shared strategy mutated key filesystem: %v", err)
	}
}
