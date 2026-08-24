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
			if _, err = store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "terminal-connection", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:terminal-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-create"}); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			health := lifecycle
			if err = store.UpdateConnectionState(context.Background(), "terminal-connection", lifecycle, health, "", at, false, nil); err != nil {
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
			installed := filepath.Join(kingdomHome, "secrets", "github-apps", "terminal-connection", "private-key.1.pem")
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
	_, err = store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: "shared-connection", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-user:shared-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-shared"}})
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

func TestGitHubKeyProvisionAndRotationPublishDatabaseSelectedGenerations(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatal(err)
	}
	kingdomHome := filepath.Join(projectRoot, ".kingdom")
	dbPath := filepath.Join(kingdomHome, "kingdom.db")
	db, err := openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewExternalStore(db)
	at := "2026-08-24T10:00:00Z"
	if _, err := store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "rotate-connection", CredentialStrategy: "app_private_key", DisplayName: "Rotate", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:rotate-connection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-rotate-create"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	writeKey := func(name string) string {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(projectRoot, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	runtime := &Runtime{config: Config{CredentialFile: filepath.Join(kingdomHome, "secrets", "operator-token")}, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	if err := runtime.handleGitHubKeyProvision([]string{"--connection-id", "rotate-connection", "--from", writeKey("first.pem")}, false); err != nil {
		t.Fatal(err)
	}
	db, err = openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	current, err := state.NewExternalStore(db).GetConnection(context.Background(), "rotate-connection")
	if err != nil || current.CredentialGeneration != 1 {
		t.Fatalf("provisioned connection=%#v err=%v", current, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(kingdomHome, "secrets", "github-apps", "rotate-connection", "private-key.1.pem")); err != nil {
		t.Fatalf("generation 1 is not durable: %v", err)
	}

	if err := runtime.handleGitHubKeyProvision([]string{"--connection-id", "rotate-connection", "--from", writeKey("second.pem")}, true); err != nil {
		t.Fatal(err)
	}
	db, err = openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	current, err = state.NewExternalStore(db).GetConnection(context.Background(), "rotate-connection")
	if err != nil || current.CredentialGeneration != 2 || current.ValidatedGeneration != 0 {
		t.Fatalf("rotated connection=%#v err=%v", current, err)
	}
	if _, err := os.Stat(filepath.Join(kingdomHome, "secrets", "github-apps", "rotate-connection", "private-key.2.pem")); err != nil {
		t.Fatalf("generation 2 is not durable: %v", err)
	}
}
