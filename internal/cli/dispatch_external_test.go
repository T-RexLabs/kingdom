package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/state"
)

func TestGitHubKeyProvisionRejectsRemovedConnectionBeforeWriting(t *testing.T) {
	home := filepath.Join(t.TempDir(), ".kingdom")
	secrets := filepath.Join(home, "secrets")
	if err := os.MkdirAll(secrets, 0o700); err != nil {
		t.Fatal(err)
	}
	credential := filepath.Join(secrets, "operator-token")
	if err := os.WriteFile(credential, []byte("test-only"), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(home, "kingdom.db")
	if err := ensureDatabaseSchema(dbPath); err != nil {
		t.Fatal(err)
	}
	db, err := openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewExternalStore(db)
	ctx := context.Background()
	if _, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-removed", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-removed", ActorID: "local-operator", CreatedAt: "2026-08-13T10:00:00Z", AuditEventID: "audit-create"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, "conn-removed", "removed", "removed", "", "2026-08-13T10:01:00Z", false, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "", AccountLogin: ""}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemPath := filepath.Join(t.TempDir(), "app.pem")
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(pemPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{config: Config{CredentialFile: credential}, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	if err := runtime.handleGitHubKeyProvision([]string{"--connection-id", "conn-removed", "--from", pemPath}, false); err == nil {
		t.Fatal("expected removed connection provisioning to fail")
	}
	keyPath := filepath.Join(secrets, "github-apps", "conn-removed", "private-key.pem")
	if _, err := os.Lstat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("removed connection key was recreated: %v", err)
	}
}
