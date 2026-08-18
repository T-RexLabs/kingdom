//go:build !windows

package github

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func testPrivateKey(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func TestLifecycleLockCreatesMissingProtectedSecretsRoot(t *testing.T) {
	project := t.TempDir()
	if err := os.Mkdir(filepath.Join(project, ".kingdom"), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(filepath.Join(project, ".kingdom", "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WithConnectionLock("connection-1", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(project, ".kingdom", "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("secrets root mode=%v", info.Mode().Perm())
	}
}

func secureKeyStore(t *testing.T) (*KeyStore, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "secrets")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return store, root
}

func TestKeyStoreRejectsEscapingAndAliasedConnectionIDsBeforeMutation(t *testing.T) {
	store, root := secureKeyStore(t)
	outside := filepath.Join(filepath.Dir(root), "outside")
	if err := os.Mkdir(outside, 0755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(outside, "private-key.pem")
	if err := os.WriteFile(sentinel, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", ".", "..", "../outside", "a/b", `a\b`, "/tmp/escape", " spaced", "trailing ", "a/../b"} {
		if _, err := store.Provision(id, testPrivateKey(t)); err == nil {
			t.Fatalf("Provision(%q) succeeded", id)
		}
		if err := store.Remove(id); err == nil {
			t.Fatalf("Remove(%q) succeeded", id)
		}
	}
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "sentinel" {
		t.Fatalf("outside sentinel changed: %q, %v", got, err)
	}
}

func TestKeyStoreRefusesSymlinkedPathComponents(t *testing.T) {
	store, root := secureKeyStore(t)
	out := t.TempDir()
	if err := os.Symlink(out, filepath.Join(root, "github-apps")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Provision("connection-safe", testPrivateKey(t)); err == nil {
		t.Fatal("provision followed github-apps symlink")
	}
	if _, err := os.Stat(filepath.Join(out, "connection-safe")); !os.IsNotExist(err) {
		t.Fatalf("created path through symlink: %v", err)
	}
}

func TestKeyStoreRefusesUnsafeDirectoryAndFileMetadata(t *testing.T) {
	store, root := secureKeyStore(t)
	if err := os.Chmod(root, 0750); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Provision("connection-safe", testPrivateKey(t)); err == nil {
		t.Fatal("accepted loose secrets root")
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Provision("connection-safe", testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	keyPath := store.path("connection-safe")
	if err := os.Chmod(keyPath, 0640); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-safe"); err == nil {
		t.Fatal("accepted loose key permissions")
	}
	if err := store.Remove("connection-safe"); err == nil {
		t.Fatal("removed key without validating permissions")
	}
}

func TestKeyStoreRefusesFinalSymlinkAndHardLink(t *testing.T) {
	store, _ := secureKeyStore(t)
	if _, err := store.Provision("connection-safe", testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	keyPath := store.path("connection-safe")
	copyPath := filepath.Join(filepath.Dir(keyPath), "copy.pem")
	if err := os.Link(keyPath, copyPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-safe"); err == nil {
		t.Fatal("accepted hard-linked private key")
	}
	if err := os.Remove(copyPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(keyPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), keyPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-safe"); err == nil {
		t.Fatal("followed final symlink")
	}
	if err := store.Remove("connection-safe"); err == nil {
		t.Fatal("removed final symlink")
	}
}
