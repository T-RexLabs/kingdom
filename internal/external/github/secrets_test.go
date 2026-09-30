//go:build !windows

package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
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
		if _, err := store.Provision(id, 1, testPrivateKey(t)); err == nil {
			t.Fatalf("Provision(%q) succeeded", id)
		}
		if err := store.RemoveAll(id); err == nil {
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
	if _, err := store.Provision("connection-safe", 1, testPrivateKey(t)); err == nil {
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
	if _, err := store.Provision("connection-safe", 1, testPrivateKey(t)); err == nil {
		t.Fatal("accepted loose secrets root")
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Provision("connection-safe", 1, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	keyPath := store.path("connection-safe", 1)
	if err := os.Chmod(keyPath, 0640); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-safe", 1); err == nil {
		t.Fatal("accepted loose key permissions")
	}
	if err := store.RemoveAll("connection-safe"); err == nil {
		t.Fatal("removed key without validating permissions")
	}
}

func TestKeyStoreRefusesFinalSymlinkAndHardLink(t *testing.T) {
	store, _ := secureKeyStore(t)
	if _, err := store.Provision("connection-safe", 1, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	keyPath := store.path("connection-safe", 1)
	copyPath := filepath.Join(filepath.Dir(keyPath), "copy.pem")
	if err := os.Link(keyPath, copyPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-safe", 1); err == nil {
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
	if _, _, err := store.Load("connection-safe", 1); err == nil {
		t.Fatal("followed final symlink")
	}
	if err := store.RemoveAll("connection-safe"); err == nil {
		t.Fatal("removed final symlink")
	}
}

func TestKeyGenerationPublicationCrashWindows(t *testing.T) {
	if os.Getenv("KINGDOM_KEY_CRASH_HELPER") == "1" {
		root := os.Getenv("KINGDOM_KEY_CRASH_ROOT")
		raw, err := os.ReadFile(os.Getenv("KINGDOM_KEY_CRASH_PEM"))
		if err != nil {
			os.Exit(20)
		}
		store, err := NewKeyStore(root)
		if err != nil {
			os.Exit(21)
		}
		_, err = store.InstallAudited("connection-1", 2, raw, true, func() error {
			db, openErr := sql.Open("sqlite", os.Getenv("KINGDOM_KEY_CRASH_DB"))
			if openErr != nil {
				os.Exit(22)
			}
			defer db.Close()
			var selected int
			if queryErr := db.QueryRow(`SELECT generation FROM publication WHERE singleton=1`).Scan(&selected); queryErr != nil || selected != 1 {
				os.Exit(24)
			}
			os.Exit(23)
			return nil
		})
		if err != nil {
			os.Exit(25)
		}
		os.Exit(26)
	}

	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	first := testPrivateKey(t)
	firstFingerprint, err := store.Provision("connection-1", 1, first)
	if err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(t.TempDir(), "second.pem")
	if err := os.WriteFile(secondPath, testPrivateKey(t), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "publication.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE publication (singleton INTEGER PRIMARY KEY, generation INTEGER NOT NULL); INSERT INTO publication VALUES (1,1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestKeyGenerationPublicationCrashWindows$")
	command.Env = append(os.Environ(),
		"KINGDOM_KEY_CRASH_HELPER=1",
		"KINGDOM_KEY_CRASH_ROOT="+root,
		"KINGDOM_KEY_CRASH_PEM="+secondPath,
		"KINGDOM_KEY_CRASH_DB="+dbPath,
	)
	err = command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("crash helper error=%v", err)
	}
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var selected int
	if err := db.QueryRow(`SELECT generation FROM publication WHERE singleton=1`).Scan(&selected); err != nil || selected != 1 {
		t.Fatalf("uncommitted generation became active: selected=%d err=%v", selected, err)
	}
	if _, fingerprint, err := store.Load("connection-1", selected); err != nil || fingerprint != firstFingerprint {
		t.Fatalf("selected generation load fingerprint=%q err=%v", fingerprint, err)
	}
	if _, _, err := store.Load("connection-1", 2); err != nil {
		t.Fatalf("crash did not leave the expected durable staged generation: %v", err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"connection-1": 1})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("connection-1", 2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup reconciliation retained unpublished generation: %v", err)
	}

	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := store.InstallAudited("connection-1", 2, second, true, func() error {
		_, updateErr := db.ExecContext(context.Background(), `UPDATE publication SET generation=2 WHERE singleton=1 AND generation=1`)
		return updateErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT generation FROM publication WHERE singleton=1`).Scan(&selected); err != nil || selected != 2 {
		t.Fatalf("committed generation was not selected: selected=%d err=%v", selected, err)
	}
	if _, fingerprint, err := store.Load("connection-1", selected); err != nil || fingerprint != secondFingerprint || fingerprint == firstFingerprint {
		t.Fatalf("committed generation load fingerprint=%q selected=%s first=%s err=%v", fingerprint, strconv.Itoa(selected), firstFingerprint, err)
	}
}

func TestKeyStoreReconcilesStaleAndUnownedGenerationsAndTombstoneRemovesAll(t *testing.T) {
	store, _ := secureKeyStore(t)
	if _, err := store.Provision("owned", 1, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Rotate("owned", 2, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Provision("unowned", 1, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"owned": 2})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("owned", 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale generation remains: %v", err)
	}
	if _, _, err := store.Load("owned", 2); err != nil {
		t.Fatalf("selected generation was removed: %v", err)
	}
	if _, _, err := store.Load("unowned", 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned generation remains: %v", err)
	}
	if _, err := store.Rotate("owned", 3, testPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveAll("owned"); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []int{2, 3} {
		if _, _, err := store.Load("owned", generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("generation %d remains after tombstone cleanup: %v", generation, err)
		}
	}
}

func TestKeyStoreReconciliationReadsSelectedGenerationAfterConnectionLock(t *testing.T) {
	store, _ := secureKeyStore(t)
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE publication (connection_id TEXT PRIMARY KEY, generation INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	connectionIDs := []string{"connection-a", "connection-b"}
	for _, connectionID := range connectionIDs {
		if _, err := store.Provision(connectionID, 1, testPrivateKey(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO publication (connection_id,generation) VALUES (?,1)`, connectionID); err != nil {
			t.Fatal(err)
		}
	}

	firstResolved := make(chan string, 1)
	releaseFirst := make(chan struct{})
	var first sync.Once
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- store.ReconcileGenerations(func(connectionID string) (int, error) {
			var generation int
			err := db.QueryRow(`SELECT generation FROM publication WHERE connection_id=?`, connectionID).Scan(&generation)
			first.Do(func() {
				firstResolved <- connectionID
				<-releaseFirst
			})
			return generation, err
		})
	}()

	var firstConnection string
	select {
	case firstConnection = <-firstResolved:
	case err := <-reconcileDone:
		t.Fatalf("reconciliation stopped before resolving a generation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("reconciliation did not reach the first generation lookup")
	}
	targetConnection := connectionIDs[0]
	if targetConnection == firstConnection {
		targetConnection = connectionIDs[1]
	}
	_, publishErr := store.InstallAudited(targetConnection, 2, testPrivateKey(t), true, func() error {
		_, err := db.Exec(`UPDATE publication SET generation=2 WHERE connection_id=? AND generation=1`, targetConnection)
		return err
	})
	close(releaseFirst)
	if publishErr != nil {
		t.Fatal(publishErr)
	}
	select {
	case err := <-reconcileDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciliation did not finish")
	}
	if _, _, err := store.Load(targetConnection, 2); err != nil {
		t.Fatalf("reconciliation removed the generation selected after enumeration: %v", err)
	}
}
