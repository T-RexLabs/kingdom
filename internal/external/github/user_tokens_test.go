package github

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func testUserTokens(now time.Time, suffix string) UserTokens {
	return UserTokens{AccessToken: "access-token-" + suffix, RefreshToken: "refresh-token-" + suffix, AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
}

func resolveCredentialGenerationsForTest(selected map[string]int) func(string) (int, error) {
	return func(connectionID string) (int, error) { return selected[connectionID], nil }
}

func TestUserTokenStoreProtectedAtomicLifecycle(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Store("connection-1", 1, testUserTokens(now, "one")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.1.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode=%v err=%v", info.Mode(), err)
	}
	for _, directory := range []string{root, filepath.Dir(filepath.Dir(path)), filepath.Dir(path)} {
		info, err := os.Stat(directory)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("directory %s mode=%v err=%v", directory, info.Mode(), err)
		}
	}
	want := errors.New("database failed")
	if err := store.InstallAudited("connection-1", 2, testUserTokens(now, "two"), func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("InstallAudited error=%v", err)
	}
	loaded, err := store.Load("connection-1", 1)
	if err != nil || loaded.AccessToken != "access-token-one" {
		t.Fatalf("rollback tokens=%#v err=%v", loaded, err)
	}
	if err := store.RemoveAll("connection-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("token file remains after removal: %v", err)
	}
}

func TestUserTokenStoreInstallAuditedRestoresExpiredPreviousBytes(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Store("connection-1", 1, testUserTokens(now, "initial")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.1.json")
	previous := []byte("{\n  \"refresh_expires_at\": \"" + now.Add(time.Hour).Format(time.RFC3339Nano) + "\",\n  \"access_token\": \"expired-access-token\",\n  \"access_expires_at\": \"" + now.Add(-time.Hour).Format(time.RFC3339Nano) + "\",\n  \"refresh_token\": \"previous-refresh-token\"\n}\n")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatal(err)
	}
	conflict := errors.New("audit conflict")
	if err := store.InstallAudited("connection-1", 2, testUserTokens(now, "replacement"), func() error { return conflict }); !errors.Is(err, conflict) {
		t.Fatalf("InstallAudited error=%v", err)
	}
	restored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, previous) {
		t.Fatalf("rollback did not restore exact previous bytes\nwant: %q\n got: %q", previous, restored)
	}
	if strings.Contains(string(restored), "replacement") {
		t.Fatalf("replacement credentials remain after rollback: %q", restored)
	}
}

func TestUserTokenStoreInstallAuditedLeavesNewFileForReconciliationOnFailure(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	conflict := errors.New("audit conflict")
	if err := store.InstallAudited("connection-1", 1, testUserTokens(now, "new"), func() error { return conflict }); !errors.Is(err, conflict) {
		t.Fatalf("InstallAudited error=%v", err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.1.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("durable failed-publication stage is missing: %v", err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{})); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned token stage remains after reconciliation: %v", err)
	}
}

func TestUserTokenStoreNewGenerationDoesNotReadOrOverwriteMalformedPreviousFile(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Store("connection-1", 1, testUserTokens(now, "initial")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.1.json")
	malformed := []byte(`{"access_token":"unsafe"}`)
	if err := os.WriteFile(path, malformed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAudited("connection-1", 2, testUserTokens(now, "replacement"), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	remaining, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, malformed) {
		t.Fatalf("malformed file was overwritten: %q", remaining)
	}
	if replacement, err := store.Load("connection-1", 2); err != nil || replacement.AccessToken != "access-token-replacement" {
		t.Fatalf("replacement generation=%#v err=%v", replacement, err)
	}
}

func TestUserTokenStoreRejectsSymlinkAndHardLinkAttacks(t *testing.T) {
	now := time.Now().UTC()
	for _, attack := range []string{"symlink-root", "hard-link-token"} {
		t.Run(attack, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, "secrets")
			store, _ := NewUserTokenStore(root)
			switch attack {
			case "symlink-root":
				outside := filepath.Join(base, "outside")
				if err := os.Mkdir(outside, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, root); err != nil {
					t.Fatal(err)
				}
			case "hard-link-token":
				if err := store.Store("connection-1", 1, testUserTokens(now, "one")); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.1.json")
				if err := os.Link(path, filepath.Join(base, "stolen")); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Store("connection-1", 1, testUserTokens(now, "two")); err == nil {
				t.Fatal("expected unsafe filesystem layout to be rejected")
			}
		})
	}
}

func TestUserTokenStoreCleansOnlyOwnedTemporaryFiles(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, _ := NewUserTokenStore(root)
	if err := store.Store("connection-1", 1, testUserTokens(now, "one")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "github-user-tokens", "connection-1")
	tmp := filepath.Join(dir, ".tokens-deadbeef.tmp")
	keep := filepath.Join(dir, "unrelated")
	if err := os.WriteFile(tmp, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"connection-1": 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temporary file remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
}

func TestUserTokenStoreReconcilesAndRemovesEveryGeneration(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, _ := NewUserTokenStore(root)
	for _, generation := range []int{1, 2, 3} {
		if err := store.Store("owned", generation, testUserTokens(now, string(rune('a'+generation)))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Store("unowned", 1, testUserTokens(now, "orphan")); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"owned": 2})); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load("owned", 2); err != nil {
		t.Fatalf("selected token generation was removed: %v", err)
	}
	for _, item := range []struct {
		id         string
		generation int
	}{{"owned", 1}, {"owned", 3}, {"unowned", 1}} {
		if _, err := store.Load(item.id, item.generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale token %s/%d remains: %v", item.id, item.generation, err)
		}
	}
	if err := store.Store("owned", 3, testUserTokens(now, "new")); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveAll("owned"); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []int{2, 3} {
		if _, err := store.Load("owned", generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("token generation %d remains after tombstone cleanup: %v", generation, err)
		}
	}
}

func TestUserTokenStoreReconciliationReadsSelectedGenerationAfterConnectionLock(t *testing.T) {
	now := time.Now().UTC()
	store, err := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
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
		if err := store.Store(connectionID, 1, testUserTokens(now, "one")); err != nil {
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
	publishErr := store.InstallAudited(targetConnection, 2, testUserTokens(now, "two"), func() error {
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
	if _, err := store.Load(targetConnection, 2); err != nil {
		t.Fatalf("reconciliation removed the generation selected after enumeration: %v", err)
	}
}
