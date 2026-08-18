package github

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testUserTokens(now time.Time, suffix string) UserTokens {
	return UserTokens{AccessToken: "access-token-" + suffix, RefreshToken: "refresh-token-" + suffix, AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}
}

func TestUserTokenStoreProtectedAtomicLifecycle(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Store("connection-1", testUserTokens(now, "one")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.json")
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
	if err := store.InstallAudited("connection-1", testUserTokens(now, "two"), func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("InstallAudited error=%v", err)
	}
	loaded, err := store.Load("connection-1")
	if err != nil || loaded.AccessToken != "access-token-one" {
		t.Fatalf("rollback tokens=%#v err=%v", loaded, err)
	}
	if err := store.Remove("connection-1"); err != nil {
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
	if err := store.Store("connection-1", testUserTokens(now, "initial")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.json")
	previous := []byte("{\n  \"refresh_expires_at\": \"" + now.Add(time.Hour).Format(time.RFC3339Nano) + "\",\n  \"access_token\": \"expired-access-token\",\n  \"access_expires_at\": \"" + now.Add(-time.Hour).Format(time.RFC3339Nano) + "\",\n  \"refresh_token\": \"previous-refresh-token\"\n}\n")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatal(err)
	}
	conflict := errors.New("audit conflict")
	if err := store.InstallAudited("connection-1", testUserTokens(now, "replacement"), func() error { return conflict }); !errors.Is(err, conflict) {
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

func TestUserTokenStoreInstallAuditedRemovesNewFileOnFailure(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	conflict := errors.New("audit conflict")
	if err := store.InstallAudited("connection-1", testUserTokens(now, "new"), func() error { return conflict }); !errors.Is(err, conflict) {
		t.Fatalf("InstallAudited error=%v", err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.json")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new token file remains after rollback: %v", err)
	}
}

func TestUserTokenStoreInstallAuditedRejectsMalformedPreviousFile(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, err := NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Store("connection-1", testUserTokens(now, "initial")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.json")
	malformed := []byte(`{"access_token":"unsafe"}`)
	if err := os.WriteFile(path, malformed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallAudited("connection-1", testUserTokens(now, "replacement"), func() error { return nil }); err == nil {
		t.Fatal("expected malformed previous token file to be rejected")
	}
	remaining, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remaining, malformed) {
		t.Fatalf("malformed file was overwritten: %q", remaining)
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
				if err := store.Store("connection-1", testUserTokens(now, "one")); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(root, "github-user-tokens", "connection-1", "tokens.json")
				if err := os.Link(path, filepath.Join(base, "stolen")); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Store("connection-1", testUserTokens(now, "two")); err == nil {
				t.Fatal("expected unsafe filesystem layout to be rejected")
			}
		})
	}
}

func TestUserTokenStoreCleansOnlyOwnedTemporaryFiles(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	store, _ := NewUserTokenStore(root)
	if err := store.Store("connection-1", testUserTokens(now, "one")); err != nil {
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
	if err := store.CleanupTemps(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temporary file remains: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
}
