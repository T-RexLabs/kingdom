//go:build darwin || linux

package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/securefs"
)

func TestProviderTerminalHandoffIsInvocationDerivedExactAndIdentityDeleted(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const invocationID = "invocation-trusted-handoff"
	wire := []byte(`{"version":"v1"}`)
	identity, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil {
		t.Fatalf("persist handoff: %v", err)
	}
	path := providerTerminalHandoffTestPath(rootPath, invocationID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat handoff: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("handoff mode = %o, want 600", info.Mode().Perm())
	}

	replayIdentity, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil || replayIdentity != identity {
		t.Fatalf("exact handoff replay identity=%#v err=%v, want %#v", replayIdentity, err, identity)
	}
	if _, err := store.PersistProviderTerminalHandoff(invocationID, []byte(`{"version":"changed"}`)); !errors.Is(err, securefs.ErrContentConflict) {
		t.Fatalf("changed handoff replay error = %v, want ErrContentConflict", err)
	}
	file, found, err := store.ReadProviderTerminalHandoff(invocationID)
	if err != nil || !found || string(file.Wire) != string(wire) || file.Identity != identity {
		t.Fatalf("read handoff file=%#v found=%t err=%v", file, found, err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); err != nil {
		t.Fatalf("identity-delete handoff: %v", err)
	}
	if _, found, err := store.ReadProviderTerminalHandoff(invocationID); err != nil || found {
		t.Fatalf("missing handoff found=%t err=%v", found, err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); err != nil {
		t.Fatalf("missing handoff removal: %v", err)
	}
}

func TestProviderTerminalHandoffWireBound(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const invocationID = "invocation-handoff-wire-boundary"
	limit := int(ProviderTerminalHandoffWireLimit)
	wire := make([]byte, limit+1)
	identity, err := store.PersistProviderTerminalHandoff(invocationID, wire[:limit])
	if err != nil || identity.SizeBytes != int64(limit) {
		t.Fatalf("persist exact-limit handoff identity=%#v err=%v", identity, err)
	}
	file, found, err := store.ReadProviderTerminalHandoff(invocationID)
	if err != nil || !found || len(file.Wire) != limit || file.Identity.SizeBytes != int64(limit) {
		t.Fatalf("read exact-limit handoff bytes=%d identity=%#v found=%t err=%v", len(file.Wire), file.Identity, found, err)
	}

	path := providerTerminalHandoffTestPath(rootPath, invocationID)
	if err := os.Truncate(path, int64(limit+1)); err != nil {
		t.Fatalf("extend handoff past wire limit: %v", err)
	}
	overflow, found, err := store.ReadProviderTerminalHandoff(invocationID)
	if !found || !errors.Is(err, securefs.ErrLimitExceeded) || overflow.Identity.SizeBytes != int64(limit+1) {
		t.Fatalf("oversized handoff identity=%#v found=%t err=%v, want bounded read", overflow.Identity, found, err)
	}

	const overflowInvocationID = "invocation-handoff-persist-overflow"
	if _, err := store.PersistProviderTerminalHandoff(overflowInvocationID, wire); err == nil {
		t.Fatal("expected persist to reject a handoff one byte over the limit")
	}
	if _, found, err := store.ReadProviderTerminalHandoff(overflowInvocationID); err != nil || found {
		t.Fatalf("rejected handoff persisted file: found=%t err=%v", found, err)
	}
}

func TestProviderTerminalHandoffCleanupRefusesReplacedFile(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const invocationID = "invocation-replaced-handoff"
	wire := []byte(`{"original":true}`)
	identity, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil {
		t.Fatalf("persist handoff: %v", err)
	}
	path := providerTerminalHandoffTestPath(rootPath, invocationID)
	displacedPath := path + ".displaced"
	if err := os.Rename(path, displacedPath); err != nil {
		t.Fatalf("displace original handoff: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"replacement":true}`), 0o600); err != nil {
		t.Fatalf("write replacement handoff: %v", err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("replacement cleanup error = %v, want ErrIdentityMismatch", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != `{"replacement":true}` {
		t.Fatalf("replacement handoff = %q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(displacedPath); err != nil || string(raw) != string(wire) {
		t.Fatalf("displaced original handoff = %q err=%v", raw, err)
	}
}

func TestProviderTerminalHandoffCleanupDoesNotFollowLeafSymlink(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const invocationID = "invocation-handoff-leaf-symlink"
	identity, err := store.PersistProviderTerminalHandoff(invocationID, []byte(`{"terminal":true}`))
	if err != nil {
		t.Fatalf("persist handoff: %v", err)
	}
	path := providerTerminalHandoffTestPath(rootPath, invocationID)
	outsidePath := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outsidePath, []byte("outside remains unchanged"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove original handoff: %v", err)
	}
	if err := os.Symlink(outsidePath, path); err != nil {
		t.Fatalf("replace handoff with symlink: %v", err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("symlink cleanup error = %v, want ErrIdentityMismatch", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("handoff symlink was removed: %v", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside remains unchanged" {
		t.Fatalf("symlink target = %q err=%v", raw, err)
	}
}

func TestProviderTerminalHandoffCleanupDoesNotFollowSymlinkedHandoffDirectory(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const invocationID = "invocation-handoff-parent-symlink"
	wire := []byte(`{"terminal":true}`)
	identity, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil {
		t.Fatalf("persist handoff: %v", err)
	}
	path := providerTerminalHandoffTestPath(rootPath, invocationID)
	handoffDirectory := filepath.Dir(path)
	displacedDirectory := handoffDirectory + ".displaced"
	if err := os.Rename(handoffDirectory, displacedDirectory); err != nil {
		t.Fatalf("displace handoff directory: %v", err)
	}
	outsideDirectory := t.TempDir()
	outsidePath := filepath.Join(outsideDirectory, filepath.Base(path))
	if err := os.WriteFile(outsidePath, []byte("outside remains unchanged"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsideDirectory, handoffDirectory); err != nil {
		t.Fatalf("replace handoff directory with symlink: %v", err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); err == nil {
		t.Fatal("expected cleanup through a symlinked handoff directory to fail")
	}
	if _, err := os.Lstat(handoffDirectory); err != nil {
		t.Fatalf("handoff directory symlink was removed: %v", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside remains unchanged" {
		t.Fatalf("symlink target = %q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(filepath.Join(displacedDirectory, filepath.Base(path))); err != nil || string(raw) != string(wire) {
		t.Fatalf("displaced handoff = %q err=%v", raw, err)
	}
}

func providerTerminalHandoffTestPath(rootPath, invocationID string) string {
	sum := sha256.Sum256([]byte(invocationID))
	return filepath.Join(rootPath, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
}
