//go:build windows

package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/securefs"

	"golang.org/x/sys/windows"
)

func TestWindowsProviderTerminalHandoffCreatesParentsAndReplaysExactly(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new runtime filesystem store: %v", err)
	}
	const invocationID = "windows-terminal-handoff"
	wire := []byte(`{"version":"v1","terminal":true}`)
	identity, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}

	handoffPath := providerTerminalHandoffTestPath(rootPath, invocationID)
	for _, directory := range []string{
		filepath.Dir(filepath.Dir(handoffPath)),
		filepath.Dir(handoffPath),
	} {
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("handoff parent %q info=%v err=%v", directory, info, err)
		}
	}

	replay, err := store.PersistProviderTerminalHandoff(invocationID, wire)
	if err != nil || replay != identity {
		t.Fatalf("exact handoff replay identity=%#v err=%v, want %#v", replay, err, identity)
	}
	if _, err := store.PersistProviderTerminalHandoff(invocationID, []byte(`{"version":"changed"}`)); !errors.Is(err, securefs.ErrContentConflict) {
		t.Fatalf("changed handoff replay error = %v, want ErrContentConflict", err)
	}
	file, found, err := store.ReadProviderTerminalHandoff(invocationID)
	if err != nil || !found || string(file.Wire) != string(wire) ||
		file.Identity.Device != identity.Device || file.Identity.Inode != identity.Inode ||
		file.Identity.SizeBytes != identity.SizeBytes || file.Identity.ContentSHA256 == "" {
		t.Fatalf("read handoff file=%#v found=%t err=%v", file, found, err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, file.Identity); err != nil {
		t.Fatalf("identity-delete terminal handoff: %v", err)
	}
	if _, found, err := store.ReadProviderTerminalHandoff(invocationID); err != nil || found {
		t.Fatalf("removed handoff found=%t err=%v", found, err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); err != nil {
		t.Fatalf("missing handoff removal: %v", err)
	}
}

func TestWindowsProviderTerminalHandoffWireBound(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new runtime filesystem store: %v", err)
	}
	const invocationID = "windows-terminal-handoff-wire-boundary"
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

	handoffPath := providerTerminalHandoffTestPath(rootPath, invocationID)
	if err := os.Truncate(handoffPath, int64(limit+1)); err != nil {
		t.Fatalf("extend handoff past wire limit: %v", err)
	}
	overflow, found, err := store.ReadProviderTerminalHandoff(invocationID)
	if !found || !errors.Is(err, securefs.ErrLimitExceeded) || overflow.Identity.SizeBytes != int64(limit+1) {
		t.Fatalf("oversized handoff identity=%#v found=%t err=%v, want bounded read", overflow.Identity, found, err)
	}

	const overflowInvocationID = "windows-terminal-handoff-persist-overflow"
	if _, err := store.PersistProviderTerminalHandoff(overflowInvocationID, wire); err == nil {
		t.Fatal("expected persist to reject a handoff one byte over the limit")
	}
	if _, found, err := store.ReadProviderTerminalHandoff(overflowInvocationID); err != nil || found {
		t.Fatalf("rejected handoff persisted file: found=%t err=%v", found, err)
	}
}

func TestWindowsProviderTerminalHandoffCleanupDoesNotFollowLeafReparsePoint(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new runtime filesystem store: %v", err)
	}
	const invocationID = "windows-terminal-handoff-leaf-reparse"
	identity, err := store.PersistProviderTerminalHandoff(invocationID, []byte(`{"terminal":true}`))
	if err != nil {
		t.Fatalf("persist terminal handoff: %v", err)
	}
	handoffPath := providerTerminalHandoffTestPath(rootPath, invocationID)
	outsidePath := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outsidePath, []byte("outside remains unchanged"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Remove(handoffPath); err != nil {
		t.Fatalf("remove original handoff: %v", err)
	}
	if err := os.Symlink(outsidePath, handoffPath); err != nil {
		if errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			t.Skipf("file symlink creation is unavailable: %v", err)
		}
		t.Fatalf("replace handoff with reparse point: %v", err)
	}
	if err := store.RemoveProviderTerminalHandoff(invocationID, identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("reparse-point cleanup error = %v, want ErrIdentityMismatch", err)
	}
	if _, err := os.Lstat(handoffPath); err != nil {
		t.Fatalf("handoff reparse point was removed: %v", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside remains unchanged" {
		t.Fatalf("reparse-point target = %q err=%v", raw, err)
	}
}

func providerTerminalHandoffTestPath(rootPath, invocationID string) string {
	sum := sha256.Sum256([]byte(invocationID))
	return filepath.Join(rootPath, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
}
