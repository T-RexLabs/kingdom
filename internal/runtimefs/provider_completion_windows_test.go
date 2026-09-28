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

	sum := sha256.Sum256([]byte(invocationID))
	handoffPath := filepath.Join(rootPath, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
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
	if err != nil || !found || string(file.Wire) != string(wire) || file.Identity != identity {
		t.Fatalf("read handoff file=%#v found=%t err=%v", file, found, err)
	}
}
