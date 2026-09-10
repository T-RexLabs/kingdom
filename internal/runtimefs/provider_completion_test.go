//go:build darwin || linux

package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/securefs"

	"golang.org/x/sys/unix"
)

func TestProviderCompletionInboxHashesExactCanonicalInvocationID(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	invocationID := "../../run-targets/direct invocation"
	inbox, err := store.EnsureProviderCompletionInbox(invocationID)
	if err != nil {
		t.Fatalf("ensure inbox: %v", err)
	}
	sum := sha256.Sum256([]byte(invocationID))
	want := filepath.Join(rootPath, "provider-completion", "receipts", hex.EncodeToString(sum[:]))
	if inbox != want {
		t.Fatalf("inbox = %q, want %q", inbox, want)
	}
	if strings.Contains(inbox, "run-targets") || strings.Contains(inbox, "direct invocation") {
		t.Fatalf("inbox exposes invocation path text: %q", inbox)
	}
	if _, err := store.EnsureProviderCompletionInbox("  " + invocationID); err == nil {
		t.Fatal("expected noncanonical invocation id rejection")
	}
	otherInbox, err := store.EnsureProviderCompletionInbox("../../run-targets/direct_invocation")
	if err != nil {
		t.Fatalf("ensure distinct inbox: %v", err)
	}
	if otherInbox == inbox {
		t.Fatal("distinct invocation ids must not share an inbox")
	}
}

func TestProviderCompletionReceiptWireBoundaryAndSpecialFiles(t *testing.T) {
	t.Parallel()

	store, inbox := newProviderCompletionTestInbox(t, "invocation-boundary")
	exact := strings.Repeat("x", int(CompletionReceiptWireLimit))
	if err := os.WriteFile(filepath.Join(inbox, "exact.json"), []byte(exact), 0o600); err != nil {
		t.Fatalf("write exact receipt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inbox, "overflow.json"), []byte(exact+"x"), 0o600); err != nil {
		t.Fatalf("write overflow receipt: %v", err)
	}
	receipt, err := store.ReadProviderCompletionReceipt("invocation-boundary", "exact.json")
	if err != nil {
		t.Fatalf("read exact receipt: %v", err)
	}
	if len(receipt.Wire) != int(CompletionReceiptWireLimit) {
		t.Fatalf("exact receipt bytes = %d", len(receipt.Wire))
	}
	overflow, err := store.ReadProviderCompletionReceipt("invocation-boundary", "overflow.json")
	if !errors.Is(err, securefs.ErrLimitExceeded) {
		t.Fatalf("overflow error = %v, want securefs.ErrLimitExceeded", err)
	}
	if overflow.Identity == (securefs.FileIdentity{}) {
		t.Fatal("overflow receipt must retain identity for safe deletion")
	}

	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(inbox, "symlink.json")); err != nil {
		t.Fatalf("create receipt symlink: %v", err)
	}
	if err := unix.Mkfifo(filepath.Join(inbox, "fifo.json"), 0o600); err != nil {
		t.Fatalf("create receipt fifo: %v", err)
	}
	started := time.Now()
	if _, err := store.ReadProviderCompletionReceipt("invocation-boundary", "fifo.json"); err == nil {
		t.Fatal("expected fifo rejection")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("fifo rejection blocked for %s", elapsed)
	}
	if _, err := store.ReadProviderCompletionReceipt("invocation-boundary", "symlink.json"); err == nil {
		t.Fatal("expected symlink rejection")
	}
	if _, err := store.ReadProviderCompletionReceipt("invocation-boundary", "../exact.json"); err == nil {
		t.Fatal("expected nested entry name rejection")
	}
}

func TestProviderCompletionEnumerationIsBoundedAndRepresentsEntryTypes(t *testing.T) {
	t.Parallel()

	store, inbox := newProviderCompletionTestInbox(t, "invocation-enumeration")
	if err := os.WriteFile(filepath.Join(inbox, "regular.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write regular entry: %v", err)
	}
	if err := os.Symlink("regular.json", filepath.Join(inbox, "symlink.json")); err != nil {
		t.Fatalf("create symlink entry: %v", err)
	}
	if err := unix.Mkfifo(filepath.Join(inbox, "fifo.json"), 0o600); err != nil {
		t.Fatalf("create fifo entry: %v", err)
	}
	if err := os.Mkdir(filepath.Join(inbox, "nested"), 0o700); err != nil {
		t.Fatalf("create nested directory: %v", err)
	}
	shortRoot, err := os.MkdirTemp("/tmp", "kd-runtimefs-socket-")
	if err != nil {
		t.Fatalf("create short socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	shortInbox := filepath.Join(shortRoot, "inbox")
	if err := os.Symlink(inbox, shortInbox); err != nil {
		t.Fatalf("create short inbox alias: %v", err)
	}
	socketPath := filepath.Join(shortInbox, "socket.json")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("create socket entry: %v", err)
	}
	defer listener.Close()

	entries, err := store.ListProviderCompletionEntries("invocation-enumeration")
	if err != nil {
		t.Fatalf("list typed entries: %v", err)
	}
	types := make(map[string]securefs.EntryType, len(entries))
	for _, entry := range entries {
		types[entry.Name] = entry.Type
	}
	want := map[string]securefs.EntryType{
		"regular.json": securefs.EntryTypeRegular,
		"symlink.json": securefs.EntryTypeSymlink,
		"fifo.json":    securefs.EntryTypeFIFO,
		"nested":       securefs.EntryTypeDirectory,
		"socket.json":  securefs.EntryTypeSocket,
	}
	for name, entryType := range want {
		if types[name] != entryType {
			t.Fatalf("entry %q type = %q, want %q", name, types[name], entryType)
		}
	}

	for index := len(entries); index <= CompletionReceiptEntryLimit; index++ {
		name := filepath.Join(inbox, "overflow-"+string(rune('a'+index)))
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatalf("write overflow entry %d: %v", index, err)
		}
	}
	entries, err = store.ListProviderCompletionEntries("invocation-enumeration")
	if !errors.Is(err, securefs.ErrLimitExceeded) {
		t.Fatalf("entry overflow error = %v, want securefs.ErrLimitExceeded", err)
	}
	if len(entries) != CompletionReceiptEntryLimit {
		t.Fatalf("bounded entry count = %d, want %d", len(entries), CompletionReceiptEntryLimit)
	}
	if err := store.RemoveProviderCompletionEntry("invocation-enumeration", "nested"); err == nil {
		t.Fatal("expected nested directory deletion rejection")
	}
}

func TestProviderCompletionInboxRejectsSymlinkParent(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	store, err := NewStore(openRuntimeFSTestDB(t), rootPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(rootPath, "provider-completion")); err != nil {
		t.Fatalf("create spool parent symlink: %v", err)
	}
	if _, err := store.EnsureProviderCompletionInbox("invocation-symlink-parent"); err == nil {
		t.Fatal("expected spool parent symlink rejection")
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatalf("read outside directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("symlink target received %d entries", len(entries))
	}
}

func TestProviderCompletionInboxRemovalIsEmptyOnly(t *testing.T) {
	t.Parallel()

	store, inbox := newProviderCompletionTestInbox(t, "invocation-removal")
	if err := os.WriteFile(filepath.Join(inbox, "receipt.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	if err := store.RemoveProviderCompletionInbox("invocation-removal"); err == nil {
		t.Fatal("expected nonempty inbox removal rejection")
	}
	if _, err := os.Stat(filepath.Join(inbox, "receipt.json")); err != nil {
		t.Fatalf("nonempty inbox content was removed: %v", err)
	}
	if err := store.RemoveProviderCompletionEntry("invocation-removal", "receipt.json"); err != nil {
		t.Fatalf("remove receipt entry: %v", err)
	}
	if err := store.RemoveProviderCompletionInbox("invocation-removal"); err != nil {
		t.Fatalf("remove empty inbox: %v", err)
	}
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatalf("empty inbox remains: %v", err)
	}
}

func newProviderCompletionTestInbox(t *testing.T, invocationID string) (*Store, string) {
	t.Helper()
	store, err := NewStore(openRuntimeFSTestDB(t), t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	inbox, err := store.EnsureProviderCompletionInbox(invocationID)
	if err != nil {
		t.Fatalf("ensure provider completion inbox: %v", err)
	}
	return store, inbox
}
