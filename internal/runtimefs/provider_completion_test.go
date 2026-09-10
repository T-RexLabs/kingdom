//go:build darwin || linux

package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	nested := ProviderCompletionEntry{Name: "nested", Type: securefs.EntryTypeDirectory}
	for _, entry := range entries {
		if entry.Name == "nested" {
			nested = entry
			break
		}
	}
	if err := store.RemoveProviderCompletionEntryIfIdentity("invocation-enumeration", nested); err == nil {
		t.Fatal("expected nested directory deletion rejection")
	}
}

func TestProviderCompletionStagingEntryNameGrammar(t *testing.T) {
	t.Parallel()

	valid := ".receipt-123e4567-e89b-42d3-a456-426614174000.tmp"
	if !IsProviderCompletionStagingEntryName(valid) {
		t.Fatalf("expected %q to match staging grammar", valid)
	}
	for _, invalid := range []string{
		"receipt-123e4567-e89b-42d3-a456-426614174000.tmp",
		".receipt-123E4567-E89B-42D3-A456-426614174000.tmp",
		".receipt-123e4567-e89b-12d3-a456-426614174000.tmp",
		".receipt-123e4567-e89b-42d3-7456-426614174000.tmp",
		".receipt-123e4567-e89b-42d3-a456-42661417400.tmp",
		".receipt-123e4567-e89b-42d3-a456-426614174000.json",
		"nested/.receipt-123e4567-e89b-42d3-a456-426614174000.tmp",
	} {
		if IsProviderCompletionStagingEntryName(invalid) {
			t.Fatalf("unexpected staging grammar match for %q", invalid)
		}
	}
}

func TestProviderCompletionStagingEntriesDoNotConsumeReceiptBound(t *testing.T) {
	t.Parallel()

	store, inbox := newProviderCompletionTestInbox(t, "invocation-publication-race")
	for index := range CompletionReceiptEntryLimit {
		name := filepath.Join(inbox, fmt.Sprintf("receipt-%02d.json", index))
		if err := os.WriteFile(name, []byte("{}"), 0o600); err != nil {
			t.Fatalf("write receipt %d: %v", index, err)
		}
	}
	stagingNames := []string{
		".receipt-123e4567-e89b-42d3-a456-426614174000.tmp",
		".receipt-123e4567-e89b-42d3-b456-426614174001.tmp",
	}
	for _, stagingName := range stagingNames {
		if err := os.WriteFile(filepath.Join(inbox, stagingName), []byte("{}"), 0o600); err != nil {
			t.Fatalf("write staging entry: %v", err)
		}
	}

	entries, err := store.ListProviderCompletionEntries("invocation-publication-race")
	if err != nil {
		t.Fatalf("list exact receipt bound during publication: %v", err)
	}
	if len(entries) != CompletionReceiptEntryLimit {
		t.Fatalf("receipt candidates = %d, want %d", len(entries), CompletionReceiptEntryLimit)
	}
	for _, entry := range entries {
		for _, stagingName := range stagingNames {
			if entry.Name == stagingName {
				t.Fatal("protocol-owned staging entry leaked into receipt candidates")
			}
		}
	}
	if err := os.WriteFile(filepath.Join(inbox, "receipt-overflow.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write overflow receipt: %v", err)
	}
	entries, err = store.ListProviderCompletionEntries("invocation-publication-race")
	if !errors.Is(err, securefs.ErrLimitExceeded) {
		t.Fatalf("receipt overflow error = %v, want securefs.ErrLimitExceeded", err)
	}
	if len(entries) != CompletionReceiptEntryLimit {
		t.Fatalf("bounded receipt candidates = %d, want %d", len(entries), CompletionReceiptEntryLimit)
	}
}

func TestProviderCompletionEnumerationAllowsSixteenReceiptsAndSixteenStagingEntries(t *testing.T) {
	t.Parallel()
	store, inbox := newProviderCompletionTestInbox(t, "invocation-staging-boundary")
	for index := range CompletionReceiptEntryLimit {
		if err := os.WriteFile(filepath.Join(inbox, fmt.Sprintf("receipt-%02d.json", index)), []byte("{}"), 0o600); err != nil {
			t.Fatalf("write receipt %d: %v", index, err)
		}
	}
	for index := range CompletionReceiptStagingEntryLimit {
		name := fmt.Sprintf(".receipt-123e4567-e89b-42d3-a456-%012x.tmp", index)
		if err := os.WriteFile(filepath.Join(inbox, name), []byte("{}"), 0o600); err != nil {
			t.Fatalf("write staging entry %d: %v", index, err)
		}
	}
	entries, err := store.ListProviderCompletionEntries("invocation-staging-boundary")
	if err != nil || len(entries) != CompletionReceiptEntryLimit {
		t.Fatalf("boundary enumeration entries=%d err=%v", len(entries), err)
	}
}

func TestProviderCompletionEnumerationRejectsCandidateAndStagingOverflow(t *testing.T) {
	tests := []struct {
		name     string
		receipts int
		staging  int
		fake     bool
	}{
		{name: "candidate one over", receipts: CompletionReceiptEntryLimit + 1, staging: 2},
		{name: "staging one over", receipts: CompletionReceiptEntryLimit, staging: CompletionReceiptStagingEntryLimit + 1},
		{name: "fake staging counts as candidate", receipts: CompletionReceiptEntryLimit, fake: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			invocationID := "invocation-overflow-" + strings.ReplaceAll(tc.name, " ", "-")
			store, inbox := newProviderCompletionTestInbox(t, invocationID)
			for index := range tc.receipts {
				if err := os.WriteFile(filepath.Join(inbox, fmt.Sprintf("receipt-%02d.json", index)), []byte("{}"), 0o600); err != nil {
					t.Fatalf("write receipt %d: %v", index, err)
				}
			}
			for index := range tc.staging {
				name := fmt.Sprintf(".receipt-123e4567-e89b-42d3-a456-%012x.tmp", index)
				if err := os.WriteFile(filepath.Join(inbox, name), []byte("{}"), 0o600); err != nil {
					t.Fatalf("write staging %d: %v", index, err)
				}
			}
			if tc.fake {
				if err := os.WriteFile(filepath.Join(inbox, ".receipt-not-a-uuid.tmp"), []byte("{}"), 0o600); err != nil {
					t.Fatalf("write fake staging entry: %v", err)
				}
			}
			if _, err := store.ListProviderCompletionEntries(invocationID); !errors.Is(err, securefs.ErrLimitExceeded) {
				t.Fatalf("overflow error = %v, want securefs.ErrLimitExceeded", err)
			}
		})
	}
}

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
	sum := sha256.Sum256([]byte(invocationID))
	path := filepath.Join(rootPath, "provider-completion", "handoffs", hex.EncodeToString(sum[:])+".json")
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
	store, err := NewStore(openRuntimeFSTestDB(t), t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, err := store.PersistProviderTerminalHandoff("invocation-handoff-overflow", make([]byte, ProviderTerminalHandoffWireLimit+1)); err == nil {
		t.Fatal("expected handoff wire overflow rejection")
	}
}

func TestRemoveProviderCompletionStagingEntriesLeavesReceiptCandidates(t *testing.T) {
	const invocationID = "invocation-staging-cleanup"
	store, inbox := newProviderCompletionTestInbox(t, invocationID)
	stagingName := ".receipt-123e4567-e89b-42d3-a456-426614174000.tmp"
	if err := os.WriteFile(filepath.Join(inbox, stagingName), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write staging entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inbox, "receipt.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write receipt candidate: %v", err)
	}
	removed, err := store.RemoveProviderCompletionStagingEntries(invocationID)
	if err != nil {
		t.Fatalf("remove staging entries: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed staging entries = %d, want 1", removed)
	}
	if _, err := os.Stat(filepath.Join(inbox, stagingName)); !os.IsNotExist(err) {
		t.Fatalf("staging entry still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(inbox, "receipt.json")); err != nil {
		t.Fatalf("receipt candidate was removed: %v", err)
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
	entries, err := store.ListProviderCompletionEntries("invocation-removal")
	if err != nil {
		t.Fatalf("list receipt entry: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("receipt entries = %d, want 1", len(entries))
	}
	if err := store.RemoveProviderCompletionEntryIfIdentity("invocation-removal", entries[0]); err != nil {
		t.Fatalf("remove receipt entry: %v", err)
	}
	if err := store.RemoveProviderCompletionInbox("invocation-removal"); err != nil {
		t.Fatalf("remove empty inbox: %v", err)
	}
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatalf("empty inbox remains: %v", err)
	}
}

func TestProviderCompletionReceiptCleanupRefusesInPlaceMutationAndLeafReplacement(t *testing.T) {
	t.Parallel()

	store, inbox := newProviderCompletionTestInbox(t, "invocation-identity")
	path := filepath.Join(inbox, "receipt.json")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	receipt, err := store.ReadProviderCompletionReceipt("invocation-identity", "receipt.json")
	if err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	if err := os.WriteFile(path, []byte("other"), 0o600); err != nil {
		t.Fatalf("mutate receipt in place: %v", err)
	}
	future := time.Now().Add(24 * time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("set deterministic mutation timestamp: %v", err)
	}
	if err := store.RemoveProviderCompletionReceipt("invocation-identity", "receipt.json", receipt.Identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("in-place cleanup error = %v, want securefs.ErrIdentityMismatch", err)
	}

	receipt, err = store.ReadProviderCompletionReceipt("invocation-identity", "receipt.json")
	if err != nil {
		t.Fatalf("read mutated receipt: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove receipt leaf: %v", err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("replace receipt leaf: %v", err)
	}
	if err := store.RemoveProviderCompletionReceipt("invocation-identity", "receipt.json", receipt.Identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("replacement cleanup error = %v, want securefs.ErrIdentityMismatch", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if string(raw) != "replacement" {
		t.Fatalf("replacement = %q", raw)
	}
}

func TestProviderCompletionReceiptCleanupRefusesInboxReplacement(t *testing.T) {
	store, inbox := newProviderCompletionTestInbox(t, "invocation-inbox-replacement")
	receiptPath := filepath.Join(inbox, "receipt.json")
	if err := os.WriteFile(receiptPath, []byte(`{"original":true}`), 0o600); err != nil {
		t.Fatalf("write original receipt: %v", err)
	}
	receipt, err := store.ReadProviderCompletionReceipt("invocation-inbox-replacement", "receipt.json")
	if err != nil {
		t.Fatalf("read original receipt: %v", err)
	}
	displacedInbox := inbox + ".displaced"
	if err := os.Rename(inbox, displacedInbox); err != nil {
		t.Fatalf("displace original inbox: %v", err)
	}
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatalf("create replacement inbox: %v", err)
	}
	replacementPath := filepath.Join(inbox, "receipt.json")
	if err := os.WriteFile(replacementPath, []byte(`{"replacement":true}`), 0o600); err != nil {
		t.Fatalf("write replacement receipt: %v", err)
	}
	if err := store.RemoveProviderCompletionReceipt("invocation-inbox-replacement", "receipt.json", receipt.Identity); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("cleanup error = %v, want securefs.ErrIdentityMismatch", err)
	}
	if _, err := os.Stat(replacementPath); err != nil {
		t.Fatalf("replacement receipt was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(displacedInbox, "receipt.json")); err != nil {
		t.Fatalf("displaced original receipt was removed: %v", err)
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
