package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"kingdom/internal/securefs"
)

const (
	// CompletionReceiptWireLimit is the maximum accepted serialized receipt size.
	CompletionReceiptWireLimit int64 = 1 << 20
	// CompletionReceiptEntryLimit is the maximum number of entries enumerated
	// from one invocation inbox in a pass.
	CompletionReceiptEntryLimit = 16
)

const completionReceiptRoot = "provider-completion/receipts"

// ProviderCompletionEntry describes an inbox entry without following it.
// Callers can record a disposition for unsupported entry types without opening
// provider-controlled objects.
type ProviderCompletionEntry struct {
	Name      string
	Type      securefs.EntryType
	SizeBytes int64
	Identity  securefs.FileIdentity
}

// ProviderCompletionReceipt is one bounded regular receipt plus its identity.
type ProviderCompletionReceipt struct {
	Wire     []byte
	Identity securefs.FileIdentity
}

// EnsureProviderCompletionInbox creates the invocation-owned inbox and returns
// its path for injection into the provider environment.
func (s *Store) EnsureProviderCompletionInbox(invocationID string) (string, error) {
	relative, err := completionReceiptDir(invocationID)
	if err != nil {
		return "", err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return "", err
	}
	defer root.Close()
	if err := root.EnsureDir(relative, 0o755); err != nil {
		return "", fmt.Errorf("create provider completion inbox: %w", err)
	}
	return filepath.Join(s.root, filepath.FromSlash(relative)), nil
}

// ListProviderCompletionEntries performs one bounded, non-recursive inbox
// enumeration. Every entry type is represented; no entry is followed.
func (s *Store) ListProviderCompletionEntries(invocationID string) ([]ProviderCompletionEntry, error) {
	relative, err := completionReceiptDir(invocationID)
	if err != nil {
		return nil, err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()
	entries, listErr := root.ReadDirBounded(relative, CompletionReceiptEntryLimit)
	result := make([]ProviderCompletionEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, ProviderCompletionEntry{
			Name:      entry.Name,
			Type:      entry.Type,
			SizeBytes: entry.SizeBytes,
			Identity:  entry.Identity,
		})
	}
	if listErr != nil {
		return result, fmt.Errorf("list provider completion inbox: %w", listErr)
	}
	return result, nil
}

// RemoveProviderCompletionEntryIfIdentity removes a non-directory transport
// entry only while it still matches the object that was dispositioned.
func (s *Store) RemoveProviderCompletionEntryIfIdentity(invocationID string, entry ProviderCompletionEntry) error {
	relative, err := completionReceiptEntry(invocationID, entry.Name, false)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveEntryIfIdentity(relative, entry.Type, entry.Identity); err != nil {
		return fmt.Errorf("remove provider completion entry: %w", err)
	}
	return nil
}

// ReadProviderCompletionReceipt reads one regular .json inbox entry within the
// central wire limit. Symlinks and other non-regular objects are rejected.
func (s *Store) ReadProviderCompletionReceipt(invocationID, entryName string) (ProviderCompletionReceipt, error) {
	relative, err := completionReceiptEntry(invocationID, entryName, true)
	if err != nil {
		return ProviderCompletionReceipt{}, err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return ProviderCompletionReceipt{}, err
	}
	defer root.Close()
	raw, identity, err := root.ReadRegularIdentified(relative, CompletionReceiptWireLimit)
	if err != nil {
		return ProviderCompletionReceipt{Identity: identity}, fmt.Errorf("read provider completion receipt: %w", err)
	}
	return ProviderCompletionReceipt{Wire: raw, Identity: identity}, nil
}

// RemoveProviderCompletionReceipt removes a receipt only if its identity still
// matches the file that was durably dispositioned.
func (s *Store) RemoveProviderCompletionReceipt(invocationID, entryName string, identity securefs.FileIdentity) error {
	relative, err := completionReceiptEntry(invocationID, entryName, true)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveRegularIfIdentity(relative, identity); err != nil {
		return fmt.Errorf("remove provider completion receipt: %w", err)
	}
	return nil
}

// RemoveProviderCompletionEntry removes one leaf through its pinned inbox
// parent. The entry may be a regular file, symlink, FIFO, socket, or device,
// but never a directory.
func (s *Store) RemoveProviderCompletionEntry(invocationID, entryName string) error {
	relative, err := completionReceiptEntry(invocationID, entryName, false)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveEntry(relative); err != nil {
		return fmt.Errorf("remove provider completion entry: %w", err)
	}
	return nil
}

// RemoveProviderCompletionInbox removes the invocation inbox only when empty.
func (s *Store) RemoveProviderCompletionInbox(invocationID string) error {
	relative, err := completionReceiptDir(invocationID)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveEmptyDir(relative); err != nil {
		return fmt.Errorf("remove provider completion inbox: %w", err)
	}
	return nil
}

// SyncProviderCompletionInbox durably publishes provider-created directory
// entry changes for one invocation inbox.
func (s *Store) SyncProviderCompletionInbox(invocationID string) error {
	relative, err := completionReceiptDir(invocationID)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.SyncDir(relative); err != nil {
		return fmt.Errorf("sync provider completion inbox: %w", err)
	}
	return nil
}

func (s *Store) openSecureRoot() (*securefs.Root, error) {
	if s == nil || strings.TrimSpace(s.root) == "" {
		return nil, fmt.Errorf("runtime fs store is required")
	}
	root, err := securefs.OpenRoot(s.root)
	if err != nil {
		return nil, fmt.Errorf("open runtime fs root: %w", err)
	}
	return root, nil
}

func completionReceiptDir(invocationID string) (string, error) {
	if invocationID == "" {
		return "", fmt.Errorf("provider invocation id is required")
	}
	if invocationID != strings.TrimSpace(invocationID) {
		return "", fmt.Errorf("provider invocation id must be canonical")
	}
	sum := sha256.Sum256([]byte(invocationID))
	return completionReceiptRoot + "/" + hex.EncodeToString(sum[:]), nil
}

func completionReceiptEntry(invocationID, entryName string, requireJSON bool) (string, error) {
	directory, err := completionReceiptDir(invocationID)
	if err != nil {
		return "", err
	}
	name := entryName
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return "", fmt.Errorf("provider completion entry name must be one path component")
	}
	if requireJSON && !strings.HasSuffix(name, ".json") {
		return "", fmt.Errorf("provider completion receipt must have a .json extension")
	}
	return directory + "/" + name, nil
}
