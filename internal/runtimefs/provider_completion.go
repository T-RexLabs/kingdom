package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"kingdom/internal/securefs"
)

const (
	// CompletionReceiptWireLimit is the maximum accepted serialized receipt size.
	CompletionReceiptWireLimit int64 = 1 << 20
	// CompletionReceiptEntryLimit is the maximum number of receipt candidates
	// returned from one invocation inbox in a pass.
	CompletionReceiptEntryLimit = 16
	// CompletionReceiptStagingEntryLimit bounds exact tool publication staging
	// entries independently from receipt candidates.
	CompletionReceiptStagingEntryLimit = 16
	// ProviderTerminalHandoffWireLimit bounds the complete trusted handoff wire.
	ProviderTerminalHandoffWireLimit int64 = 32 * 1024 * 1024
)

const (
	completionReceiptRoot       = "provider-completion/receipts"
	providerTerminalHandoffRoot = "provider-completion/handoffs"
)

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

// ProviderTerminalHandoffFile is one bounded trusted handoff plus the identity
// required for post-database cleanup.
type ProviderTerminalHandoffFile struct {
	Wire     []byte
	Identity securefs.FileIdentity
}

// PersistProviderTerminalHandoff atomically creates the invocation-derived
// trusted handoff or accepts an exact replay.
func (s *Store) PersistProviderTerminalHandoff(invocationID string, wire []byte) (securefs.FileIdentity, error) {
	if int64(len(wire)) > ProviderTerminalHandoffWireLimit {
		return securefs.FileIdentity{}, fmt.Errorf("provider terminal handoff exceeds %d bytes", ProviderTerminalHandoffWireLimit)
	}
	relative, err := providerTerminalHandoffPath(invocationID)
	if err != nil {
		return securefs.FileIdentity{}, err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return securefs.FileIdentity{}, err
	}
	defer root.Close()
	if err := root.EnsureDir(providerTerminalHandoffRoot, 0o700); err != nil {
		return securefs.FileIdentity{}, fmt.Errorf("create provider terminal handoff directory: %w", err)
	}
	identity, err := root.CreateRegularExact(relative, wire, 0o600, 0o700)
	if err != nil {
		return securefs.FileIdentity{}, fmt.Errorf("persist provider terminal handoff: %w", err)
	}
	return identity, nil
}

// ReadProviderTerminalHandoff reads the invocation-derived trusted handoff
// without following links. A missing file is not an error.
func (s *Store) ReadProviderTerminalHandoff(invocationID string) (ProviderTerminalHandoffFile, bool, error) {
	relative, err := providerTerminalHandoffPath(invocationID)
	if err != nil {
		return ProviderTerminalHandoffFile{}, false, err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return ProviderTerminalHandoffFile{}, false, err
	}
	defer root.Close()
	wire, identity, err := root.ReadRegularIdentified(relative, ProviderTerminalHandoffWireLimit)
	if errors.Is(err, fs.ErrNotExist) {
		return ProviderTerminalHandoffFile{}, false, nil
	}
	file := ProviderTerminalHandoffFile{Wire: wire, Identity: identity}
	if err != nil {
		return file, true, fmt.Errorf("read provider terminal handoff: %w", err)
	}
	return file, true, nil
}

// RemoveProviderTerminalHandoff removes only the exact file that was read or
// persisted. A missing file is harmless cleanup convergence.
func (s *Store) RemoveProviderTerminalHandoff(invocationID string, identity securefs.FileIdentity) error {
	relative, err := providerTerminalHandoffPath(invocationID)
	if err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveRegularIfIdentity(relative, identity); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove provider terminal handoff: %w", err)
	}
	return nil
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
// enumeration. Every candidate entry type is represented without following it;
// the tool's one live publication staging entry is excluded.
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
	entries, listErr := root.ReadDirBounded(relative, CompletionReceiptEntryLimit+CompletionReceiptStagingEntryLimit)
	overflow := errors.Is(listErr, securefs.ErrLimitExceeded)
	result := make([]ProviderCompletionEntry, 0, min(len(entries), CompletionReceiptEntryLimit))
	stagingCount := 0
	for _, entry := range entries {
		if IsProviderCompletionStagingEntryName(entry.Name) {
			stagingCount++
			if stagingCount > CompletionReceiptStagingEntryLimit {
				overflow = true
			}
			continue
		}
		if len(result) == CompletionReceiptEntryLimit {
			overflow = true
			continue
		}
		result = append(result, ProviderCompletionEntry{
			Name:      entry.Name,
			Type:      entry.Type,
			SizeBytes: entry.SizeBytes,
			Identity:  entry.Identity,
		})
	}
	if listErr != nil && !errors.Is(listErr, securefs.ErrLimitExceeded) {
		return result, fmt.Errorf("list provider completion inbox: %w", listErr)
	}
	if overflow {
		return result, fmt.Errorf("list provider completion inbox: %w: more than %d receipt entries", securefs.ErrLimitExceeded, CompletionReceiptEntryLimit)
	}
	return result, nil
}

// IsProviderCompletionStagingEntryName reports whether name is exactly the
// hidden UUID-v4 entry used while the completion tool atomically publishes a
// receipt. Staging entries are protocol state, not receipt candidates.
func IsProviderCompletionStagingEntryName(name string) bool {
	const prefix = ".receipt-"
	const suffix = ".tmp"
	if len(name) != len(prefix)+36+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	uuid := name[len(prefix) : len(name)-len(suffix)]
	for index, character := range uuid {
		switch index {
		case 8, 13, 18, 23:
			if character != '-' {
				return false
			}
		case 14:
			if character != '4' {
				return false
			}
		case 19:
			if character != '8' && character != '9' && character != 'a' && character != 'b' {
				return false
			}
		default:
			if !isLowerHex(character) {
				return false
			}
		}
	}
	return true
}

func isLowerHex(character rune) bool {
	return character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
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

// RemoveProviderCompletionStagingEntries removes only crash-abandoned tool
// staging files. Callers must invoke it only after the invocation is terminal.
func (s *Store) RemoveProviderCompletionStagingEntries(invocationID string) (int, error) {
	relative, err := completionReceiptDir(invocationID)
	if err != nil {
		return 0, err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return 0, err
	}
	defer root.Close()
	entries, listErr := root.ReadDirBounded(relative, CompletionReceiptEntryLimit+CompletionReceiptStagingEntryLimit)
	removed := 0
	for _, entry := range entries {
		if !IsProviderCompletionStagingEntryName(entry.Name) {
			continue
		}
		entryPath := relative + "/" + entry.Name
		if err := root.RemoveEntryIfIdentity(entryPath, entry.Type, entry.Identity); err != nil {
			return removed, fmt.Errorf("remove provider completion staging entry: %w", err)
		}
		removed++
	}
	if listErr != nil {
		return removed, fmt.Errorf("list provider completion staging entries: %w", listErr)
	}
	return removed, nil
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

func providerTerminalHandoffPath(invocationID string) (string, error) {
	if invocationID == "" {
		return "", fmt.Errorf("provider invocation id is required")
	}
	if invocationID != strings.TrimSpace(invocationID) {
		return "", fmt.Errorf("provider invocation id must be canonical")
	}
	sum := sha256.Sum256([]byte(invocationID))
	return providerTerminalHandoffRoot + "/" + hex.EncodeToString(sum[:]) + ".json", nil
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
