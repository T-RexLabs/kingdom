package runtimefs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"kingdom/internal/providerlimits"
	"kingdom/internal/securefs"
)

const ProviderTerminalHandoffWireLimit = providerlimits.TerminalHandoffWireBytes

const providerTerminalHandoffRoot = "provider-completion/handoffs"

// ProviderTerminalHandoffFile is one bounded runtime-owned terminal handoff
// plus the identity required for post-database cleanup.
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
	for attempt := 0; attempt < 3; attempt++ {
		wire, identity, readErr := root.ReadRegularIdentified(relative, ProviderTerminalHandoffWireLimit)
		if errors.Is(readErr, fs.ErrNotExist) {
			return ProviderTerminalHandoffFile{}, false, nil
		}
		file := ProviderTerminalHandoffFile{Wire: wire, Identity: identity}
		if readErr == nil {
			return file, true, nil
		}
		if !errors.Is(readErr, securefs.ErrIdentityMismatch) || attempt == 2 {
			return file, true, fmt.Errorf("read provider terminal handoff: %w", readErr)
		}
		// A concurrent atomic publication or exact-file cleanup may change the
		// identity between the no-follow precheck and open. Retry only this
		// bounded identity race; persistent churn still fails closed.
		time.Sleep(time.Duration(attempt+1) * time.Millisecond)
	}
	return ProviderTerminalHandoffFile{}, false, fmt.Errorf("read provider terminal handoff exhausted identity retries")
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
