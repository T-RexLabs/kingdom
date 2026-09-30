package runtimefs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/securefs"
)

// Only compact control metadata belongs in the envelope. Content has no byte
// ceiling and is staged separately in fixed-size immutable chunks.
const ProviderTerminalHandoffWireLimit = 1024 * 1024

const providerTerminalHandoffRoot = "provider-completion/handoffs"

func providerContentPath(ref providerexecution.ContentReference, ordinal int64) string {
	invocation := sha256.Sum256([]byte(ref.InvocationID))
	return fmt.Sprintf("provider-completion/content/%x/%s/%016x", invocation, ref.SHA256, ordinal)
}

// PersistProviderContent stages complete content before the handoff envelope is
// published. Each chunk crosses securefs's exact-publication/fsync boundary.
func (s *Store) PersistProviderContent(invocationID, text string) (providerexecution.ContentReference, error) {
	ref := providerexecution.ReferenceText(invocationID, text)
	return ref, s.PersistProviderContentReader(ref, strings.NewReader(text))
}

func (s *Store) PersistProviderContentReader(ref providerexecution.ContentReference, reader io.Reader) error {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return err
	}
	// Verify before publishing under the immutable digest namespace. A corrupt
	// or interrupted source must not poison a later exact replay of this ref.
	staging, err := os.CreateTemp("", "kingdom-provider-content-*")
	if err != nil {
		return err
	}
	defer func() { staging.Close(); os.Remove(staging.Name()) }()
	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(staging, h), reader, ref.SizeBytes); err != nil {
		return err
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("provider content exceeds declared size or failed to reach EOF")
	}
	if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("provider content hash mismatch")
	}
	if _, err := staging.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader = staging
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	buffer := make([]byte, providerexecution.ContentChunkBytes)
	remaining := ref.SizeBytes
	for ordinal := int64(0); remaining > 0; ordinal++ {
		chunk := buffer[:min(remaining, int64(len(buffer)))]
		if _, err := io.ReadFull(reader, chunk); err != nil {
			return err
		}
		if _, err := root.CreateRegularExact(providerContentPath(ref, ordinal), chunk, 0o600, 0o700); err != nil {
			return err
		}
		remaining -= int64(len(chunk))
	}
	return nil
}

// CopyProviderContent verifies the complete digest and exact size. Callers must
// not publish bytes copied to their staging transaction until this returns nil.
func (s *Store) CopyProviderContent(ref providerexecution.ContentReference, dst io.Writer) error {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	h := sha256.New()
	remaining := ref.SizeBytes
	for ordinal := int64(0); remaining > 0; ordinal++ {
		want := min(remaining, int64(providerexecution.ContentChunkBytes))
		raw, err := root.ReadRegular(providerContentPath(ref, ordinal), want)
		if err != nil {
			return fmt.Errorf("read provider content: %w", err)
		}
		if int64(len(raw)) != want {
			return fmt.Errorf("provider content size mismatch")
		}
		if _, err := io.MultiWriter(dst, h).Write(raw); err != nil {
			return err
		}
		remaining -= want
	}
	if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("provider content hash mismatch")
	}
	return nil
}

func (s *Store) OpenProviderContent(ctx context.Context, ref providerexecution.ContentReference) (io.ReadCloser, error) {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return nil, err
	}
	return providerexecution.OpenContentCopy(ctx, func(_ context.Context, dst io.Writer) error { return s.CopyProviderContent(ref, dst) }), nil
}

// RemoveProviderContent retires only invocation-derived staging chunks after
// SQLite ingestion. Repeated cleanup converges; symlinks are never followed.
func (s *Store) RemoveProviderContent(ref providerexecution.ContentReference) error {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return err
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	remaining := ref.SizeBytes
	for ordinal := int64(0); remaining > 0; ordinal++ {
		path := providerContentPath(ref, ordinal)
		_, identity, err := root.ReadRegularIdentified(path, providerexecution.ContentChunkBytes)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err == nil {
			if err := root.RemoveRegularIfIdentity(path, identity); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
		remaining -= min(remaining, int64(providerexecution.ContentChunkBytes))
	}
	return nil
}

// RemoveProviderInvocationContent also retires orphaned staging left by a
// crash before envelope publication. Call only after canonical DB handoff or
// terminal invocation authority; each no-follow batch has bounded memory.
func (s *Store) RemoveProviderInvocationContent(ctx context.Context, invocationID string) error {
	if invocationID == "" || invocationID != strings.TrimSpace(invocationID) {
		return fmt.Errorf("canonical provider invocation id is required")
	}
	root, err := s.openSecureRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	digest := sha256.Sum256([]byte(invocationID))
	path := fmt.Sprintf("provider-completion/content/%x", digest)
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		progress, err := root.RemoveTreeBatch(path, cursor, 128)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if progress.Complete {
			return nil
		}
		cursor = progress.Cursor
	}
}

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
