package opencode

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"os"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode/protocol"
)

// stagedWriter spools provider content to disk while digesting it, so complete
// evidence never has to fit in memory.
type stagedWriter struct {
	file *os.File
	hash hash.Hash
	size int64
	err  error
}

func newStagedWriter(pattern string) (*stagedWriter, error) {
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, err
	}
	return &stagedWriter{file: file, hash: sha256.New()}, nil
}

func (w *stagedWriter) Write(raw []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.file.Write(raw)
	_, _ = w.hash.Write(raw[:n])
	w.size += int64(n)
	w.err = err
	return n, err
}

// finish makes the content immutable and returns the staged reference.
func (w *stagedWriter) finish(invocationID string) (providerexecution.StagedContent, error) {
	err := w.err
	if err == nil {
		err = w.file.Sync()
	}
	if closeErr := w.file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(w.file.Name())
		return providerexecution.StagedContent{}, fmt.Errorf("stage provider content: %w", err)
	}
	ref := providerexecution.ContentReference{InvocationID: invocationID, SHA256: hex.EncodeToString(w.hash.Sum(nil)), SizeBytes: w.size}
	return providerexecution.StagedContent{Reference: ref, Path: w.file.Name()}, nil
}

func (w *stagedWriter) discard() {
	_ = w.file.Close()
	_ = os.Remove(w.file.Name())
}

func stagedSource(content providerexecution.StagedContent) protocol.Source {
	return protocol.Source{Ref: content.Reference, Open: content.Open}
}
