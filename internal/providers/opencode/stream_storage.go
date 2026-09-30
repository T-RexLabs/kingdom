package opencode

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/processguardian"
)

// textSpool retains provider prose on disk, not in the session heap. Errors are
// sticky: a failed write must never turn a truncated answer into success.
type textSpool struct {
	file     *os.File
	err      error
	size     int64
	preview  string
	summary  *providerexecution.TextSummary
	retained bool
}

const streamPreviewBytes = 32 * 1024

func (s *textSpool) append(text string) {
	if s.err != nil || text == "" {
		return
	}
	if s.file == nil {
		s.file, s.err = os.CreateTemp("", "kingdom-provider-text-*")
		if s.err != nil {
			return
		}
	}
	if s.size > 0 {
		s.write("\n")
	}
	s.write(text)
}

func (s *textSpool) write(text string) {
	if s.err != nil {
		return
	}
	var n int
	n, s.err = io.WriteString(s.file, text)
	s.size += int64(n)
	if len(s.preview) < streamPreviewBytes {
		s.preview += text[:min(len(text), streamPreviewBytes-len(s.preview))]
	}
}

func (s *textSpool) text() string {
	if s.file == nil || s.err != nil {
		return ""
	}
	if s.summary == nil {
		summary, err := providerexecution.InspectText(context.Background(), io.NewSectionReader(s.file, 0, s.size))
		s.err = err
		s.summary = &summary
	}
	if s.err != nil {
		return ""
	}
	return s.summary.Preview
}

func (s *textSpool) content(invocationID string) (*providerexecution.StagedContent, error) {
	s.text()
	if s.err != nil {
		return nil, s.err
	}
	if s.file == nil || s.summary == nil || s.summary.End == s.summary.Offset {
		return nil, nil
	}
	if err := s.file.Sync(); err != nil {
		s.err = err
		return nil, err
	}
	h := sha256.New()
	size := s.summary.End - s.summary.Offset
	if _, err := io.Copy(h, io.NewSectionReader(s.file, s.summary.Offset, size)); err != nil {
		return nil, err
	}
	s.retained = true
	return &providerexecution.StagedContent{Reference: providerexecution.ContentReference{InvocationID: invocationID, SHA256: hex.EncodeToString(h.Sum(nil)), SizeBytes: size}, Path: s.file.Name(), Offset: s.summary.Offset}, nil
}

func (s *textSpool) close() {
	if s.file != nil {
		_ = s.file.Close()
		if !s.retained {
			_ = os.Remove(s.file.Name())
		}
		s.file = nil
	}
}

// sessionAttemptRecord contains only a cursor and terminal state in memory.
// Readers pin an independent descriptor and byte length, so a concurrent append
// cannot expose half a JSON record or change a snapshot already being replayed.
type sessionAttemptRecord struct {
	path             string
	count            int
	size             int64
	terminal         string
	err              error
	paths            []string
	readers          int
	releaseRequested bool
}

func (r *sessionAttemptRecord) append(event AttemptStreamEvent) {
	if r.err != nil {
		return
	}
	var f *os.File
	if r.path == "" {
		f, r.err = os.CreateTemp("", "kingdom-provider-events-*")
		if r.err == nil {
			r.path = f.Name()
		}
	} else {
		f, r.err = os.OpenFile(r.path, os.O_WRONLY|os.O_APPEND, 0)
	}
	if r.err != nil {
		return
	}
	event.Seq = r.count + 1
	r.err = json.NewEncoder(f).Encode(event)
	if r.err == nil && isTerminalAttemptEvent(event.EventType) {
		r.err = f.Sync()
	}
	info, statErr := f.Stat()
	closeErr := f.Close()
	if r.err == nil {
		r.err = firstNonNilError(statErr, closeErr)
	}
	if r.err != nil {
		return
	}
	r.count++
	r.size = info.Size()
	if isTerminalAttemptEvent(event.EventType) {
		r.terminal = event.EventType
	}
}

func (r *sessionAttemptRecord) reader() (io.ReadCloser, error) {
	if r.err != nil {
		return nil, fmt.Errorf("provider event journal: %w", r.err)
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	return &journalReader{Reader: bufio.NewReader(io.NewSectionReader(f, 0, r.size)), Closer: f}, nil
}

type journalReader struct {
	io.Reader
	io.Closer
}

func (a *SessionAdapter) eventJournalError(attemptID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if record := a.attempts[attemptID]; record != nil && record.err != nil {
		return fmt.Errorf("provider event journal: %w", record.err)
	}
	return nil
}

// Close releases replay journals after the adapter's consumers have finished.
// Running attempts must be cancelled and awaited through Cancel first.
func (a *SessionAdapter) Close() error {
	return a.CloseContext(context.Background())
}

func (a *SessionAdapter) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		if a.lifecycleCancel != nil {
			a.lifecycleCancel()
		}
		if a.closeCh != nil {
			close(a.closeCh)
		}
	}
	if len(a.attemptDone) != 0 {
		a.mu.Unlock()
		return fmt.Errorf("provider attempts are still running")
	}
	if a.closeWaitDone == nil {
		a.closeWaitDone = make(chan struct{})
	}
	a.mu.Unlock()
	a.closeWaitOnce.Do(func() { go func() { a.streamWG.Wait(); a.helperWG.Wait(); close(a.closeWaitDone) }() })
	select {
	case <-a.closeWaitDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, record := range a.attempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.path != "" {
			if err := os.Remove(record.path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		for _, path := range record.paths {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		delete(a.attempts, id)
	}
	for _, path := range a.helperPaths {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	a.helperPaths = nil
	a.attempts = make(map[string]*sessionAttemptRecord)
	a.logicalToActual = make(map[string]string)
	a.inFlight = make(map[string]processguardian.Handle)
	a.attemptCancels = make(map[string]context.CancelFunc)
	a.attemptDone = make(map[string]chan struct{})
	return nil
}

func (a *SessionAdapter) retainHelperPaths(paths ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, path := range paths {
		if path == "" {
			continue
		}
		if a.closed {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				a.helperPaths = append(a.helperPaths, path)
			}
		} else {
			a.helperPaths = append(a.helperPaths, path)
		}
	}
}

func (a *SessionAdapter) retainContentPath(attemptID, path string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if record := a.attempts[attemptID]; record != nil {
		record.paths = append(record.paths, path)
	}
}

// releaseAttempt acknowledges ingestion of one exact execution generation.
// A repeated callback cannot retire a later invocation reusing the attempt ID.
// Existing snapshot readers finish before the record's storage is removed.
func (a *SessionAdapter) releaseAttempt(attemptID string, record *sessionAttemptRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.attempts[attemptID] != record {
		return nil
	}
	if a.attemptDone[attemptID] != nil {
		return fmt.Errorf("provider attempt is still running")
	}
	record.releaseRequested = true
	if record.readers > 0 {
		return nil
	}
	return a.removeAttemptStorageLocked(attemptID, record)
}

func (a *SessionAdapter) removeAttemptStorageLocked(attemptID string, record *sessionAttemptRecord) error {
	for _, path := range append([]string{record.path}, record.paths...) {
		if path != "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	delete(a.attempts, attemptID)
	return nil
}

func (a *SessionAdapter) finishReplayReader(attemptID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if record := a.attempts[attemptID]; record != nil {
		record.readers--
		if record.readers == 0 && record.releaseRequested {
			_ = a.removeAttemptStorageLocked(attemptID, record)
		}
	}
}

// capturedOutput drains without a byte ceiling. Only its preview lives in the
// heap; the complete file and digest remain available to durable ingestion.
type capturedOutput struct {
	Path    string
	Size    int64
	SHA256  string
	Preview []byte
}

type outputCapture struct {
	file   *os.File
	hash   hash.Hash
	output capturedOutput
	err    error
}

func newOutputCapture() (*outputCapture, error) {
	f, err := os.CreateTemp("", "kingdom-provider-diagnostic-*")
	if err != nil {
		return nil, err
	}
	return &outputCapture{file: f, hash: sha256.New(), output: capturedOutput{Path: f.Name()}}, nil
}

func (c *outputCapture) Write(raw []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.file.Write(raw)
	_, _ = c.hash.Write(raw[:n])
	c.output.Size += int64(n)
	c.output.Preview = appendStderrTail(c.output.Preview, raw[:n])
	c.err = err
	return n, err
}

func (c *outputCapture) finish() (capturedOutput, error) {
	if c.file != nil {
		c.err = firstNonNilError(c.err, c.file.Sync())
		c.err = firstNonNilError(c.err, c.file.Close())
		c.file = nil
		c.output.SHA256 = hex.EncodeToString(c.hash.Sum(nil))
	}
	return c.output, c.err
}

type invocationCapture struct{ stdout, stderr *outputCapture }

func captureCommandOutput(reader io.Reader) (capturedOutput, error) {
	capture, err := newOutputCapture()
	if err != nil {
		return capturedOutput{}, err
	}
	_, readErr := io.CopyBuffer(capture, reader, make([]byte, 64*1024))
	out, finishErr := capture.finish()
	return out, firstNonNilError(readErr, finishErr)
}

func newInvocationCapture() (*invocationCapture, error) {
	out, err := newOutputCapture()
	if err != nil {
		return nil, err
	}
	stderr, err := newOutputCapture()
	if err != nil {
		out.finish()
		os.Remove(out.output.Path)
		return nil, err
	}
	return &invocationCapture{stdout: out, stderr: stderr}, nil
}

func (c *invocationCapture) finish(invocationID string) ([]providerexecution.StagedContent, error) {
	var refs []providerexecution.StagedContent
	var finishErr error
	for _, capture := range []*outputCapture{c.stdout, c.stderr} {
		out, err := capture.finish()
		finishErr = errors.Join(finishErr, err)
		refs = append(refs, providerexecution.StagedContent{Reference: providerexecution.ContentReference{InvocationID: invocationID, SHA256: out.SHA256, SizeBytes: out.Size}, Path: out.Path})
	}
	if finishErr != nil {
		return nil, finishErr
	}
	return refs, nil
}
