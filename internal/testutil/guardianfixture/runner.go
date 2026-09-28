// Package guardianfixture provides deterministic process-guardian test doubles.
package guardianfixture

import (
	"bytes"
	"context"
	"io"
	"sync"

	"kingdom/internal/processguardian"
)

// OutputRunner returns a new finite stdout/stderr pair for every start without
// launching a platform-specific command.
type OutputRunner struct {
	stdout []byte
	stderr []byte

	mu      sync.Mutex
	starts  int
	cancels int
}

func NewOutputRunner(stdout, stderr []byte) *OutputRunner {
	return &OutputRunner{stdout: append([]byte(nil), stdout...), stderr: append([]byte(nil), stderr...)}
}

func (r *OutputRunner) Start(_ context.Context, _ processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
	r.mu.Lock()
	r.starts++
	r.mu.Unlock()
	return &outputHandle{runner: r, stdout: io.NopCloser(bytes.NewReader(r.stdout)), stderr: io.NopCloser(bytes.NewReader(r.stderr))}, nil
}

func (r *OutputRunner) Starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts
}

func (r *OutputRunner) Cancels() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancels
}

type outputHandle struct {
	runner *OutputRunner
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (*outputHandle) GuardID() string         { return "guardian-fixture" }
func (*outputHandle) Stdin() io.WriteCloser   { return discardWriteCloser{} }
func (h *outputHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *outputHandle) Stderr() io.ReadCloser { return h.stderr }
func (h *outputHandle) Cancel(context.Context) error {
	h.runner.mu.Lock()
	h.runner.cancels++
	h.runner.mu.Unlock()
	return nil
}
func (*outputHandle) Wait(context.Context) (processguardian.Result, error) {
	return processguardian.Result{Quiescent: true}, nil
}

type discardWriteCloser struct{}

func (discardWriteCloser) Write(payload []byte) (int, error) { return io.Discard.Write(payload) }
func (discardWriteCloser) Close() error                      { return nil }
