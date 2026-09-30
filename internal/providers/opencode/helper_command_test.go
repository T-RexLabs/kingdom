package opencode

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

type helperFailureHandle struct{ streamingHandle }

func (*helperFailureHandle) Wait(context.Context) (processguardian.Result, error) {
	return processguardian.Result{Quiescent: true, ExitCode: 1}, nil
}

func TestHelperCommandFailureRetainsCompleteDiagnosticsUntilClosed(t *testing.T) {
	stdout := strings.Repeat("stdout diagnostic\n", 100000)
	stderr := strings.Repeat("stderr diagnostic\n", 100000)
	handle := &helperFailureHandle{streamingHandle{stdout: io.NopCloser(strings.NewReader(stdout)), stderr: io.NopCloser(strings.NewReader(stderr))}}
	output, err := CollectHelperCommandOutput(context.Background(), handle)
	if err == nil {
		t.Fatal("failed helper command was accepted")
	}
	diagnosticErr := output.DiagnosticError(err)
	var diagnostic *HelperCommandError
	if !errors.As(diagnosticErr, &diagnostic) || !errors.Is(diagnosticErr, err) {
		t.Fatalf("missing diagnostic ownership or cause: %v", diagnosticErr)
	}
	t.Cleanup(func() { _ = diagnostic.Close() })
	if len(diagnostic.Error()) > 10*1024 {
		t.Fatal("diagnostic display copied full streams into memory")
	}
	for i, capture := range []capturedOutput{output.stdout, output.stderr} {
		f, err := os.Open(capture.Path)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		n, err := io.Copy(hash, f)
		_ = f.Close()
		want := []string{stdout, stderr}[i]
		if err != nil || n != int64(len(want)) || fmt.Sprintf("%x", hash.Sum(nil)) != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) {
			t.Fatalf("incomplete diagnostic stream: bytes=%d err=%v", n, err)
		}
	}
	if err := diagnostic.Close(); err != nil {
		t.Fatal(err)
	}
	for _, capture := range []capturedOutput{output.stdout, output.stderr} {
		if _, err := os.Stat(capture.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired diagnostic remains: %v", err)
		}
	}
}

type helperDrainHandle struct {
	streamingHandle
	writer    *io.PipeWriter
	cancelled chan struct{}
	once      sync.Once
}

func (h *helperDrainHandle) Cancel(context.Context) error {
	h.once.Do(func() {
		_ = h.writer.CloseWithError(context.DeadlineExceeded)
		close(h.cancelled)
	})
	return nil
}

func TestHelperCommandDeadlineStillCancelsWhileDraining(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	handle := &helperDrainHandle{
		streamingHandle: streamingHandle{stdout: reader, stderr: io.NopCloser(strings.NewReader(""))},
		writer:          writer, cancelled: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		output, err := CollectHelperCommandOutput(ctx, handle)
		defer output.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("drain error=%v, want deadline", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = writer.CloseWithError(context.DeadlineExceeded)
		<-done
		t.Fatal("helper deadline did not cancel blocked stream drain")
	}
	select {
	case <-handle.cancelled:
	default:
		t.Fatal("deadline did not request guardian cancellation")
	}
}
