package providerexecution

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

type repeatedContent struct {
	remaining int64
	value     byte
}

func (r *repeatedContent) Read(dst []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(dst)), r.remaining))
	for i := range dst[:n] {
		dst[i] = r.value
	}
	r.remaining -= int64(n)
	return n, nil
}

func TestReferencedTerminalSuccessRequiresCompleteVerifiedReader(t *testing.T) {
	const size = 12 * 1024 * 1024
	h := sha256.New()
	_, _ = io.Copy(h, &repeatedContent{remaining: size, value: 'x'})
	ref := ContentReference{InvocationID: "invocation", SHA256: fmt.Sprintf("%x", h.Sum(nil)), SizeBytes: size}
	zero := 0
	o := InvocationStreamObservation{CanonicalTerminalTextRef: &ref, NormalizedFinishReason: "stop", FinalTurnID: "turn", FinalStepID: "step", FinalSequence: 1, ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true}
	if err := ValidateServerResponseSuccess(o); err == nil {
		t.Fatal("reference and process exit alone authorized success")
	}
	o.TerminalTextOpener = func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(&repeatedContent{remaining: size, value: 'x'}), nil
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := ValidateServerResponseSuccess(o); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4*1024*1024 {
		t.Fatalf("validation allocated complete answer: %d bytes", allocated)
	}
	o.CanonicalTerminalText = "a misleading inline preview"
	if err := ValidateServerResponseSuccess(o); err == nil {
		t.Fatal("accepted conflicting inline text")
	}
	o.CanonicalTerminalText = ""
	o.TerminalTextOpener = func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(&repeatedContent{remaining: size, value: 'y'}), nil
	}
	if err := ValidateServerResponseSuccess(o); err == nil {
		t.Fatal("accepted corrupt content")
	}
	o.TerminalTextOpener = func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("short")), nil }
	if err := ValidateServerResponseSuccess(o); err == nil {
		t.Fatal("accepted truncated content")
	}
}

func TestCopiedContentReaderEarlyCloseJoinsProducer(t *testing.T) {
	done := make(chan struct{})
	reader := OpenContentCopy(context.Background(), func(_ context.Context, w io.Writer) error {
		defer close(done)
		_, err := io.Copy(w, &repeatedContent{remaining: 16 * 1024 * 1024, value: 'x'})
		return err
	})
	var prefix [1]byte
	if _, err := reader.Read(prefix[:]); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	default:
		t.Fatal("Close left a blocked content producer")
	}
}

func TestCopiedContentReaderCloseCancelsBlockedSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, producerDone := make(chan struct{}), make(chan struct{})
	reader := OpenContentCopy(ctx, func(ctx context.Context, w io.Writer) error {
		defer close(producerDone)
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	closed := make(chan error, 1)
	go func() { closed <- reader.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-closed
		t.Fatal("Close did not cancel blocked source I/O")
	}
	select {
	case <-producerDone:
	default:
		t.Fatal("Close returned before the producer exited")
	}
	if ctx.Err() != nil {
		t.Fatal("Close cancelled the caller's context")
	}
}
