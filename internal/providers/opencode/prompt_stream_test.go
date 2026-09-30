package opencode

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

type countedPromptReader struct {
	io.Reader
	closed *int
}

func (r countedPromptReader) Close() error { *r.closed++; return nil }

func TestReplayablePromptSourcesPreserveContentAndCloseReaders(t *testing.T) {
	for _, kind := range []string{"reader", "writer"} {
		t.Run(kind, func(t *testing.T) {
			text := strings.Repeat("界 prompt\n", 600000) + "last\n\n"
			calls, closed := 0, 0
			for turn := 0; turn < 2; turn++ {
				runner := &promptCaptureRunner{output: successfulProviderEvents("ok")}
				adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
				if err != nil {
					t.Fatal(err)
				}
				req := streamStorageRequest()
				req.Prompt = ""
				if kind == "reader" {
					req.OpenPrompt = func(context.Context) (io.ReadCloser, error) {
						calls++
						return countedPromptReader{strings.NewReader(text), &closed}, nil
					}
				} else {
					req.WritePrompt = func(_ context.Context, w io.Writer) error { calls++; _, err := io.WriteString(w, text); return err }
				}
				if _, err := adapter.Execute(context.Background(), req); err != nil {
					t.Fatal(err)
				}
				if got, want := runner.input.String(), strings.Join(providerInvocationPromptParts(text, req.AttemptID, req.InvocationID), ""); got != want {
					t.Fatal("streamed prompt framing changed content")
				}
				if err := adapter.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 2 || (kind == "reader" && closed != 2) {
				t.Fatalf("calls=%d closed=%d", calls, closed)
			}
		})
	}
}

func TestStalledPromptSourceIsClosedAndRemainsTimeout(t *testing.T) {
	runner := &promptCaptureRunner{output: successfulProviderEvents("unused")}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner, StreamInactivityTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	reader, writer := io.Pipe()
	defer writer.Close()
	req := streamStorageRequest()
	req.Prompt = ""
	req.OpenPrompt = func(context.Context) (io.ReadCloser, error) { return reader, nil }
	response, err := adapter.Execute(context.Background(), req)
	if err == nil || response.Outcome.ReasonCode != "timeout" {
		t.Fatalf("source stall outcome=%+v err=%v", response.Outcome, err)
	}
}

func TestAdapterCloseRetiresContentAndUnconsumedReplay(t *testing.T) {
	runner := &promptCaptureRunner{output: successfulProviderEvents(strings.Repeat("x", 64*1024))}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.Execute(context.Background(), streamStorageRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.Stream(context.Background(), StreamRequest{AttemptID: "attempt"}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	for _, content := range append(response.StreamObservation.StagedDiagnosticContent, *response.StreamObservation.StagedTerminalContent) {
		if _, err := os.Stat(content.Path); !os.IsNotExist(err) {
			t.Fatalf("owned scratch content survived Close: %v", err)
		}
	}
	if _, err := adapter.Execute(context.Background(), streamStorageRequest()); err == nil {
		t.Fatal("closed adapter launched a new attempt")
	}
}

type warmLifecycleRunner struct{ handle *warmLifecycleHandle }

func (r warmLifecycleRunner) Start(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
	return r.handle, nil
}

type warmLifecycleHandle struct {
	streamingHandle
	started, cancelled, release chan struct{}
	once                        sync.Once
}

func (h *warmLifecycleHandle) Cancel(context.Context) error {
	h.once.Do(func() { close(h.cancelled) })
	return nil
}
func (h *warmLifecycleHandle) Wait(context.Context) (processguardian.Result, error) {
	close(h.started)
	<-h.release
	return processguardian.Result{Quiescent: true}, nil
}

func TestAdapterCloseContextCancelsWarmupAndHonorsDrainDeadline(t *testing.T) {
	h := &warmLifecycleHandle{streamingHandle: streamingHandle{stdout: io.NopCloser(strings.NewReader("ok")), stderr: io.NopCloser(strings.NewReader(""))}, started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Agent: "planner", Runner: warmLifecycleRunner{h}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- adapter.WarmRuntime(context.Background()) }()
	<-h.started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := adapter.CloseContext(ctx); err != context.DeadlineExceeded {
		t.Fatalf("close returned before helper quiescence: %v", err)
	}
	select {
	case <-h.cancelled:
	case <-time.After(time.Second):
		t.Fatal("close did not cancel owned warmup")
	}
	close(h.release)
	<-done
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
}
