package opencode

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"kingdom/internal/processguardian"
)

// Continuation tests stop at an observed lifecycle boundary, not after a
// machine-dependent number of subprocesses fit into a wall-clock interval.
func cancelAfterQuiescentCommands(cancel context.CancelFunc, count int) func(context.Context, ProcessGuardQuiescence) error {
	return func(_ context.Context, observation ProcessGuardQuiescence) error {
		if !observation.Result.Quiescent {
			return fmt.Errorf("command %d did not become quiescent", observation.CommandOrdinal)
		}
		if observation.CommandOrdinal == count {
			cancel()
		}
		return nil
	}
}

type timedProviderFrame struct {
	delay time.Duration
	json  string
}
type timedProviderReader struct {
	frames  []timedProviderFrame
	current *strings.Reader
}

func (r *timedProviderReader) Read(buffer []byte) (int, error) {
	for {
		if r.current != nil {
			n, err := r.current.Read(buffer)
			if n > 0 {
				return n, err
			}
			if err != io.EOF {
				return n, err
			}
			r.current = nil
		}
		if len(r.frames) == 0 {
			return 0, io.EOF
		}
		frame := r.frames[0]
		r.frames = r.frames[1:]
		// Inside synctest this advances virtual time only once the decoder and
		// watchdog are blocked. File I/O and host scheduling do not create gaps.
		time.Sleep(frame.delay)
		r.current = strings.NewReader(frame.json + "\n")
	}
}

type watchdogTestRunner struct {
	GuardianRunner
	cancels atomic.Int32
}

func (r *watchdogTestRunner) Start(ctx context.Context, command processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
	handle, err := r.GuardianRunner.Start(ctx, command, persist)
	if err != nil {
		return nil, err
	}
	return &watchdogTestHandle{Handle: handle, cancels: &r.cancels}, nil
}

type watchdogTestHandle struct {
	processguardian.Handle
	cancels *atomic.Int32
}

func (h *watchdogTestHandle) Cancel(ctx context.Context) error {
	h.cancels.Add(1)
	return h.Handle.Cancel(ctx)
}

func heartbeatTestRunner(count int, interval, finalGap time.Duration) *watchdogTestRunner {
	return &watchdogTestRunner{GuardianRunner: &streamingFixture{output: func(int) io.Reader {
		frames := make([]timedProviderFrame, 0, count+2)
		for index := range count {
			delay := interval
			if index == 0 {
				delay = 0
			}
			frames = append(frames, timedProviderFrame{delay, `{"type":"heartbeat","sessionID":"ses_heartbeat"}`})
		}
		frames = append(frames, timedProviderFrame{finalGap, `{"type":"text","sessionID":"ses_heartbeat","part":{"messageID":"turn_heartbeat","text":"Completed after heartbeats."}}`}, timedProviderFrame{0, `{"type":"step_finish","sessionID":"ses_heartbeat","part":{"id":"step_heartbeat","messageID":"turn_heartbeat","reason":"stop"}}`})
		return &timedProviderReader{frames: frames}
	}}}
}

func TestSessionAdapterHeartbeatSilenceStillTimesOutBeforeLateStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const inactivity = 250 * time.Millisecond
		runner := heartbeatTestRunner(10, 50*time.Millisecond, inactivity+50*time.Millisecond)
		adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner, FirstEventTimeout: 2 * time.Second, StreamInactivityTimeout: inactivity})
		if err != nil {
			t.Fatal(err)
		}
		defer adapter.Close()
		response, err := adapter.Execute(context.Background(), streamStorageRequest())
		if err == nil {
			t.Fatal("expected inactivity timeout")
		}
		if response.Accepted || !response.StreamObservation.TimedOut || response.Outcome.ReasonCode != "timeout" {
			t.Fatalf("late stop repaired the timeout: %+v", response)
		}
		payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt"), AttemptEventTypeFailed)
		if payload["timeout_type"] != "stream_inactivity_timeout" || payload["timeout_ms"] != float64(inactivity.Milliseconds()) {
			t.Fatalf("incorrect watchdog boundary: %+v", payload)
		}
		if runner.cancels.Load() != 1 {
			t.Fatalf("guardian cancellations=%d, want one timeout cancellation", runner.cancels.Load())
		}
	})
}
