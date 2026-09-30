package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func TestTruncatedProviderFramesCannotPreserveSuccess(t *testing.T) {
	for _, suffix := range []string{"{", `{"type":"error"`, `{"ignored":[`, `{"part":{"text":"late"}`} {
		for _, newline := range []string{"", "\n"} {
			for _, padding := range []int{0, streamPreviewBytes} {
				t.Run(fmt.Sprintf("%s/newline=%t/padding=%d", suffix, newline != "", padding), func(t *testing.T) {
					runner := &streamingFixture{output: func(int) io.Reader {
						return strings.NewReader(string(successfulProviderEvents("complete")) + strings.Repeat(" ", padding) + suffix + newline)
					}}
					adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
					if err != nil {
						t.Fatal(err)
					}
					defer adapter.Close()
					response, err := adapter.Execute(context.Background(), streamStorageRequest())
					if err == nil || response.Accepted || response.StreamObservation.MalformedProtocolCount != 1 {
						t.Fatalf("accepted=%t malformed=%d err=%v", response.Accepted, response.StreamObservation.MalformedProtocolCount, err)
					}
				})
			}
		}
	}
}

func TestNumericCompactionCannotRepairObjectKeys(t *testing.T) {
	number := strings.Repeat("1", streamPreviewBytes)
	for _, property := range []string{
		number + `:0`,
		`"ignored":{` + number + `:0}`,
		`"ignored":[{}, {"valid":0,` + number + `:0}]`,
		`"part":{"state":{"input":{` + number + `:0}}}`,
	} {
		line := `{"type":"step_finish",` + property + `}`
		if json.Valid([]byte(line)) {
			t.Fatal("fixture must be invalid JSON")
		}
		events, malformed := 0, 0
		err := decodeOpencodeRunEventStream(strings.NewReader(line+"\n"+`{"type":"text","part":{"text":"later"}}`), nil,
			func(opencodeRunEvent) error { events++; return nil },
			func(string, error) { malformed++ })
		if err != nil || events != 1 || malformed != 1 {
			t.Errorf("events=%d malformed=%d err=%v", events, malformed, err)
		}
	}
}

func TestSurrogateLookaheadPreservesFollowingPair(t *testing.T) {
	for padding := streamPreviewBytes - 16; padding < streamPreviewBytes; padding++ {
		t.Run(fmt.Sprint(padding), func(t *testing.T) {
			literal := `"` + strings.Repeat("x", padding) + `\ud800\ud83d\ude00"`
			var want string
			if err := json.Unmarshal([]byte(literal), &want); err != nil {
				t.Fatal(err)
			}
			var actual strings.Builder
			err := decodeOpencodeRunEventStream(strings.NewReader(`{"type":"text","part":{"text":`+literal+`}}`), nil,
				func(event opencodeRunEvent) error {
					return event.textChunks(func(chunk string, _ bool) error { _, err := actual.WriteString(chunk); return err })
				}, func(_ string, err error) { t.Errorf("valid JSON rejected: %v", err) })
			if err != nil || actual.String() != want {
				t.Fatalf("decoded text differs at surrogate boundary: err=%v", err)
			}
		})
	}
}

func TestInvocationCaptureFinishesBothStreamsAfterFailure(t *testing.T) {
	capture, err := newInvocationCapture()
	if err != nil {
		t.Fatal(err)
	}
	stdoutFile, stderrFile := capture.stdout.file, capture.stderr.file
	t.Cleanup(func() {
		stdoutFile.Close()
		stderrFile.Close()
		os.Remove(stdoutFile.Name())
		os.Remove(stderrFile.Name())
	})
	stdoutErr, stderrErr := errors.New("stdout write failed"), errors.New("stderr write failed")
	capture.stdout.err, capture.stderr.err = stdoutErr, stderrErr
	for range 2 {
		refs, err := capture.finish("invocation")
		if len(refs) != 0 || !errors.Is(err, stdoutErr) || !errors.Is(err, stderrErr) {
			t.Errorf("finish did not retain both errors without publishing incomplete refs: refs=%v err=%v", refs, err)
		}
		for _, file := range []*os.File{stdoutFile, stderrFile} {
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("capture descriptor remains open: %s: %v", file.Name(), err)
			}
		}
	}
}

func TestStaleContentReleaseCannotRetireNewInvocation(t *testing.T) {
	runner := &streamingFixture{output: func(turn int) io.Reader {
		return strings.NewReader(string(successfulProviderEvents(fmt.Sprintf("answer-%d", turn))))
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	request := streamStorageRequest()
	first, err := adapter.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.StreamObservation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
	request.InvocationID = "invocation-2"
	second, err := adapter.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.StreamObservation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	if err := providerexecution.CopyTerminalText(context.Background(), second.StreamObservation, &text); err != nil || text.String() != "answer-2" {
		t.Fatalf("stale release destroyed new content: text=%q err=%v", text.String(), err)
	}
	events, failures, err := adapter.Stream(context.Background(), StreamRequest{AttemptID: request.AttemptID})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range events {
		count++
	}
	for err := range failures {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("new invocation journal is empty")
	}
	if err := second.StreamObservation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
}
