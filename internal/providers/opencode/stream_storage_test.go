package opencode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/processguardian"
)

type streamingFixture struct {
	starts int
	output func(int) io.Reader
	stderr string
}

func (r *streamingFixture) Start(ctx context.Context, _ processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
	r.starts++
	ownership := processguardian.Ownership{GuardID: fmt.Sprintf("guard-%d", r.starts), GuardianPID: 1, ChildPID: 1, TreeID: 1, Platform: "unix_process_group", QuiescenceCapability: "fixture"}
	if persist != nil {
		if err := persist(ctx, ownership); err != nil {
			return nil, err
		}
	}
	return &streamingHandle{stdout: io.NopCloser(r.output(r.starts)), stderr: io.NopCloser(strings.NewReader(r.stderr)), ownership: ownership}, nil
}

type streamingHandle struct {
	stdout, stderr io.ReadCloser
	ownership      processguardian.Ownership
}

func (h *streamingHandle) GuardID() string              { return h.ownership.GuardID }
func (h *streamingHandle) Stdin() io.WriteCloser        { return unusedWriteCloser{} }
func (h *streamingHandle) Stdout() io.ReadCloser        { return h.stdout }
func (h *streamingHandle) Stderr() io.ReadCloser        { return h.stderr }
func (h *streamingHandle) Cancel(context.Context) error { return nil }
func (h *streamingHandle) Wait(context.Context) (processguardian.Result, error) {
	return processguardian.Result{Ownership: h.ownership, Quiescent: true}, nil
}

func streamStorageRequest() ExecuteRequest {
	return ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session", AttemptID: "attempt", RunID: "run", NodeRunID: "node", Prompt: "test", InvocationID: "invocation", OccurredAt: "2026-09-29T12:00:00Z"}
}

func TestSessionDiskStreamBeyondFormerByteCountAndTextCeilings(t *testing.T) {
	const count = 5000
	const finalBytes = 9 * 1024 * 1024
	line := `{"type":"text","part":{"text":"` + strings.Repeat("p", 8192) + `"}}` + "\n"
	runner := &streamingFixture{stderr: strings.Repeat("diagnostic\n", 200000), output: func(int) io.Reader {
		readers := make([]io.Reader, 0, count+3)
		for range count {
			readers = append(readers, strings.NewReader(line))
		}
		readers = append(readers, strings.NewReader(`{"type":"step_finish","part":{"reason":"tool-calls"}}`+"\n"), strings.NewReader(string(successfulProviderEvents(strings.Repeat("z", finalBytes)))))
		return io.MultiReader(readers...)
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	response, err := adapter.Execute(context.Background(), streamStorageRequest())
	if err != nil || !response.Accepted || response.StreamObservation.CanonicalTerminalTextRef == nil || response.StreamObservation.CanonicalTerminalTextRef.SizeBytes != finalBytes || response.OutputText != "" {
		t.Fatalf("accepted=%t bytes=%d err=%v", response.Accepted, len(response.OutputText), err)
	}
	if err := providerexecution.CopyTerminalText(context.Background(), response.StreamObservation, io.Discard); err != nil {
		t.Fatal(err)
	}
	if adapter.attempts["attempt"].count <= 4096 {
		t.Fatal("event journal lost events beyond old count cap")
	}
	if len(response.StreamObservation.StagedDiagnosticContent) != 2 {
		t.Fatal("missing full diagnostic references")
	}
	for index, staged := range response.StreamObservation.StagedDiagnosticContent {
		defer os.Remove(staged.Path)
		file, err := os.Open(staged.Path)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		n, err := io.Copy(h, file)
		file.Close()
		if err != nil || n != staged.Reference.SizeBytes || fmt.Sprintf("%x", h.Sum(nil)) != staged.Reference.SHA256 {
			t.Fatal("diagnostic content was truncated or changed")
		}
		if index == 0 && n <= 32*1024*1024 {
			t.Fatalf("test stream only %d bytes", n)
		}
	}
}

func TestContinuationBeyondThreeRetainsEveryGuardianCallback(t *testing.T) {
	runner := &streamingFixture{output: func(turn int) io.Reader {
		if turn < 7 {
			return strings.NewReader(`{"type":"step_finish","sessionID":"session","part":{"reason":"tool-calls"}}` + "\n")
		}
		return strings.NewReader(string(successfulProviderEvents("complete")))
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	request := streamStorageRequest()
	var ownership, quiescence []int
	request.OnProcessGuardOwnership = func(_ context.Context, o ProcessGuardOwnership) error {
		ownership = append(ownership, o.CommandOrdinal)
		return nil
	}
	request.OnProcessGuardQuiescence = func(_ context.Context, o ProcessGuardQuiescence) error {
		quiescence = append(quiescence, o.CommandOrdinal)
		return nil
	}
	response, err := adapter.Execute(context.Background(), request)
	if err != nil || !response.Accepted || runner.starts != 7 {
		t.Fatalf("starts=%d accepted=%t err=%v", runner.starts, response.Accepted, err)
	}
	for _, staged := range response.StreamObservation.StagedDiagnosticContent {
		defer os.Remove(staged.Path)
	}
	if len(ownership) != 7 || len(quiescence) != 7 {
		t.Fatalf("guardian lineage: %v / %v", ownership, quiescence)
	}
	for i := range ownership {
		if ownership[i] != i+1 || quiescence[i] != i+1 {
			t.Fatal("guardian ordinal mismatch")
		}
	}
	if len(response.StreamObservation.ProcessGuards) != 1 || response.StreamObservation.ProcessGuards[0].CommandOrdinal != 7 {
		t.Fatal("guardian metadata must retain only the last preview")
	}
}

func TestLargeStreamCannotHideMalformedOrLaterEvents(t *testing.T) {
	for _, suffix := range []string{"malformed\n", `{"type":"text","part":{"text":"late"}}` + "\n"} {
		runner := &streamingFixture{output: func(int) io.Reader {
			return io.MultiReader(strings.NewReader(string(successfulProviderEvents(strings.Repeat("x", 9*1024*1024)))), strings.NewReader(suffix))
		}}
		adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
		if err != nil {
			t.Fatal(err)
		}
		response, err := adapter.Execute(context.Background(), streamStorageRequest())
		adapter.Close()
		for _, staged := range response.StreamObservation.StagedDiagnosticContent {
			os.Remove(staged.Path)
		}
		if err == nil || response.Accepted {
			t.Fatal("large terminal text hid invalid later evidence")
		}
	}
}
