package opencode

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/processguardian"
)

func TestOpenCodeProcessGuardRetryReusesAcknowledgementTimestamp(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "opencode.db")
	runner := &repeatedOwnershipCallbackRunner{handle: acknowledgementTestHandle{}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath: os.Args[0], WorkDir: t.TempDir(), DatabasePath: databasePath, Runner: runner,
	})
	if err != nil {
		t.Fatalf("create session adapter: %v", err)
	}
	var acknowledgements []ProcessGuardOwnership
	started, err := adapter.startOpencodeRun(context.Background(), ExecuteRequest{
		AttemptID: "attempt_acknowledgement_timestamp",
		OnProcessGuardOwnership: func(_ context.Context, ownership ProcessGuardOwnership) error {
			acknowledgements = append(acknowledgements, ownership)
			return nil
		},
	}, "session_acknowledgement_timestamp", 1)
	if err != nil {
		t.Fatalf("start OpenCode run: %v", err)
	}
	started.releaseDatabaseStartup()
	if len(acknowledgements) != 2 {
		t.Fatalf("expected two ownership callback attempts, got %d", len(acknowledgements))
	}
	if acknowledgements[0].AcknowledgedAt == "" || acknowledgements[0].AcknowledgedAt != acknowledgements[1].AcknowledgedAt {
		t.Fatalf("ownership retry changed acknowledgement timestamp: first=%q second=%q", acknowledgements[0].AcknowledgedAt, acknowledgements[1].AcknowledgedAt)
	}
	for _, arg := range runner.command.Args {
		if arg == "prompt" {
			t.Fatal("prompt was passed as a command-line argument")
		}
	}
}

type repeatedOwnershipCallbackRunner struct {
	handle  processguardian.Handle
	command processguardian.Command
}

func (r *repeatedOwnershipCallbackRunner) Start(ctx context.Context, command processguardian.Command, persist processguardian.PersistOwnership) (processguardian.Handle, error) {
	r.command = command
	if persist != nil {
		ownership := processguardian.Ownership{
			GuardID: "guard_acknowledgement_timestamp", GuardianPID: 11, GuardianIdentityToken: "guardian",
			ChildPID: 12, ChildIdentityToken: "child", TreeID: 12, TreeIdentityToken: "tree",
			Platform: "unix_process_group", QuiescenceCapability: "capability",
		}
		for range 2 {
			if err := persist(ctx, ownership); err != nil {
				return nil, err
			}
		}
	}
	return r.handle, nil
}

type acknowledgementTestHandle struct{}

func (acknowledgementTestHandle) GuardID() string { return "guard_acknowledgement_timestamp" }
func (acknowledgementTestHandle) Stdin() io.WriteCloser {
	return acknowledgementTestWriteCloser{}
}
func (acknowledgementTestHandle) Stdout() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}
func (acknowledgementTestHandle) Stderr() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}
func (acknowledgementTestHandle) Cancel(context.Context) error { return nil }
func (acknowledgementTestHandle) Wait(context.Context) (processguardian.Result, error) {
	return processguardian.Result{Quiescent: true}, nil
}

type acknowledgementTestWriteCloser struct{}

func (acknowledgementTestWriteCloser) Write(payload []byte) (int, error) { return len(payload), nil }
func (acknowledgementTestWriteCloser) Close() error                      { return nil }
