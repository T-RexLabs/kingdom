package opencode

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

func TestSessionAdapterUnpersistedLaunchAbortDoesNotIngestDurableQuiescence(t *testing.T) {
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: t.TempDir(), CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &SessionAdapter{binaryPath: os.Args[0], databasePath: filepath.Join(t.TempDir(), "opencode.db"), runner: runner}
	persistenceErr := errors.New("ownership transaction failed")
	quiescenceErr := errors.New("durable evidence is incomplete")
	var ownershipCalls, quiescenceCalls int
	_, err = adapter.startOpencodeRun(context.Background(), ExecuteRequest{
		WorkspacePath: t.TempDir(),
		OnProcessGuardOwnership: func(context.Context, ProcessGuardOwnership) error {
			ownershipCalls++
			return persistenceErr
		},
		OnProcessGuardQuiescence: func(context.Context, ProcessGuardQuiescence) error {
			quiescenceCalls++
			return quiescenceErr
		},
	}, "", 1)
	if !errors.Is(err, persistenceErr) {
		t.Fatalf("ownership failure was lost: %v", err)
	}
	if ownershipCalls != 1 || quiescenceCalls != 0 || errors.Is(err, quiescenceErr) {
		t.Fatalf("unpersisted abort attempted durable ingestion: ownership=%d quiescence=%d err=%v", ownershipCalls, quiescenceCalls, err)
	}
	var launchErr *processguardian.LaunchError
	if !errors.As(err, &launchErr) || !launchErr.Result.Quiescent {
		t.Fatalf("unpersisted launch did not preserve stopped-tree proof: %v", err)
	}
}

func TestSessionAdapterPersistedLaunchAbortIngestsDurableQuiescence(t *testing.T) {
	runtimeRoot := t.TempDir()
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &SessionAdapter{binaryPath: os.Args[0], databasePath: filepath.Join(t.TempDir(), "opencode.db"), runner: runner}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ownership processguardian.Ownership
	quiescenceCalls := 0
	_, err = adapter.startOpencodeRun(ctx, ExecuteRequest{
		WorkspacePath: t.TempDir(),
		OnProcessGuardOwnership: func(_ context.Context, guard ProcessGuardOwnership) error {
			ownership = guard.Ownership
			cancel()
			return nil
		},
		OnProcessGuardQuiescence: func(callbackCtx context.Context, guard ProcessGuardQuiescence) error {
			quiescenceCalls++
			if callbackCtx.Err() != nil || guard.CommandOrdinal != 2 || guard.ExitedAt == "" || guard.QuiescentAt == "" || !guard.Result.Quiescent {
				t.Errorf("invalid abort callback: ctx=%v guard=%+v", callbackCtx.Err(), guard)
			}
			evidence, digest, raw, readErr := processguardian.ReadQuiescenceEvidence(runtimeRoot, ownership.GuardID)
			if readErr != nil || digest != guard.Result.EvidenceSHA256 || raw != guard.Result.EvidenceJSON || evidence.Ownership != ownership {
				t.Errorf("abort callback did not retain durable proof: digest=%q result=%+v err=%v", digest, guard.Result, readErr)
			}
			return nil
		},
	}, "", 2)
	if !errors.Is(err, context.Canceled) || quiescenceCalls != 1 {
		t.Fatalf("persisted abort did not ingest proof after cancellation: calls=%d err=%v", quiescenceCalls, err)
	}
}

func TestSessionAdapterPersistedLaunchAbortDoesNotSuppressMissingProof(t *testing.T) {
	runtimeRoot := filepath.Join(t.TempDir(), "guardian")
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: runtimeRoot, CancelGrace: 100 * time.Millisecond, CancelForce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &SessionAdapter{binaryPath: os.Args[0], databasePath: filepath.Join(t.TempDir(), "opencode.db"), runner: runner}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proofErr := errors.New("persisted guard requires durable quiescence evidence")
	quiescenceCalls := 0
	_, err = adapter.startOpencodeRun(ctx, ExecuteRequest{
		WorkspacePath: t.TempDir(),
		OnProcessGuardOwnership: func(context.Context, ProcessGuardOwnership) error {
			// Make evidence publication fail after ownership has committed.
			if err := os.Remove(runtimeRoot); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(runtimeRoot, nil, 0o600); err != nil {
				t.Error(err)
			}
			cancel()
			return nil
		},
		OnProcessGuardQuiescence: func(_ context.Context, guard ProcessGuardQuiescence) error {
			quiescenceCalls++
			if guard.Result.EvidenceJSON != "" || guard.Result.EvidenceSHA256 != "" {
				t.Errorf("expected evidence publication failure, got %+v", guard.Result)
			}
			return proofErr
		},
	}, "", 1)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, proofErr) || quiescenceCalls != 1 {
		t.Fatalf("persisted abort suppressed missing durable proof: calls=%d err=%v", quiescenceCalls, err)
	}
}
