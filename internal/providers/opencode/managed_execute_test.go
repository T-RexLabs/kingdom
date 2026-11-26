package opencode

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

func preparedManagedExecution(t *testing.T) (*ManagedService, *ownerFixture, ExecuteRequest, Prepared, *atomic.Int32) {
	t.Helper()
	h, a, req := managedOwner(t)
	closes := new(atomic.Int32)
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		return &testManagedHandle{endpoint: recordedManagedEndpoint(t, h, p), transport: managedProtocolTransport(t), authorize: func(context.Context, providerexecution.RequestIdentity) error { return nil }, close: func(context.Context) error { closes.Add(1); return nil }}, nil
	}, close: func(context.Context, string) error { closes.Add(1); return nil }}
	s := serviceManaged(t, a, worker)
	prepared, err := s.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.OpenPrompt = func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("fixture prompt")), nil
	}
	t.Cleanup(func() {
		if err := s.CloseContext(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s, h, req, prepared, closes
}

func TestManagedExecuteCannotBypassCanonicalRemoteDispatchGate(t *testing.T) {
	s, h, req, prepared, closes := preparedManagedExecution(t)
	h.exec(`UPDATE planning_provider_invocations SET invocation_state='open',provider_session_id=?`, prepared.ProviderSessionID)
	h.exec(`UPDATE planning_operations SET provider_session_id=?`, prepared.ProviderSessionID)
	tx, err := h.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	binding := providerrequest.Binding{ID: "binding-invocation", Owner: providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: req.InvocationID}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}, Identity: prepared.Identity, Backend: providerexecution.BackendOpenShell, RemoteAllocation: prepared.RemoteRuntime.Allocation}
	if err = providerrequest.BindRemoteTx(context.Background(), tx, binding, prepared.RemoteRuntime, h.now); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	response, err := s.Execute(context.Background(), req)
	if !errors.Is(err, providerrequest.ErrDispatchClosed) {
		t.Fatalf("canonical dispatch gate bypassed: %v", err)
	}
	if response.StreamObservation.Request != nil {
		t.Fatal("unclosed canonical resources produced terminal")
	}
	if closes.Load() == 0 {
		t.Fatal("blocked dispatch retained live server")
	}
	var issued int
	if err = h.db.QueryRow(`SELECT count(*) FROM provider_requests WHERE dispatch_issued_at IS NOT NULL`).Scan(&issued); err != nil || issued != 0 {
		t.Fatal("dispatch was issued", issued, err)
	}
	var guardians int
	if err = h.db.QueryRow(`SELECT count(*) FROM provider_process_guards`).Scan(&guardians); err != nil || guardians != 0 {
		t.Fatal("remote execution created local guardian", guardians, err)
	}
}

func TestManagedCancelJoinsExecutionBeforeClosingCapture(t *testing.T) {
	s, _, req, _, closes := preparedManagedExecution(t)
	entry := s.entries[req.InvocationID]
	capture := entry.session.capture
	entered, release := make(chan struct{}), make(chan struct{})
	req.OpenPrompt = func(context.Context) (io.ReadCloser, error) {
		close(entered)
		<-release
		return io.NopCloser(strings.NewReader("fixture")), nil
	}
	finished := make(chan error, 1)
	go func() { _, err := s.Execute(context.Background(), req); finished <- err }()
	<-entered
	if _, err := s.Execute(context.Background(), req); err == nil {
		t.Fatal("concurrent execution admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.Cancel(ctx, req.AttemptID)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel did not preserve active execution: %v", err)
	}
	select {
	case <-entry.request.stop:
	default:
		t.Fatal("cancel did not request stop")
	}
	if closes.Load() != 0 {
		t.Fatal("server closed while execution still owns snapshots")
	}
	select {
	case <-capture.done:
		t.Fatal("capture stopped before execution joined")
	default:
	}
	close(release)
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("stop cause lost: %v", err)
	}
	if closes.Load() == 0 {
		t.Fatal("early execution error skipped server closure")
	}
	select {
	case <-capture.done:
	default:
		t.Fatal("execution cleanup did not join capture")
	}
	s.mu.Lock()
	remaining := len(s.entries)
	s.mu.Unlock()
	if remaining != 0 {
		t.Fatal("closed execution retained ephemeral ownership")
	}
}

func TestManagedExecuteRetainsUncertainCleanupForRecovery(t *testing.T) {
	s, _, req, _, _ := preparedManagedExecution(t)
	entry := s.entries[req.InvocationID]
	unresolved := errors.New("remote closure uncertain")
	handle := entry.handle.(*testManagedHandle)
	handle.close = func(context.Context) error { return unresolved }
	req.OpenPrompt = func(context.Context) (io.ReadCloser, error) { return nil, errors.New("input unavailable") }
	if _, err := s.Execute(context.Background(), req); !errors.Is(err, unresolved) {
		t.Fatal("cleanup uncertainty lost", err)
	}
	s.mu.Lock()
	retained := s.entries[req.InvocationID] == entry
	s.mu.Unlock()
	if !retained {
		t.Fatal("uncertain remote owner removed")
	}
	handle.close = func(context.Context) error { return nil }
	if err := s.Drain(context.Background(), req.InvocationID); err != nil {
		t.Fatal(err)
	}
}
