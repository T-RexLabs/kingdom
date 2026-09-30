package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMaintenanceAdmissionBoundsWorkersAndReleasesCapacity(t *testing.T) {
	loop, _ := newMaintenanceAdmissionFixture(t, 2)
	release := make(chan struct{})
	work := func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, attempt := range []string{"first", "second"} {
		if !loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "finalize", attemptID: attempt, runID: "run"}, work) {
			t.Fatalf("failed to admit %s within capacity", attempt)
		}
	}
	if loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "cleanup", attemptID: "third", runID: "other-run"}, work) {
		t.Fatal("maintenance exceeded its worker bound")
	}
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
	if !loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "cleanup", attemptID: "third", runID: "other-run"}, work) {
		t.Fatal("completed maintenance did not release capacity")
	}
	waitMaintenanceAdmissionDrain(t, loop)
}

func TestMaintenanceBusyCanonicalRunDoesNotConsumeCapacity(t *testing.T) {
	loop, _ := newMaintenanceAdmissionFixture(t, 1)
	unlockBusy := loop.lockRunWorkspace("busy-run")
	defer unlockBusy()
	if loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "finalize", attemptID: "blocked", runID: "busy-run", canonical: true}, func(context.Context) error {
		return errors.New("busy canonical worker must not run")
	}) {
		t.Fatal("maintenance acquired a reserved canonical run")
	}
	release := make(chan struct{})
	reserved := make(chan bool, 1)
	if !loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "finalize", attemptID: "free", runID: "free-run", canonical: true}, func(ctx context.Context) error {
		unlock, ok := loop.tryReserveCompletionWorkspace(ctx, "free-run")
		reserved <- ok
		if ok {
			unlock()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}) {
		t.Fatal("busy canonical candidate consumed the unrelated run's capacity")
	}
	select {
	case ok := <-reserved:
		if !ok {
			t.Fatal("canonical worker could not reuse its admitted reservation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated canonical worker did not start")
	}
	if unlock, ok := loop.tryLockRunWorkspace("free-run"); ok {
		unlock()
		t.Fatal("nested reservation released the outer canonical reservation")
	}
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
	unlock, ok := loop.tryLockRunWorkspace("free-run")
	if !ok {
		t.Fatal("canonical reservation survived worker completion")
	}
	unlock()
}

func TestMaintenanceAttemptReservationExcludesExecution(t *testing.T) {
	loop, _ := newMaintenanceAdmissionFixture(t, 2)
	release := make(chan struct{})
	work := func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	request := maintenanceRequest{kind: "private_cleanup", attemptID: "attempt", runID: "run"}
	if !loop.tryScheduleMaintenance(context.Background(), request, work) {
		t.Fatal("cleanup was not admitted")
	}
	if loop.markExecutionInflight("attempt", "run") {
		t.Fatal("execution overlapped cleanup of its own private workspace")
	}
	request.kind = "finalize"
	if loop.tryScheduleMaintenance(context.Background(), request, work) {
		t.Fatal("different maintenance categories overlapped the same attempt")
	}
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
	if !loop.markExecutionInflight("attempt", "run") {
		t.Fatal("cleanup retained the attempt reservation after completion")
	}
	defer loop.clearExecutionInflight("attempt")
	if loop.tryScheduleMaintenance(context.Background(), request, work) {
		t.Fatal("ordinary maintenance overlapped an active execution")
	}
	request.kind, request.allowExecuting = "cancel_stop", true
	if !loop.tryScheduleMaintenance(context.Background(), request, work) {
		t.Fatal("cancellation could not reach an active execution")
	}
	waitMaintenanceAdmissionDrain(t, loop)
}

func TestPrivateMaintenanceNeverReservesCanonicalWorkspace(t *testing.T) {
	loop, _ := newMaintenanceAdmissionFixture(t, 1)
	unlockCanonical := loop.lockRunWorkspace("run")
	release := make(chan struct{})
	admitted := loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "private_cleanup", attemptID: "attempt", runID: "run"}, func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	unlockCanonical()
	if !admitted {
		t.Fatal("private cleanup waited for an unrelated canonical reservation")
	}
	unlock, ok := loop.tryLockRunWorkspace("run")
	if !ok {
		t.Fatal("active private cleanup reserved the canonical workspace")
	}
	unlock()
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
}

func TestMaintenanceUsesRuntimeLifetimeAndHonorsDrainCancellation(t *testing.T) {
	loop, stopRuntime := newMaintenanceAdmissionFixture(t, 1)
	caller, stopCaller := context.WithCancel(context.Background())
	defer stopCaller()
	started := make(chan context.Context, 1)
	if !loop.tryScheduleMaintenance(caller, maintenanceRequest{kind: "finalize", attemptID: "attempt", runID: "run"}, func(ctx context.Context) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}) {
		t.Fatal("maintenance was not admitted")
	}
	var workerCtx context.Context
	select {
	case workerCtx = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not start")
	}
	stopCaller()
	if workerCtx.Err() != nil {
		t.Fatal("admission caller cancellation stopped runtime-owned maintenance")
	}
	if err := loop.waitForMaintenance(caller); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter returned %v", err)
	}
	if !loop.maintenanceActive("finalize") {
		t.Fatal("cancelled waiter stopped admitted maintenance")
	}
	stopRuntime()
	waitMaintenanceAdmissionDrain(t, loop)
	if loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "finalize", attemptID: "new-attempt", runID: "run"}, func(context.Context) error { return nil }) {
		t.Fatal("maintenance was admitted after runtime cancellation")
	}
}

func TestMaintenanceCancellationStopHasIndependentBoundedCapacity(t *testing.T) {
	loop, _ := newMaintenanceAdmissionFixture(t, defaultMaintenanceLimit)
	release := make(chan struct{})
	work := func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for n := 0; n < defaultMaintenanceLimit; n++ {
		if !loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "finalize", attemptID: fmt.Sprintf("filesystem-%d", n), runID: fmt.Sprintf("run-%d", n), canonical: true}, work) {
			t.Fatal("could not saturate ordinary filesystem workers")
		}
	}
	if !loop.markExecutionInflight("running-provider", "provider-run") {
		t.Fatal("reserve active provider")
	}
	defer loop.clearExecutionInflight("running-provider")
	stop := maintenanceRequest{kind: "cancel_stop", attemptID: "running-provider", runID: "provider-run", allowExecuting: true}
	if !loop.tryScheduleMaintenance(context.Background(), stop, work) {
		t.Fatal("filesystem saturation prevented a bounded provider stop request")
	}
	if loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "cancel_stop", attemptID: "another-provider", runID: "another-run", allowExecuting: true}, work) {
		t.Fatal("provider stop admission exceeded its independent one-worker bound")
	}
	if loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "cancel_finish", attemptID: "quiescent-provider", runID: "quiescent-run", canonical: true}, work) {
		t.Fatal("workspace restoration bypassed the ordinary filesystem bound")
	}
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
}

func TestForceCloseCancelsMaintenanceAdmittedDuringGracefulDrain(t *testing.T) {
	loop, _ := completionTimestampFixture(t)
	original, stopOriginal := context.WithCancel(context.Background())
	loop.maintenance.runtimeCtx = original
	mode := newRuntimeCloseMode()
	loop.closeMode = mode
	runtime := &Runtime{orchestrator: loop, closeMode: mode, cancelMaintenance: stopOriginal}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	loop.now = func() time.Time {
		// The outbox worker calls now only after drain admission selected its
		// runtime context. Hold it there without relying on scheduler timing.
		once.Do(func() { close(started); <-release })
		return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	}
	drainCtx, stopDrain := context.WithCancel(context.Background())
	drained := make(chan error, 1)
	go func() { drained <- runtime.drainMaintenance(drainCtx) }()
	t.Cleanup(func() {
		stopDrain()
		stopOriginal()
		close(release)
		select {
		case <-drained:
		case <-time.After(5 * time.Second):
			t.Error("graceful maintenance drain did not stop")
		}
		waitMaintenanceAdmissionDrain(t, loop)
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("graceful drain did not admit its pending outbox")
	}
	loop.execMu.Lock()
	workerCtx := loop.maintenance.runtimeCtx
	loop.execMu.Unlock()
	if err := runtime.ForceClose(); err != nil {
		t.Fatal(err)
	}
	if workerCtx.Err() == nil {
		t.Fatal("forced shutdown did not cancel maintenance admitted by graceful drain")
	}
}

func TestGracefulDrainRearmsCancelledMaintenanceGeneration(t *testing.T) {
	loop, result := completionTimestampFixture(t)
	previous, stopPrevious := context.WithCancel(context.Background())
	stopPrevious()
	loop.maintenance.runtimeCtx = previous
	runtime := &Runtime{orchestrator: loop, closeMode: newRuntimeCloseMode(), cancelMaintenance: stopPrevious}
	t.Cleanup(func() { runtime.stopMaintenance(); waitMaintenanceAdmissionDrain(t, loop) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.drainMaintenance(ctx); err != nil {
		t.Fatalf("graceful retry stranded pending work behind a cancelled context: %v", err)
	}
	var status string
	if err := loop.db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id=?`, result.attemptID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("graceful retry left outbox %s", status)
	}
}

func newMaintenanceAdmissionFixture(t *testing.T, limit int) (*orchestrationLoop, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	loop := &orchestrationLoop{maintenance: maintenanceState{limit: limit, runtimeCtx: ctx}}
	t.Cleanup(func() {
		cancel()
		waitMaintenanceAdmissionDrain(t, loop)
	})
	return loop, cancel
}

func waitMaintenanceAdmissionDrain(t *testing.T, loop *orchestrationLoop) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := loop.waitForMaintenance(ctx); err != nil {
		t.Fatalf("maintenance did not drain: %v", err)
	}
}
