package opencode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDatabaseStartupGateSerializesSameDatabaseOnly(t *testing.T) {
	registry := databaseStartupGateRegistry{gates: map[string]chan struct{}{}}
	database := filepath.Join(t.TempDir(), "opencode.db")
	releaseFirst, err := registry.acquire(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	go func() {
		release, err := registry.acquire(context.Background(), database)
		if err == nil {
			acquired <- release
		}
	}()
	select {
	case <-acquired:
		t.Fatal("same database startup was not serialized")
	case <-time.After(25 * time.Millisecond):
	}

	otherRelease, err := registry.acquire(context.Background(), filepath.Join(t.TempDir(), "opencode.db"))
	if err != nil {
		t.Fatal(err)
	}
	otherRelease()
	releaseFirst()
	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("same database waiter did not resume")
	}
}

func TestSessionAdapterCoordinatesConcurrentProcessesSharingDatabase(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-opencode")
	script := `#!/bin/sh
lock="$OPENCODE_DB.startup"
if ! mkdir "$lock" 2>/dev/null; then
  printf 'database is locked\n' >&2
  exit 1
fi
sleep 0.05
rmdir "$lock"
printf '{"type":"text","sessionID":"ses_%s","part":{"type":"text","text":"ok"}}\n' "$$"
sleep 0.05
printf '{"type":"step_finish","sessionID":"ses_%s","part":{"type":"step_finish","reason":"stop"}}\n' "$$"
`
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "opencode.db")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binary, DatabasePath: database, FirstEventTimeout: 10 * time.Second, StreamInactivityTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	const executions = 4
	errs := make(chan error, executions)
	var wg sync.WaitGroup
	for i := 0; i < executions; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := adapter.Execute(context.Background(), ExecuteRequest{
				CompletionMode: CompletionModeServerResponse,
				SessionID:      fmt.Sprintf("session-%d", index), AttemptID: fmt.Sprintf("attempt-%d", index), RunID: "run-1", NodeRunID: fmt.Sprintf("node-%d", index),
				WorkspacePath: dir, Prompt: "test", OccurredAt: "2026-08-14T00:00:00Z",
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent process failed: %v", err)
		}
	}
}

func TestSessionAdapterRequestDatabaseGateCollidesWithExportDatabase(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-opencode")
	script := `#!/bin/sh
lock="$OPENCODE_DB.startup"
if [ "$1" = "export" ]; then
  if ! mkdir "$lock" 2>/dev/null; then exit 2; fi
  : > "$EXPORT_STARTED"
  while [ ! -f "$EXPORT_RELEASE" ]; do sleep 0.005; done
  rmdir "$lock"
  printf '{}\n'
  exit 0
fi
: > "$RUN_STARTED"
if ! mkdir "$lock" 2>/dev/null; then
  printf 'database is locked\n' >&2
  exit 1
fi
rmdir "$lock"
printf '{"type":"text","sessionID":"ses_override","part":{"type":"text","text":"ok"}}\n'
printf '{"type":"step_finish","sessionID":"ses_override","part":{"type":"step_finish","reason":"stop"}}\n'
`
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	actualDatabase := filepath.Join(dir, "actual.db")
	exportStarted := filepath.Join(dir, "export-started")
	exportRelease := filepath.Join(dir, "export-release")
	runStarted := filepath.Join(dir, "run-started")
	t.Setenv("EXPORT_STARTED", exportStarted)
	t.Setenv("EXPORT_RELEASE", exportRelease)
	t.Setenv("RUN_STARTED", runStarted)
	exportDone := make(chan error, 1)
	go func() {
		_, err := (cliSessionDiagnosticExporter{binaryPath: binary}).Export(context.Background(), sessionDiagnosticExportRequest{ProviderSessionID: "ses_export", DatabasePath: actualDatabase})
		exportDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(exportStarted); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("export did not acquire the database startup gate")
		}
		time.Sleep(5 * time.Millisecond)
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binary, DatabasePath: filepath.Join(dir, "adapter-default.db"), FirstEventTimeout: 5 * time.Second, StreamInactivityTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, err := adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session-override", AttemptID: "attempt-override", RunID: "run-1", NodeRunID: "node-1", WorkspacePath: dir, DatabasePath: actualDatabase, Prompt: "test", OccurredAt: "2026-08-14T00:00:00Z"})
		runDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := os.Stat(runStarted); err == nil {
		t.Fatal("run started while export held its effective database gate")
	}
	if err := os.WriteFile(exportRelease, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-exportDone; err != nil {
		t.Fatalf("export failed: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run failed after export released gate: %v", err)
	}
}

func TestSessionAdapterDifferentRequestDatabasesStartConcurrently(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-opencode")
	script := `#!/bin/sh
if [ "$OPENCODE_DB" = "$DB_ONE" ]; then
  own="$DB_ONE_READY"
  other="$DB_TWO_READY"
else
  own="$DB_TWO_READY"
  other="$DB_ONE_READY"
fi
: > "$own"
count=0
while [ ! -f "$other" ] && [ "$count" -lt 200 ]; do
  sleep 0.005
  count=$((count + 1))
done
if [ ! -f "$other" ]; then
  printf 'other database did not start concurrently\n' >&2
  exit 1
fi
printf '{"type":"text","sessionID":"ses_%s","part":{"type":"text","text":"ok"}}\n' "$$"
printf '{"type":"step_finish","sessionID":"ses_%s","part":{"type":"step_finish","reason":"stop"}}\n' "$$"
`
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	databaseOne := filepath.Join(dir, "one.db")
	databaseTwo := filepath.Join(dir, "two.db")
	t.Setenv("DB_ONE", databaseOne)
	t.Setenv("DB_TWO", databaseTwo)
	t.Setenv("DB_ONE_READY", filepath.Join(dir, "one-ready"))
	t.Setenv("DB_TWO_READY", filepath.Join(dir, "two-ready"))
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binary, DatabasePath: filepath.Join(dir, "shared-default.db"), FirstEventTimeout: 5 * time.Second, StreamInactivityTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for i, database := range []string{databaseOne, databaseTwo} {
		go func(index int, databasePath string) {
			_, err := adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: fmt.Sprintf("session-%d", index), AttemptID: fmt.Sprintf("attempt-%d", index), RunID: "run-1", NodeRunID: fmt.Sprintf("node-%d", index), WorkspacePath: dir, DatabasePath: databasePath, Prompt: "test", OccurredAt: "2026-08-14T00:00:00Z"})
			errs <- err
		}(i, database)
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("different database run failed: %v", err)
		}
	}
}

func TestDatabaseStartupGateWaiterHonorsCancellation(t *testing.T) {
	registry := databaseStartupGateRegistry{gates: map[string]chan struct{}{}}
	database := filepath.Join(t.TempDir(), "opencode.db")
	release, err := registry.acquire(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := registry.acquire(ctx, database); err == nil {
		t.Fatal("expected cancelled database startup wait")
	}
}
