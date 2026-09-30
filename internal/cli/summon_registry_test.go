package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// useIsolatedSummonRegistry gives one test its own registry.
//
// TestMain already keeps the whole package away from the operator's real cache
// directory; this narrows it further to tests that assert on the file itself,
// so a sibling registering a summon cannot move their bytes underneath them.
// Callers must not run in parallel: the seam is a package global.
func useIsolatedSummonRegistry(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kingdom", defaultSummonRegistryFileName)
	original := summonRegistryPathFunc
	summonRegistryPathFunc = func() (string, error) { return path, nil }
	t.Cleanup(func() { summonRegistryPathFunc = original })
	return path
}

func readRegistryInstances(t *testing.T, path string) []summonInstanceRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	registry := summonInstanceRegistry{}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatalf("registry did not decode: %v (contents: %q)", err, string(raw))
	}
	return registry.Instances
}

func recordForPID(pid int) summonInstanceRecord {
	return summonInstanceRecord{
		PID:         pid,
		ConfigPath:  fmt.Sprintf("/tmp/project-%d/.kingdom/config.yaml", pid),
		KingdomHome: fmt.Sprintf("/tmp/project-%d/.kingdom", pid),
		Listen:      fmt.Sprintf("127.0.0.1:%d", 9000+(pid%1000)),
		BasePath:    "/api/v1",
		StartedAt:   "2026-08-19T10:00:00Z",
	}
}

func TestSummonRegistrySurvivesConcurrentRegistration(t *testing.T) {
	path := useIsolatedSummonRegistry(t)

	// Every writer is this live process, so no entry can be pruned as dead and
	// the final registry must hold exactly the survivors.
	self := os.Getpid()
	const writers = 24
	var wg sync.WaitGroup
	errs := make(chan error, writers*2)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			record := recordForPID(self)
			record.Listen = fmt.Sprintf("127.0.0.1:%d", 9000+i)
			if err := registerSummonInstance(record); err != nil {
				errs <- fmt.Errorf("register %d: %w", i, err)
				return
			}
			// A read racing every write must never observe a half-written or
			// blended file.
			if _, err := listActiveSummonInstances(); err != nil {
				errs <- fmt.Errorf("list %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent registry access failed: %v", err)
	}

	instances := readRegistryInstances(t, path)
	if len(instances) != 1 {
		t.Fatalf("expected one record for a single pid, got %d: %#v", len(instances), instances)
	}
	if instances[0].PID != self {
		t.Fatalf("expected pid %d, got %d", self, instances[0].PID)
	}
}

func TestSummonRegistryKeepsEveryLiveInstanceUnderConcurrency(t *testing.T) {
	path := useIsolatedSummonRegistry(t)

	// Distinct live PIDs, so a lost update shows up as a missing record rather
	// than as corruption. Without the lock the read-modify-write races and
	// writers overwrite each other.
	const writers = 12
	processes := make([]*os.Process, 0, writers)
	for i := 0; i < writers; i++ {
		process := startSleepingProcess(t)
		processes = append(processes, process)
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for _, process := range processes {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			if err := registerSummonInstance(recordForPID(pid)); err != nil {
				errs <- err
			}
		}(process.Pid)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("register failed: %v", err)
	}

	instances := readRegistryInstances(t, path)
	if len(instances) != writers {
		t.Fatalf("expected %d records to survive concurrent registration, got %d", writers, len(instances))
	}
}

func TestSummonRegistryRebuildsUnreadableFile(t *testing.T) {
	path := useIsolatedSummonRegistry(t)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create registry dir: %v", err)
	}
	// The exact damage two unsynchronized in-place writes used to produce: one
	// document truncated by the tail of another.
	corrupt := `{
  "instances": []
}{
      "pid": 11200,
      "kingdom_home": "/var/folders/tmp/.kingdom"
    }
  ]
}`
	if err := os.WriteFile(path, []byte(corrupt), 0o600); err != nil {
		t.Fatalf("seed corrupt registry: %v", err)
	}

	// Reading must not fail: `kd summon status` and CLI address discovery run
	// through this path and would otherwise be dead until someone deleted the
	// file by hand.
	instances, err := listActiveSummonInstances()
	if err != nil {
		t.Fatalf("expected a damaged registry to read as empty, got %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("expected no instances from a damaged registry, got %#v", instances)
	}

	self := os.Getpid()
	if err := registerSummonInstance(recordForPID(self)); err != nil {
		t.Fatalf("expected registration to repair a damaged registry, got %v", err)
	}

	repaired := readRegistryInstances(t, path)
	if len(repaired) != 1 || repaired[0].PID != self {
		t.Fatalf("expected the registry to be rebuilt around the live summon, got %#v", repaired)
	}
}

func TestSummonRegistryReadsDoNotWrite(t *testing.T) {
	path := useIsolatedSummonRegistry(t)

	process := startSleepingProcess(t)
	if err := registerSummonInstance(recordForPID(process.Pid)); err != nil {
		t.Fatalf("register summon instance: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat registry: %v", err)
	}

	// A stale entry is dropped from the answer, but a command that only asked a
	// question must not queue behind a lock or rewrite the file to do it.
	if err := process.Kill(); err != nil {
		t.Fatalf("kill process: %v", err)
	}
	if _, err := process.Wait(); err != nil {
		t.Fatalf("reap process: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	instances, err := listActiveSummonInstances()
	if err != nil {
		t.Fatalf("list active summon instances: %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("expected a dead pid to be filtered out of the answer, got %#v", instances)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat registry after read: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatalf("expected a read to leave the registry untouched, mtime %v -> %v, size %d -> %d",
			before.ModTime(), after.ModTime(), before.Size(), after.Size())
	}
}

func TestSummonRegistryDropsDeadInstancesOnNextWrite(t *testing.T) {
	path := useIsolatedSummonRegistry(t)

	dead := startSleepingProcess(t)
	if err := registerSummonInstance(recordForPID(dead.Pid)); err != nil {
		t.Fatalf("register dead-to-be instance: %v", err)
	}
	if err := dead.Kill(); err != nil {
		t.Fatalf("kill process: %v", err)
	}
	if _, err := dead.Wait(); err != nil {
		t.Fatalf("reap process: %v", err)
	}

	self := os.Getpid()
	if err := registerSummonInstance(recordForPID(self)); err != nil {
		t.Fatalf("register live instance: %v", err)
	}

	instances := readRegistryInstances(t, path)
	if len(instances) != 1 || instances[0].PID != self {
		t.Fatalf("expected the dead record to be pruned by the next write, got %#v", instances)
	}
}

func TestSummonRegistryTestsNeverTargetTheRealCacheDirectory(t *testing.T) {
	// The guarantee TestMain exists to provide. If this fails, some test can
	// reach the operator's own registry.
	path, err := summonRegistryPathFunc()
	if err != nil {
		t.Fatalf("resolve registry path: %v", err)
	}
	realCacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("user cache dir unavailable: %v", err)
	}
	if strings.HasPrefix(path, filepath.Join(realCacheDir, "kingdom")) {
		t.Fatalf("tests resolve the real summon registry at %q", path)
	}
}

func startSleepingProcess(t *testing.T) *os.Process {
	t.Helper()
	process, err := os.StartProcess("/bin/sleep", []string{"sleep", "60"}, &os.ProcAttr{})
	if err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	t.Cleanup(func() {
		_ = process.Kill()
		_, _ = process.Wait()
	})
	return process
}
