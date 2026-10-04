//go:build linux

package processguardian

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func newLinuxProcFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "sys/kernel/random")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "boot_id"), []byte("boot-id\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func writeLinuxProcFixture(t *testing.T, root string, pid, group int, state string, threads int, ticks uint64, instance string) {
	t.Helper()
	path := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(path, "fd"), 0700); err != nil {
		t.Fatal(err)
	}
	fields := []string{state, "1", strconv.Itoa(group), "1", "0", "-1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "20", "0", strconv.Itoa(threads), "0", strconv.FormatUint(ticks, 10)}
	if err := os.WriteFile(filepath.Join(path, "stat"), []byte(fmt.Sprintf("%d (worker (name)) %s", pid, strings.Join(fields, " "))), 0600); err != nil {
		t.Fatal(err)
	}
	env := ""
	if instance != "" {
		env = processIdentityEnvironmentName + "=" + instance + "\x00"
	}
	if err := os.WriteFile(filepath.Join(path, "environ"), []byte(env), 0600); err != nil {
		t.Fatal(err)
	}
}
func linuxFixtureOwnership(identity string) Ownership {
	return Ownership{TreeID: 123, ChildPID: 123, TreeIdentityToken: identity, ChildIdentityToken: identity}
}
func TestLinuxZombieGroupPreservesLiveThreads(t *testing.T) {
	for _, state := range []string{"Z", "X"} {
		t.Run(state, func(t *testing.T) {
			root := newLinuxProcFixture(t)
			writeLinuxProcFixture(t, root, 123, 123, state, 2, 42, "")
			absent, err := linuxProcessGroupQuiescentAt(root, 123)
			if err != nil || absent {
				t.Fatalf("leader with executable threads absent=%t err=%v", absent, err)
			}
			// The generic identity comparator must receive an inspection error,
			// rather than a shortened token that could look like PID reuse.
			if snapshot, err := linuxProcessIdentitySnapshotAt(root, 123); err == nil || snapshot.identity != "" || processIdentityNotFound(err) {
				t.Fatalf("threaded zombie identity became a clean mismatch: snapshot=%+v err=%v", snapshot, err)
			}
		})
	}
}
func TestLinuxProcInspectionFailuresCannotProveAbsence(t *testing.T) {
	for _, failure := range []string{"malformed-stat", "zero-threads", "unreadable-stat", "unreadable-environ", "unreadable-fd", "empty-boot", "zero-start"} {
		t.Run(failure, func(t *testing.T) {
			root := newLinuxProcFixture(t)
			writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "expected")
			path := filepath.Join(root, "123")
			switch failure {
			case "malformed-stat":
				os.WriteFile(filepath.Join(path, "stat"), []byte("malformed"), 0600)
			case "zero-threads":
				writeLinuxProcFixture(t, root, 123, 123, "Z", 0, 42, "")
			case "unreadable-stat":
				os.Remove(filepath.Join(path, "stat"))
				os.Mkdir(filepath.Join(path, "stat"), 0700)
			case "unreadable-environ":
				os.Remove(filepath.Join(path, "environ"))
				os.Mkdir(filepath.Join(path, "environ"), 0700)
			case "unreadable-fd":
				os.Remove(filepath.Join(path, "fd"))
				os.WriteFile(filepath.Join(path, "fd"), []byte("invalid"), 0600)
			case "empty-boot":
				os.WriteFile(filepath.Join(root, "sys/kernel/random/boot_id"), nil, 0600)
			case "zero-start":
				writeLinuxProcFixture(t, root, 123, 123, "S", 1, 0, "expected")
			}
			absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
			if absent || err == nil {
				t.Fatalf("inspection failure became proof: absent=%t err=%v", absent, err)
			}
		})
	}
}
func TestLinuxTerminatedLeaderRequiresDescendantGenerationBeforeSignal(t *testing.T) {
	for _, leader := range []string{"zombie", "missing"} {
		for _, marker := range []string{"expected", "replacement", "expected-extra", ""} {
			t.Run(leader+"/"+marker, func(t *testing.T) {
				root := newLinuxProcFixture(t)
				if leader == "zombie" {
					writeLinuxProcFixture(t, root, 123, 123, "Z", 1, 42, "")
				}
				writeLinuxProcFixture(t, root, 124, 123, "S", 1, 44, marker)
				absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
				if absent || ((err == nil) != (marker == "expected")) {
					t.Fatalf("descendant marker=%q absent=%t err=%v", marker, absent, err)
				}
			})
		}
	}
}
func TestLinuxLiveMissingGenerationMarkerRemainsUnproven(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "")
	absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
	if absent || err == nil {
		t.Fatalf("missing live marker absent=%t err=%v", absent, err)
	}
}
func TestLinuxZombieGenerationUsesDelimitedBase(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "Z", 1, 42, "")
	writeLinuxProcFixture(t, root, 124, 123, "S", 1, 44, "replacement")
	absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:420:instance:expected"))
	if !absent || err != nil {
		t.Fatalf("different start ticks absent=%t err=%v", absent, err)
	}
}
func TestLinuxZombieOnlyMissingMarkersProveQuiescence(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "Z", 1, 42, "")
	if err := os.Remove(filepath.Join(root, "123", "environ")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "123", "fd")); err != nil {
		t.Fatal(err)
	}
	absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
	if !absent || err != nil {
		t.Fatalf("zombie-only group absent=%t err=%v", absent, err)
	}
}

func TestLinuxSnapshotPinsProcessDirectoryAcrossSameTickReplacement(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "original")
	originalFD := strings.Repeat("a", 32)
	replacementFD := strings.Repeat("b", 32)
	if err := os.Symlink("/tmp/"+linuxProcessIdentityFilePrefix+originalFD, filepath.Join(root, "123", "fd", "7")); err != nil {
		t.Fatal(err)
	}
	process, err := os.OpenRoot(filepath.Join(root, "123"))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	// A renamed fixture retains its files; a dead real proc PID object usually
	// returns ESRCH instead. Both must prevent reads from its replacement.
	if err := os.Rename(filepath.Join(root, "123"), filepath.Join(root, "retired")); err != nil {
		t.Fatal(err)
	}
	writeLinuxProcFixture(t, root, 123, 123, "D", 1, 42, "replacement")
	if err := os.Symlink("/tmp/"+linuxProcessIdentityFilePrefix+replacementFD, filepath.Join(root, "123", "fd", "7")); err != nil {
		t.Fatal(err)
	}
	pinned, err := linuxProcessIdentitySnapshotFromRoot("boot-id", process, 123, false)
	if err != nil || pinned.identity != "boot-id:42:instance:original:fd:"+originalFD || pinned.stat.state != 'S' {
		t.Fatalf("mixed generation snapshot=%+v err=%v", pinned, err)
	}
	current, err := linuxProcessIdentitySnapshotAt(root, 123)
	if err != nil || current.identity != "boot-id:42:instance:replacement:fd:"+replacementFD || current.stat.state != 'D' {
		t.Fatalf("replacement snapshot=%+v err=%v", current, err)
	}
	absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership(pinned.identity))
	if !absent || err != nil {
		t.Fatalf("replacement authorized stale group signaling: absent=%t err=%v", absent, err)
	}
	if err := os.Remove(filepath.Join(root, "retired", "stat")); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := linuxProcessIdentitySnapshotFromRoot("boot-id", process, 123, false); err == nil || snapshot.identity != "" || !processIdentityNotFound(err) {
		t.Fatalf("vanished pinned process inspected replacement: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestLinuxQuiescenceRescansMemberBornAfterDirectoryListing(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "original")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	// Model a fork after listing followed by the listed parent's exit.
	writeLinuxProcFixture(t, root, 124, 123, "S", 1, 44, "original")
	writeLinuxProcFixture(t, root, 123, 123, "Z", 1, 42, "")
	calls := 0
	absent, _, err := linuxStableProcessGroupInspection(func() (bool, []string, error) {
		calls++
		if calls == 1 {
			return linuxScanProcessGroup(root, 123, entries)
		}
		fresh, err := os.ReadDir(root)
		if err != nil {
			return false, nil, err
		}
		return linuxScanProcessGroup(root, 123, fresh)
	})
	if absent || err != nil || calls != 2 {
		t.Fatalf("unlisted live descendant became quiescent: absent=%t scans=%d err=%v", absent, calls, err)
	}
}

func TestLinuxChangingZombieMembershipRemainsUnproven(t *testing.T) {
	calls := 0
	absent, _, err := linuxStableProcessGroupInspection(func() (bool, []string, error) {
		calls++
		if calls == 1 {
			return true, []string{"123:42"}, nil
		}
		return true, []string{"123:42", "124:44"}, nil
	})
	if absent || err == nil {
		t.Fatalf("changing membership became proof: absent=%t err=%v", absent, err)
	}
}

func TestLinuxQuiescenceRejectsListedMemberThatForksAndDisappears(t *testing.T) {
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "original")
	first, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	writeLinuxProcFixture(t, root, 124, 123, "S", 1, 44, "original")
	writeLinuxProcFixture(t, root, 123, 123, "Z", 1, 42, "")
	second, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	absent, _, err := linuxStableProcessGroupInspection(func() (bool, []string, error) {
		calls++
		if calls == 1 {
			return linuxScanProcessGroup(root, 123, first)
		}
		// The newly listed member forks, exits, and is reaped before its stat read.
		writeLinuxProcFixture(t, root, 125, 123, "S", 1, 46, "original")
		if err := os.RemoveAll(filepath.Join(root, "124")); err != nil {
			t.Fatal(err)
		}
		return linuxScanProcessGroup(root, 123, second)
	})
	if absent || err == nil {
		t.Fatalf("vanished member hid a live child: absent=%t scans=%d err=%v", absent, calls, err)
	}
}
