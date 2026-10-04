//go:build linux

package processguardian

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLinuxMarkerPermissionClassificationPreservesOtherFailures(t *testing.T) {
	valid := []byte(processIdentityEnvironmentName + "=expected\x00")
	malformed := []byte(processIdentityEnvironmentName + "=first\x00" + processIdentityEnvironmentName + "=second\x00")
	tests := []struct {
		name                    string
		environment             []byte
		environmentErr, fileErr error
		permission              bool
	}{
		{"environment-eacces", nil, syscall.EACCES, nil, true},
		{"descriptor-eperm", valid, nil, fmt.Errorf("readlink descriptor: %w", syscall.EPERM), true},
		{"both-permission", nil, syscall.EACCES, syscall.EPERM, true},
		{"joined-permissions", valid, nil, errors.Join(syscall.EACCES, syscall.EPERM), true},
		{"permission-and-missing", nil, syscall.EACCES, syscall.ENOENT, false},
		{"permission-and-io", nil, syscall.EACCES, syscall.EIO, false},
		{"joined-permission-and-io", valid, nil, errors.Join(syscall.EACCES, syscall.EIO), false},
		{"permission-and-invalid-descriptor-token", nil, syscall.EACCES, errors.New("invalid process identity file token"), false},
		{"malformed-environment-and-permission", malformed, nil, syscall.EACCES, false},
		{"malformed-environment", malformed, nil, nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance, err := linuxLiveIdentityMarkers(test.environment, test.environmentErr, test.fileErr)
			if err == nil || instance != "" || !errors.Is(err, errLinuxIdentityInspection) || processIdentityNotFound(err) {
				t.Fatalf("marker failure became a clean identity: instance=%q err=%v", instance, err)
			}
			if errors.Is(err, errLinuxIdentityMarkersPermission) != test.permission {
				t.Fatalf("permission category=%t want=%t err=%v", errors.Is(err, errLinuxIdentityMarkersPermission), test.permission, err)
			}
			for _, errno := range []error{syscall.EACCES, syscall.EPERM, syscall.ENOENT, syscall.EIO} {
				original := errors.Is(test.environmentErr, errno) || errors.Is(test.fileErr, errno)
				if errors.Is(err, errno) != original {
					t.Fatalf("original errno %v lost/added: %v", errno, err)
				}
			}
		})
	}
}

func TestLinuxPersistentMarkerPermissionCannotProveAbsence(t *testing.T) {
	_, denied := linuxLiveIdentityMarkers(nil, syscall.EACCES, syscall.EPERM)
	calls := 0
	absent, err := linuxSettleAcknowledgedTreeInspection(func() (bool, error) { calls++; return false, denied })
	if absent || calls < 2 || !errors.Is(err, errLinuxIdentityInspection) || !errors.Is(err, syscall.EACCES) || !errors.Is(err, syscall.EPERM) || processIdentityNotFound(err) {
		t.Fatalf("persistent denial became proof: absent=%t inspections=%d err=%v", absent, calls, err)
	}
}

func TestLinuxMixedMarkerFailureDoesNotRetry(t *testing.T) {
	_, failure := linuxLiveIdentityMarkers(nil, syscall.EACCES, syscall.EIO)
	calls := 0
	absent, err := linuxSettleAcknowledgedTreeInspection(func() (bool, error) { calls++; return false, failure })
	if absent || calls != 1 || !errors.Is(err, syscall.EACCES) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("mixed failure was retried/hidden: absent=%t inspections=%d err=%v", absent, calls, err)
	}
}

func TestLinuxPermissionReinspectionUsesCurrentTreeProof(t *testing.T) {
	tests := []struct {
		name, state, descendant, instance string
		threads                           int
		absent, unproven                  bool
	}{
		{"same-live-generation", "S", "", "expected", 1, false, false},
		{"terminal-only", "Z", "", "", 1, true, false},
		{"terminal-with-owned-live-descendant", "Z", "expected", "", 1, false, false},
		{"terminal-with-unproven-live-descendant", "Z", "replacement", "", 1, false, true},
		{"terminal-with-live-threads", "Z", "", "", 2, false, true},
		{"same-tick-replacement", "S", "", "replacement", 1, true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := newLinuxProcFixture(t)
			writeLinuxProcFixture(t, root, 123, 123, test.state, test.threads, 42, test.instance)
			if test.descendant != "" {
				writeLinuxProcFixture(t, root, 124, 123, "S", 1, 44, test.descendant)
			}
			_, denied := linuxLiveIdentityMarkers(nil, syscall.EACCES, syscall.EPERM)
			calls := 0
			absent, err := linuxSettleAcknowledgedTreeInspection(func() (bool, error) {
				calls++
				if calls == 1 {
					return false, denied
				}
				return linuxAcknowledgedTreeAbsentOnceAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
			})
			if calls < 2 || absent != test.absent || (err != nil) != test.unproven {
				t.Fatalf("fresh proof failed: absent=%t inspections=%d err=%v", absent, calls, err)
			}
		})
	}
}

func TestLinuxPersistentProcMarkerPermissionRemainsInspectionError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires an unprivileged user to enforce fixture file modes")
	}
	root := newLinuxProcFixture(t)
	writeLinuxProcFixture(t, root, 123, 123, "S", 1, 42, "expected")
	path := filepath.Join(root, "123", "environ")
	if err := os.Chmod(path, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0600) })
	snapshot, err := linuxProcessIdentitySnapshotAt(root, 123)
	if snapshot.identity != "" || !errors.Is(err, errLinuxIdentityInspection) || !errors.Is(err, syscall.EACCES) || processIdentityNotFound(err) {
		t.Fatalf("generic identity inspection could become a clean mismatch: snapshot=%+v err=%v", snapshot, err)
	}
	absent, err := linuxAcknowledgedTreeAbsentAt(root, linuxFixtureOwnership("boot-id:42:instance:expected"))
	if absent || !errors.Is(err, errLinuxIdentityInspection) || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("persistent proc denial became absence: absent=%t err=%v", absent, err)
	}
}
