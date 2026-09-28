//go:build windows

package processguardian

import (
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsProcessIdentityOnlyTreatsMissingPIDAsAbsent(t *testing.T) {
	if !processIdentityNotFound(windows.ERROR_INVALID_PARAMETER) {
		t.Fatal("invalid process id was not classified as absent")
	}
	if processIdentityNotFound(windows.ERROR_ACCESS_DENIED) {
		t.Fatal("access denied was classified as an absent process")
	}
}

func TestWindowsInspectionAccessDeniedIsNotProofOfAbsence(t *testing.T) {
	if windowsProcessNotFound(windows.ERROR_ACCESS_DENIED) {
		t.Fatal("process access denied was classified as process absence")
	}
	if windowsJobObjectNotFound(windows.ERROR_ACCESS_DENIED) {
		t.Fatal("job access denied was classified as job absence")
	}
	if !windowsJobObjectNotFound(windows.ERROR_FILE_NOT_FOUND) {
		t.Fatal("missing job object was not recognized")
	}
}

func TestWindowsGuardianFallbackDoesNotAcceptInspectionFailureAsQuiescence(t *testing.T) {
	if err := windowsProcessOpenError(windows.ERROR_ACCESS_DENIED); err == nil {
		t.Fatal("access denied while inspecting a provider leader authorized quiescence")
	}
	if err := windowsProcessOpenError(windows.ERROR_INVALID_PARAMETER); err != nil {
		t.Fatalf("missing provider leader should be considered absent: %v", err)
	}
}
