package processguardian

import (
	"reflect"
	"testing"
)

func TestNormalizeWindowsEnvironmentPreservesPerDriveDirectories(t *testing.T) {
	got := normalizeWindowsEnvironment([]string{
		"=C:=C:\\Kingdom",
		"Path=C:\\Windows",
		"=D:=D:\\Projects",
		"PATH=D:\\Tools",
	})
	want := []string{
		"=C:=C:\\Kingdom",
		"=D:=D:\\Projects",
		"PATH=D:\\Tools",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized Windows environment=%q, want %q", got, want)
	}
}
