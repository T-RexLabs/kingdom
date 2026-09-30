package filemode

import "testing"

func TestValidateBasicPermissions(t *testing.T) {
	for _, bits := range []int64{0, 0600, 0700, 0751, 0777} {
		if err := Validate(bits); err != nil {
			t.Fatalf("%04o: %v", bits, err)
		}
	}
	for _, bits := range []int64{-1, 01000, 04000, 0100644} {
		if err := Validate(bits); err == nil {
			t.Fatalf("accepted unsupported mode %o", bits)
		}
	}
}

func TestCompareOnlyHostSupportedPermissions(t *testing.T) {
	if equalForOS(0700, 0755, "linux") || equalForOS(0600, 0644, "darwin") {
		t.Fatal("POSIX comparison ignored basic permission changes")
	}
	if !equalForOS(0751, 0666, "windows") {
		t.Fatal("Windows comparison retained unsupported execute bits")
	}
	if equalForOS(0444, 0644, "windows") {
		t.Fatal("Windows comparison ignored read-only change")
	}
}
