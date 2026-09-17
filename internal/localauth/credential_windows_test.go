//go:build windows

package localauth

import (
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/securefs"
)

func TestEnsureCredentialCreatesCurrentUserOnlyACL(t *testing.T) {
	path, err := EnsureCredential(filepath.Join(t.TempDir(), ".kingdom"))
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{filepath.Dir(path), path} {
		file, err := os.Open(candidate)
		if err != nil {
			t.Fatal(err)
		}
		validateErr := securefs.ValidateCurrentUserOnly(file)
		closeErr := file.Close()
		if validateErr != nil {
			t.Fatalf("validate %q: %v", candidate, validateErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}
