//go:build darwin || linux || windows

package processguardian

import (
	"os"
	"testing"
)

func TestProcessIdentityMatchesExactGeneration(t *testing.T) {
	identity, err := ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatalf("identify current process: %v", err)
	}
	if matches, err := ProcessIdentityMatches(os.Getpid(), identity); err != nil || !matches {
		t.Fatalf("current process identity match=%t err=%v, want match", matches, err)
	}
	if matches, err := ProcessIdentityMatches(os.Getpid(), identity+"-old"); err != nil || matches {
		t.Fatalf("stale process identity match=%t err=%v, want mismatch", matches, err)
	}
}

func TestProcessIdentityMatchesAbsentProcess(t *testing.T) {
	if matches, err := ProcessIdentityMatches(1<<30, "previous-process-generation"); err != nil || matches {
		t.Fatalf("absent process identity match=%t err=%v, want clean mismatch", matches, err)
	}
}
