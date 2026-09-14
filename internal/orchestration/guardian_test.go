package orchestration

import (
	"os"
	"testing"

	"kingdom/internal/processguardian"
)

func TestMain(m *testing.M) {
	if exitCode, handled := processguardian.DispatchHiddenMode(os.Args); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}
