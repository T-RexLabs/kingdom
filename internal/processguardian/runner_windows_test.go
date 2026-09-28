//go:build windows

package processguardian

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if exitCode, handled := runStdinTestProvider(os.Args); handled {
		os.Exit(exitCode)
	}
	if exitCode, handled := DispatchHiddenMode(os.Args); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}
