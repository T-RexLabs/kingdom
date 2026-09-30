package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain keeps this package's tests out of the operator's real cache
// directory.
//
// The summon registry lives under `os.UserCacheDir()` because it tracks summons
// across every project on the machine. Tests that start a summon, stop one, or
// merely ask for its status all reach it, and several of them run in parallel —
// so a test run alongside a real `kd summon` used to interleave writes with it,
// corrupt the shared file, and take unrelated tests down with it. A test that
// registered and then unregistered its own record could even delete the file
// outright.
//
// Redirecting the seam once, here, is what makes that impossible: an individual
// test cannot reintroduce the hazard by forgetting to opt out of it.
func TestMain(m *testing.M) {
	os.Exit(runPackageTests(m))
}

func runPackageTests(m *testing.M) int {
	// The marked child runs only TestCLIProcessHelper and is forcibly terminated
	// by its parent. It does not access the summon registry, so avoid creating a
	// temporary registry root that would be left behind when the process is
	// killed.
	if isCLIProcessHelperInvocation() {
		return m.Run()
	}

	root, err := os.MkdirTemp("", "kingdom-cli-tests-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create test cache root: %v\n", err)
		return 1
	}
	defer os.RemoveAll(root)

	summonRegistryPathFunc = func() (string, error) {
		return filepath.Join(root, "kingdom", defaultSummonRegistryFileName), nil
	}
	return m.Run()
}
