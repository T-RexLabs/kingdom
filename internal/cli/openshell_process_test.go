package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"kingdom/internal/execution/openshell/deployment"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const setupLockHelper = "KINGDOM_TEST_SETUP_LOCK_HELPER"

func TestOpenShellLockProcessHelper(t *testing.T) {
	mode := os.Getenv(setupLockHelper)
	if mode == "" {
		return
	}
	path := os.Getenv("KINGDOM_TEST_SETUP_CONFIG")
	release, e := lockSummonConfiguration(path, mode == "detached-parent")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if mode == "detached-parent" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestOpenShellLockProcessHelper$")
		cmd.Env = append(os.Environ(), setupLockHelper+"=foreground")
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if e = cmd.Run(); e != nil {
			t.Fatal(e)
		}
		return
	}
	fmt.Fprintln(os.Stdout, "runtime lock ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}
func TestConfigureExcludesForegroundOwnerAndDetachedChild(t *testing.T) {
	for _, mode := range []string{"foreground", "detached-parent"} {
		t.Run(mode, func(t *testing.T) {
			o, _ := openShellFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOpenShellLockProcessHelper$")
			cmd.Env = append(os.Environ(), setupLockHelper+"="+mode, "KINGDOM_TEST_SETUP_CONFIG="+o.Config)
			stdin, e := cmd.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			stdout, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			cmd.Stderr = os.Stderr
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func() { _ = stdin.Close(); _ = cmd.Wait() }()
			line, e := bufio.NewReader(stdout).ReadString('\n')
			if e != nil || !strings.Contains(line, "runtime lock ready") {
				t.Fatalf("runtime lock startup failed: %s %v", line, e)
			}
			called := false
			check := func(context.Context, deployment.Bundle) (deployment.CheckResult, error) {
				called = true
				return deployment.CheckResult{}, nil
			}
			if configureOpenShell(context.Background(), io.Discard, o, check) == nil || called {
				t.Fatal("configuration raced a separate runtime process")
			}
			_ = stdin.Close()
			if e = cmd.Wait(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
