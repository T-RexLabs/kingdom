package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/processguardian"
)

const (
	cliTestProcessHelperEnv          = "KINGDOM_CLI_TEST_PROCESS_HELPER"
	cliTestProcessHelperStopPathEnv  = "KINGDOM_CLI_TEST_STOP_REQUEST_PATH"
	cliTestProcessHelperStopTokenEnv = "KINGDOM_CLI_TEST_STOP_CONTROL_TOKEN"
	cliTestProcessHelperWait         = "wait"
	cliTestProcessHelperStop         = "graceful-stop"
	cliTestProcessHelperStartupStop  = "startup-stop"
	cliTestProcessHelperRun          = "^TestCLIProcessHelper$"
)

// TestCLIProcessHelper is selected explicitly by startCLIProcessHelper. The
// environment marker keeps its normal package-test invocation as a no-op.
func TestCLIProcessHelper(t *testing.T) {
	if !isCLIProcessHelperInvocation() {
		return
	}
	signals := make(chan os.Signal, 1)
	stopControlToken := os.Getenv(cliTestProcessHelperStopTokenEnv)
	cleanup, err := listenForSummonStop(signals, os.Getenv(cliTestProcessHelperStopPathEnv), stopControlToken)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if os.Getenv(cliTestProcessHelperEnv) == cliTestProcessHelperStartupStop {
		startupCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, finishStartupWatch := watchSummonStartupStop(signals, cancel)
		defer finishStartupWatch()
		if _, err := io.WriteString(os.Stdout, "starting\n"); err != nil {
			t.Fatal(err)
		}
		<-startupCtx.Done()
		if !finishStartupWatch() {
			t.Fatal("startup stop watcher did not observe the stop request")
		}
		if _, err := io.WriteString(os.Stdout, "stopped\n"); err != nil {
			t.Fatal(err)
		}
		return
	}
	if os.Getenv(cliTestProcessHelperEnv) == cliTestProcessHelperStop {
		if _, err := io.WriteString(os.Stdout, "ready\n"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-signals:
			if _, err := io.WriteString(os.Stdout, "graceful\n"); err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("graceful stop control was not received")
		}
		select {
		case <-signals:
			if _, err := io.WriteString(os.Stdout, "forced\n"); err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("forced stop control was not received")
		}
		return
	}
	for {
		select {
		case <-signals:
			return
		case <-time.After(time.Hour):
		}
	}
}

func isCLIProcessHelperInvocation() bool {
	mode := os.Getenv(cliTestProcessHelperEnv)
	if mode != cliTestProcessHelperWait && mode != cliTestProcessHelperStop && mode != cliTestProcessHelperStartupStop {
		return false
	}
	for _, arg := range os.Args[1:] {
		if arg == "-test.run="+cliTestProcessHelperRun {
			return true
		}
	}
	return false
}

type cliTestProcess struct {
	cmd                  *exec.Cmd
	done                 chan struct{}
	waitErr              error
	stopRequestPath      string
	processIdentityToken string
	stopControlToken     string
}

func startCLIProcessHelper(t *testing.T, stopRequestPaths ...string) *cliTestProcess {
	t.Helper()
	if len(stopRequestPaths) > 1 {
		t.Fatal("at most one stop request path may be supplied")
	}
	stopRequestPath := ""
	if len(stopRequestPaths) == 1 {
		stopRequestPath = stopRequestPaths[0]
	}
	if stopRequestPath == "" {
		stopRequestPath = filepath.Join(t.TempDir(), defaultSummonStopRequestFileName)
	}
	stopControlToken, err := generateSummonStopControlToken()
	if err != nil {
		t.Fatalf("generate test helper stop control token: %v", err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}

	cmd := exec.Command(executable, "-test.run="+cliTestProcessHelperRun, "-test.timeout=0")
	cmd.Env = cliTestProcessHelperEnvironmentForMode(cliTestProcessHelperWait, stopRequestPath, stopControlToken)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start test helper process: %v", err)
	}
	identityToken, err := processguardian.ProcessIdentityToken(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("identify test helper process: %v", err)
	}

	process := &cliTestProcess{cmd: cmd, done: make(chan struct{}), stopRequestPath: stopRequestPath, processIdentityToken: identityToken, stopControlToken: stopControlToken}
	go func() {
		process.waitErr = cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.killAndWait(t)
	})
	return process
}

func cliTestProcessHelperEnvironmentForMode(mode, stopRequestPath, stopControlToken string) []string {
	environment := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.EqualFold(name, cliTestProcessHelperEnv) || strings.EqualFold(name, cliTestProcessHelperStopPathEnv) || strings.EqualFold(name, cliTestProcessHelperStopTokenEnv) {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, cliTestProcessHelperEnv+"="+mode)
	environment = append(environment, cliTestProcessHelperStopPathEnv+"="+stopRequestPath)
	return append(environment, cliTestProcessHelperStopTokenEnv+"="+stopControlToken)
}

func (p *cliTestProcess) PID() int {
	return p.cmd.Process.Pid
}

func (p *cliTestProcess) Done() <-chan struct{} {
	return p.done
}

func (p *cliTestProcess) Kill() error {
	return p.cmd.Process.Kill()
}

func (p *cliTestProcess) Wait() error {
	<-p.done
	return p.waitErr
}

func (p *cliTestProcess) killAndWait(t *testing.T) {
	t.Helper()
	if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("kill test helper process: %v", err)
	}
	_ = p.Wait()
}
