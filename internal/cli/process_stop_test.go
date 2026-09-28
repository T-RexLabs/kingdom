package cli

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSummonStopRequestSignalsGracefulShutdown(t *testing.T) {
	stopRequestPath := filepath.Join(t.TempDir(), defaultSummonStopRequestFileName)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	stopControlToken, err := generateSummonStopControlToken()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run="+cliTestProcessHelperRun, "-test.timeout=0")
	command.Env = cliTestProcessHelperEnvironmentForMode(cliTestProcessHelperStop, stopRequestPath, stopControlToken)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("start graceful-stop helper: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	reader := bufio.NewReader(stdout)
	lines := make(chan string, 4)
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			lines <- line
		}
	}()
	if line, err := awaitSummonStopHelperLine(lines, 5*time.Second); err != nil || line != "ready\n" {
		t.Fatalf("helper readiness line=%q err=%v", line, err)
	}
	if err := writeSummonStopRequest(stopRequestPath, stopControlToken+"-previous-generation"); err != nil {
		t.Fatalf("write stale-generation stop request: %v", err)
	}
	select {
	case line := <-lines:
		t.Fatalf("stale-generation stop request triggered helper output %q", line)
	case <-time.After(250 * time.Millisecond):
	}
	if err := writeSummonStopRequest(stopRequestPath, stopControlToken); err != nil {
		t.Fatalf("write summon stop request: %v", err)
	}
	if line, err := awaitSummonStopHelperLine(lines, 5*time.Second); err != nil || line != "graceful\n" {
		t.Fatalf("helper shutdown line=%q err=%v, want graceful completion", line, err)
	}
	if err := writeSummonStopRequest(stopRequestPath, stopControlToken); err != nil {
		t.Fatalf("write graceful shutdown escalation request: %v", err)
	}
	if line, err := awaitSummonStopHelperLine(lines, 5*time.Second); err != nil || line != "forced\n" {
		t.Fatalf("helper escalation line=%q err=%v, want second graceful-stop request", line, err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for gracefully stopped helper: %v", err)
	}
}

func TestSummonStopRequestCancelsStartupBeforeReadiness(t *testing.T) {
	stopRequestPath := filepath.Join(t.TempDir(), defaultSummonStopRequestFileName)
	stopControlToken, err := generateSummonStopControlToken()
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run="+cliTestProcessHelperRun, "-test.timeout=0")
	command.Env = cliTestProcessHelperEnvironmentForMode(cliTestProcessHelperStartupStop, stopRequestPath, stopControlToken)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("start startup-stop helper: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	lines := make(chan string, 2)
	go func() {
		reader := bufio.NewReader(stdout)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			lines <- line
		}
	}()
	if line, err := awaitSummonStopHelperLine(lines, 5*time.Second); err != nil || line != "starting\n" {
		t.Fatalf("startup helper line=%q err=%v, want startup before readiness", line, err)
	}
	if err := writeSummonStopRequest(stopRequestPath, stopControlToken); err != nil {
		t.Fatalf("write startup stop request: %v", err)
	}
	if line, err := awaitSummonStopHelperLine(lines, 5*time.Second); err != nil || line != "stopped\n" {
		t.Fatalf("startup helper line=%q err=%v, want prompt startup cancellation", line, err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait for startup-stop helper: %v", err)
	}
}

func awaitSummonStopHelperLine(lines <-chan string, timeout time.Duration) (string, error) {
	select {
	case line := <-lines:
		return line, nil
	case <-time.After(timeout):
		return "", os.ErrDeadlineExceeded
	}
}

func TestConsumeSummonStopRequestRequiresExactControlToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), defaultSummonStopRequestFileName)
	if err := writeSummonStopRequest(path, "previous-process-generation"); err != nil {
		t.Fatalf("write stop request: %v", err)
	}
	if consumed, err := consumeSummonStopRequest(path, "replacement-process-generation"); err != nil || consumed {
		t.Fatalf("replacement consumed prior-generation stop request: consumed=%t err=%v", consumed, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("mismatched stop request was removed: %v", err)
	}
	if consumed, err := consumeSummonStopRequest(path, "previous-process-generation"); err != nil || !consumed {
		t.Fatalf("own stop request consumed=%t err=%v, want consumed", consumed, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("consumed stop request remains on disk: %v", err)
	}
}
