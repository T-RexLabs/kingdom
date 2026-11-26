package opencode

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/processguardian"
)

func newTestBridge(t *testing.T, bound *boundRun) (*commandBridge, *[]int) {
	t.Helper()
	runner, err := processguardian.NewRunner(processguardian.Config{RuntimeRoot: t.TempDir(), CancelGrace: 100 * time.Millisecond, CancelForce: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	bridge := newCommandBridge(bound.authority, localExecutor{runner: runner}, os.Environ())
	req := ExecuteRequest{}
	ordinals := bound.guardianRecorder(&req, bound.attempt, bound.invoke)
	if err := bridge.register(bound.identity, &commandRegistration{workspace: bound.workspace, ownership: req.OnCommandGuardianOwnership, quiescence: req.OnCommandGuardianQuiescence}); err != nil {
		t.Fatal(err)
	}
	return bridge, ordinals
}

func shellCall(bound *boundRun, callID, command string) commandInput {
	input, _ := json.Marshal(map[string]any{"command": command, "timeout": 20000})
	return commandInput{Identity: bound.identity, MessageID: "msg_assistant", CallID: callID, Command: command, Cwd: bound.workspace, Timeout: 20000, ToolInput: input}
}

func TestCommandRunsGuardedAndIsClaimedOnce(t *testing.T) {
	bound := newBoundRun(t)
	bridge, ordinals := newTestBridge(t, bound)
	call := shellCall(bound, "call-1", "echo ran >> runs.txt; echo output")
	first, err := bridge.invoke(context.Background(), call)
	if err != nil || strings.TrimSpace(first.Output) != "output" || first.Exit != 0 || !first.Quiescent || first.GuardID == "" {
		t.Fatalf("result %#v error %v", first, err)
	}
	replay, err := bridge.invoke(context.Background(), call)
	if err != nil || replay != first {
		t.Fatalf("replay %#v error %v", replay, err)
	}
	if raw, _ := os.ReadFile(filepath.Join(bound.workspace, "runs.txt")); string(raw) != "ran\n" {
		t.Fatalf("a replayed tool call ran the command again: %q", raw)
	}
	if len(*ordinals) != 1 {
		t.Fatalf("guardian ordinals %v", *ordinals)
	}
	var completed int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls WHERE guard_id=? AND completed_at IS NOT NULL`, first.GuardID).Scan(&completed); err != nil || completed != 1 {
		t.Fatalf("claim completion %d %v", completed, err)
	}
}

func TestCommandRunsInItsRequestedWorkspaceDirectory(t *testing.T) {
	for _, spelling := range []string{"relative", "absolute", "symlink"} {
		t.Run(spelling, func(t *testing.T) {
			bound := newBoundRun(t)
			bridge, _ := newTestBridge(t, bound)
			directory := filepath.Join(bound.workspace, "packages", "app")
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			workdir := "packages/app"
			if spelling == "absolute" {
				workdir = directory
			}
			if spelling == "symlink" {
				workdir = "app-link"
				if err := os.Symlink(directory, filepath.Join(bound.workspace, workdir)); err != nil {
					t.Fatal(err)
				}
			}
			call := shellCall(bound, "call-nested", "pwd; echo ran >> runs.txt")
			call.Cwd = directory
			call.ToolInput, _ = json.Marshal(map[string]any{"command": call.Command, "timeout": call.Timeout, "workdir": workdir})
			result, err := bridge.invoke(context.Background(), call)
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := filepath.EvalSymlinks(directory)
			if err != nil || strings.TrimSpace(result.Output) != canonical {
				t.Fatalf("cwd=%q want=%q err=%v", result.Output, canonical, err)
			}
			if _, err := bridge.invoke(context.Background(), call); err != nil {
				t.Fatal(err)
			}
			if raw, err := os.ReadFile(filepath.Join(directory, "runs.txt")); err != nil || string(raw) != "ran\n" {
				t.Fatalf("nested command replay: %q %v", raw, err)
			}
			if _, err := os.Stat(filepath.Join(bound.workspace, "runs.txt")); !os.IsNotExist(err) {
				t.Fatalf("command ran in root: %v", err)
			}
		})
	}
}

func TestCommandRejectsInvalidWorkspaceDirectoriesBeforeClaiming(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(bound.workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bound.workspace, "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, workdir := range []string{"missing", "file", "escape", "..", external} {
		t.Run(workdir, func(t *testing.T) {
			call := shellCall(bound, "call-invalid", "true")
			call.Cwd = filepath.Join(bound.workspace, workdir)
			if filepath.IsAbs(workdir) {
				call.Cwd = workdir
			}
			call.ToolInput, _ = json.Marshal(map[string]any{"command": call.Command, "timeout": call.Timeout, "workdir": workdir})
			if _, err := bridge.invoke(context.Background(), call); err == nil {
				t.Fatal("invalid directory accepted")
			}
		})
	}
	var claims int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("refused directories left claims: %d %v", claims, err)
	}
}

func TestCommandKeepsExitStatusAndBoundsOutput(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	result, err := bridge.invoke(context.Background(), shellCall(bound, "call-1", "head -c 200000 /dev/zero | tr '\\000' x; exit 3"))
	if err != nil || result.Exit != 3 || !result.Truncated || len(result.Output) != commandOutputLimit {
		t.Fatalf("exit=%d truncated=%v output=%d error=%v", result.Exit, result.Truncated, len(result.Output), err)
	}
}

func TestCommandIsRefusedBeforeAnyClaimWhenItIsNotTheToolCall(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	elsewhere := shellCall(bound, "call-1", "true")
	elsewhere.Cwd = filepath.Dir(bound.workspace)
	background := shellCall(bound, "call-2", "true")
	background.ToolInput = json.RawMessage(`{"command":"true","timeout":20000,"background":true}`)
	different := shellCall(bound, "call-3", "true")
	different.ToolInput = json.RawMessage(`{"command":"false","timeout":20000}`)
	timeout := shellCall(bound, "call-4", "true")
	timeout.Timeout = 5
	foreign := shellCall(bound, "call-5", "true")
	foreign.Identity.RequestID = "msg_other"
	if err := os.Mkdir(filepath.Join(bound.workspace, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	differentDirectory := shellCall(bound, "call-6", "true")
	differentDirectory.Cwd = filepath.Join(bound.workspace, "nested")
	for name, call := range map[string]commandInput{"cwd": elsewhere, "background": background, "other command": different, "other timeout": timeout, "unregistered": foreign, "other directory": differentDirectory} {
		t.Run(name, func(t *testing.T) {
			if _, err := bridge.invoke(context.Background(), call); err == nil {
				t.Fatal("accepted a command that is not its tool call")
			}
		})
	}
	var claims int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("refused commands left claims: %d %v", claims, err)
	}
}

func TestCommandCannotStartAfterAdmissionCloses(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	if err := bridge.cancel(context.Background(), bound.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.invoke(context.Background(), shellCall(bound, "call-1", "touch late.txt")); err == nil {
		t.Fatal("a command started after cancellation")
	}
	if _, err := os.Stat(filepath.Join(bound.workspace, "late.txt")); err == nil {
		t.Fatal("a refused command had an effect")
	}
}

func TestCommandCancellationStopsTheTreeAndClosesTheClaim(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	finished := make(chan error, 1)
	go func() {
		_, err := bridge.invoke(context.Background(), shellCall(bound, "call-1", "sleep 30; touch survived.txt"))
		finished <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var claims int
		if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("command was never claimed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := bridge.cancel(context.Background(), bound.identity); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the invoking call never returned")
	}
	var open int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls WHERE completed_at IS NULL AND aborted_at IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatalf("cancelled command left an unresolved claim: %d %v", open, err)
	}
	if _, err := os.Stat(filepath.Join(bound.workspace, "survived.txt")); err == nil {
		t.Fatal("the command continued after cancellation")
	}
}

func TestCommandWithUnknownOutcomeAfterRestartIsNotRunAgain(t *testing.T) {
	bound := newBoundRun(t)
	call := shellCall(bound, "call-1", "echo ran >> runs.txt")
	first, _ := newTestBridge(t, bound)
	if _, err := first.invoke(context.Background(), call); err != nil {
		t.Fatal(err)
	}
	restarted, _ := newTestBridge(t, bound)
	if _, err := restarted.invoke(context.Background(), call); err == nil {
		t.Fatal("a command from before a restart returned a result or ran again")
	}
	if raw, _ := os.ReadFile(filepath.Join(bound.workspace, "runs.txt")); string(raw) != "ran\n" {
		t.Fatalf("command ran again after restart: %q", raw)
	}
}

func TestCommandEnvironmentWithholdsKingdomServerSettings(t *testing.T) {
	env := commandEnvironment([]string{"PATH=/bin", "OPENAI_API_KEY=provider", "OPENCODE_PASSWORD=secret", "OPENCODE_CONFIG_CONTENT={}", "KINGDOM_AUTHORITY_TOKEN=token"})
	if strings.Join(env, ",") != "PATH=/bin,OPENAI_API_KEY=provider" {
		t.Fatalf("environment %v", env)
	}
}

func TestCommandRefusedAtGuardianAdmissionNeverRanAndClosesItsClaim(t *testing.T) {
	bound := newBoundRun(t)
	bridge, _ := newTestBridge(t, bound)
	// Cancellation closes admission in the database before the guardian's
	// ownership is acknowledged, so the acknowledgement is refused.
	bridge.registrations[bound.identity].ownership = func(ctx context.Context, _ CommandGuardianOwnership) error {
		if err := bound.authority.CloseAdmission(ctx, bound.identity); err != nil {
			return err
		}
		return fmt.Errorf("guardian admission closed")
	}
	if _, err := bridge.invoke(context.Background(), shellCall(bound, "call-1", "touch ran.txt")); err == nil {
		t.Fatal("a refused command reported success")
	}
	if _, err := os.Stat(filepath.Join(bound.workspace, "ran.txt")); err == nil {
		t.Fatal("a command ran although its guardian was refused")
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), bound.db, "binding-"+bound.invoke); err != nil {
		t.Fatalf("the unlaunched claim stayed open: %v", err)
	}
}
