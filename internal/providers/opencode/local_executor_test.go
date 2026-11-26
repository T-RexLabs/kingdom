package opencode

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"kingdom/internal/processguardian"
)

// A valid host path must not bypass a refusal from the selected executor.
type refusingLocalExecutor struct {
	localExecutor
	mu           sync.Mutex
	launches     []processguardian.Command
	workspaces   []string
	directories  []string
	launchErr    error
	workspaceErr error
	directoryErr error
}

func (e *refusingLocalExecutor) Start(_ context.Context, command processguardian.Command, _ processguardian.PersistOwnership) (processguardian.Handle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.launches = append(e.launches, command)
	return nil, e.launchErr
}

func (e *refusingLocalExecutor) ResolveWorkspace(path string) (string, error) {
	e.workspaces = append(e.workspaces, path)
	if e.workspaceErr != nil {
		return "", e.workspaceErr
	}
	return e.localExecutor.ResolveWorkspace(path)
}

func (e *refusingLocalExecutor) ResolveCommandDirectory(workspace, directory string) (string, error) {
	e.directories = append(e.directories, directory)
	if e.directoryErr != nil {
		return "", e.directoryErr
	}
	return e.localExecutor.ResolveCommandDirectory(workspace, directory)
}

func TestServiceAndCommandsUseOneLocalExecutorWithoutFallback(t *testing.T) {
	bound := newBoundRun(t)
	refusal := errors.New("selected local executor refused launch")
	local := &refusingLocalExecutor{launchErr: refusal}
	service, err := NewService(ServiceConfig{
		BinaryPath: "fixture-opencode", WorkDir: bound.workspace,
		RuntimeRoot: t.TempDir(), Env: []string{}, Requests: bound.authority,
		LocalExecutor: local,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.CloseContext(context.Background()) })
	if _, err := service.Client(context.Background()); !errors.Is(err, refusal) {
		t.Fatalf("server did not retain selected executor failure: %v", err)
	}
	req := ExecuteRequest{}
	bound.guardianRecorder(&req, bound.attempt, bound.invoke)
	bridge := service.requests.commands
	if err := bridge.register(bound.identity, &commandRegistration{
		workspace: bound.workspace, ownership: req.OnCommandGuardianOwnership, quiescence: req.OnCommandGuardianQuiescence,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.invoke(context.Background(), shellCall(bound, "selected-executor", "echo unexpected")); !errors.Is(err, refusal) {
		t.Fatalf("shell did not retain selected executor failure: %v", err)
	}
	if len(local.launches) != 2 || local.launches[0].Path != "fixture-opencode" || local.launches[1].Path == "fixture-opencode" {
		t.Fatalf("server and shell did not reach the same executor: %#v", local.launches)
	}
	canonical, err := filepath.EvalSymlinks(bound.workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range local.launches {
		if command.Dir != canonical {
			t.Fatalf("executor received mixed workspace paths: %q, expected %q", command.Dir, canonical)
		}
	}
	if len(local.workspaces) != 1 || len(local.directories) != 2 {
		t.Fatalf("path checks bypassed selected executor: workspaces=%v directories=%v", local.workspaces, local.directories)
	}
}

func TestServiceRejectsSelectedLocalWorkspaceFailure(t *testing.T) {
	refusal := errors.New("workspace unavailable")
	local := &refusingLocalExecutor{workspaceErr: refusal}
	_, err := NewService(ServiceConfig{
		WorkDir: t.TempDir(), RuntimeRoot: t.TempDir(), Env: []string{},
		Requests: testAuthority(t), LocalExecutor: local,
	})
	if !errors.Is(err, refusal) || len(local.launches) != 0 {
		t.Fatalf("workspace failure did not stop construction: %v; launches=%v", err, local.launches)
	}
}

func TestCommandRejectsSelectedLocalDirectoryFailureBeforeClaim(t *testing.T) {
	bound := newBoundRun(t)
	refusal := errors.New("directory unavailable")
	local := &refusingLocalExecutor{directoryErr: refusal}
	bridge := newCommandBridge(bound.authority, local, nil)
	if err := bridge.register(bound.identity, &commandRegistration{workspace: bound.workspace}); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.invoke(context.Background(), shellCall(bound, "rejected-directory", "true")); !errors.Is(err, refusal) {
		t.Fatalf("directory check fell back to the host filesystem: %v", err)
	}
	var claims int
	if err := bound.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&claims); err != nil || claims != 0 || len(local.launches) != 0 {
		t.Fatalf("refused directory reached execution: claims=%d launches=%v error=%v", claims, local.launches, err)
	}
}

type workspaceRefusingLocalExecutor struct {
	LocalExecutor
	failure error
}

func (e workspaceRefusingLocalExecutor) ResolveWorkspace(string) (string, error) {
	return "", e.failure
}

func TestPrepareUsesSelectedLocalWorkspaceResolver(t *testing.T) {
	service, endpoint := serviceFixture(t, nil)
	setServiceLocalExecutor(service, serviceRunnerFunc(func(context.Context, processguardian.Command, processguardian.PersistOwnership) (processguardian.Handle, error) {
		return newServiceTestHandle(endpoint), nil
	}))
	if _, err := service.Client(context.Background()); err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("request workspace unavailable")
	setServiceLocalExecutor(service, workspaceRefusingLocalExecutor{LocalExecutor: service.local, failure: refusal})
	_, err := service.Prepare(context.Background(), ExecuteRequest{
		AttemptID: "attempt", InvocationID: "invocation", WorkspacePath: t.TempDir(),
		Model: "provider/model", Agent: "builder", System: "Test instruction",
	})
	if !errors.Is(err, refusal) {
		t.Fatalf("request preparation bypassed selected workspace resolver: %v", err)
	}
}
