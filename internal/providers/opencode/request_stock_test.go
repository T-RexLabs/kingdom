package opencode

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

// stockHarness runs managed requests against the real OpenCode server with a
// deterministic model and a migrated canonical database.
type stockHarness struct {
	*ownerFixture
	service *Service
	model   *fakeModel
}

func newStockHarness(t *testing.T) *stockHarness {
	t.Helper()
	binary := os.Getenv("KINGDOM_OPENCODE_V2_TEST_BINARY")
	if binary == "" {
		t.Skip("set KINGDOM_OPENCODE_V2_TEST_BINARY for isolated stock verification")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &stockHarness{ownerFixture: newOwnerFixture(t, root), model: newFakeModel(t)}
	config, err := json.Marshal(map[string]any{"providers": map[string]any{"fixture": map[string]any{
		"name": "Local Fixture", "package": "@opencode/ai/providers/openai-compatible",
		"settings": map[string]any{"baseURL": h.model.URL(), "apiKey": "disposable", "encoding": "none"},
		"models":   map[string]any{"scripted": map[string]any{"name": "scripted", "capabilities": map[string]any{"tools": true, "input": []string{"text"}, "output": []string{"text"}}, "limit": map[string]any{"context": 200000, "output": 4096}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"), "LANG=en_US.UTF-8", "HOME=" + filepath.Join(root, "home"), "TMPDIR=" + filepath.Join(root, "tmp"),
		"OPENCODE_DB=" + filepath.Join(root, "opencode.db"), "OPENCODE_CONFIG_CONTENT=" + string(config),
		"XDG_CONFIG_HOME=" + filepath.Join(root, "xdg-config"), "XDG_DATA_HOME=" + filepath.Join(root, "xdg-data"),
		"XDG_CACHE_HOME=" + filepath.Join(root, "xdg-cache"), "XDG_STATE_HOME=" + filepath.Join(root, "xdg-state"),
	}
	for _, name := range []string{"home", "tmp", "xdg-config", "xdg-data", "xdg-cache", "xdg-state"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	service, err := NewService(ServiceConfig{
		BinaryPath: binary, WorkDir: h.workspace, RuntimeRoot: filepath.Join(root, "runtime"), Env: env,
		Requests: providerrequest.NewAuthority(h.db, func() time.Time { return time.Now().UTC() }),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.service = service
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.CloseContext(ctx); err != nil {
			t.Errorf("close service: %v", err)
		}
	})
	return h
}

func noGuardians(req *ExecuteRequest) {
	req.OnProcessGuardOwnership = func(context.Context, ProcessGuardOwnership) error {
		return errors.New("unexpected command guardian")
	}
	req.OnProcessGuardQuiescence = func(context.Context, ProcessGuardQuiescence) error {
		return errors.New("unexpected command guardian")
	}
}

func (h *stockHarness) planningRequest(invocation, operation, prompt string) ExecuteRequest {
	req := ExecuteRequest{
		InvocationID: invocation, AttemptID: operation, WorkspacePath: h.workspace, Model: "fixture/scripted", Agent: "planning-assistant",
		System: "PINNED SYSTEM " + invocation, Prompt: prompt, FirstEventTimeout: 30 * time.Second, StreamInactivityTimeout: 30 * time.Second,
	}
	noGuardians(&req)
	return req
}

func (h *stockHarness) run(session, operation, invocation string, req ExecuteRequest) (Prepared, ExecuteResponse, error) {
	h.t.Helper()
	h.planningOwner(session, operation, invocation)
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		h.t.Fatal(err)
	}
	h.bindPlanning(prepared, operation, invocation)
	response, err := h.service.Execute(context.Background(), req)
	return prepared, response, err
}

func TestStockPlanningRequestIsVerifiedAndRecorded(t *testing.T) {
	h := newStockHarness(t)
	var progress strings.Builder
	req := h.planningRequest("inv-1", "op-1", "TEXT:hello")
	req.OnProgress = func(text string) { progress.WriteString(text) }
	prepared, response, err := h.run("session-1", "op-1", "inv-1", req)
	if err != nil {
		t.Fatal(err)
	}
	observation := response.StreamObservation
	if err := providerexecution.ValidateServerResponseSuccess(observation); err != nil {
		t.Fatalf("verified response failed structural validation: %v", err)
	}
	if observation.Request == nil || observation.Request.Identity != prepared.Identity || observation.Request.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded {
		t.Fatalf("unexpected request observation: %#v", observation.Request)
	}
	if response.OutputPreview != "hello" || progress.String() != "hello" {
		t.Fatalf("preview=%q progress=%q", response.OutputPreview, progress.String())
	}
	binding, found, err := providerrequest.ForOwner(context.Background(), h.db, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: "inv-1"})
	if err != nil || !found {
		t.Fatalf("binding %v %v", found, err)
	}
	want, _ := observation.Request.Terminal.Reference()
	if binding.DispatchIssuedAt == "" || binding.AdmissionClosedAt == "" || binding.TerminalReference == nil || *binding.TerminalReference != want {
		t.Fatalf("binding facts %#v", binding)
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), h.db, binding.ID); err != nil {
		t.Fatal(err)
	}
	calls := h.model.Calls()
	if len(calls) != 1 || len(calls[0].System) == 0 || !strings.Contains(strings.Join(calls[0].System, "\n"), "PINNED SYSTEM inv-1") {
		t.Fatalf("model did not receive the pinned system instruction: %#v", calls)
	}
	if observation.ReleaseContent == nil {
		t.Fatal("staged content cannot be released")
	}
	if err := observation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
}

func (h *stockHarness) builderRequest(invocation, attempt, prompt string) ExecuteRequest {
	req := h.planningRequest(invocation, attempt, prompt)
	req.Agent = "builder"
	return req
}

func TestStockNativeWriteIsAdmittedReceiptedAndMatched(t *testing.T) {
	h := newStockHarness(t)
	h.runOwner("attempt-1", "inv-run")
	req := h.builderRequest("inv-run", "attempt-1", "WRITE:created.txt")
	var activity []ExecuteActivityObservation
	req.OnActivity = func(observation ExecuteActivityObservation) { activity = append(activity, observation) }
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindRun(prepared, "attempt-1", "inv-run")
	response, err := h.service.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(h.workspace, "created.txt")); err != nil || string(raw) != "hello\n" {
		t.Fatalf("native write did not happen exactly once: %q %v", raw, err)
	}
	if err := providerexecution.ValidateServerResponseSuccess(response.StreamObservation); err != nil {
		t.Fatal(err)
	}
	var statuses []string
	for _, observation := range activity {
		if observation.Kind == ExecuteActivityKindTool && observation.ToolName == "write" {
			statuses = append(statuses, observation.ToolStatus)
		}
	}
	if strings.Join(statuses, ",") != "pending,running,completed" {
		t.Fatalf("tool activity %v", statuses)
	}
	var grants, completed int
	if err := h.db.QueryRow(`SELECT count(*),count(outcome) FROM provider_native_grants`).Scan(&grants, &completed); err != nil || grants != 1 || completed != 1 {
		t.Fatalf("grants=%d completed=%d err=%v", grants, completed, err)
	}
	tools := response.StreamObservation.Request.Terminal.Tools
	if len(tools) != 1 || tools[0].Name != "write" || tools[0].Status != "completed" {
		t.Fatalf("unexpected projected tools %#v", tools)
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), h.db, "binding-inv-run"); err != nil {
		t.Fatal(err)
	}
}

func TestStockPlanningProfileCannotWrite(t *testing.T) {
	h := newStockHarness(t)
	_, response, err := h.run("session-ro", "op-ro", "inv-ro", h.planningRequest("inv-ro", "op-ro", "WRITE:forbidden.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(h.workspace, "forbidden.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a read-only profile wrote a file: %v", statErr)
	}
	var grants int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&grants); err != nil || grants != 0 {
		t.Fatalf("a denied tool received a grant: %d %v", grants, err)
	}
	if response.StreamObservation.Request == nil {
		t.Fatal("the denied request must still be proven")
	}
}

func TestStockContinuationReusesSessionAndSendsOnlyNewInput(t *testing.T) {
	h := newStockHarness(t)
	first := h.planningRequest("inv-a", "op-a", "TEXT:first")
	prepared, _, err := h.run("session-c", "op-a", "inv-a", first)
	if err != nil {
		t.Fatal(err)
	}
	// The first settlement is a prerequisite for reuse in canonical state.
	h.settleSuccessfully("session-c", "op-a", "inv-a")

	h.planningOwnerNext("session-c", "op-b", "inv-b")
	second := h.planningRequest("inv-b", "op-b", "TEXT:full-bootstrap-prompt")
	second.ProviderSessionID = prepared.ProviderSessionID
	second.OpenContinuationPrompt = openText("TEXT:second")
	next, err := h.service.Prepare(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Reused || next.ProviderSessionID != prepared.ProviderSessionID || next.Identity.ProfileID == prepared.Identity.ProfileID || next.Identity.RequestID == prepared.Identity.RequestID {
		t.Fatalf("reuse must keep the session and change profile and request: %#v %#v", prepared, next)
	}
	h.bindPlanning(next, "op-b", "inv-b")
	response, err := h.service.Execute(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if response.OutputPreview != "second" {
		t.Fatalf("preview %q", response.OutputPreview)
	}
	calls := h.model.Calls()
	last := calls[len(calls)-1]
	if last.LastUser != "TEXT:second" || last.Messages <= calls[0].Messages {
		t.Fatalf("continuation did not extend the same history with only the new input: %#v", calls)
	}
	if !strings.Contains(strings.Join(last.System, "\n"), "PINNED SYSTEM inv-b") || strings.Contains(strings.Join(last.System, "\n"), "PINNED SYSTEM inv-a") {
		t.Fatalf("each request must run under its own pinned system: %#v", last.System)
	}
}

func TestStockUntrackedSessionIsNotContinued(t *testing.T) {
	h := newStockHarness(t)
	h.planningOwner("session-u", "op-u", "inv-u")
	req := h.planningRequest("inv-u", "op-u", "TEXT:fresh")
	req.ProviderSessionID = "ses_00000000000000000000000000"
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Reused || prepared.ProviderSessionID == req.ProviderSessionID {
		t.Fatalf("an unknown session must not be adopted: %#v", prepared)
	}
	if err := h.service.Discard(context.Background(), "inv-u"); err != nil {
		t.Fatal(err)
	}
}

func TestStockCancellationInterruptsOnlyThisRequestAndIsProven(t *testing.T) {
	h := newStockHarness(t)
	req := h.planningRequest("inv-cancel", "op-cancel", "SLEEP:30 TEXT:late")
	h.planningOwner("session-x", "op-cancel", "inv-cancel")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindPlanning(prepared, "op-cancel", "inv-cancel")
	started := make(chan struct{})
	req.OnInvocationStarted = func() { close(started) }
	done := make(chan struct{})
	var response ExecuteResponse
	var execErr error
	go func() {
		defer close(done)
		response, execErr = h.service.Execute(context.Background(), req)
	}()
	<-started
	time.Sleep(500 * time.Millisecond)
	if err := h.service.Cancel(context.Background(), CancelRequest{AttemptID: "op-cancel"}); err != nil {
		t.Fatal(err)
	}
	<-done
	if !errors.Is(execErr, context.Canceled) {
		t.Fatalf("cancelled request must report cancellation: %v", execErr)
	}
	observation := response.StreamObservation
	if observation.Request == nil || observation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted || !observation.Cancelled {
		t.Fatalf("cancellation is not proven by interrupted evidence: %#v", observation.Request)
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), h.db, "binding-inv-cancel"); err != nil {
		t.Fatal(err)
	}
}

func TestStockSilentRequestIsInterruptedAtItsLimit(t *testing.T) {
	h := newStockHarness(t)
	req := h.planningRequest("inv-quiet", "op-quiet", "SLEEP:30 TEXT:late")
	req.FirstEventTimeout, req.StreamInactivityTimeout = 5*time.Second, 5*time.Second
	_, response, err := h.run("session-q", "op-quiet", "inv-quiet", req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a silent request must time out: %v", err)
	}
	if !response.StreamObservation.TimedOut || response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
		t.Fatalf("timeout is not proven by interrupted evidence: %#v", response.StreamObservation.Request)
	}
}

func openText(text string) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(text)), nil }
}

func TestStockManagedCommandRunsUnderTheGuardianAndIsClaimed(t *testing.T) {
	h := newStockHarness(t)
	h.runOwner("attempt-sh", "inv-sh")
	command := "echo guarded > guarded.txt && pwd"
	req := h.builderRequest("inv-sh", "attempt-sh", "SHELL64:"+base64.StdEncoding.EncodeToString([]byte(command)))
	ordinals := h.guardianRecorder(&req, "attempt-sh", "inv-sh")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindRun(prepared, "attempt-sh", "inv-sh")
	response, err := h.service.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(h.workspace, "guarded.txt")); err != nil || string(raw) != "guarded\n" {
		t.Fatalf("guarded command effect: %q %v", raw, err)
	}
	if len(*ordinals) != 1 || (*ordinals)[0] != 1 {
		t.Fatalf("guardian ownership ordinals %v", *ordinals)
	}
	tools := response.StreamObservation.Request.Terminal.Tools
	if len(tools) != 1 || tools[0].Name != "shell" || tools[0].Status != "completed" {
		t.Fatalf("unexpected projected tools %#v", tools)
	}
	var completed int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_command_calls WHERE guard_id IS NOT NULL AND completed_at IS NOT NULL`).Scan(&completed); err != nil || completed != 1 {
		t.Fatalf("completed command claims %d %v", completed, err)
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), h.db, "binding-inv-sh"); err != nil {
		t.Fatal(err)
	}
	if err := providerexecution.ValidateServerResponseSuccess(response.StreamObservation); err != nil {
		t.Fatal(err)
	}
}

func TestStockUnavailableModelFailureIsProvenWithStructuredError(t *testing.T) {
	h := newStockHarness(t)
	req := h.planningRequest("inv-bad", "op-bad", "TEXT:x")
	req.Model = "fixture/missing"
	_, response, err := h.run("session-bad", "op-bad", "inv-bad", req)
	if err != nil {
		t.Fatal(err)
	}
	observation := response.StreamObservation
	if observation.Request.Terminal.Outcome != providerexecution.RequestOutcomeFailed || !observation.ProviderErrorPresent || !strings.Contains(observation.ProviderError, "fixture/missing") {
		t.Fatalf("provider failure is not carried as structured evidence: %#v", observation)
	}
	if err := providerexecution.ValidateServerResponseSuccess(observation); err == nil {
		t.Fatal("a failed request must not validate as a successful response")
	}
	if calls := h.model.Calls(); len(calls) != 0 {
		t.Fatalf("an unavailable model reached the provider: %#v", calls)
	}
}

func TestStockCancellingOneRequestLeavesAConcurrentRequestUntouched(t *testing.T) {
	h := newStockHarness(t)
	slow := h.planningRequest("inv-slow", "op-slow", "SLEEP:30 TEXT:slow")
	h.planningOwner("session-slow", "op-slow", "inv-slow")
	slowPrepared, err := h.service.Prepare(context.Background(), slow)
	if err != nil {
		t.Fatal(err)
	}
	h.bindPlanning(slowPrepared, "op-slow", "inv-slow")
	started := make(chan struct{})
	slow.OnInvocationStarted = func() { close(started) }
	type outcome struct {
		response ExecuteResponse
		err      error
	}
	slowDone := make(chan outcome, 1)
	go func() {
		response, err := h.service.Execute(context.Background(), slow)
		slowDone <- outcome{response, err}
	}()
	<-started

	_, fast, err := h.run("session-fast", "op-fast", "inv-fast", h.planningRequest("inv-fast", "op-fast", "TEXT:fast"))
	if err != nil || fast.OutputPreview != "fast" {
		t.Fatalf("a concurrent request was disturbed: %q %v", fast.OutputPreview, err)
	}
	if err := h.service.Cancel(context.Background(), CancelRequest{AttemptID: "op-slow"}); err != nil {
		t.Fatal(err)
	}
	slowResult := <-slowDone
	if !errors.Is(slowResult.err, context.Canceled) || slowResult.response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
		t.Fatalf("slow request %#v %v", slowResult.response.StreamObservation.Request, slowResult.err)
	}
	if fast.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded {
		t.Fatal("the unrelated request did not complete")
	}
}

func TestStockPrepareRefusesWhatCannotBeProven(t *testing.T) {
	h := newStockHarness(t)
	h.planningOwner("session-p", "op-p", "inv-p")
	for name, mutate := range map[string]func(*ExecuteRequest){
		"relative workspace": func(r *ExecuteRequest) { r.WorkspacePath = "relative" },
		"no system":          func(r *ExecuteRequest) { r.System = " " },
		"no agent":           func(r *ExecuteRequest) { r.Agent = "" },
		"malformed model":    func(r *ExecuteRequest) { r.Model = "no-provider" },
	} {
		t.Run(name, func(t *testing.T) {
			req := h.planningRequest("inv-p", "op-p", "TEXT:x")
			mutate(&req)
			if _, err := h.service.Prepare(context.Background(), req); err == nil {
				t.Fatal("prepared an unprovable request")
			}
		})
	}
	req := h.planningRequest("inv-p", "op-p", "TEXT:x")
	if _, err := h.service.Prepare(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Prepare(context.Background(), req); err == nil {
		t.Fatal("prepared the same invocation twice")
	}
	if _, err := h.service.Execute(context.Background(), h.planningRequest("inv-unprepared", "op-x", "TEXT:x")); err == nil {
		t.Fatal("executed an unprepared invocation")
	}
}
