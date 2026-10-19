package opencode

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/processguardian"
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

func (h *stockHarness) planningRequest(invocation, operation, prompt string) ExecuteRequest {
	req := ExecuteRequest{
		InvocationID: invocation, AttemptID: operation, WorkspacePath: h.workspace, Model: "fixture/scripted", Agent: "planning-assistant",
		System: "PINNED SYSTEM " + invocation, Prompt: prompt, FirstEventTimeout: 30 * time.Second, StreamInactivityTimeout: 30 * time.Second,
	}
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

func TestStockPlanningRequestIsVerifiedAndLeftForSettlement(t *testing.T) {
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
	if response.Outcome.State != "success" {
		t.Fatalf("outcome %#v", response.Outcome)
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
	// The terminal becomes the request's evidence when it settles.
	if binding.DispatchIssuedAt == "" || binding.AdmissionClosedAt == "" || binding.TerminalReference != nil {
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
	first.KeepSession = true
	prepared, firstResponse, err := h.run("session-c", "op-a", "inv-a", first)
	if err != nil {
		t.Fatal(err)
	}
	// The first settlement is a prerequisite for reuse in canonical state, and
	// it is what records the request's terminal.
	h.settleSuccessfully("session-c", "op-a", "inv-a", firstResponse.StreamObservation.Request.Terminal)

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
	if response.Outcome.State != "failed" || !strings.Contains(response.Outcome.FailureText, "fixture/missing") {
		t.Fatalf("outcome %#v", response.Outcome)
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

func TestStockUnavailableToolCallIsHarmlessAndStillProven(t *testing.T) {
	h := newStockHarness(t)
	h.runOwner("attempt-tool", "inv-tool")
	req := h.builderRequest("inv-tool", "attempt-tool", "TOOL:worktree_remove")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindRun(prepared, "attempt-tool", "inv-tool")
	response, err := h.service.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	tools := response.StreamObservation.Request.Terminal.Tools
	if len(tools) != 1 || tools[0].Name != "worktree_remove" || tools[0].Status != "error" {
		t.Fatalf("a call to a tool outside the profile must fail: %#v", tools)
	}
	if offered := h.model.Calls()[0].Tools; strings.Join(offered, ",") != "edit,read,shell,write" {
		t.Fatalf("a builder profile must be offered only its own tools: %v", offered)
	}
	var grants int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_native_grants`).Scan(&grants); err != nil || grants != 0 {
		t.Fatalf("a refused tool received a grant: %d %v", grants, err)
	}
	if err := providerexecution.ValidateServerResponseSuccess(response.StreamObservation); err != nil {
		t.Fatal(err)
	}
}

func TestStockPlanningProfileIsOfferedOnlyReadTools(t *testing.T) {
	h := newStockHarness(t)
	if _, _, err := h.run("session-tools", "op-tools", "inv-tools", h.planningRequest("inv-tools", "op-tools", "TEXT:x")); err != nil {
		t.Fatal(err)
	}
	if offered := h.model.Calls()[0].Tools; strings.Join(offered, ",") != "read" {
		t.Fatalf("a planning profile must be offered only read tools: %v", offered)
	}
}

func TestStockRequestOutlivingTheSettleLimitStillSettles(t *testing.T) {
	h := newStockHarness(t)
	h.service.requests.settleLimit = 3 * time.Second
	_, response, err := h.run("session-long", "op-long", "inv-long", h.planningRequest("inv-long", "op-long", "SLEEP:6 TEXT:late"))
	if err != nil {
		t.Fatalf("a request that runs longer than the settle limit must still complete: %v", err)
	}
	if response.OutputPreview != "late" || response.StreamObservation.Request.Terminal.Outcome != providerexecution.RequestOutcomeSucceeded {
		t.Fatalf("response %q %#v", response.OutputPreview, response.StreamObservation.Request)
	}
}

func TestStockRequestCancelledBeforeDispatchIsAbandonedAndSettleable(t *testing.T) {
	h := newStockHarness(t)
	req := h.planningRequest("inv-early", "op-early", "TEXT:never sent")
	h.planningOwner("session-early", "op-early", "inv-early")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindPlanning(prepared, "op-early", "inv-early")
	if err := h.service.Cancel(context.Background(), CancelRequest{AttemptID: "op-early"}); err != nil {
		t.Fatal(err)
	}
	response, err := h.service.Execute(context.Background(), req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a request cancelled before dispatch must report cancellation: %v", err)
	}
	observation := response.StreamObservation
	if observation.Request == nil || observation.Request.Terminal.Outcome != providerexecution.RequestOutcomeFailed || !observation.Cancelled || response.Outcome.State != "cancelled" {
		t.Fatalf("an undispatched request is not settleable: %#v %#v", observation.Request, response.Outcome)
	}
	if err := providerrequest.RequireResourcesClosedTx(context.Background(), h.db, "binding-inv-early"); err != nil {
		t.Fatal(err)
	}
	if err := observation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
}

func TestStockConcurrentRequestsRunInTheirOwnWorkspaces(t *testing.T) {
	h := newStockHarness(t)
	const requests = 4
	directories := make([]string, requests)
	prepared := make([]Prepared, requests)
	reqs := make([]ExecuteRequest, requests)
	for i := range directories {
		directory, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		directories[i] = directory
		attempt, invocation := fmt.Sprintf("attempt-%d", i), fmt.Sprintf("inv-%d", i)
		h.runOwner(attempt, invocation)
		reqs[i] = h.builderRequest(invocation, attempt, "WRITE:created.txt")
		reqs[i].WorkspacePath = directory
	}
	var wg sync.WaitGroup
	errs := make([]error, requests)
	for i := range reqs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if prepared[i], err = h.service.Prepare(context.Background(), reqs[i]); err != nil {
				errs[i] = err
			}
		}()
	}
	wg.Wait()
	for i := range reqs {
		if errs[i] != nil {
			t.Fatalf("prepare in a workspace other than the server's: %v", errs[i])
		}
		h.bindRun(prepared[i], fmt.Sprintf("attempt-%d", i), fmt.Sprintf("inv-%d", i))
	}
	for i := range reqs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = h.service.Execute(context.Background(), reqs[i])
		}()
	}
	wg.Wait()
	for i, directory := range directories {
		if errs[i] != nil {
			t.Fatalf("execute %d: %v", i, errs[i])
		}
		if raw, err := os.ReadFile(filepath.Join(directory, "created.txt")); err != nil || string(raw) != "hello\n" {
			t.Fatalf("workspace %d did not receive its own write: %q %v", i, raw, err)
		}
	}
	if _, err := os.Stat(filepath.Join(h.workspace, "created.txt")); err == nil {
		t.Fatal("a request wrote outside its own workspace")
	}
}

func TestStockCommandOutsideTheWorkspaceIsRefusedWithTheDirectoryToUse(t *testing.T) {
	h := newStockHarness(t)
	h.runOwner("attempt-wd", "inv-wd")
	command := "touch escaped.txt"
	elsewhere := t.TempDir()
	req := h.builderRequest("inv-wd", "attempt-wd", "SHELL64:"+base64.StdEncoding.EncodeToString([]byte(command))+" WORKDIR:"+elsewhere)
	h.guardianRecorder(&req, "attempt-wd", "inv-wd")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindRun(prepared, "attempt-wd", "inv-wd")
	if _, err := h.service.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(elsewhere, "escaped.txt")); statErr == nil {
		t.Fatal("a command ran outside its workspace")
	}
	var claims int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&claims); err != nil || claims != 0 {
		t.Fatalf("a refused command was claimed: %d %v", claims, err)
	}
	var refusals []string
	for _, call := range h.model.Calls() {
		for _, result := range call.ToolResults {
			if strings.Contains(result, "must run in its workspace root") {
				refusals = append(refusals, result)
			}
		}
	}
	if len(refusals) == 0 || !strings.Contains(refusals[0], h.workspace) {
		t.Fatalf("the refusal must name the directory to use, got %q", refusals)
	}
}

func TestStockRequestWhoseAdmissionClosedBeforeDispatchSettlesAsCancelled(t *testing.T) {
	h := newStockHarness(t)
	req := h.planningRequest("inv-race", "op-race", "TEXT:never sent")
	h.planningOwner("session-race", "op-race", "inv-race")
	prepared, err := h.service.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	h.bindPlanning(prepared, "op-race", "inv-race")
	// An operator cancellation closes admission before it publishes the stop.
	if err := providerrequest.NewAuthority(h.db, nil).CloseAdmission(context.Background(), prepared.Identity); err != nil {
		t.Fatal(err)
	}
	response, err := h.service.Execute(context.Background(), req)
	if err == nil || response.Outcome.State != "cancelled" || !response.StreamObservation.Cancelled {
		t.Fatalf("a dispatch closed by cancellation must settle as cancelled: err=%v outcome=%#v", err, response.Outcome)
	}
	if err := response.StreamObservation.ReleaseContent(); err != nil {
		t.Fatal(err)
	}
}

func TestStockServerInstanceIsRecordedWhileRunningAndClosedWhenStopped(t *testing.T) {
	h := newStockHarness(t)
	ctx := context.Background()
	if _, err := h.service.Client(ctx); err != nil {
		t.Fatal(err)
	}
	instances, err := providerrequest.OpenServerInstances(ctx, h.db)
	if err != nil || len(instances) != 1 || !strings.HasPrefix(instances[0].ID, "svc_") {
		t.Fatalf("a running server must be recorded: %#v %v", instances, err)
	}
	var ownership processguardian.Ownership
	if err := json.Unmarshal([]byte(instances[0].OwnershipJSON), &ownership); err != nil || ownership.GuardID != instances[0].GuardID || ownership.ChildPID == 0 || ownership.QuiescenceCapability != "" {
		t.Fatalf("the recorded ownership must identify the process and withhold the stop capability: %#v %v", ownership, err)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := h.service.CloseContext(closeCtx); err != nil {
		t.Fatal(err)
	}
	if open, err := providerrequest.OpenServerInstances(ctx, h.db); err != nil || len(open) != 0 {
		t.Fatalf("a stopped server must be closed: %#v %v", open, err)
	}
	var basis string
	if err := h.db.QueryRow(`SELECT quiescence_basis FROM provider_server_instances WHERE instance_id=?`, instances[0].ID).Scan(&basis); err != nil || basis != string(providerrequest.ClosedByGuardian) {
		t.Fatalf("closure basis %q %v", basis, err)
	}
	evidence, err := os.ReadDir(filepath.Join(h.service.guardianRoot, "provider-process-guards", "quiescence"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if len(evidence) != 0 {
		t.Fatalf("the evidence that proved the stop must be removed once recorded: %d files", len(evidence))
	}
}
