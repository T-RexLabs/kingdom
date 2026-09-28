package planning

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func TestCanonicalPlanningRetryCodeRejectsTextOnlyProviderFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		meta ProviderExecutionMetadata
		want errorcatalog.Code
	}{
		{name: "timeout", raw: "timeout", want: errorcatalog.CodeTimeout},
		{name: "unrecognized provider code", raw: "unknown_provider_failure", want: errorcatalog.CodeProviderUnknownFailure},
		{name: "incomplete", raw: "provider_incomplete_turn", want: errorcatalog.CodeProviderIncompleteTurn},
		{name: "empty", raw: "provider_empty_output", want: errorcatalog.CodeProviderEmptyOutput},
		{name: "invalid", raw: "llm_invalid_response", want: errorcatalog.CodeLLMInvalidResponse},
		{name: "budget", raw: "budget_exceeded", want: errorcatalog.CodeBudgetExceeded},
		{name: "unstructured internal", raw: "internal_error", want: errorcatalog.CodeProviderUnknownFailure},
		{name: "structured internal", raw: "internal_error", meta: ProviderExecutionMetadata{FailureReasonCode: "internal_error"}, want: errorcatalog.CodeInternalError},
		{name: "provider prose", raw: "provider upstream failed", want: errorcatalog.CodeProviderUnknownFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canonicalPlanningRetryCode(tt.raw, tt.meta); got != tt.want {
				t.Fatalf("canonical code = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPlanningRetryWaitsForDueTimeAndUsesFreshProviderSession(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 2, 12, 0, 0, 123456789, time.UTC))
	agent := &retrySequenceAgent{observed: make(chan struct{}, 2), diagnostics: `{"retry_after_ms":10000}`}
	service := NewServiceWithRetryEngine(store, agent, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	session, err := service.CreateSession(context.Background(), "Retry", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this", AnswerPayloadJSON: `{"text":"Plan this"}`})
	if err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain first invocation: %v", err)
	}
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), operation.Operation.OperationID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list retry operation events: %v", err)
	}
	for index, event := range events {
		if want := index + 1; event.Seq != want {
			t.Fatalf("retry operation event sequence at index %d = %d, want %d; events=%#v", index, event.Seq, want, events)
		}
	}
	stored, ok, err := store.GetOperation(context.Background(), operation.Operation.OperationID)
	if err != nil || !ok || stored.Status != state.PlanningOperationStatusQueued {
		t.Fatalf("scheduled retry operation: operation=%+v ok=%t err=%v", stored, ok, err)
	}
	var dueAt string
	if err := db.QueryRow(`SELECT due_at FROM retry_decisions WHERE planning_operation_id = ?`, stored.OperationID).Scan(&dueAt); err != nil {
		t.Fatal(err)
	}
	if dueAt != clock.Now().Add(10*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("retry due_at=%s, want provider hint max at %s", dueAt, clock.Now().Add(10*time.Second).Format(time.RFC3339Nano))
	}
	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain early recovery: %v", err)
	}
	if calls := agent.callCount(); calls != 1 {
		t.Fatalf("provider calls before due = %d, want 1", calls)
	}
	due, err := time.Parse(time.RFC3339Nano, dueAt)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(due)
	if due, err := store.PlanningOperationRetryDue(context.Background(), stored.OperationID, clock.String()); err != nil || !due {
		t.Fatalf("retry should be due: due=%t err=%v now=%s deadline=%s", due, err, clock.String(), dueAt)
	}
	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain due recovery: %v", err)
	}
	if calls := agent.callCount(); calls != 2 {
		t.Fatalf("provider calls after due = %d, want 2", calls)
	}
	if sessions := agent.providerSessions(); len(sessions) != 2 || sessions[0] != "" || sessions[1] != "" {
		t.Fatalf("provider session lineage = %v, want two fresh sessions", sessions)
	}
	var resolution string
	var messages int
	if err := db.QueryRow(`SELECT resolution FROM retry_decisions WHERE planning_operation_id = ?`, stored.OperationID).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM planning_messages WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if resolution != "recovered" || messages != 3 {
		t.Fatalf("resolution=%q messages=%d, want recovered and one user/assistant pair", resolution, messages)
	}
}

func TestPlanningRetryDoesNotRetryMeaningfulProgressFailure(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 2, 13, 0, 0, 0, time.UTC))
	agent := &retrySequenceAgent{meaningfulFailure: true, observed: make(chan struct{}, 1)}
	service := NewServiceWithRetryEngine(store, agent, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	session, err := service.CreateSession(context.Background(), "Retry", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this", AnswerPayloadJSON: `{"text":"Plan this"}`})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := store.GetOperation(context.Background(), op.Operation.OperationID)
	if err != nil || !ok || stored.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("meaningful failure operation=%+v ok=%t err=%v", stored, ok, err)
	}
	var decision string
	if err := db.QueryRow(`SELECT decision FROM retry_decisions WHERE planning_operation_id = ?`, stored.OperationID).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	if decision != string(state.RetryDecisionFailFast) || agent.callCount() != 1 {
		t.Fatalf("decision=%q calls=%d, want fail_fast and one call", decision, agent.callCount())
	}
}

func TestPlanningFormalizeRetryForcesFreshProviderSession(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 2, 13, 30, 0, 0, time.UTC))
	agent := &formalizeRetrySequenceAgent{observed: make(chan struct{}, 2)}
	service := NewServiceWithRetryEngine(store, agent, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	session, err := service.CreateSession(context.Background(), "Retry", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
	if err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain first formalize invocation: %v", err)
	}
	var dueAt string
	if err := db.QueryRow(`SELECT due_at FROM retry_decisions WHERE planning_operation_id = ?`, op.Operation.OperationID).Scan(&dueAt); err != nil {
		t.Fatal(err)
	}
	due, err := time.Parse(time.RFC3339Nano, dueAt)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(due)
	if err := service.RecoverOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain retried formalize invocation: %v", err)
	}
	if got := agent.forceNewSessions(); len(got) != 2 || !got[0] || !got[1] {
		t.Fatalf("formalize session lineage=%v, want fresh provider sessions when no durable session is available", got)
	}
	if got := agent.providerSessionInputs(); len(got) != 2 || got[0] != "" || got[1] != "" {
		t.Fatalf("formalize retry provider session lineage=%v, want two fresh sessions", got)
	}
}

func TestPlanningRetryWakeStartsAtPersistedDueTimeAfterRecovery(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 2, 14, 0, 0, 0, time.UTC))
	first := &retrySequenceAgent{observed: make(chan struct{}, 1), diagnostics: `{"retry_after_ms":10000}`}
	service := NewServiceWithRetryEngine(store, first, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	session, err := service.CreateSession(context.Background(), "Retry", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this", AnswerPayloadJSON: `{"text":"Plan this"}`}); err != nil {
		t.Fatal(err)
	}
	<-first.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := &retrySequenceAgent{observed: make(chan struct{}, 1)}
	restarted := NewServiceWithRetryEngine(store, second, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	timers := newRetryWakeTimerFactory()
	restarted.retryWakeTimerFactory = timers.New
	if err := restarted.RecoverOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	timer := timers.Next(t)
	if timer.delay != 10*time.Second {
		t.Fatalf("retry wake delay=%s, want 10s", timer.delay)
	}
	if calls := second.callCount(); calls != 0 {
		t.Fatalf("planner started before due time: calls=%d", calls)
	}
	clock.Set(clock.Now().Add(10 * time.Second))
	timer.Fire(clock.Now())
	<-second.observed
	if err := restarted.runner.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := second.callCount(); calls != 1 {
		t.Fatalf("planner calls after due wake=%d, want 1", calls)
	}
}

func TestPlanningRetryWakeIsCancelledOnServiceClose(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	store := state.NewPlanningStore(stateTestDB(t))
	clock := newRetryTestClock(time.Date(2026, 6, 2, 15, 0, 0, 0, time.UTC))
	service := NewServiceWithRetryEngine(store, &retrySequenceAgent{}, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	timers := newRetryWakeTimerFactory()
	service.retryWakeTimerFactory = timers.New
	service.schedulePlanningRetryWake(clock.Now().Add(time.Minute))
	timer := timers.Next(t)
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	timer.mu.Lock()
	stopped := timer.stopped
	timer.mu.Unlock()
	if !stopped {
		t.Fatal("service close did not cancel the pending retry wake")
	}
}

func TestPlanningRetryWakeBacksOffWhenDueClaimCannotProgressAndResetsOnClaim(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 2, 16, 0, 0, 0, time.UTC))
	agent := &retrySequenceAgent{observed: make(chan struct{}, 2)}
	service := NewServiceWithRetryEngine(store, agent, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	timers := newRetryWakeTimerFactory()
	service.retryWakeTimerFactory = timers.New

	session, err := service.CreateSession(context.Background(), "Retry", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this", AnswerPayloadJSON: `{"text":"Plan this"}`})
	if err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain first invocation: %v", err)
	}
	var dueAt string
	if err := db.QueryRow(`SELECT due_at FROM retry_decisions WHERE planning_operation_id = ?`, op.Operation.OperationID).Scan(&dueAt); err != nil {
		t.Fatal(err)
	}
	due, err := time.Parse(time.RFC3339Nano, dueAt)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(due)

	// Simulate a recovery worker that cannot claim the due operation yet.
	service.runner.mu.Lock()
	service.runner.running[op.Operation.OperationID] = struct{}{}
	service.runner.mu.Unlock()
	first := timers.Next(t)
	first.Fire(clock.Now())
	second := timers.Next(t)
	if second.delay != planningRetryWakeBackoffBase {
		t.Fatalf("first no-progress retry wake delay=%s, want %s", second.delay, planningRetryWakeBackoffBase)
	}
	second.Fire(clock.Now())
	third := timers.Next(t)
	if third.delay != 2*planningRetryWakeBackoffBase {
		t.Fatalf("second no-progress retry wake delay=%s, want %s", third.delay, 2*planningRetryWakeBackoffBase)
	}

	service.runner.mu.Lock()
	delete(service.runner.running, op.Operation.OperationID)
	service.runner.mu.Unlock()
	third.Fire(clock.Now())
	<-agent.observed
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain recovered retry: %v", err)
	}
	service.retryWakeMu.Lock()
	failures := service.retryWakeFailures
	service.retryWakeMu.Unlock()
	if failures != 0 || agent.callCount() != 2 {
		t.Fatalf("successful due claim failures=%d calls=%d, want reset and two calls", failures, agent.callCount())
	}
}

type retrySequenceAgent struct {
	mu                sync.Mutex
	calls             int
	sessions          []string
	meaningfulFailure bool
	observed          chan struct{}
	diagnostics       string
}

type formalizeRetrySequenceAgent struct {
	mu       sync.Mutex
	calls    int
	forceNew []bool
	sessions []string
	observed chan struct{}
}

func (a *formalizeRetrySequenceAgent) ContinueTurnStream(context.Context, AgentContext, func(string)) (AgentTurn, error) {
	return AgentTurn{}, errors.New("unexpected reply")
}

func (a *formalizeRetrySequenceAgent) FormalizePlanStream(_ context.Context, input AgentContext, _ func(string)) (FormalizedPlan, error) {
	a.mu.Lock()
	a.calls++
	call := a.calls
	a.forceNew = append(a.forceNew, input.ForceNewSession)
	a.sessions = append(a.sessions, providerSessionIDForInvocation(input.ProviderInvocation))
	observed := a.observed
	a.mu.Unlock()
	if observed != nil {
		observed <- struct{}{}
	}
	if call == 1 {
		metadata := ProviderExecutionMetadata{OutcomeState: "failed", FailureReasonCode: "timeout", FailureReasonText: "timed out"}
		return FormalizedPlan{}, executionError(metadata, errors.New("timed out"))
	}
	metadata, err := testSuccessMetadata(input, "# Retried Plan\n\n## Objective\n\nUse a fresh provider session.")
	if err != nil {
		return FormalizedPlan{}, err
	}
	return FormalizedPlan{DecreeTitle: "Retried Plan", DecreeText: metadata.FinalText, Metadata: metadata}, nil
}

func (a *formalizeRetrySequenceAgent) GenerateTitle(context.Context, AgentContext, func(string)) (GeneratedTitle, error) {
	return GeneratedTitle{}, errors.New("unexpected title")
}

func (a *formalizeRetrySequenceAgent) forceNewSessions() []bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]bool(nil), a.forceNew...)
}

func (a *formalizeRetrySequenceAgent) providerSessionInputs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.sessions...)
}

func (a *retrySequenceAgent) ContinueTurnStream(_ context.Context, input AgentContext, _ func(string)) (AgentTurn, error) {
	a.mu.Lock()
	a.calls++
	call := a.calls
	if input.ProviderInvocation != nil {
		a.sessions = append(a.sessions, strings.TrimSpace(input.ProviderInvocation.ProviderSessionID))
	}
	meaningful := a.meaningfulFailure
	observed := a.observed
	diagnostics := a.diagnostics
	a.mu.Unlock()
	if observed != nil {
		observed <- struct{}{}
	}
	if call == 1 {
		metadata := ProviderExecutionMetadata{OutcomeState: "failed", FailureReasonCode: "timeout", FailureReasonText: "timed out", ProviderDiagnosticsJSON: diagnostics, StreamObservation: opencode.InvocationStreamObservation{MeaningfulProgress: meaningful}}
		return AgentTurn{}, executionError(metadata, errors.New("timed out"))
	}
	metadata, err := testSuccessMetadata(input, "Recovered response")
	if err != nil {
		return AgentTurn{}, err
	}
	return AgentTurn{ContentMarkdown: "Recovered response", Metadata: metadata}, nil
}

func (a *retrySequenceAgent) FormalizePlanStream(context.Context, AgentContext, func(string)) (FormalizedPlan, error) {
	return FormalizedPlan{}, errors.New("unexpected formalize")
}

func (a *retrySequenceAgent) GenerateTitle(context.Context, AgentContext, func(string)) (GeneratedTitle, error) {
	return GeneratedTitle{}, errors.New("unexpected title")
}

func (a *retrySequenceAgent) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (a *retrySequenceAgent) providerSessions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.sessions...)
}

type retryTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newRetryTestClock(now time.Time) *retryTestClock { return &retryTestClock{now: now.UTC()} }
func (c *retryTestClock) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Format(time.RFC3339Nano)
}
func (c *retryTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *retryTestClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now.UTC()
	c.mu.Unlock()
}

type retryWakeTimerFactory struct{ timers chan *retryWakeTestTimer }

func newRetryWakeTimerFactory() *retryWakeTimerFactory {
	return &retryWakeTimerFactory{timers: make(chan *retryWakeTestTimer, 8)}
}

func (f *retryWakeTimerFactory) New(delay time.Duration) retryWakeTimer {
	timer := &retryWakeTestTimer{ch: make(chan time.Time, 1), delay: delay}
	f.timers <- timer
	return timer
}

func (f *retryWakeTimerFactory) Next(t *testing.T) *retryWakeTestTimer {
	t.Helper()
	select {
	case timer := <-f.timers:
		return timer
	case <-time.After(time.Second):
		t.Fatal("retry wake timer was not scheduled")
		return nil
	}
}

type retryWakeTestTimer struct {
	mu      sync.Mutex
	stopped bool
	ch      chan time.Time
	delay   time.Duration
}

func (t *retryWakeTestTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.stopped = true
	return true
}

func (t *retryWakeTestTimer) Chan() <-chan time.Time { return t.ch }

func (t *retryWakeTestTimer) Fire(now time.Time) {
	t.mu.Lock()
	stopped := t.stopped
	t.mu.Unlock()
	if !stopped {
		t.ch <- now
	}
}

func loadPlanningRetryEngine(t *testing.T) *retryengine.Engine {
	t.Helper()
	policy, err := retryengine.LoadPolicyFromFile(filepath.Join("..", "..", "policies", "retry_policy.yaml"), filepath.Join("..", "..", "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
