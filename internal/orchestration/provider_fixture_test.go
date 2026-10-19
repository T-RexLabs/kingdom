package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/testutil/providerledger"
)

// fixtureProvider runs a responder behind the real request-ownership layer, so
// every dispatch, terminal and settlement fact is recorded as it is in
// production while the test controls the model's answer.
func fixtureProvider(db *sql.DB, responder opencode.Responder) *opencode.FixtureProvider {
	return opencode.NewFixtureProvider(providerrequest.NewAuthority(db, nil), responder)
}

// memoryProvider answers every request with a canned response.
func memoryProvider(db *sql.DB) *opencode.FixtureProvider {
	return opencode.NewMemoryAdapter(providerrequest.NewAuthority(db, nil))
}

// fixturePrompt is the prompt that jobs started directly by tests carry.
func fixturePrompt(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("fixture prompt")), nil
}

// requestFixture drives provider requests for invocations that a test
// activates by hand. Each request is prepared before its invocation is
// activated, and later runs to a chosen answer through the real
// request-ownership layer, so the recorded terminal is what settlement needs.
type requestFixture struct {
	provider  *opencode.FixtureProvider
	authority *providerrequest.Authority
	db        *sql.DB

	mu       sync.Mutex
	answers  map[string]fixtureAnswer
	fallback opencode.Responder
	executed map[string]int
}

type fixtureAnswer struct {
	response opencode.ExecuteResponse
	err      error
}

func newRequestFixture(db *sql.DB, now func() time.Time) *requestFixture {
	authority := providerrequest.NewAuthority(db, now)
	fixture := &requestFixture{authority: authority, db: db, answers: map[string]fixtureAnswer{}, executed: map[string]int{}}
	fixture.provider = opencode.NewFixtureProvider(authority, fixture)
	return fixture
}

func (f *requestFixture) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	f.mu.Lock()
	f.executed[req.InvocationID]++
	answer, ok := f.answers[req.InvocationID]
	fallback := f.fallback
	f.mu.Unlock()
	if ok {
		return answer.response, answer.err
	}
	if fallback == nil {
		return opencode.ExecuteResponse{}, fmt.Errorf("no answer was set for provider invocation %s", req.InvocationID)
	}
	return fallback.Execute(ctx, req)
}

// Cancel forwards to the fallback responder when it observes cancellation, and
// otherwise reports that the server holds no such attempt.
func (f *requestFixture) Cancel(ctx context.Context, req opencode.CancelRequest) error {
	f.mu.Lock()
	fallback := f.fallback
	f.mu.Unlock()
	if responder, ok := fallback.(opencode.CancelResponder); ok {
		return responder.Cancel(ctx, req)
	}
	return &opencode.AttemptNotFoundError{AttemptID: req.AttemptID}
}

// respondWith answers every request that has no answer of its own.
func (f *requestFixture) respondWith(responder opencode.Responder) {
	f.mu.Lock()
	f.fallback = responder
	f.mu.Unlock()
}

func (f *requestFixture) request(attemptID, invocationID string) opencode.ExecuteRequest {
	return opencode.ExecuteRequest{AttemptID: attemptID, InvocationID: invocationID, WorkspacePath: os.TempDir(), Prompt: "fixture prompt"}
}

// prepare fixes the identity that the invocation's activation binds.
func (f *requestFixture) prepare(t testing.TB, attemptID, invocationID string) opencode.Prepared {
	t.Helper()
	prepared, err := f.provider.Prepare(context.Background(), f.request(attemptID, invocationID))
	if err != nil {
		t.Fatalf("prepare provider request %s: %v", invocationID, err)
	}
	return prepared
}

// run executes a prepared, activated request to the given answer.
func (f *requestFixture) run(t testing.TB, attemptID, invocationID string, response opencode.ExecuteResponse, answerErr error) opencode.ExecuteResponse {
	t.Helper()
	f.mu.Lock()
	f.answers[invocationID] = fixtureAnswer{response: response, err: answerErr}
	f.mu.Unlock()
	executed, err := f.provider.Execute(context.Background(), f.request(attemptID, invocationID))
	if err != nil && !errors.Is(err, answerErr) {
		t.Fatalf("execute provider request %s: %v", invocationID, err)
	}
	return executed
}

// complete runs a prepared, activated request to a successful answer and
// records it on the job exactly as the orchestrator does.
func (f *requestFixture) complete(t testing.TB, job *asyncExecutionResult, output string) {
	t.Helper()
	response := f.run(t, job.attemptID, job.providerInvocationID, opencode.ExecuteResponse{OutputText: output}, nil)
	applyExecuteResponseToAsyncResult(job, response, nil)
	job.outcome = normalizeExecutionOutcome(job.outcome, nil)
}

// activate prepares the invocation's request and activates the invocation under
// the request's identity, exactly as dispatch binds a prepared request.
func (f *requestFixture) activate(t testing.TB, store *executionstore.Store, req executionstore.ActivateProviderInvocationRequest) executionstore.ProviderInvocation {
	t.Helper()
	prepared := f.prepare(t, req.AttemptID, req.ProviderInvocationID)
	req.RequestIdentity, req.ProviderSessionID = prepared.Identity, prepared.ProviderSessionID
	invocation, err := store.ActivateProviderInvocation(context.Background(), req)
	if err != nil {
		t.Fatalf("activate provider invocation %s: %v", req.ProviderInvocationID, err)
	}
	return invocation
}

// issueDispatch records that the server began work on the request, as a worker
// does just before it runs.
func (f *requestFixture) issueDispatch(t testing.TB, invocationID string) providerrequest.Binding {
	t.Helper()
	ctx := context.Background()
	binding, bound, err := providerrequest.ForOwner(ctx, f.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID})
	if err != nil || !bound {
		t.Fatalf("load request binding for %s: bound=%t err=%v", invocationID, bound, err)
	}
	if binding.DispatchIssuedAt == "" {
		if err := f.authority.IssueDispatch(ctx, binding.Identity); err != nil {
			t.Fatalf("issue dispatch for %s: %v", invocationID, err)
		}
	}
	return binding
}

// claimCommand dispatches the request if needed, then admits one managed
// command, which a command guardian can acknowledge.
func (f *requestFixture) claimCommand(t testing.TB, invocationID string) providerrequest.CommandClaim {
	t.Helper()
	binding := f.issueDispatch(t, invocationID)
	var commands int
	if err := f.db.QueryRowContext(context.Background(), `SELECT COUNT(1) FROM provider_command_calls WHERE binding_id = ?`, binding.ID).Scan(&commands); err != nil {
		t.Fatalf("count commands for %s: %v", invocationID, err)
	}
	claim, err := f.authority.ClaimCommand(context.Background(), providerrequest.NativeCall{
		Identity: binding.Identity, MessageID: "msg_fixture_command", CallID: fmt.Sprintf("call_fixture_%d", commands+1),
		Tool: "shell", Input: []byte(`{"command":"true"}`),
	})
	if err != nil {
		t.Fatalf("claim command for %s: %v", invocationID, err)
	}
	return claim
}

// executions reports how many times the server was asked to run the request.
func (f *requestFixture) executions(invocationID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executed[invocationID]
}

// responderFunc answers requests with a function.
type responderFunc func(context.Context, opencode.ExecuteRequest) (opencode.ExecuteResponse, error)

func (f responderFunc) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	return f(ctx, req)
}

// openBoundRequest gives an attempt an unresolved provider request bound to a
// synthetic identity, as dispatch leaves it while the server still works. The
// attempt keeps its recorded state and is left without a lease, so only the
// open request tells that its work has not ended.
func openBoundRequest(t *testing.T, db *sql.DB, store *executionstore.Store, attemptID, runID, nodeRunID, invocationID, providerSessionID string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state); err != nil {
		t.Fatalf("load attempt %s: %v", attemptID, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', lease_owner = 'bound-request-holder', lease_version = 1, lease_expires_at = '2999-01-01T00:00:00Z' WHERE attempt_id = ?`, attemptID); err != nil {
		t.Fatal(err)
	}
	session := providerledger.SessionFor(invocationID, providerSessionID)
	if _, err := store.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		RequestIdentity: providerledger.Prepared(invocationID, session).Identity, ProviderInvocationID: invocationID, AttemptID: attemptID,
		RunID: runID, NodeRunID: nodeRunID, LogicalSessionID: "logical-" + invocationID, ProviderSessionID: session,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:01Z", OpenedAt: "2026-03-01T12:00:01Z",
		ExpectedLeaseOwner: "bound-request-holder", ExpectedLeaseVersion: 1,
	}); err != nil {
		t.Fatalf("open bound request %s: %v", invocationID, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = ?, lease_owner = '', lease_expires_at = NULL WHERE attempt_id = ?`, state, attemptID); err != nil {
		t.Fatal(err)
	}
}
