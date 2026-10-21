package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

type testManagedWorker struct {
	provision func(context.Context, ManagedProvision) (ManagedHandle, error)
	close     func(context.Context, string) error
}

func (w *testManagedWorker) Provision(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
	return w.provision(ctx, p)
}
func (w *testManagedWorker) CloseInvocation(ctx context.Context, id string) error {
	return w.close(ctx, id)
}

type testManagedHandle struct {
	endpoint  ManagedEndpoint
	transport http.RoundTripper
	authorize func(context.Context, providerexecution.RequestIdentity) error
	close     func(context.Context) error
}

func (h *testManagedHandle) Endpoint() ManagedEndpoint    { return h.endpoint }
func (h *testManagedHandle) Transport() http.RoundTripper { return h.transport }
func (h *testManagedHandle) AuthorizeRequest(ctx context.Context, id providerexecution.RequestIdentity) error {
	return h.authorize(ctx, id)
}
func (*testManagedHandle) RecoverReceipts(context.Context, providerexecution.RequestIdentity) error {
	return nil
}
func (h *testManagedHandle) Close(ctx context.Context) error { return h.close(ctx) }

func managedOwner(t *testing.T, snapshots ...access.Snapshot) (*ownerFixture, *providerrequest.Authority, ExecuteRequest) {
	h := newOwnerFixture(t, t.TempDir())
	if len(snapshots) > 0 {
		tx, err := h.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err = access.BindProjectTx(context.Background(), tx, snapshots[0].ProjectID); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	h.planningOwner("session", "operation", "invocation")
	h.exec(`UPDATE planning_provider_invocations SET invocation_state='issued'`)
	tx, err := h.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) > 0 {
		if err := access.CapturePlanningTx(context.Background(), tx, "operation", snapshots[0]); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	err = providerrequest.ReservePreparationTx(context.Background(), tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: "invocation"}, providerrequest.Lease{Owner: "worker", Version: 1}, h.now)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return h, providerrequest.NewAuthority(h.db, time.Now), ExecuteRequest{InvocationID: "invocation", AttemptID: "operation", WorkspacePath: "/not-a-host-workspace", Model: "fixture/model", Agent: "planner", System: "system"}
}
func serviceManaged(t *testing.T, a *providerrequest.Authority, w ManagedWorker) *ManagedService {
	s, err := NewManagedService(ManagedServiceConfig{Worker: w, Requests: a, CaptureRoot: t.TempDir(), StopLimit: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestManagedPrepareRetainsUncertainProvisionUntilClosed(t *testing.T) {
	_, a, req := managedOwner(t)
	var provisions, closes int
	unresolved := true
	worker := &testManagedWorker{provision: func(context.Context, ManagedProvision) (ManagedHandle, error) {
		provisions++
		return nil, errors.New("uncertain provision")
	}, close: func(context.Context, string) error {
		closes++
		if unresolved {
			return errors.New("closure unknown")
		}
		return nil
	}}
	s := serviceManaged(t, a, worker)
	if _, err := s.Prepare(context.Background(), req); err == nil {
		t.Fatal("uncertain provision accepted")
	}
	if len(s.entries) != 1 || closes != 1 {
		t.Fatal("remote debt forgotten")
	}
	if _, err := s.Prepare(context.Background(), req); err == nil || provisions != 1 {
		t.Fatal("duplicate admitted")
	}
	if err := s.Discard(context.Background(), req.InvocationID); err == nil || len(s.entries) != 1 {
		t.Fatal("failed close removed debt")
	}
	unresolved = false
	if err := s.CloseContext(context.Background()); err != nil || len(s.entries) != 0 {
		t.Fatal(err)
	}
	if _, err := s.Prepare(context.Background(), req); err == nil || provisions != 1 {
		t.Fatal("closed service admitted")
	}
	if _, err := s.Execute(context.Background(), req); !errors.Is(err, ErrManagedExecutionUnavailable) {
		t.Fatal(err)
	}
}
func TestManagedPrepareRejectsWrongCanonicalAttemptBeforeProvision(t *testing.T) {
	_, a, req := managedOwner(t)
	req.AttemptID = "foreign"
	worker := &testManagedWorker{provision: func(context.Context, ManagedProvision) (ManagedHandle, error) {
		t.Fatal("provision reached")
		return nil, nil
	}}
	s := serviceManaged(t, a, worker)
	if _, err := s.Prepare(context.Background(), req); err == nil {
		t.Fatal("wrong attempt admitted")
	}
}
func TestManagedCloseCancelsPreparationAndRetainsDeadlineDebt(t *testing.T) {
	_, a, req := managedOwner(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var closes atomic.Int32
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}, close: func(context.Context, string) error { closes.Add(1); return nil }}
	s := serviceManaged(t, a, worker)
	done := make(chan error, 1)
	go func() { _, err := s.Prepare(context.Background(), req); done <- err }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("close did not preserve deadline", err)
	}
	s.mu.Lock()
	count := len(s.entries)
	s.mu.Unlock()
	if count != 1 {
		t.Fatal("in-flight ownership forgotten")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("cancelled prepare succeeded")
	}
	if err := s.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closes.Load() == 0 {
		t.Fatal("uncertain provision never reconciled")
	}
}

func recordedManagedEndpoint(t *testing.T, h *ownerFixture, p ManagedProvision) ManagedEndpoint {
	t.Helper()
	ctx := context.Background()
	tx, err := h.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	r, err := providerrequest.ReserveRemoteRuntimeTx(ctx, tx, p.Preparation.Owner, p.Preparation.Lease, "binding-invocation", remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), h.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = remote.ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID); err != nil {
		t.Fatal(err)
	}
	if err = remote.RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID, r.SpecDigest, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	r.Reference.SandboxID = "sandbox"
	r.Reference.Generation = "generation"
	r.Reference.MainInstanceID = "main"
	if err = remote.BindRuntimeTx(ctx, tx, r.Owner, r.Reference); err != nil {
		t.Fatal(err)
	}
	runtime := &pb.RuntimeBinding{SandboxId: "sandbox", BoundaryGeneration: "generation", MainProcessInstanceId: "main", DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}
	expected := managed.Owner{GatewayID: "gateway", WorkspaceID: "workspace", SubjectID: "subject", Runtime: runtime}
	start := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "sandbox", RequestId: uuid.NewString(), Runtime: runtime}, Command: &pb.CommandSpec{Argv: []string{"/opencode"}, Workdir: "/workspace", OutputLimitBytes: 1024}}
	if _, err = remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.ProviderServer, expected, start); err != nil {
		t.Fatal(err)
	}
	if _, err = remote.ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, start.Execution.RequestId); err != nil {
		t.Fatal(err)
	}
	original, _ := managed.Digest(start)
	effective, _ := managed.Digest(start.Command)
	id := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: "gateway", WorkspaceId: "workspace", SubjectId: "subject", Method: managed.StartMethod, RequestId: start.Execution.RequestId, OriginalRequestSha256: original, EffectiveSpecSha256: effective, BoundaryRequestId: start.Execution.RequestId, Runtime: runtime}
	if err = remote.RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, id); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return ManagedEndpoint{Runtime: r.Reference, Server: id, Workspace: "/workspace"}
}
func TestManagedPrepareRejectsUnrecordedEndpointBeforeHTTP(t *testing.T) {
	h, a, req := managedOwner(t)
	closes := 0
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		endpoint := recordedManagedEndpoint(t, h, p)
		endpoint.Server.Runtime.MainProcessInstanceId = "replacement"
		return &testManagedHandle{endpoint: endpoint, transport: clientTestTransport(func(*http.Request) (*http.Response, error) { t.Fatal("unverified endpoint reached"); return nil, nil }), close: func(context.Context) error { closes++; return nil }}, nil
	}}
	s := serviceManaged(t, a, worker)
	if _, err := s.Prepare(context.Background(), req); err == nil {
		t.Fatal("unrecorded identity accepted")
	}
	if closes != 1 || len(s.entries) != 0 {
		t.Fatal("failed prepare ownership not closed")
	}
}

func managedProtocolTransport(t *testing.T) http.RoundTripper {
	var agent Agent
	return clientTestTransport(func(r *http.Request) (*http.Response, error) {
		response := &http.Response{StatusCode: 200, Header: make(http.Header), Request: r}
		if r.URL.Path == "/api/event" {
			reader, writer := io.Pipe()
			response.Body = reader
			go func() {
				defer writer.Close()
				io.WriteString(writer, "data: {\"type\":\"server.connected\"}\n\n")
				<-r.Context().Done()
			}()
			return response, nil
		}
		var body any
		switch {
		case r.URL.Path == "/api/info":
			body = map[string]any{"version": "2.0.0", "pid": 1}
		case r.URL.Path == "/api/plugin":
			body = map[string]any{"data": []any{map[string]any{"id": pluginID, "state": map[string]string{"status": "active"}}}}
		case r.URL.Path == "/api/config":
			body = []any{map[string]any{"type": "document", "info": map[string]any{"formatter": false}}}
		case r.URL.Path == "/api/rpc/kingdom/register":
			var input struct {
				Input struct {
					ProfileID, System string
					Actions           []string
				}
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				return nil, err
			}
			agent = Agent{ID: input.Input.ProfileID, System: input.Input.System, Mode: "primary", Permissions: []Permission{{Action: "*", Resource: "*", Effect: "deny"}}}
			for _, action := range input.Input.Actions {
				agent.Permissions = append(agent.Permissions, Permission{Action: action, Resource: "*", Effect: "allow"})
			}
			body = map[string]any{"output": profileRegistration{InstanceID: "main", Generation: "plugin-generation", Location: "/workspace"}}
		case strings.HasPrefix(r.URL.Path, "/api/agent/"):
			body = map[string]any{"data": agent}
		case r.URL.Path == "/api/session":
			var input map[string]any
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				return nil, err
			}
			body = map[string]any{"data": input}
		case strings.HasSuffix(r.URL.Path, "/inbox"):
			body = map[string]any{"data": []any{}}
		default:
			t.Errorf("unexpected provider request %s", r.URL.Path)
			return nil, errors.New("unexpected request")
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		response.Body = io.NopCloser(bytes.NewReader(raw))
		return response, nil
	})
}
func TestManagedPreparePinsCanonicalEndpointAndNeverDispatches(t *testing.T) {
	h, a, req := managedOwner(t)
	var authorized providerexecution.RequestIdentity
	var closes int
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		return &testManagedHandle{endpoint: recordedManagedEndpoint(t, h, p), transport: managedProtocolTransport(t), authorize: func(_ context.Context, id providerexecution.RequestIdentity) error { authorized = id; return nil }, close: func(context.Context) error { closes++; return nil }}, nil
	}}
	s := serviceManaged(t, a, worker)
	prepared, err := s.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Backend != providerexecution.BackendOpenShell || prepared.RemoteRuntime.Allocation == 0 || prepared.Identity != authorized || prepared.Identity.Location != "/workspace" {
		t.Fatal("unbound preparation", prepared)
	}
	// The model has not received a prompt. Canonical activation remains separate.
	var bindings int
	if err := h.db.QueryRow(`SELECT count(*) FROM provider_requests`).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatal("prepare activated itself", bindings, err)
	}
	if _, err := s.Execute(context.Background(), req); !errors.Is(err, ErrManagedExecutionUnavailable) {
		t.Fatal(err)
	}
	if err := s.Discard(context.Background(), req.InvocationID); err != nil {
		t.Fatal(err)
	}
	if closes != 1 || len(s.entries) != 0 {
		t.Fatal("owned server was not closed")
	}
}
func TestManagedPrepareRechecksLeaseAfterPluginAuthorization(t *testing.T) {
	h, a, req := managedOwner(t)
	closes := 0
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		return &testManagedHandle{endpoint: recordedManagedEndpoint(t, h, p), transport: managedProtocolTransport(t), authorize: func(context.Context, providerexecution.RequestIdentity) error {
			h.exec(`UPDATE planning_operations SET lease_version=2`)
			return nil
		}, close: func(context.Context) error { closes++; return nil }}, nil
	}}
	s := serviceManaged(t, a, worker)
	if _, err := s.Prepare(context.Background(), req); err == nil {
		t.Fatal("stale lease returned prepared")
	}
	if closes != 1 || len(s.entries) != 0 {
		t.Fatal("stale preparation debt remained unhandled")
	}
}

func TestManagedPrepareRejectsHostedSelectionSubstitution(t *testing.T) {
	actor := uuid.NewString()
	snapshot := access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "fixture/model", ThinkingEffort: "medium", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "fixture"}}}
	_, a, req := managedOwner(t, snapshot)
	s := serviceManaged(t, a, &testManagedWorker{provision: func(context.Context, ManagedProvision) (ManagedHandle, error) {
		t.Fatal("substituted selection reached worker")
		return nil, nil
	}})
	for _, selection := range []struct{ model, variant string }{{"fixture/other", "medium"}, {"fixture/model", "high"}, {"fixture/model", ""}} {
		req.Model, req.Variant = selection.model, selection.variant
		if _, err := s.Prepare(context.Background(), req); err == nil {
			t.Fatal("hosted selection substituted")
		}
	}
}

func TestManagedPrepareRejectsAlreadyTerminatedServer(t *testing.T) {
	h, a, req := managedOwner(t)
	worker := &testManagedWorker{provision: func(ctx context.Context, p ManagedProvision) (ManagedHandle, error) {
		endpoint := recordedManagedEndpoint(t, h, p)
		tx, err := h.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		reservation, err := remote.ReadRuntime(ctx, tx, endpoint.Runtime.Allocation)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(nil)
		receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: endpoint.Server, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{Complete: true, Sha256: digest[:]}}
		receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
		if err = remote.RecordManagedReceiptTx(ctx, tx, reservation.Owner, endpoint.Runtime, endpoint.Server.RequestId, receipt); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return &testManagedHandle{endpoint: endpoint, transport: managedProtocolTransport(t), authorize: func(context.Context, providerexecution.RequestIdentity) error { return nil }, close: func(context.Context) error { return nil }}, nil
	}}
	s := serviceManaged(t, a, worker)
	if _, err := s.Prepare(context.Background(), req); err == nil {
		s.CloseContext(context.Background())
		t.Fatal("terminated server accepted as prepared")
	}
}
