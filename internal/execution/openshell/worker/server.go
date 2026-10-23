package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/tunnel"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providerlauncher"
)

type ModelBroker interface {
	Issue(context.Context, capability.Binding) (modelproxy.Lease, error)
	Activate(context.Context, string, providerexecution.RequestIdentity) error
	RevokeBinding(context.Context, capability.Binding) error
	RequireClosed(context.Context, capability.Binding) error
}
type PluginBroker interface {
	Issue(context.Context, capability.Binding) (pluginauthority.Lease, error)
	Activate(context.Context, string, providerexecution.RequestIdentity) error
	FenceBinding(context.Context, capability.Binding) error
	RevokeBinding(context.Context, capability.Binding) error
	RequireClosed(context.Context, capability.Binding) error
}
type ServerServices struct {
	Models    ModelBroker
	Plugins   PluginBroker
	Forwarder tunnel.Forwarder
}

// ServerProfile comes from operator-pinned image metadata and the trusted model
// catalog. It cannot be supplied by an HTTP client or provider-generated text.
type ServerProfile struct {
	LauncherSHA256, OpenCodeSHA256 string
	ContextLimit, OutputLimit      int
	Lifetime                       time.Duration
}
type launch struct{ id, model, plugin string }
type Server struct {
	worker            *Worker
	reservation       remote.Reservation
	owner             managed.Owner
	binding           capability.Binding
	services          ServerServices
	client            *managed.Client
	launch            launch
	identity          *pb.CommandIdentity
	job               providerlauncher.Job
	provider, control *tunnel.Transport
}

func (*Server) String() string                  { return "managed provider server" }
func (*Server) GoString() string                { return "managed provider server" }
func (s *Server) Identity() *pb.CommandIdentity { return proto.Clone(s.identity).(*pb.CommandIdentity) }
func (s *Server) Transport() http.RoundTripper  { return s.provider }
func (w *Worker) binding(ctx context.Context, p providerrequest.Preparation, r remote.Reservation) (capability.Binding, error) {
	if r.Reference.Target != w.config.Target || r.Owner.Kind != string(p.Owner.Kind) || r.Owner.InvocationID != p.Owner.InvocationID || r.Owner.LeaseOwner != p.Lease.Owner || r.Owner.LeaseVersion != p.Lease.Version {
		return capability.Binding{}, remote.ErrConflict
	}
	snapshot, e := access.Invocation(ctx, w.db, string(p.Owner.Kind), p.Owner.InvocationID)
	if e != nil {
		return capability.Binding{}, e
	}
	return capability.Binding{Preparation: p, Runtime: r.Reference, Access: snapshot}, nil
}
func readLaunch(ctx context.Context, q remote.Reader, allocation int64) (launch, error) {
	var l launch
	e := q.QueryRowContext(ctx, `SELECT operation_id,model_lease_id,plugin_lease_id FROM remote_provider_launches WHERE allocation=?`, allocation).Scan(&l.id, &l.model, &l.plugin)
	return l, e
}
func (w *Worker) exactRuntime(ctx context.Context, q remote.Reader, r remote.Reservation) error {
	saved, e := remote.ReadRuntime(ctx, q, r.Reference.Allocation)
	if e != nil {
		return e
	}
	if saved.Owner != r.Owner || saved.Reference != r.Reference {
		return remote.ErrConflict
	}
	return nil
}

// LaunchServer issues capabilities once, only after verified import. An issuance
// intent without a retained encrypted job is unresolved after restart: tokens
// are never reissued or reconstructed from the capability registry.
func (w *Worker) LaunchServer(ctx, lifetime context.Context, p providerrequest.Preparation, r remote.Reservation, owner managed.Owner, profile ServerProfile, services ServerServices) (*Server, error) {
	if lifetime == nil || services.Models == nil || services.Plugins == nil || services.Forwarder == nil || !ownerMatches(owner, r.Reference) || w.config.Egress.ModelURL == "" || w.config.Egress.CallbackURL == "" || profile.Lifetime < time.Second || profile.Lifetime > 24*time.Hour {
		return nil, remote.ErrConflict
	}
	if !validBinaryDigest(profile.LauncherSHA256) || !validBinaryDigest(profile.OpenCodeSHA256) || profile.ContextLimit < 1 || profile.ContextLimit > 2000000 || profile.OutputLimit < 1 || profile.OutputLimit > 200000 || profile.OutputLimit > profile.ContextLimit {
		return nil, remote.ErrConflict
	}
	binding, e := w.binding(ctx, p, r)
	if e != nil {
		return nil, e
	}
	providerID, modelID := launcherModel(binding.Access.Selection)
	var l launch
	fresh := false
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		l, e = readLaunch(ctx, tx, r.Reference.Allocation)
		if e == nil {
			return w.exactRuntime(ctx, tx, r)
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e = w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
			return e
		}
		if e = remote.RequireWorkspaceImportTx(ctx, tx, r.Owner, r.Reference); e != nil {
			return e
		}
		l.id = uuid.NewString()
		_, e = tx.ExecContext(ctx, `INSERT INTO remote_provider_launches(allocation,operation_id)VALUES(?,?)`, r.Reference.Allocation, l.id)
		fresh = e == nil
		return e
	})
	if e != nil {
		return nil, e
	}
	if fresh {
		model, e := services.Models.Issue(ctx, binding)
		if e != nil {
			return nil, ErrUnresolved
		}
		if e = w.saveLease(ctx, r, "model", model.ID); e != nil {
			return nil, e
		}
		l.model = model.ID
		plugin, e := services.Plugins.Issue(ctx, binding)
		if e != nil {
			return nil, ErrUnresolved
		}
		if e = w.saveLease(ctx, r, "plugin", plugin.ID); e != nil {
			return nil, e
		}
		l.plugin = plugin.ID
		if model.BaseURL != w.config.Egress.ModelURL || plugin.BaseURL != w.config.Egress.CallbackURL {
			return nil, remote.ErrConflict
		}
		control, e := randomHex()
		if e != nil {
			return nil, e
		}
		password, e := randomHex()
		if e != nil {
			return nil, e
		}
		modelAPI := ""
		switch model.Protocol {
		case "openai":
			modelAPI = providerlauncher.ChatCompletionsAPI
		case "openai_chatgpt":
			modelAPI = providerlauncher.ResponsesAPI
		default:
			return nil, remote.ErrConflict
		}
		job := providerlauncher.Job{Version: providerlauncher.JobVersion, ModelAPI: modelAPI, LauncherSHA256: profile.LauncherSHA256, OpenCodeSHA256: profile.OpenCodeSHA256, ControlToken: control, ServerPassword: password, ProxyURL: model.BaseURL, ProxyToken: model.Token(), ProviderID: providerID, ModelID: modelID, CallbackURL: plugin.BaseURL, CallbackToken: plugin.Token(), InstanceID: r.Reference.MainInstanceID, ContextLimit: profile.ContextLimit, OutputLimit: profile.OutputLimit, LifetimeSeconds: int64(profile.Lifetime / time.Second)}
		spec, e := providerlauncher.Command(job)
		if e != nil {
			return nil, e
		}
		request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: l.id, Runtime: proto.Clone(owner.Runtime).(*pb.RuntimeBinding)}, Command: spec}
		e = w.transaction(ctx, func(tx *sql.Tx) error {
			if e := w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
				return e
			}
			if _, e := remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.ProviderServer, owner, request); e != nil {
				return e
			}
			return w.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
		})
		if e != nil {
			return nil, e
		}
	}
	request, e := w.requests.ReadManaged(ctx, w.db, r.Owner, r.Reference, l.id)
	if e != nil {
		return nil, ErrUnresolved
	}
	job, e := providerlauncher.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil {
		return nil, e
	}
	fixed, e := providerlauncher.Command(job)
	if e != nil || !proto.Equal(fixed, request.Command) || job.LauncherSHA256 != profile.LauncherSHA256 || job.OpenCodeSHA256 != profile.OpenCodeSHA256 || job.ProxyURL != w.config.Egress.ModelURL || job.CallbackURL != w.config.Egress.CallbackURL || job.ProviderID != providerID || job.ModelID != modelID || job.InstanceID != r.Reference.MainInstanceID || l.model == "" || l.plugin == "" {
		return nil, remote.ErrConflict
	}
	client, e := managed.New(ctx, w.wire, owner, w.config.Workspace, nativeName(r.Reference))
	if e != nil {
		return nil, e
	}
	var send bool
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		op, e := remote.ReadOperation(ctx, tx, l.id)
		if e != nil {
			return e
		}
		if op.Delivery != remote.Prepared {
			return nil
		}
		if e = w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
			return e
		}
		send, e = w.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, l.id)
		return e
	})
	if e != nil {
		return nil, e
	}
	var status *pb.ExecutionStatus
	if send {
		status, e = client.Start(ctx, l.id, request.Command)
	} else {
		status, e = client.ObserveStart(ctx, request)
	}
	if e != nil {
		return nil, ErrUnresolved
	}
	if e = w.retainIdentity(ctx, r, status); e != nil {
		return nil, e
	}
	if status.State == pb.ExecutionState_EXECUTION_STATE_CLOSED {
		_, e = w.retainTerminal(ctx, client, r, status)
		if e != nil {
			return nil, e
		}
		return nil, ErrUnresolved
	}
	server := &Server{worker: w, reservation: r, owner: owner, binding: binding, services: services, client: client, launch: l, identity: status.Identity, job: job}
	server.control, e = server.openTunnel(lifetime, providerlauncher.Port, "Bearer "+job.ControlToken)
	if e != nil {
		return nil, e
	}
	if e = server.waitReady(ctx); e != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		server.control.Close(cleanup)
		cancel()
		return nil, e
	}
	server.provider, e = server.openTunnel(lifetime, providerlauncher.ProviderPort, "Basic "+base64.StdEncoding.EncodeToString([]byte("opencode:"+job.ServerPassword)))
	if e != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		server.control.Close(cleanup)
		cancel()
		return nil, e
	}
	return server, nil
}
func randomHex() (string, error) {
	b := make([]byte, 32)
	_, e := rand.Read(b)
	return hex.EncodeToString(b), e
}
func (w *Worker) saveLease(ctx context.Context, r remote.Reservation, kind, id string) error {
	if id == "" || (kind != "model" && kind != "plugin") {
		return remote.ErrConflict
	}
	return w.transaction(ctx, func(tx *sql.Tx) error {
		if e := w.exactRuntime(ctx, tx, r); e != nil {
			return e
		}
		result, e := tx.ExecContext(ctx, `UPDATE remote_provider_launches SET `+kind+`_lease_id=? WHERE allocation=? AND (`+kind+`_lease_id='' OR `+kind+`_lease_id=?)`, id, r.Reference.Allocation, id)
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil {
			return e
		}
		if n != 1 {
			return remote.ErrConflict
		}
		return nil
	})
}
func (s *Server) openTunnel(ctx context.Context, port uint32, authorization string) (*tunnel.Transport, error) {
	return tunnel.New(ctx, tunnel.Config{Forwarder: s.services.Forwarder, Target: tunnel.Target{Workspace: s.worker.config.Workspace, Allocation: uint64(s.reservation.Reference.Allocation), Owner: s.owner, Port: port}, MaxConnections: 4, OpenTimeout: 5 * time.Second, Verify: func(ctx context.Context, _ tunnel.Target) error {
		owner, e := managed.Discover(ctx, s.worker.wire, s.worker.config.Workspace, nativeName(s.reservation.Reference), s.worker.config.Target.GatewayID, s.worker.config.ProfileSHA256)
		if e != nil {
			return e
		}
		if !sameOwner(owner, s.owner) {
			return remote.ErrConflict
		}
		return nil
	}}, authorization)
}
func (s *Server) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := &http.Client{Transport: s.control, CheckRedirect: func(*http.Request, []*http.Request) error { return remote.ErrConflict }}
	for {
		request, _ := http.NewRequestWithContext(ctx, "GET", "https://managed.invalid/v1/ready", nil)
		response, e := client.Do(request)
		if e == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return nil
			}
			if response.StatusCode != http.StatusServiceUnavailable {
				return ErrUnresolved
			}
		}
		status, e := s.client.Get(ctx, s.identity)
		if e != nil || status.State == pb.ExecutionState_EXECUTION_STATE_CLOSED {
			return ErrUnresolved
		}
		select {
		case <-ctx.Done():
			return ErrUnresolved
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// AuthorizeRequest pins the acknowledged request; it does not issue a prompt.
func (s *Server) AuthorizeRequest(ctx context.Context, identity providerexecution.RequestIdentity) error {
	if identity.InstanceID != s.reservation.Reference.MainInstanceID {
		return remote.ErrConflict
	}
	if e := s.services.Models.Activate(ctx, s.launch.model, identity); e != nil {
		return e
	}
	return s.services.Plugins.Activate(ctx, s.launch.plugin, identity)
}

func validBinaryDigest(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32 && strings.ToLower(value) == value
}

func launcherModel(selection access.Selection) (string, string) {
	return selection.Connection.Provider, strings.TrimPrefix(selection.Model, selection.Connection.Provider+"/")
}

// CloseTransport joins owned forwarding streams. It proves no remote process
// closure; callers must retain the server's typed receipt independently.
func (s *Server) CloseTransport(ctx context.Context) error {
	var a, b error
	if s.provider != nil {
		a = s.provider.Close(ctx)
	}
	if s.control != nil {
		b = s.control.Close(ctx)
	}
	return errors.Join(a, b)
}
