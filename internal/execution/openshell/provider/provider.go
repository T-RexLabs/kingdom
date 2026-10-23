package provider

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/central/capability"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providerlauncher"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/sqlitex"
)

type Commands interface {
	Cancel(context.Context, providerexecution.RequestIdentity) error
}
type Config struct {
	DB        *sql.DB
	Worker    *worker.Worker
	Sources   SourceStore
	Transfers worker.Transfers
	Server    worker.ServerProfile
	Services  worker.ServerServices
	Commands  Commands
	Registry  *capability.Registry
	Disposer  *worker.Disposer
	SpoolRoot string
	Now       func() time.Time
}
type Provider struct {
	c         Config
	manifests manifestStore
	mu        sync.Mutex
	handles   map[string]*handle
	locks     map[string]*gate
}
type gate struct {
	token chan struct{}
	users int
}
type handle struct {
	parent      *Provider
	manifest    remote.WorkerManifest
	reservation remote.Reservation
	owner       managed.Owner
	server      *worker.Server
	cancel      context.CancelFunc
}

var _ opencode.ManagedWorker = (*Provider)(nil)
var _ opencode.ManagedHandle = (*handle)(nil)

func New(c Config) (*Provider, error) {
	if c.DB == nil || c.Worker == nil || c.Sources == nil || c.Commands == nil || c.Registry == nil || c.Disposer == nil || c.Services.Models == nil || c.Services.Plugins == nil || c.Services.Forwarder == nil || c.Transfers.Forwarder == nil || c.SpoolRoot == "" {
		return nil, remote.ErrConflict
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	pins, e := pinsJSON(c.Worker.ConfigurationDigest(), c.Server, c.Transfers.HelperSHA256, c.Transfers.Limits, c.Transfers.Timeout, c.Transfers.StageRoot, c.SpoolRoot)
	if e != nil {
		return nil, e
	}
	return &Provider{c: c, manifests: manifestStore{c.DB, c.Sources, pins, c.Now}, handles: map[string]*handle{}, locks: map[string]*gate{}}, nil
}
func (p *Provider) lock(ctx context.Context, id string) (func(), error) {
	p.mu.Lock()
	g := p.locks[id]
	if g == nil {
		g = &gate{token: make(chan struct{}, 1)}
		g.token <- struct{}{}
		p.locks[id] = g
	}
	g.users++
	p.mu.Unlock()
	releaseRef := func() {
		p.mu.Lock()
		g.users--
		if g.users == 0 {
			delete(p.locks, id)
		}
		p.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	case <-g.token:
	}
	return func() { g.token <- struct{}{}; releaseRef() }, nil
}
func (p *Provider) Provision(ctx context.Context, in opencode.ManagedProvision) (opencode.ManagedHandle, error) {
	if in.InvocationID == "" || in.InvocationID != in.Preparation.Owner.InvocationID {
		return nil, remote.ErrConflict
	}
	unlock, e := p.lock(ctx, in.InvocationID)
	if e != nil {
		return nil, e
	}
	defer unlock()
	m, e := p.manifests.ensure(ctx, in.Preparation)
	if e != nil {
		return nil, e
	}
	p.mu.Lock()
	existing := p.handles[in.InvocationID]
	p.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	if e = p.transaction(ctx, func(tx *sql.Tx) error { return p.c.Registry.BindProjectTx(ctx, tx) }); e != nil {
		return nil, e
	}
	r, e := p.c.Worker.ReserveManifest(ctx, in.Preparation, m)
	if e != nil {
		return nil, e
	}
	m.Allocation = r.Reference.Allocation
	r, o, e := p.c.Worker.Provision(ctx, in.Preparation, r)
	if e != nil {
		return nil, e
	}
	transfers := p.c.Transfers
	transfers.Source = pinnedSource{p.c.Sources, m}
	if e = p.c.Worker.Import(ctx, in.Preparation, r, o, m.ImportID, transfers); e != nil {
		return nil, e
	}
	lifetime, cancel := context.WithCancel(context.Background())
	server, e := p.c.Worker.LaunchServer(ctx, lifetime, in.Preparation, r, o, p.c.Server, p.c.Services)
	if e != nil {
		cancel()
		return nil, e
	}
	h := &handle{p, m, r, o, server, cancel}
	p.mu.Lock()
	p.handles[in.InvocationID] = h
	p.mu.Unlock()
	return h, nil
}
func (h *handle) Endpoint() opencode.ManagedEndpoint {
	return opencode.ManagedEndpoint{Runtime: h.reservation.Reference, Server: proto.Clone(h.server.Identity()).(*pb.CommandIdentity), Workspace: providerlauncher.WorkspacePath}
}
func (h *handle) Transport() http.RoundTripper { return h.server.Transport() }
func (h *handle) AuthorizeRequest(ctx context.Context, id providerexecution.RequestIdentity) error {
	return h.server.AuthorizeRequest(ctx, id)
}
func (h *handle) CancelTools(ctx context.Context, id providerexecution.RequestIdentity) error {
	b, e := providerrequest.Get(ctx, h.parent.c.DB, h.manifest.Owner.BindingID)
	if e != nil {
		return e
	}
	if b.Identity != id || b.RemoteAllocation != h.manifest.Allocation {
		return remote.ErrConflict
	}
	return h.parent.c.Commands.Cancel(ctx, id)
}
func (h *handle) RecoverReceipts(ctx context.Context, id providerexecution.RequestIdentity) error {
	if e := h.CancelTools(ctx, id); e != nil {
		return e
	}
	var unsupported int
	e := h.parent.c.DB.QueryRowContext(ctx, `SELECT count(*) FROM provider_native_grants g LEFT JOIN remote_native_tools n ON n.grant_id=g.grant_id WHERE g.binding_id=? AND n.grant_id IS NULL`, h.manifest.Owner.BindingID).Scan(&unsupported)
	if e != nil {
		return e
	}
	if unsupported != 0 {
		return remote.ErrConflict
	}
	return nil
}
func (h *handle) Close(ctx context.Context) error {
	return h.parent.CloseInvocation(ctx, h.manifest.Owner.InvocationID)
}
func (h *handle) Export(ctx context.Context) error {
	return h.parent.ExportInvocation(ctx, h.manifest.Owner.InvocationID)
}
func (h *handle) Discard(ctx context.Context) error {
	return h.parent.DiscardInvocation(ctx, h.manifest.Owner.InvocationID)
}
func (p *Provider) transaction(ctx context.Context, f func(*sql.Tx) error) error {
	_, e := sqlitex.Transaction(ctx, p.c.DB, func(_ context.Context, tx *sql.Tx) error { return f(tx) }, sqlitex.Options{})
	return e
}
func (p *Provider) CloseInvocation(ctx context.Context, id string) error {
	unlock, e := p.lock(ctx, id)
	if e != nil {
		return e
	}
	defer unlock()
	return p.close(ctx, id)
}
func (p *Provider) close(ctx context.Context, id string) error {
	m, e := p.manifests.load(ctx, id)
	if errors.Is(e, sql.ErrNoRows) {
		return p.closeBeforeManifest(ctx, id)
	}
	if e != nil {
		return e
	}
	if m.Allocation == 0 {
		return p.transaction(ctx, func(tx *sql.Tx) error {
			return providerrequest.CloseOwnerRemoteAdmissionTx(ctx, tx, preparation(m).Owner, p.c.Now().UTC().Format(time.RFC3339Nano))
		})
	}
	r, e := remote.ReadRuntime(ctx, p.c.DB, m.Allocation)
	if e != nil {
		return e
	}
	if e = p.transaction(ctx, func(tx *sql.Tx) error {
		return providerrequest.FenceRemoteCleanupTx(ctx, tx, m.Owner, r.Reference, p.c.Now())
	}); e != nil {
		return e
	}
	if r.Reference.SandboxID == "" {
		op, e := remote.ReadOperation(ctx, p.c.DB, m.CreationID)
		if e != nil {
			return e
		}
		if op.Delivery == remote.Prepared {
			return p.transaction(ctx, func(tx *sql.Tx) error { return remote.MarkRuntimeNeverCreatedTx(ctx, tx, m.Owner, r.Reference) })
		}
		// Provision is observational for an already claimed create. The fenced
		// ledger prevents a prepared operation from entering dispatch here.
		r, _, e = p.c.Worker.Provision(ctx, preparation(m), r)
		if e != nil {
			return e
		}
	}
	o, e := remote.ReadRuntimeOwner(ctx, p.c.DB, m.Owner, r.Reference)
	if e != nil {
		return e
	}
	b, e := providerrequest.Get(ctx, p.c.DB, m.Owner.BindingID)
	if e == nil {
		if e = p.c.Commands.Cancel(ctx, b.Identity); e != nil {
			return e
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if _, e = p.c.Worker.StopServer(ctx, preparation(m), r, o, p.c.Services); e != nil {
		return e
	}
	if e = p.c.Worker.CloseTransfers(ctx, preparation(m), r, o); e != nil {
		return e
	}
	if e = p.sealCapabilities(ctx, m, r); e != nil {
		return e
	}
	if _, e = remote.ReadWriterClosures(ctx, p.c.DB, m.Owner, r.Reference); e != nil {
		return e
	}
	p.mu.Lock()
	h := p.handles[id]
	p.mu.Unlock()
	if h != nil {
		h.cancel()
		if e = h.server.CloseTransport(ctx); e != nil {
			return e
		}
	}
	p.mu.Lock()
	delete(p.handles, id)
	p.mu.Unlock()
	return nil
}
func (p *Provider) sealCapabilities(ctx context.Context, m remote.WorkerManifest, r remote.Reservation) error {
	snapshot, e := access.Invocation(ctx, p.c.DB, m.Owner.Kind, m.Owner.InvocationID)
	if e != nil {
		return e
	}
	binding := capability.Binding{Preparation: preparation(m), Runtime: r.Reference, Access: snapshot}
	for _, purpose := range []capability.Purpose{capability.Model, capability.Plugin} {
		proof, e := p.c.Registry.SealBindingClosure(ctx, purpose, binding)
		if e != nil {
			return e
		}
		if e = p.transaction(ctx, func(tx *sql.Tx) error { return capability.RecordClosureTx(ctx, tx, proof) }); e != nil {
			return e
		}
	}
	return providerrequest.RequireRemoteCapabilitiesClosed(ctx, p.c.DB, preparation(m).Owner, r.Reference.Allocation)
}
func (p *Provider) ExportInvocation(ctx context.Context, id string) error {
	unlock, e := p.lock(ctx, id)
	if e != nil {
		return e
	}
	defer unlock()
	if e = p.close(ctx, id); e != nil {
		return e
	}
	m, e := p.manifests.load(ctx, id)
	if e != nil {
		return e
	}
	closed, e := providerrequest.ReadRemoteWriterClosure(ctx, p.c.DB, m.Owner.BindingID)
	if e != nil {
		return e
	}
	if !closed.SuccessEligible {
		return remote.ErrConflict
	}
	r, e := remote.ReadRuntime(ctx, p.c.DB, m.Allocation)
	if e != nil {
		return e
	}
	o, e := remote.ReadRuntimeOwner(ctx, p.c.DB, m.Owner, r.Reference)
	if e != nil {
		return e
	}
	d, e := p.c.Worker.Export(ctx, preparation(m), r, o, m.ExportID, remote.WorkspaceExport, p.c.Transfers, p.c.Services, p.c.SpoolRoot)
	if e != nil {
		return e
	}
	return d.Staged.Close()
}
func (p *Provider) DiscardInvocation(ctx context.Context, id string) error {
	unlock, e := p.lock(ctx, id)
	if e != nil {
		return e
	}
	defer unlock()
	if e = p.close(ctx, id); e != nil {
		return e
	}
	m, e := p.manifests.load(ctx, id)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if m.Allocation == 0 {
		return nil
	}
	r, e := remote.ReadRuntime(ctx, p.c.DB, m.Allocation)
	if e != nil {
		return e
	}
	never, e := remote.ReadRuntimeNeverCreated(ctx, p.c.DB, m.Owner, r.Reference)
	if e != nil {
		return e
	}
	if never {
		return nil
	}
	return p.c.Disposer.Dispose(ctx, r)
}

func (p *Provider) closeBeforeManifest(ctx context.Context, id string) error {
	return p.transaction(ctx, func(tx *sql.Tx) error {
		// Capture occurs before the manifest transaction. Fence the issued owner
		// even when no source has committed, so a late capture cannot create debt.
		rows, e := tx.QueryContext(ctx, `SELECT owner_kind FROM provider_preparations WHERE invocation_id=?`, id)
		if e != nil {
			return e
		}
		var kinds []string
		for rows.Next() {
			var kind string
			if e = rows.Scan(&kind); e != nil {
				rows.Close()
				return e
			}
			kinds = append(kinds, kind)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if len(kinds) > 1 {
			return remote.ErrConflict
		}
		if len(kinds) == 1 {
			if e = providerrequest.CloseOwnerRemoteAdmissionTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerKind(kinds[0]), InvocationID: id}, p.c.Now().UTC().Format(time.RFC3339Nano)); e != nil {
				return e
			}
		}
		var debt int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_runtimes WHERE invocation_id=?`, id).Scan(&debt); e != nil {
			return e
		}
		if debt != 0 {
			return worker.ErrUnresolved
		}
		return nil
	})
}
