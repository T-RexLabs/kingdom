// Package stack constructs the managed execution pipeline from explicit trusted
// dependencies. It owns no listener, project database, registry, or credentials.
package stack

import (
	"context"
	"crypto/x509"
	"database/sql"
	"errors"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/execution/openshell/commands"
	assembly "kingdom/internal/execution/openshell/provider"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/openshell/workspaceoutput"
	"kingdom/internal/execution/openshell/workspacesource"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/workspacetransfer"
	"net/http"
	"path/filepath"
	"time"
)

type Config struct {
	DB                           *sql.DB
	Requests                     *remote.RequestStore
	Gateway                      *worker.Gateway
	Registry                     *capability.Registry
	Authority                    *providerrequest.Authority
	Worker                       worker.Config
	Server                       worker.ServerProfile
	Root, RuntimeRoot            string
	HelperSHA256, FileToolSHA256 string
	ModelAuthorizer              modelproxy.Authorizer
	PluginAuthorizer             pluginauthority.Authorizer
	Resolver                     modelproxy.Resolver
	ModelOrigins                 []modelproxy.OriginPolicy
	OnModelFailure               func(modelproxy.Failure)
	ModelTLSRoots                *x509.CertPool
	TransferTimeout, StopTimeout time.Duration
}

// Stack always returns its initialized handles, including on construction
// failure. The caller must retain them until their Close methods succeed.
type Stack struct {
	Worker   *worker.Worker
	Provider *opencode.ManagedService
	Models   *modelproxy.Broker
	Sources  *workspacesource.Store
	Callback http.Handler
	Results  *runtimefs.Store
}

func New(c Config) (s *Stack, err error) {
	s = &Stack{}
	if c.DB == nil || c.Requests == nil || c.Gateway == nil || c.Registry == nil || c.Authority == nil || c.ModelAuthorizer == nil || c.PluginAuthorizer == nil || c.Resolver == nil || !filepath.IsAbs(c.Root) || !filepath.IsAbs(c.RuntimeRoot) {
		return s, errors.New("explicit managed stack dependencies required")
	}
	w, e := worker.New(c.DB, c.Requests, c.Gateway.Native, c.Gateway.Managed, c.Worker)
	if e != nil {
		return s, e
	}
	s.Worker = w
	models, e := modelproxy.New(modelproxy.Config{Registry: c.Registry, PublicBaseURL: c.Worker.Egress.ModelURL, Authorizer: c.ModelAuthorizer, Resolver: c.Resolver, Origins: c.ModelOrigins, TLSRoots: c.ModelTLSRoots, OnFailure: c.OnModelFailure})
	if e != nil {
		return s, e
	}
	s.Models = models
	plugins, e := pluginauthority.New(c.Registry, c.PluginAuthorizer, c.Worker.Egress.CallbackURL)
	if e != nil {
		return s, e
	}
	executor, e := commands.New(c.DB, c.Requests, c.Gateway.Managed, commands.Config{Workspace: c.Worker.Workspace, Target: c.Worker.Target, ProfileSHA256: c.Worker.ProfileSHA256, HelperSHA256: c.FileToolSHA256, CleanupTimeout: c.StopTimeout})
	if e != nil {
		return s, e
	}
	callback, e := opencode.NewManagedCallbackHandler(plugins, executor, executor)
	if e != nil {
		return s, e
	}
	s.Callback = callback
	sources, e := workspacesource.New(filepath.Join(c.Root, "source"), workspacesource.CanonicalResolver{DB: c.DB, Authority: c.Authority}, workspacetransfer.DefaultLimits())
	if e != nil {
		return s, e
	}
	s.Sources = sources
	store, e := runtimefs.NewStore(c.DB, c.RuntimeRoot)
	if e != nil {
		return s, e
	}
	s.Results = store
	output, e := workspaceoutput.New(workspaceoutput.Config{DB: c.DB, Worker: w, Store: store, Sources: sources, SpoolRoot: filepath.Join(c.Root, "spool"), StageRoot: filepath.Join(c.Root, "stage"), Limits: workspacetransfer.DefaultLimits()})
	if e != nil {
		return s, e
	}
	disposer, e := worker.NewDisposer(w, prerequisites{c.DB})
	if e != nil {
		return s, e
	}
	provider, e := assembly.New(assembly.Config{DB: c.DB, Worker: w, Sources: sources, Output: output, Transfers: worker.Transfers{Forwarder: c.Gateway.Forwarder, StageRoot: filepath.Join(c.Root, "stage"), HelperSHA256: c.HelperSHA256, Limits: workspacetransfer.DefaultLimits(), Timeout: c.TransferTimeout}, Server: c.Server, Services: worker.ServerServices{Models: models, Plugins: plugins, Forwarder: c.Gateway.Forwarder}, Commands: executor, Registry: c.Registry, Disposer: disposer, SpoolRoot: filepath.Join(c.Root, "spool")})
	if e != nil {
		return s, e
	}
	s.Provider, e = opencode.NewManagedService(opencode.ManagedServiceConfig{Worker: provider, Requests: c.Authority, CaptureRoot: filepath.Join(c.Root, "capture"), StopLimit: c.StopTimeout})
	return s, e
}

type prerequisites struct{ db *sql.DB }

func (p prerequisites) RequireClosed(ctx context.Context, o remote.Owner, r remote.Reference) error {
	return providerrequest.RequireRemoteDisposalPrerequisites(ctx, p.db, o, r)
}
