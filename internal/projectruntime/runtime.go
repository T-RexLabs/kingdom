// Package projectruntime assembles one project's provider, workflow runtime,
// and API. The caller owns the database, theme store, listener, and signals.
package projectruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"kingdom/internal/api"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/external"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/shutdown"
	"kingdom/internal/theme"
)

type Mode string

const (
	Local        Mode = "local"
	Hosted       Mode = "hosted"
	LocalManaged Mode = "local_managed"
)

var ErrHostedProviderUnavailable = errors.New("hosted project execution requires a managed provider backend")

type Config struct {
	Mode                                           Mode
	DB                                             *sql.DB
	ThemeStore                                     *theme.Store
	Paths                                          kingdom.BootstrapPaths
	ProjectRoot, RuntimeRoot, BasePath             string
	PublicAPIBase                                  string
	ProviderEngine, ProviderBinary, GovernanceMode string
	AutonomousApprovals                            *bool
	PublishMode, YoloOutputRoot                    string
	WorktreeRetention                              time.Duration
	OperatorCredential                             []byte
	Authenticator                                  api.Authenticator
	// ProviderService is dedicated to this project. Ownership transfers after
	// configuration validation succeeds, including on construction failure.
	ProviderService      opencode.Backend
	ProviderModelCatalog api.ProviderModelCatalog
	HostedSelection      *api.HostedSelectionConfig
	LocalSelection       *api.LocalSelectionConfig

	ExternalAdapter                              external.Adapter
	GitHubSharedAppSlug, GitHubSharedAppClientID string
}

type consumerRuntime interface {
	Start(context.Context) error
	GracefulClose(context.Context) error
	ForceClose() error
}
type consumerAPI interface {
	Close(context.Context) error
	ForceClose()
}
type providerLifecycle interface {
	CloseContext(context.Context) error
	ForceClose() error
}

type Instance struct {
	Handler         *api.Server
	Runtime         *kingdom.Runtime
	ProviderService opencode.Backend
	runtime         consumerRuntime
	api             consumerAPI
	provider        providerLifecycle
}

func validate(cfg Config) error {
	if cfg.ProviderService != nil {
		v := reflect.ValueOf(cfg.ProviderService)
		if v.Kind() == reflect.Pointer && v.IsNil() {
			return errors.New("provider backend must not be a typed nil")
		}
	}
	if cfg.Mode != Local && cfg.Mode != Hosted && cfg.Mode != LocalManaged {
		return errors.New("project runtime mode must be explicit")
	}
	if cfg.Mode == Hosted {
		if cfg.Authenticator == nil || cfg.ProviderService == nil || len(cfg.OperatorCredential) != 0 {
			return errors.New("hosted project requires an explicit provider and authenticator without a local credential")
		}
		if cfg.ProviderService.ExecutionBackend() != providerexecution.BackendOpenShell || cfg.ProviderModelCatalog == nil {
			return ErrHostedProviderUnavailable
		}
		managed, ok := cfg.ProviderService.(*opencode.ManagedService)
		if !ok || managed.ValidateConfiguration() != nil {
			return ErrHostedProviderUnavailable
		}
	}
	if cfg.Mode == LocalManaged {
		if cfg.Authenticator != nil || cfg.HostedSelection != nil || cfg.LocalSelection == nil || cfg.ProviderService == nil || cfg.ProviderModelCatalog == nil || len(cfg.OperatorCredential) == 0 {
			return errors.New("local managed project requires explicit local ownership, catalog, provider and operator credential")
		}
		managed, ok := cfg.ProviderService.(*opencode.ManagedService)
		if !ok || managed.ValidateConfiguration() != nil {
			return errors.New("local managed provider is not initialized")
		}
	}
	if cfg.Mode == Local && cfg.LocalSelection != nil {
		return errors.New("host-local project cannot use managed credential selection")
	}
	if cfg.Mode == Local && cfg.ProviderService != nil && cfg.ProviderService.ExecutionBackend() != providerexecution.BackendLocal {
		return errors.New("local project requires a local provider backend")
	}
	if cfg.Mode == Local && cfg.Authenticator != nil {
		return errors.New("local project cannot use hosted authentication")
	}
	if cfg.DB == nil || cfg.ThemeStore == nil || cfg.ProjectRoot == "" || cfg.RuntimeRoot == "" {
		return errors.New("project runtime requires database, theme store, project and runtime roots")
	}
	if _, err := api.ValidateBasePath(cfg.BasePath); err != nil {
		return err
	}
	return nil
}

func New(ctx context.Context, cfg Config) (instance *Instance, err error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := cfg.ProviderService
	if p == nil {
		p, err = opencode.NewService(opencode.ServiceConfig{
			BinaryPath:  cfg.ProviderBinary,
			WorkDir:     cfg.ProjectRoot,
			RuntimeRoot: opencode.ServiceRuntimeRoot(cfg.RuntimeRoot),
			Requests:    providerrequest.NewAuthority(cfg.DB, nil)})
		if err != nil {
			return nil, fmt.Errorf("configure OpenCode service: %w", err)
		}
	}
	instance = &Instance{ProviderService: p, provider: p}
	// Never leave partially constructed consumers running after a failed New.
	defer func() {
		if err != nil {
			cleanupErr := instance.rollback()
			err = errors.Join(err, cleanupErr)
			if cleanupErr == nil {
				instance = nil
			}
		}
	}()
	catalog := cfg.ProviderModelCatalog
	if catalog == nil {
		source, ok := p.(api.OpenCodeModelSource)
		if !ok {
			return instance, errors.New("provider requires an explicit model catalog")
		}
		catalog = api.NewOpenCodeProviderModelCatalog(source, nil, api.ProviderModelCatalogTTL)
	}
	runtime, err := kingdom.NewRuntime(kingdom.Config{DB: cfg.DB,
		StartupContext:      ctx,
		Paths:               cfg.Paths,
		ProviderEngine:      cfg.ProviderEngine,
		GovernanceMode:      cfg.GovernanceMode,
		AutonomousApprovals: cfg.AutonomousApprovals,
		PublishMode:         cfg.PublishMode,
		WorkspaceRoot:       cfg.RuntimeRoot,
		GuardianRuntimeRoot: opencode.ServiceRuntimeRoot(cfg.RuntimeRoot),
		YoloOutputRoot:      cfg.YoloOutputRoot,
		WorktreeRetention:   cfg.WorktreeRetention,
		Provider:            p})
	if err != nil {
		return instance, fmt.Errorf("bootstrap runtime: %w", err)
	}
	instance.Runtime = runtime
	instance.runtime = runtime
	handler, err := api.NewHandler(api.ServerConfig{
		DB:                         cfg.DB,
		ThemeStore:                 cfg.ThemeStore,
		StartupContext:             ctx,
		BasePath:                   cfg.BasePath,
		PublicAPIBase:              cfg.PublicAPIBase,
		AutonomousApprovalsDefault: cfg.AutonomousApprovals,
		ProviderEngine:             cfg.ProviderEngine,
		ProviderService:            p,
		ProviderModelCatalog:       catalog,
		HostedSelection:            cfg.HostedSelection,
		LocalSelection:             cfg.LocalSelection,
		ProviderPolicyPath:         cfg.Paths.ProviderPolicyPath,
		ProviderPolicySchemaPath:   cfg.Paths.ProviderPolicySchemaPath,
		ProjectRoot:                cfg.ProjectRoot,
		RuntimeRoot:                cfg.RuntimeRoot,
		RetryEngine:                runtime.RetryEngine(),
		DefaultPublishMode:         cfg.PublishMode,
		OperatorCredential:         cfg.OperatorCredential,
		Authenticator:              cfg.Authenticator,
		ExternalAdapter:            cfg.ExternalAdapter,
		GitHubSharedAppSlug:        cfg.GitHubSharedAppSlug,
		GitHubSharedAppClientID:    cfg.GitHubSharedAppClientID})
	if err != nil {
		return instance, fmt.Errorf("build api handler: %w", err)
	}
	instance.Handler = handler
	instance.api = handler
	runtime.SetPreDispatchRecovery(handler.RecoverPlanningOperations)
	return instance, nil
}

func (i *Instance) Start(ctx context.Context) error {
	if err := i.runtime.Start(ctx); err != nil {
		return errors.Join(fmt.Errorf("start runtime: %w", err), i.rollback())
	}
	return nil
}

// Close drains both consumers concurrently. The provider remains available
// until the caller completes HTTP shutdown and then calls CloseProvider.
func (i *Instance) Close(ctx context.Context) error {
	closers := []shutdown.Closer{}
	if i.api != nil {
		closers = append(closers, i.api)
	}
	if i.runtime != nil {
		closers = append(closers, shutdown.CloseFunc(i.runtime.GracefulClose))
	}
	return (shutdown.CompositeCloser{Closers: closers}).Close(ctx)
}
func (i *Instance) CloseProvider(ctx context.Context) error {
	if i.provider == nil {
		return nil
	}
	return i.provider.CloseContext(ctx)
}

// ForceClose does not close caller-owned databases. The caller must retain
// them when interrupted consumers may still own terminal writes.
func (i *Instance) ForceClose() error {
	var err error
	if i.runtime != nil {
		err = i.runtime.ForceClose()
	}
	if i.api != nil {
		i.api.ForceClose()
	}
	if i.provider != nil {
		err = errors.Join(err, i.provider.ForceClose())
	}
	return err
}
func (i *Instance) rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := i.Close(ctx)
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(err, i.CloseProvider(ctx))
}
