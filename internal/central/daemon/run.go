// Package daemon composes the central application with managed project workers.
package daemon

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"kingdom/internal/central/application"
	"kingdom/internal/central/hostedbuilder"
	"kingdom/internal/central/modelcatalog"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/openshell/worker"
	webui "kingdom/web"
)

func Run(ctx context.Context, path string, out io.Writer) error {
	c, e := load(path)
	if e != nil {
		return e
	}
	defer clear(c.key)
	app, e := application.New(ctx, application.Config{
		DataRoot: c.file.DataRoot, PublicOrigin: c.file.PublicOrigin,
		MasterKey: c.key, InitialOwners: c.owners, OIDC: c.oidc, GitHub: c.github,
		Portal: webui.CentralHandler(),
		Projects: func(shared application.Shared) (application.ProjectServices, error) {
			models, e := modelcatalog.New(modelcatalog.Config{Connections: shared.Connections, Origins: c.origins, TLSRoots: c.roots, Profiles: c.file.ModelProfiles})
			if e != nil {
				return application.ProjectServices{}, e
			}
			providers := map[string]string{"openai": "openai"}
			for _, p := range c.file.ModelProfiles {
				providers[p.Provider] = "openai"
			}
			resolver := &modelproxy.APIKeyResolver{Store: shared.Connections, Providers: providers}
			builder, e := hostedbuilder.New(ctx, hostedbuilder.Config{
				RootDir:      filepath.Join(c.file.DataRoot, "projects"),
				PublicOrigin: c.file.PublicOrigin, WorkerOrigin: c.file.WorkerOrigin,
				GatewayID: c.file.Gateway.GatewayID, WorkerID: c.file.Gateway.WorkerID,
				CatalogDB: shared.CatalogDB, Catalog: shared.Catalog,
				Connections: shared.Connections, Registry: shared.Registry,
				RequestKey: func(_ context.Context, id string) ([]byte, error) { return shared.RequestKey(id) },
				Models: func(_ context.Context, id string) (hostedbuilder.Models, error) {
					return hostedbuilder.Models{Catalog: models.ForProject(id), Cached: models, Resolver: resolver}, nil
				},
				Endpoint: c.endpoint, Image: c.file.Gateway.Image, ProfileSHA256: c.profile,
				Server: worker.ServerProfile{
					LauncherSHA256: c.file.Gateway.Launcher, OpenCodeSHA256: c.file.Gateway.OpenCode,
					ContextLimit: c.contextLimit, OutputLimit: c.outputLimit, Lifetime: c.lifetime,
				},
				HelperSHA256: c.file.Gateway.Helper, FileToolSHA256: c.file.Gateway.FileTool,
				AllowedIPs: c.file.Gateway.AllowedIPs, ModelOrigins: c.origins, ModelTLSRoots: c.roots,
			})
			if e != nil {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return application.ProjectServices{}, errors.Join(e, models.Close(cleanup))
			}
			return application.ProjectServices{Build: builder.Build, Workers: http.HandlerFunc(builder.Workers), Models: models, Close: models.Close}, nil
		},
	})
	if e != nil {
		return e
	}
	listener, e := net.Listen("tcp", c.file.Listen)
	if e != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return errors.Join(e, app.Close(cleanup))
	}
	if out != nil {
		_, _ = fmt.Fprintf(out, "Kingdom Central is listening at %s\n", c.file.PublicOrigin)
	}
	return serve(ctx, listener, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{c.certificate}}, app)
}

type applicationHandler interface {
	http.Handler
	Close(context.Context) error
}

func serve(ctx context.Context, listener net.Listener, configuration *tls.Config, app applicationHandler) error {
	server := &http.Server{Handler: app, TLSConfig: configuration, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 32 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	var serveErr error
	returned := false
	select {
	case serveErr = <-done:
		returned = true
	case <-ctx.Done():
	}
	// Consumer cancellation and HTTP drain run together. A streaming request
	// must be able to observe application shutdown while HTTP waits for it.
	cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	closed := make(chan error, 1)
	go func() { closed <- app.Close(cleanup) }()
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	shutdownErr := server.Shutdown(shutdown)
	cancel()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	if !returned {
		serveErr = <-done
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr, <-closed)
}
