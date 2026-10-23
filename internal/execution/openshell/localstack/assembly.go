package localstack

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"golang.org/x/net/netutil"
	"kingdom/internal/api"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/localauthority"

	"kingdom/internal/execution/openshell/stack"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providers/opencode"
	_ "modernc.org/sqlite"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Assembly returns ownership even after partial construction failure. Close must
// succeed before its state database or caller-owned project database is closed.
type Assembly struct {
	Provider    *opencode.BackendRouter
	Selection   *api.LocalSelectionConfig
	Catalog     api.ProviderModelCatalog
	credentials Credentials
	pipeline    *stack.Stack
	registry    *capability.Registry
	gateway     *worker.Gateway
	state       *sql.DB
	server      *http.Server
	listener    net.Listener
	done        chan error
}

func privateDir(path string) error {
	if e := os.MkdirAll(path, 0700); e != nil {
		return e
	}
	info, e := os.Lstat(path)
	if e != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return ErrConfig
	}
	return nil
}
func derived(key []byte, domain string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte("kingdom.local-managed.v1\x00" + domain))
	return h.Sum(nil)
}

type CredentialsFactory func(context.Context, *sql.DB, []byte, string, string, ModelConfig) (Credentials, error)

func New(ctx, lifetime context.Context, db *sql.DB, home, projectRoot, binary string, c Config) (*Assembly, error) {
	return NewWithCredentials(ctx, lifetime, db, home, projectRoot, binary, c, APIKeyCredentials)
}
func NewWithCredentials(ctx, lifetime context.Context, db *sql.DB, home, projectRoot, binary string, c Config, factory CredentialsFactory) (a *Assembly, err error) {
	a = &Assembly{}
	if ctx == nil || lifetime == nil || db == nil {
		return a, ErrConfig
	}
	project, e := access.ProjectIncarnation(ctx, db)
	if e != nil {
		return a, e
	}
	root := filepath.Join(c.StateRoot, project)
	for _, dir := range []string{c.StateRoot, root, filepath.Join(root, "source"), filepath.Join(root, "stage"), filepath.Join(root, "spool")} {
		if e = privateDir(dir); e != nil {
			return a, e
		}
	}
	callbackTLS, e := c.callbackTLS()
	if e != nil {
		return a, e
	}
	raw, e := privateFile(c.MasterKeyFile, 128)
	if e != nil {
		return a, e
	}
	defer clear(raw)
	master, e := hex.DecodeString(strings.TrimSpace(string(raw)))
	if e != nil || len(master) != 32 {
		return a, ErrConfig
	}
	defer clear(master)
	path := filepath.Join(root, "owner.db")
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e == nil {
		e = f.Close()
	}
	if e != nil && !os.IsExist(e) {
		return a, e
	}
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return a, ErrConfig
	}
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	a.state, e = sql.Open("sqlite", u.String())
	if e != nil {
		return a, e
	}
	a.state.SetMaxOpenConns(1)
	vaultKey := derived(master, "vault:"+project)
	if c.Model.AuthKind == "chatgpt" {
		clear(vaultKey)
		vaultKey = OwnerCredentialKey(master, c.OwnerID)
	}
	if factory == nil {
		clear(vaultKey)
		return a, ErrConfig
	}
	credentials, e := factory(ctx, a.state, vaultKey, c.OwnerID, project, c.Model)
	a.credentials = credentials
	clear(vaultKey)
	if e != nil {
		return a, e
	}
	a.Catalog = credentials
	a.Selection = &api.LocalSelectionConfig{OwnerID: c.OwnerID, ProjectID: project, Resolver: credentials}
	a.registry, e = capability.New(ctx, capability.Config{DatabasePath: filepath.Join(root, "capabilities.db"), Lifetime: time.Duration(c.LifetimeSeconds) * time.Second})
	if e != nil {
		return a, e
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return a, e
	}
	defer tx.Rollback()
	if e = providerrequest.BindLocalOwnerTx(ctx, tx, project, c.OwnerID); e != nil {
		return a, e
	}
	if e = a.registry.BindProjectTx(ctx, tx); e != nil {
		return a, e
	}
	if e = tx.Commit(); e != nil {
		return a, e
	}
	a.gateway, e = worker.Dial(worker.Endpoint{Address: c.Gateway.Address, CAFile: c.Gateway.CAFile, CertificateFile: c.Gateway.CertificateFile, KeyFile: c.Gateway.KeyFile})
	if e != nil {
		return a, e
	}
	if e = initializeMappings(ctx, a.state); e != nil {
		return a, e
	}
	mapping, e := workspace(ctx, a.state, a.gateway.Native, project, c.GatewayID, project, time.Now())
	if e != nil {
		return a, e
	}
	authority := providerrequest.NewAuthority(db, time.Now)
	auth, e := localauthority.New(authority, credentials, c.OwnerID, project)
	if e != nil {
		return a, e
	}
	key := derived(master, "requests:"+project)
	requests, e := remote.NewRequestStore(key)
	clear(key)
	if e != nil {
		return a, e
	}
	profile, _ := hex.DecodeString(c.ProfileSHA256)
	modelEndpoint := c.Model.Definition.Endpoint
	if c.Model.AuthKind == "chatgpt" {
		modelEndpoint = "https://api.openai.com/v1"
	}
	endpoint, _ := url.Parse(modelEndpoint)
	var roots *x509.CertPool
	if c.Model.CAFile != "" {
		raw, e := os.ReadFile(c.Model.CAFile)
		if e != nil {
			return a, ErrConfig
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(raw) {
			return a, ErrConfig
		}
	}
	origin := modelproxy.OriginPolicy{Origin: endpoint.Scheme + "://" + endpoint.Host}
	for _, raw := range c.Model.AllowedPrivateAddresses {
		prefix, e := netip.ParsePrefix(raw)
		if e != nil {
			return a, ErrConfig
		}
		origin.AllowedPrivateAddresses = append(origin.AllowedPrivateAddresses, prefix)
	}
	a.pipeline, e = stack.New(stack.Config{DB: db, Requests: requests, Gateway: a.gateway, Registry: a.registry, Authority: authority, Root: root, RuntimeRoot: filepath.Join(home, "runtime"), Worker: worker.Config{Target: remote.Target{WorkerID: c.WorkerID, GatewayID: c.GatewayID, WorkspaceID: mapping.ID}, Workspace: mapping.Name, Image: c.Image, ProfileSHA256: profile, Egress: worker.Egress{ModelURL: c.Callback.Origin + "/model/v1", CallbackURL: c.Callback.Origin + "/plugin", AllowedIPs: c.Callback.AllowedIPs}}, Server: worker.ServerProfile{LauncherSHA256: c.LauncherSHA256, OpenCodeSHA256: c.OpenCodeSHA256, ContextLimit: c.ContextLimit, OutputLimit: c.OutputLimit, Lifetime: time.Duration(c.LifetimeSeconds) * time.Second}, HelperSHA256: c.HelperSHA256, FileToolSHA256: c.FileToolSHA256, ModelAuthorizer: localauthority.Model{Authority: auth}, PluginAuthorizer: localauthority.Plugin{Authority: auth}, Resolver: credentials, ModelOrigins: []modelproxy.OriginPolicy{origin}, ModelTLSRoots: roots, TransferTimeout: time.Minute, StopTimeout: time.Minute})
	if e != nil {
		return a, e
	}
	local, e := opencode.NewService(opencode.ServiceConfig{BinaryPath: binary, WorkDir: projectRoot, RuntimeRoot: opencode.ServiceRuntimeRoot(filepath.Join(home, "runtime")), Requests: authority})
	if e != nil {
		return a, e
	}
	a.Provider, e = opencode.NewBackendRouter(db, providerexecution.BackendOpenShell, local, a.pipeline.Provider)
	if e != nil {
		local.CloseContext(ctx)
		return a, e
	}
	mux := http.NewServeMux()
	mux.Handle("/model/", http.StripPrefix("/model", a.pipeline.Models.Handler()))
	mux.Handle("/plugin/", http.StripPrefix("/plugin", a.pipeline.Callback))
	a.server = &http.Server{Handler: fixedRoutes(mux), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return lifetime }}
	a.listener, e = net.Listen("tcp", c.Callback.Listen)
	if e != nil {
		return a, e
	}
	a.done = make(chan error, 1)
	go func() { a.done <- a.server.Serve(tls.NewListener(netutil.LimitListener(a.listener, 32), callbackTLS)) }()
	return a, nil
}
func bytesTrim(v []byte) []byte { return bytes.TrimSpace(v) }
func fixedRoutes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.Method != http.MethodPost || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/model/v1/responses", "/model/v1/chat/completions", "/plugin/v1/native/invoke", "/plugin/v1/command/invoke", "/plugin/v1/command/cancel":
			next.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}
func (a *Assembly) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.Provider != nil {
		if e := a.Provider.CloseContext(ctx); e != nil {
			return e
		}
	} else if a.pipeline != nil && a.pipeline.Provider != nil {
		if e := a.pipeline.Provider.CloseContext(ctx); e != nil {
			return e
		}
	}
	if a.pipeline != nil && a.pipeline.Models != nil {
		if e := a.pipeline.Models.Close(ctx); e != nil {
			return e
		}
	}
	if a.registry != nil {
		if e := a.registry.Close(ctx); e != nil {
			return e
		}
		a.registry = nil
	}
	if a.server != nil {
		if e := a.server.Shutdown(ctx); e != nil {
			return e
		}
		if a.done != nil {
			select {
			case e := <-a.done:
				if e != nil && !errors.Is(e, http.ErrServerClosed) {
					return e
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		a.server = nil
	}
	if a.gateway != nil {
		if e := a.gateway.Close(); e != nil {
			return e
		}
		a.gateway = nil
	}
	if a.pipeline != nil && a.pipeline.Sources != nil {
		if e := a.pipeline.Sources.Close(); e != nil {
			return e
		}
		a.pipeline.Sources = nil
	}
	if closer, ok := a.credentials.(interface{ Close(context.Context) error }); ok {
		if e := closer.Close(ctx); e != nil {
			return e
		}
		a.credentials = nil
	}
	if a.state != nil {
		if e := a.state.Close(); e != nil {
			return e
		}
		a.state = nil
	}
	return nil
}

// OwnerCredentialKey separates the explicit local subscription vault from project request keys.
func OwnerCredentialKey(master []byte, owner string) []byte {
	return derived(master, "credentials:"+owner)
}
