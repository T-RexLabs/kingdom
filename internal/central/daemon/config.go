package daemon

import (
	"bytes"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/google/uuid"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/modelcatalog"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/repositories"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/securefs"
)

var ErrConfiguration = errors.New("central configuration is invalid")

type ownerConfig struct {
	Issuer  string `json:"issuer"`
	Subject string `json:"subject"`
}
type oidcConfig struct {
	Issuer            string   `json:"issuer"`
	ClientID          string   `json:"client_id"`
	ClientSecretFile  string   `json:"client_secret_file"`
	AdditionalOrigins []string `json:"additional_endpoint_origins"`
}
type githubConfig struct {
	ClientID         string `json:"client_id"`
	ClientSecretFile string `json:"client_secret_file"`
}
type originConfig struct {
	Origin           string   `json:"origin"`
	PrivateAddresses []string `json:"allowed_private_addresses"`
}
type gatewayConfig struct {
	Address         string   `json:"address"`
	GatewayID       string   `json:"gateway_id"`
	WorkerID        string   `json:"worker_id"`
	CAFile          string   `json:"ca_file"`
	CertificateFile string   `json:"certificate_file"`
	KeyFile         string   `json:"key_file"`
	Image           string   `json:"worker_image"`
	Profile         string   `json:"profile_sha256"`
	Launcher        string   `json:"launcher_sha256"`
	OpenCode        string   `json:"opencode_sha256"`
	Helper          string   `json:"workspace_helper_sha256"`
	FileTool        string   `json:"file_tool_sha256"`
	AllowedIPs      []string `json:"allowed_service_ips"`
	Lifetime        string   `json:"server_lifetime"`
	ContextLimit    int      `json:"context_limit"`
	OutputLimit     int      `json:"output_limit"`
}
type repositoryAppConfig struct {
	AppID          int64                `json:"app_id"`
	PrivateKeyFile string               `json:"private_key_file"`
	Grants         []repositories.Grant `json:"grants"`
}
type personalAccountConfig struct {
	OpenCodeBinary string `json:"opencode_binary"`
	RuntimeRoot    string `json:"runtime_root"`
}

type fileConfig struct {
	RepositoryApp    *repositoryAppConfig   `json:"repository_app"`
	Version          int                    `json:"version"`
	Listen           string                 `json:"listen"`
	PublicOrigin     string                 `json:"public_origin"`
	WorkerOrigin     string                 `json:"worker_origin"`
	DataRoot         string                 `json:"data_root"`
	MasterKeyFile    string                 `json:"master_key_file"`
	TLSCertificate   string                 `json:"tls_certificate_file"`
	TLSKey           string                 `json:"tls_key_file"`
	InitialOwners    []ownerConfig          `json:"initial_owners"`
	OIDC             *oidcConfig            `json:"oidc"`
	GitHub           *githubConfig          `json:"github"`
	Gateway          gatewayConfig          `json:"openshell"`
	ModelOrigins     []originConfig         `json:"model_origins"`
	ModelProfiles    []modelcatalog.Profile `json:"model_profiles"`
	ProviderCAFile   string                 `json:"provider_ca_file"`
	PersonalAccounts *personalAccountConfig `json:"personal_accounts"`
}
type configuration struct {
	repositories              *repositories.Config
	file                      fileConfig
	key                       []byte
	certificate               tls.Certificate
	endpoint                  worker.Endpoint
	profile                   []byte
	lifetime                  time.Duration
	contextLimit, outputLimit int
	owners                    []catalog.Identity
	oidc                      *auth.OIDCConfig
	github                    *auth.GitHubConfig
	origins                   []modelproxy.OriginPolicy
	roots                     *x509.CertPool
}

func load(path string) (result configuration, err error) {
	defer func() {
		if err != nil {
			clear(result.key)
			result.key = nil
		}
	}()
	raw, e := os.Open(path)
	if e != nil {
		return result, ErrConfiguration
	}
	defer raw.Close()
	encoded, e := io.ReadAll(io.LimitReader(raw, (128<<10)+1))
	if e != nil || len(encoded) > 128<<10 {
		return result, ErrConfiguration
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var f fileConfig
	if decoder.Decode(&f) != nil {
		return result, ErrConfiguration
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return result, ErrConfiguration
	}
	if f.Version != 1 || f.PublicOrigin == "" || f.DataRoot == "" || f.MasterKeyFile == "" || f.TLSCertificate == "" || f.TLSKey == "" || len(f.InitialOwners) == 0 || f.OIDC == nil && f.GitHub == nil {
		return result, ErrConfiguration
	}
	origin, e := url.Parse(f.PublicOrigin)
	if e != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.Path != "" || origin.RawPath != "" || origin.Opaque != "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" {
		return result, ErrConfiguration
	}
	if f.Listen == "" {
		f.Listen = "127.0.0.1:9090"
	}
	if _, _, e = net.SplitHostPort(f.Listen); e != nil {
		return result, ErrConfiguration
	}
	base, e := filepath.Abs(filepath.Dir(path))
	if e != nil {
		return result, ErrConfiguration
	}
	resolve := func(value string) string {
		if value == "" {
			return ""
		}
		if filepath.IsAbs(value) {
			return filepath.Clean(value)
		}
		return filepath.Join(base, value)
	}
	f.DataRoot = resolve(f.DataRoot)
	if f.PersonalAccounts == nil {
		f.PersonalAccounts = &personalAccountConfig{OpenCodeBinary: "/usr/local/bin/opencode", RuntimeRoot: "/run/kingdom/accounts"}
	}
	f.PersonalAccounts.OpenCodeBinary = resolve(f.PersonalAccounts.OpenCodeBinary)
	f.PersonalAccounts.RuntimeRoot = resolve(f.PersonalAccounts.RuntimeRoot)
	if f.PersonalAccounts.OpenCodeBinary == "" || f.PersonalAccounts.RuntimeRoot == "" {
		return result, ErrConfiguration
	}
	f.MasterKeyFile = resolve(f.MasterKeyFile)
	key, e := readFile(f.MasterKeyFile, true, 32)
	if e != nil || len(key) != 32 {
		return result, ErrConfiguration
	}
	result.key = key
	certificate, e := readFile(resolve(f.TLSCertificate), false, 1<<20)
	if e != nil {
		return result, ErrConfiguration
	}
	tlsKey, e := readFile(resolve(f.TLSKey), true, 1<<20)
	if e != nil {
		return result, ErrConfiguration
	}
	defer clear(tlsKey)
	result.certificate, e = tls.X509KeyPair(certificate, tlsKey)
	if e != nil {
		return result, ErrConfiguration
	}
	if f.OIDC != nil {
		secret, e := readText(resolve(f.OIDC.ClientSecretFile))
		if e != nil {
			return result, ErrConfiguration
		}
		result.oidc = &auth.OIDCConfig{Issuer: f.OIDC.Issuer, ClientID: f.OIDC.ClientID, ClientSecret: secret, AdditionalEndpointOrigins: f.OIDC.AdditionalOrigins}
	}
	if f.RepositoryApp != nil {
		if f.RepositoryApp.AppID <= 0 || len(f.RepositoryApp.Grants) == 0 || len(f.RepositoryApp.Grants) > 1000 {
			return result, ErrConfiguration
		}
		for _, grant := range f.RepositoryApp.Grants {
			id, e := uuid.Parse(grant.ProjectID)
			if e != nil || id == uuid.Nil || id.String() != grant.ProjectID {
				return result, ErrConfiguration
			}
		}
		raw, e := readFile(resolve(f.RepositoryApp.PrivateKeyFile), true, 64<<10)
		if e != nil {
			return result, ErrConfiguration
		}
		block, rest := pem.Decode(raw)
		invalidPEM := block == nil || len(bytes.TrimSpace(rest)) != 0
		clear(raw)
		if invalidPEM {
			return result, ErrConfiguration
		}
		var key *rsa.PrivateKey
		if block.Type == "RSA PRIVATE KEY" {
			key, e = x509.ParsePKCS1PrivateKey(block.Bytes)
		} else if block.Type == "PRIVATE KEY" {
			var parsed any
			parsed, e = x509.ParsePKCS8PrivateKey(block.Bytes)
			if e == nil {
				var ok bool
				key, ok = parsed.(*rsa.PrivateKey)
				if !ok {
					return result, ErrConfiguration
				}
			}
		} else {
			return result, ErrConfiguration
		}
		clear(block.Bytes)
		if e != nil {
			return result, ErrConfiguration
		}
		config := repositories.Config{AppID: f.RepositoryApp.AppID, PrivateKey: key, Grants: f.RepositoryApp.Grants}
		probe, e := repositories.NewClient(config)
		if e != nil {
			return result, ErrConfiguration
		}
		probe.Close()
		result.repositories = &config
	}
	if f.GitHub != nil {
		secret, e := readText(resolve(f.GitHub.ClientSecretFile))
		if e != nil {
			return result, ErrConfiguration
		}
		result.github = &auth.GitHubConfig{ClientID: f.GitHub.ClientID, ClientSecret: secret}
	}
	for _, o := range f.InitialOwners {
		if o.Issuer == "" || o.Subject == "" {
			return result, ErrConfiguration
		}
		result.owners = append(result.owners, catalog.Identity{Issuer: o.Issuer, Subject: o.Subject})
	}
	g := f.Gateway
	result.endpoint = worker.Endpoint{Address: g.Address, CAFile: resolve(g.CAFile), CertificateFile: resolve(g.CertificateFile), KeyFile: resolve(g.KeyFile)}
	if _, _, e = net.SplitHostPort(g.Address); e != nil || g.GatewayID == "" {
		return result, ErrConfiguration
	}
	endpointKey, e := readFile(result.endpoint.KeyFile, true, 1<<20)
	if e != nil {
		return result, ErrConfiguration
	}
	clear(endpointKey)
	result.profile, e = hex.DecodeString(g.Profile)
	if e != nil || len(result.profile) != 32 || hex.EncodeToString(result.profile) != g.Profile {
		return result, ErrConfiguration
	}
	for _, hash := range []string{g.Launcher, g.OpenCode, g.Helper, g.FileTool} {
		b, e := hex.DecodeString(hash)
		if e != nil || len(b) != 32 || hex.EncodeToString(b) != hash {
			return result, ErrConfiguration
		}
	}
	if g.Lifetime == "" {
		g.Lifetime = "1h"
	}
	result.lifetime, e = time.ParseDuration(g.Lifetime)
	if e != nil || result.lifetime < time.Second || result.lifetime > 24*time.Hour || result.lifetime%time.Second != 0 {
		return result, ErrConfiguration
	}
	result.contextLimit, result.outputLimit = g.ContextLimit, g.OutputLimit
	if result.contextLimit == 0 {
		result.contextLimit = 100000
	}
	if result.outputLimit == 0 {
		result.outputLimit = 4096
	}
	if result.contextLimit < 1 || result.contextLimit > 2000000 || result.outputLimit < 1 || result.outputLimit > 200000 || result.outputLimit > result.contextLimit {
		return result, ErrConfiguration
	}
	if g.WorkerID == "" {
		f.Gateway.WorkerID = "kingdom-central"
	}
	if len(f.ModelOrigins) == 0 {
		f.ModelOrigins = []originConfig{{Origin: "https://api.openai.com"}}
	}
	for _, o := range f.ModelOrigins {
		p := modelproxy.OriginPolicy{Origin: o.Origin}
		for _, value := range o.PrivateAddresses {
			address, e := netip.ParsePrefix(value)
			if e != nil {
				return result, ErrConfiguration
			}
			p.AllowedPrivateAddresses = append(p.AllowedPrivateAddresses, address)
		}
		result.origins = append(result.origins, p)
	}
	if f.ProviderCAFile != "" {
		ca, e := readFile(resolve(f.ProviderCAFile), false, 1<<20)
		if e != nil {
			return result, ErrConfiguration
		}
		result.roots, e = x509.SystemCertPool()
		if e != nil {
			result.roots = x509.NewCertPool()
		}
		if !result.roots.AppendCertsFromPEM(ca) {
			return result, ErrConfiguration
		}
	}
	result.file = f
	return result, nil
}
func readFile(path string, private bool, max int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrConfiguration
	}
	root, e := securefs.OpenRoot(filepath.Dir(path))
	if e != nil {
		return nil, ErrConfiguration
	}
	defer root.Close()
	file, e := root.OpenRegular(filepath.Base(path))
	if e != nil {
		return nil, ErrConfiguration
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil || private && info.Mode().Perm()&0077 != 0 || info.Size() < 1 || info.Size() > max {
		return nil, ErrConfiguration
	}
	b, e := io.ReadAll(io.LimitReader(file, max+1))
	if e != nil || int64(len(b)) > max {
		return nil, ErrConfiguration
	}
	return b, nil
}
func readText(path string) (string, error) {
	b, e := readFile(path, true, 8192)
	if e != nil {
		return "", e
	}
	defer clear(b)
	v := strings.TrimSpace(string(b))
	if v == "" || strings.ContainsAny(v, "\r\n\x00") {
		return "", ErrConfiguration
	}
	return v, nil
}
