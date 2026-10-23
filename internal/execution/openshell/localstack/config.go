// Package localstack assembles explicitly opted-in local OpenShell execution.
package localstack

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"kingdom/internal/execution/openshell/localowner"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/securefs"
)

var ErrConfig = errors.New("invalid local OpenShell operator configuration")

type Config struct {
	Version   int    `json:"version"`
	StateRoot string `json:"state_root"`
	OwnerID   string `json:"owner_id"`
	GatewayID string `json:"gateway_id"`
	WorkerID  string `json:"worker_id"`
	Gateway   struct {
		Address         string `json:"address"`
		CAFile          string `json:"ca_file"`
		CertificateFile string `json:"certificate_file"`
		KeyFile         string `json:"key_file"`
	} `json:"gateway"`
	Image           string `json:"image"`
	ProfileSHA256   string `json:"profile_sha256"`
	HelperSHA256    string `json:"helper_sha256"`
	FileToolSHA256  string `json:"file_tool_sha256"`
	LauncherSHA256  string `json:"launcher_sha256"`
	OpenCodeSHA256  string `json:"opencode_sha256"`
	ContextLimit    int    `json:"context_limit"`
	OutputLimit     int    `json:"output_limit"`
	LifetimeSeconds int    `json:"lifetime_seconds"`
	MasterKeyFile   string `json:"master_key_file"`
	Callback        struct {
		Listen          string   `json:"listen"`
		Origin          string   `json:"origin"`
		CertificateFile string   `json:"certificate_file"`
		KeyFile         string   `json:"key_file"`
		CAFile          string   `json:"ca_file"`
		AllowedIPs      []string `json:"allowed_ips"`
	} `json:"callback"`
	Model ModelConfig `json:"model"`
}

// privateFile uses a no-follow directory traversal and verifies the opened
// descriptor. Secrets and operator configuration are never loaded by defaults.
func privateFile(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrConfig
	}
	root, e := securefs.OpenRoot(filepath.Dir(path))
	if e != nil {
		return nil, ErrConfig
	}
	defer root.Close()
	f, e := root.OpenRegular(filepath.Base(path))
	if e != nil {
		return nil, ErrConfig
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > limit {
		return nil, ErrConfig
	}
	raw, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || int64(len(raw)) > limit {
		return nil, ErrConfig
	}
	return raw, nil
}
func Load(path string) (Config, error) {
	var c Config
	raw, e := privateFile(path, 256<<10)
	if e != nil {
		return c, e
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF {
		return c, ErrConfig
	}
	if c.Version != 1 || !filepath.IsAbs(c.StateRoot) || c.GatewayID == "" || c.WorkerID == "" || c.ContextLimit < 1 || c.OutputLimit < 1 || c.LifetimeSeconds < 1 || c.LifetimeSeconds > 86400 {
		return c, ErrConfig
	}
	for _, v := range []string{c.ProfileSHA256, c.HelperSHA256, c.FileToolSHA256, c.LauncherSHA256, c.OpenCodeSHA256} {
		b, e := hex.DecodeString(v)
		if e != nil || len(b) != 32 || v != strings.ToLower(v) {
			return c, ErrConfig
		}
	}
	parts := strings.Split(c.Image, "@sha256:")
	if len(parts) != 2 || parts[0] == "" {
		return c, ErrConfig
	}
	b, e := hex.DecodeString(parts[1])
	if e != nil || len(b) != 32 {
		return c, ErrConfig
	}
	for _, p := range []string{c.Gateway.CAFile, c.Gateway.CertificateFile, c.Gateway.KeyFile, c.MasterKeyFile, c.Callback.CertificateFile, c.Callback.KeyFile, c.Callback.CAFile} {
		if !filepath.IsAbs(p) {
			return c, ErrConfig
		}
	}
	if c.Model.AuthKind == "api_key" {
		if !filepath.IsAbs(c.Model.APIKeyFile) || c.Model.RegistrationID != "" {
			return c, ErrConfig
		}
	} else if c.Model.AuthKind == "chatgpt" {
		if c.Model.RegistrationID == "" || c.Model.APIKeyFile != "" || c.Model.CAFile != "" || len(c.Model.AllowedPrivateAddresses) != 0 || len(c.Model.Definition.Models) != 0 || (c.Model.Definition.Provider != "" && c.Model.Definition.Provider != "openai") || (c.Model.Definition.Endpoint != "" && c.Model.Definition.Endpoint != "https://api.openai.com/v1") {
			return c, ErrConfig
		}
	} else {
		return c, ErrConfig
	}
	u, e := url.Parse(c.Callback.Origin)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.Path != "" || u.RawPath != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return c, ErrConfig
	}
	_, port, e := net.SplitHostPort(c.Callback.Listen)
	if e != nil || port == "0" {
		return c, ErrConfig
	}
	advertised := u.Port()
	if advertised == "" {
		advertised = "443"
	}
	if advertised != port {
		return c, ErrConfig
	}
	return c, nil
}
func (c Config) callbackTLS() (*tls.Config, error) {
	key, e := privateFile(c.Callback.KeyFile, 64<<10)
	if e != nil {
		return nil, e
	}
	defer clear(key)
	certificate, e := os.ReadFile(c.Callback.CertificateFile)
	if e != nil {
		return nil, ErrConfig
	}
	cert, e := tls.X509KeyPair(certificate, key)
	if e != nil || len(cert.Certificate) == 0 {
		return nil, ErrConfig
	}
	raw, e := os.ReadFile(c.Callback.CAFile)
	if e != nil {
		return nil, ErrConfig
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		return nil, ErrConfig
	}
	leaf, e := x509.ParseCertificate(cert.Certificate[0])
	if e != nil {
		return nil, ErrConfig
	}
	intermediates := x509.NewCertPool()
	for _, raw := range cert.Certificate[1:] {
		v, e := x509.ParseCertificate(raw)
		if e != nil {
			return nil, ErrConfig
		}
		intermediates.AddCert(v)
	}
	u, _ := url.Parse(c.Callback.Origin)
	if _, e = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: u.Hostname(), CurrentTime: time.Now()}); e != nil {
		return nil, ErrConfig
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}, nil
}

type ModelConfig struct {
	AuthKind       string `json:"auth_kind"`
	RegistrationID string `json:"registration_id,omitempty"`

	ConnectionID            string                `json:"connection_id"`
	Definition              localowner.Definition `json:"definition"`
	APIKeyFile              string                `json:"api_key_file"`
	CAFile                  string                `json:"ca_file,omitempty"`
	AllowedPrivateAddresses []string              `json:"allowed_private_addresses,omitempty"`
}
