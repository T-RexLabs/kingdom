// Package deployment records explicitly exported, immutable managed artifacts.
// A bundle is an operator assertion, not an execution or remote attestation.
package deployment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"strings"

	"kingdom/internal/execution/openshell/localstack"
)

const Kind = "kingdom.openshell.deployment"

var ErrInvalid = errors.New("invalid OpenShell deployment bundle")
var imagePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$`)

type Profile struct {
	SchemaVersion              int    `json:"schema_version"`
	GatewayBinarySHA256        string `json:"gateway_binary_sha256"`
	SupervisorImage            string `json:"supervisor_image"`
	SandboxRuntimeImage        string `json:"sandbox_runtime_image"`
	SandboxRuntimeBinarySHA256 string `json:"sandbox_runtime_binary_sha256"`
}

func (p Profile) Digest() string {
	raw, _ := json.Marshal(p)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

type Gateway struct {
	ID              string `json:"id"`
	Address         string `json:"address"`
	Image           string `json:"image"`
	CAFile          string `json:"ca_file"`
	CertificateFile string `json:"certificate_file"`
	KeyFile         string `json:"key_file"`
}
type Worker struct {
	Image                 string `json:"image"`
	LauncherSHA256        string `json:"launcher_sha256"`
	OpenCodeSHA256        string `json:"opencode_sha256"`
	WorkspaceHelperSHA256 string `json:"workspace_helper_sha256"`
	FileToolSHA256        string `json:"file_tool_sha256"`
}
type Callback struct {
	Listen          string   `json:"listen"`
	Origin          string   `json:"origin"`
	CertificateFile string   `json:"certificate_file"`
	KeyFile         string   `json:"key_file"`
	CAFile          string   `json:"ca_file"`
	AllowedIPs      []string `json:"allowed_ips"`
}
type Limits struct {
	Context  int `json:"context_limit"`
	Output   int `json:"output_limit"`
	Lifetime int `json:"lifetime_seconds"`
}
type Environment struct {
	Gateway  Gateway  `json:"gateway"`
	WorkerID string   `json:"worker_id"`
	Callback Callback `json:"callback"`
	Limits   Limits   `json:"limits"`
}
type Bundle struct {
	Kind          string   `json:"kind"`
	Version       int      `json:"version"`
	Gateway       Gateway  `json:"gateway"`
	Profile       Profile  `json:"profile"`
	ProfileSHA256 string   `json:"profile_sha256"`
	Worker        Worker   `json:"worker"`
	WorkerID      string   `json:"worker_id"`
	Callback      Callback `json:"callback"`
	Limits        Limits   `json:"limits"`
}

func StrictJSON(raw []byte, out any) error {
	if len(raw) == 0 || len(raw) > 256<<10 {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueValue(d, 0); err != nil {
		return ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}
func uniqueValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrInvalid
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			if !ok || seen[s] {
				return ErrInvalid
			}
			seen[s] = true
			if err = uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err = uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvalid
	}
	end, err := d.Token()
	if err != nil {
		return err
	}
	if delim == '{' && end != json.Delim('}') || delim == '[' && end != json.Delim(']') {
		return ErrInvalid
	}
	return nil
}
func Decode(raw []byte) (Bundle, error) {
	var b Bundle
	if StrictJSON(raw, &b) != nil || b.Validate() != nil {
		return Bundle{}, ErrInvalid
	}
	return b, nil
}
func hashValid(raw string) bool {
	h, e := hex.DecodeString(raw)
	return e == nil && len(h) == 32 && raw == strings.ToLower(raw)
}
func (b Bundle) Validate() error {
	if b.Kind != Kind || b.Version != 1 || b.Profile.SchemaVersion != 1 || b.ProfileSHA256 != b.Profile.Digest() {
		return ErrInvalid
	}
	for _, image := range []string{b.Gateway.Image, b.Worker.Image, b.Profile.SupervisorImage, b.Profile.SandboxRuntimeImage} {
		if !imagePattern.MatchString(image) {
			return ErrInvalid
		}
	}
	for _, hash := range []string{b.Profile.GatewayBinarySHA256, b.Profile.SandboxRuntimeBinarySHA256, b.Worker.LauncherSHA256, b.Worker.OpenCodeSHA256, b.Worker.WorkspaceHelperSHA256, b.Worker.FileToolSHA256} {
		if !hashValid(hash) {
			return ErrInvalid
		}
	}
	if b.Gateway.ID == "" || strings.ContainsAny(b.Gateway.ID, "\x00\r\n") || b.WorkerID == "" {
		return ErrInvalid
	}
	host, port, err := net.SplitHostPort(b.Gateway.Address)
	if err != nil || host == "" || port == "" || strings.ContainsAny(host, "/\\\x00\r\n") {
		return ErrInvalid
	}
	if len(b.Callback.AllowedIPs) == 0 {
		return ErrInvalid
	}
	for _, ip := range b.Callback.AllowedIPs {
		if _, err := netip.ParseAddr(ip); err != nil {
			return ErrInvalid
		}
	}
	for _, p := range []string{b.Gateway.CAFile, b.Gateway.CertificateFile, b.Gateway.KeyFile, b.Callback.CertificateFile, b.Callback.KeyFile, b.Callback.CAFile} {
		if !filepath.IsAbs(p) {
			return ErrInvalid
		}
	}
	c := b.LocalConfig()
	c.OwnerID = "00000000-0000-4000-8000-000000000001"
	c.StateRoot = "/private/kingdom"
	c.MasterKeyFile = "/private/kingdom/master.key"
	c.Model.AuthKind = "api_key"
	c.Model.APIKeyFile = "/private/model.key"
	if localstack.Validate(c) != nil {
		return ErrInvalid
	}
	return nil
}

// LocalConfig supplies deployment fields only. Identity and model authority are
// generated or explicitly selected by local configuration, never by a bundle.
func (b Bundle) LocalConfig() localstack.Config {
	var c localstack.Config
	c.Version = 1
	c.GatewayID = b.Gateway.ID
	c.WorkerID = b.WorkerID
	c.Gateway.Address = b.Gateway.Address
	c.Gateway.CAFile = b.Gateway.CAFile
	c.Gateway.CertificateFile = b.Gateway.CertificateFile
	c.Gateway.KeyFile = b.Gateway.KeyFile
	c.Image = b.Worker.Image
	c.ProfileSHA256 = b.ProfileSHA256
	c.HelperSHA256 = b.Worker.WorkspaceHelperSHA256
	c.FileToolSHA256 = b.Worker.FileToolSHA256
	c.LauncherSHA256 = b.Worker.LauncherSHA256
	c.OpenCodeSHA256 = b.Worker.OpenCodeSHA256
	c.ContextLimit = b.Limits.Context
	c.OutputLimit = b.Limits.Output
	c.LifetimeSeconds = b.Limits.Lifetime
	c.Callback.Listen = b.Callback.Listen
	c.Callback.Origin = b.Callback.Origin
	c.Callback.CertificateFile = b.Callback.CertificateFile
	c.Callback.KeyFile = b.Callback.KeyFile
	c.Callback.CAFile = b.Callback.CAFile
	c.Callback.AllowedIPs = append([]string(nil), b.Callback.AllowedIPs...)
	return c
}
