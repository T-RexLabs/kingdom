// Package providerlauncher owns a fixed OpenCode child through its stdin lease.
package providerlauncher

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/types/known/durationpb"
	"io"
	pb "kingdom/internal/execution/openshell/managedpb"
	"net/url"
	"strings"
	"time"
)

const JobVersion = 2
const CommandName = "serve-v2"
const ChatCompletionsAPI = "chat_completions"
const ResponsesAPI = "responses"

const LauncherPath = "/usr/local/libexec/kingdom-provider-launcher"
const OpenCodePath = "/usr/local/bin/opencode"
const PluginPath = "/usr/local/lib/kingdom-plugin"
const WorkspacePath = "/workspace/project"
const RuntimePath = "/workspace/.kingdom-provider"
const Port uint32 = 4098
const ProviderPort uint32 = 4096
const MaxJobBytes = 16 << 10
const MaxOutputBytes = 4096
const OpenCodeVersion = "2.0.30"

// Tokens are invocation-scoped capabilities, never upstream API/OAuth secrets.
// The worker encrypts this exact job before dispatch. Immutable image policy
// must deny writes to all executable/plugin paths and their ancestors.
type Job struct {
	Version         int    `json:"version"`
	LauncherSHA256  string `json:"launcher_sha256"`
	OpenCodeSHA256  string `json:"opencode_sha256"`
	ControlToken    string `json:"control_token"`
	ServerPassword  string `json:"server_password"`
	ProxyURL        string `json:"proxy_url"`
	ProxyToken      string `json:"proxy_token"`
	ProviderID      string `json:"provider_id"`
	ModelID         string `json:"model_id"`
	ModelAPI        string `json:"model_api"`
	CallbackURL     string `json:"callback_url"`
	CallbackToken   string `json:"callback_token"`
	InstanceID      string `json:"instance_id"`
	ContextLimit    int    `json:"context_limit"`
	OutputLimit     int    `json:"output_limit"`
	LifetimeSeconds int64  `json:"lifetime_seconds"`
}

func (Job) String() string   { return "scoped provider launcher job" }
func (Job) GoString() string { return "scoped provider launcher job" }
func hex256(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}
func modelCapability(s string) bool {
	b, err := base64.RawURLEncoding.DecodeString(s)
	return err == nil && len(b) == 32 && base64.RawURLEncoding.EncodeToString(b) == s
}
func identifier(s string) bool {
	if s == "" || len(s) > 256 || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < 33 || r == 127 {
			return false
		}
	}
	return true
}
func endpoint(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.Opaque == "" && len(s) <= 2048
}
func (j Job) Validate() error {
	if j.Version != JobVersion || !hex256(j.LauncherSHA256) || !hex256(j.OpenCodeSHA256) || !hex256(j.ControlToken) || !hex256(j.ServerPassword) || !modelCapability(j.ProxyToken) || !modelCapability(j.CallbackToken) || !endpoint(j.ProxyURL) || !endpoint(j.CallbackURL) || !identifier(j.ProviderID) || strings.Contains(j.ProviderID, "/") || !identifier(j.ModelID) || !identifier(j.InstanceID) || j.ContextLimit < 1 || j.ContextLimit > 2000000 || j.OutputLimit < 1 || j.OutputLimit > 200000 || j.OutputLimit > j.ContextLimit || j.LifetimeSeconds < 1 || j.LifetimeSeconds > 86400 {
		return errors.New("invalid provider launcher job")
	}
	if j.ModelAPI != ChatCompletionsAPI && j.ModelAPI != ResponsesAPI {
		return errors.New("invalid provider model API")
	}
	return nil
}
func DecodeJob(r io.Reader) (Job, error) {
	var j Job
	b, e := io.ReadAll(io.LimitReader(r, MaxJobBytes+1))
	if e != nil {
		return j, e
	}
	if len(b) > MaxJobBytes {
		return j, errors.New("provider launcher job exceeds limit")
	}
	keys := json.NewDecoder(bytes.NewReader(b))
	opening, err := keys.Token()
	if err != nil || opening != json.Delim('{') {
		return Job{}, errors.New("invalid provider launcher job")
	}
	seen := make(map[string]bool)
	for keys.More() {
		key, err := keys.Token()
		name, ok := key.(string)
		if err != nil || !ok || seen[name] {
			return Job{}, errors.New("duplicate provider launcher field")
		}
		seen[name] = true
		var value json.RawMessage
		if err := keys.Decode(&value); err != nil {
			return Job{}, errors.New("invalid provider launcher job")
		}
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&j); e != nil {
		return Job{}, errors.New("invalid provider launcher job")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return Job{}, errors.New("trailing provider launcher input")
	}
	return j, j.Validate()
}
func Command(j Job) (*pb.CommandSpec, error) {
	if e := j.Validate(); e != nil {
		return nil, e
	}
	b, e := json.Marshal(j)
	if e != nil {
		return nil, e
	}
	return &pb.CommandSpec{Argv: []string{LauncherPath, CommandName}, Workdir: WorkspacePath, Stdin: append(b, '\n'), ExecutionTimeout: durationpb.New(time.Duration(j.LifetimeSeconds) * time.Second), OutputLimitBytes: MaxOutputBytes}, nil
}
