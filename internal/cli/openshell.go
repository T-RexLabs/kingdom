package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"kingdom/internal/execution/openshell/deployment"
	"kingdom/internal/execution/openshell/localowner"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/securefs"
	"net/netip"
	"path/filepath"
	"strings"
	"time"
)

func setupRead(path string, private bool) ([]byte, error) {
	if private {
		return localstack.ReadPrivate(path, 256<<10)
	}
	root, e := securefs.OpenRoot(filepath.Dir(path))
	if e != nil {
		return nil, e
	}
	defer root.Close()
	return root.ReadRegular(filepath.Base(path), 256<<10)
}
func absoluteOption(path string) string {
	if path == "" {
		return ""
	}
	v, e := filepath.Abs(path)
	if e != nil {
		return ""
	}
	return v
}
func runOpenShellCommand(ctx context.Context, out io.Writer, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		writeCommandHelp(out, []string{"openshell"})
		return nil
	}
	if len(args) > 1 && (args[1] == "--help" || args[1] == "help") {
		writeCommandHelp(out, []string{"openshell", args[0]})
		return nil
	}
	switch args[0] {
	case "export":
		return runOpenShellExport(ctx, out, args[1:])
	case "configure":
		return runOpenShellConfigure(ctx, out, args[1:])
	case "check":
		return runOpenShellCheck(ctx, out, args[1:])
	default:
		return errors.New("unknown OpenShell command")
	}
}
func setupFlags(name string) *flag.FlagSet {
	f := flag.NewFlagSet("openshell "+name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}
func runOpenShellExport(ctx context.Context, out io.Writer, args []string) error {
	f := setupFlags("export")
	dockerContext := f.String("docker-context", "", "Explicit Docker context")
	gateway := f.String("gateway-image", "", "Pinned gateway image")
	worker := f.String("worker-image", "", "Pinned worker image")
	profilePath := f.String("profile", "", "Reviewed profile JSON")
	envPath := f.String("environment", "", "Private environment JSON")
	destination := f.String("out", "", "New private bundle file")
	if err := f.Parse(args); err != nil || f.NArg() != 0 || *dockerContext == "" || *destination == "" {
		return errors.New("export requires --docker-context, --gateway-image, --worker-image, --profile, --environment, and --out")
	}
	var profile deployment.Profile
	raw, e := setupRead(absoluteOption(*profilePath), false)
	if e != nil {
		return e
	}
	if e = deployment.StrictJSON(raw, &profile); e != nil {
		return e
	}
	var env deployment.Environment
	raw, e = setupRead(absoluteOption(*envPath), true)
	if e != nil {
		return e
	}
	if e = deployment.StrictJSON(raw, &env); e != nil {
		return e
	}
	b, e := deployment.Export(ctx, deployment.ExportOptions{DockerContext: *dockerContext, GatewayImage: *gateway, WorkerImage: *worker, Profile: profile, Environment: env})
	if e != nil {
		return e
	}
	if e = localstack.CheckTLS(b.LocalConfig()); e != nil {
		return e
	}
	raw, e = json.MarshalIndent(b, "", "  ")
	if e != nil {
		return e
	}
	path := absoluteOption(*destination)
	root, e := securefs.OpenRoot(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer root.Close()
	if _, e = root.CreateRegularExact(filepath.Base(path), append(raw, '\n'), 0600, 0700); e != nil {
		return e
	}
	_, e = fmt.Fprintln(out, "OpenShell deployment bundle exported. No gateway or sandbox was started.")
	return e
}

type openShellOptions struct {
	Config, Bundle, StateRoot, APIKeyFile, Model, Thinking, ModelProfile, Registration, Owner, MasterKey, ModelCA string
	ModelPrivate                                                                                                  setupCIDRs
}
type setupCIDRs []string

func (v *setupCIDRs) String() string { return strings.Join(*v, ",") }
func (v *setupCIDRs) Set(raw string) error {
	if _, err := netip.ParsePrefix(raw); err != nil {
		return errors.New("invalid model private address CIDR")
	}
	*v = append(*v, raw)
	return nil
}

func runOpenShellConfigure(ctx context.Context, out io.Writer, args []string) error {
	f := setupFlags("configure")
	var o openShellOptions
	f.StringVar(&o.Config, "config", ".kingdom/config.yaml", "Project configuration")
	f.StringVar(&o.Bundle, "bundle", "", "Private deployment bundle")
	f.StringVar(&o.StateRoot, "state-root", "", "Private managed state directory")
	f.StringVar(&o.APIKeyFile, "api-key-file", "", "Private API key file")
	f.StringVar(&o.Model, "model", "", "Exact OpenAI model ID")
	f.StringVar(&o.Thinking, "thinking-effort", "", "Exact thinking effort")
	f.StringVar(&o.ModelProfile, "model-profile", "", "Advanced provider/model JSON")
	f.StringVar(&o.ModelCA, "model-ca-file", "", "Explicit private model CA file")
	f.Var(&o.ModelPrivate, "model-allowed-private-address", "Explicit private model CIDR (repeatable)")
	f.StringVar(&o.Registration, "chatgpt-registration", "", "Existing Kingdom ChatGPT registration")
	f.StringVar(&o.Owner, "owner-id", "", "Existing ChatGPT owner ID")
	f.StringVar(&o.MasterKey, "master-key-file", "", "Existing ChatGPT master key file")
	if e := f.Parse(args); e != nil || f.NArg() != 0 {
		return errors.New("invalid OpenShell configure flags")
	}
	return configureOpenShell(ctx, out, o, deployment.Check)
}
func setupModel(o openShellOptions) (localstack.ModelConfig, error) {
	var m localstack.ModelConfig
	if o.Registration != "" {
		if o.APIKeyFile != "" || o.Model != "" || o.Thinking != "" || o.ModelProfile != "" || o.ModelCA != "" || len(o.ModelPrivate) > 0 || o.Owner == "" || o.MasterKey == "" || o.StateRoot == "" {
			return m, errors.New("ChatGPT configuration requires existing --state-root, --owner-id, --master-key-file and --chatgpt-registration only")
		}
		m.AuthKind = "chatgpt"
		m.RegistrationID = o.Registration
		return m, nil
	}
	if o.APIKeyFile == "" || o.Owner != "" || o.MasterKey != "" || (o.Model == "") == (o.ModelProfile == "") || o.ModelProfile != "" && o.Thinking != "" {
		return m, errors.New("use --api-key-file with --model, or an advanced --model-profile")
	}
	m.AuthKind = "api_key"
	m.APIKeyFile = absoluteOption(o.APIKeyFile)
	m.CAFile = absoluteOption(o.ModelCA)
	m.AllowedPrivateAddresses = append([]string(nil), o.ModelPrivate...)
	for _, raw := range m.AllowedPrivateAddresses {
		if _, err := netip.ParsePrefix(raw); err != nil {
			return m, errors.New("invalid model private address CIDR")
		}
	}
	if o.ModelProfile != "" {
		raw, e := setupRead(absoluteOption(o.ModelProfile), false)
		if e != nil {
			return m, e
		}
		if e = deployment.StrictJSON(raw, &m.Definition); e != nil {
			return m, e
		}
	} else {
		efforts := []string{}
		if o.Thinking != "" {
			efforts = append(efforts, o.Thinking)
		}
		m.Definition = localowner.Definition{Name: "OpenAI", Provider: "openai", Endpoint: "https://api.openai.com/v1", Models: []localowner.Model{{ID: o.Model, Efforts: efforts}}}
	}
	if m.Definition.Validate() != nil {
		return m, errors.New("invalid explicit model metadata")
	}
	return m, nil
}
func runOpenShellCheck(ctx context.Context, out io.Writer, args []string) error {
	f := setupFlags("check")
	path := f.String("config", ".kingdom/config.yaml", "Project configuration")
	if e := f.Parse(args); e != nil || f.NArg() != 0 {
		return errors.New("invalid OpenShell check flags")
	}
	config, e := readInstallConfig(absoluteOption(*path))
	if e != nil {
		return e
	}
	if config.Execution.Backend != "openshell" {
		return errors.New("project does not use OpenShell")
	}
	operator, e := localstack.Load(config.Execution.ManagedConfig)
	if e != nil {
		return e
	}
	record, e := readSetupRecord(operator.StateRoot)
	if e != nil {
		return e
	}
	if record.ProjectConfig != absoluteOption(*path) {
		return errors.New("managed setup belongs to another project configuration")
	}
	if e = verifySetupRecord(record, operator); e != nil {
		return e
	}
	if e = localstack.CheckFiles(operator); e != nil {
		return e
	}
	if e = checkSetupChatGPT(ctx, operator); e != nil {
		return e
	}
	check, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	result, e := deployment.Check(check, record.Bundle)
	if e != nil {
		return e
	}
	return json.NewEncoder(out).Encode(result)
}
