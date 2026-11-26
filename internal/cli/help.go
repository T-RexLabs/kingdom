package cli

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

type flagBinder func(*flag.FlagSet)

type helpTopic struct {
	path               []string
	summary            string
	usage              []string
	flags              flagBinder
	flagOrder          []string
	flagAliases        [][]string
	notes              []string
	includeGlobalFlags bool
	subcommands        [][]string
}

type rootHelpSection struct {
	title       string
	commands    [][]string
	description string
}

type globalFlagHelp struct {
	syntax      string
	description string
}

var rootHelpSections = []rootHelpSection{
	{
		title:       "Workspace and Runtime",
		description: "Bootstrap and manage the local Kingdom runtime.",
		commands: [][]string{
			{"init"},
			{"clean"},
			{"summon"},
			{"summon", "status"},
			{"summon", "stop"},
			{"chatgpt"},
			{"openshell"},
			{"connections", "github", "key", "provision"},
			{"connections", "github", "key", "rotate"},
		},
	},
	{
		title:       "Work Orders and Runs",
		description: "Create work orders and inspect or control execution.",
		commands: [][]string{
			{"create", "work-order"},
			{"run", "start"},
			{"run", "status"},
			{"run", "resume"},
			{"graph", "show"},
			{"node", "rerun-from"},
			{"timeline", "follow"},
		},
	},
	{
		title:       "Artifacts and Approvals",
		description: "Export run outputs and record operator decisions.",
		commands: [][]string{
			{"run", "artifacts", "export"},
			{"approvals", "list"},
			{"approvals", "plan-decide"},
			{"approvals", "decide"},
		},
	},
}

var globalHelpFlags = []globalFlagHelp{
	{syntax: "--server <url>", description: "API base URL (default: http://127.0.0.1:8080)"},
	{syntax: "--credential-file <path>", description: "protected local operator credential file (normally discovered from .kingdom)"},
	{syntax: "--idempotency-key <key>", description: "resume one ambiguous mutation with the exact key reported by kd"},
	{syntax: "--json", description: "emit JSON output where supported"},
	{syntax: "--timeout <duration>", description: "request timeout for non-streaming API commands (default: 30s)"},
}

var helpTopics = []*helpTopic{
	{path: []string{"openshell"}, summary: "Configure optional local OpenShell execution.", usage: []string{"kd openshell export|configure|check [flags]"}, subcommands: [][]string{{"openshell", "export"}, {"openshell", "configure"}, {"openshell", "check"}}, notes: []string{"Uses an existing reviewed gateway. Does not build, install, or provision OpenShell. Normal local execution never reads a deployment bundle."}},
	{path: []string{"openshell", "export"}, summary: "Export exact artifacts from an explicit Docker context.", usage: []string{"kd openshell export --docker-context CONTEXT --gateway-image REPO@sha256:DIGEST --worker-image REPO@sha256:DIGEST --profile PROFILE.json --environment ENVIRONMENT.json --out BUNDLE.json"}, notes: []string{"Reads existing pinned ARM64 images. Temporary inspection containers are never started. The private environment JSON supplies gateway and callback TLS file references, addresses, worker ID, and limits."}},
	{path: []string{"openshell", "configure"}, summary: "Enable local OpenShell with one retained owner and key.", usage: []string{"kd openshell configure --bundle BUNDLE.json --api-key-file KEY --model MODEL [--thinking-effort EFFORT] [--config .kingdom/config.yaml] [--state-root PATH]", "kd openshell configure --bundle BUNDLE.json --api-key-file KEY --model-profile MODELS.json [--model-ca-file CA] [--model-allowed-private-address CIDR]", "kd openshell configure --bundle BUNDLE.json --chatgpt-registration ID --owner-id UUID --master-key-file KEY --state-root PATH"}, notes: []string{"Run kd init first and stop the project runtime. Exact retries preserve identities and key material. Different inputs never silently replace a retained setup. Private model CIDRs can be repeated; they are never inferred."}},
	{path: []string{"openshell", "check"}, summary: "Check private configuration and authenticated gateway metadata.", usage: []string{"kd openshell check [--config .kingdom/config.yaml]"}, notes: []string{"Uses read-only Health and GetGatewayInfo calls. It does not create a sandbox, run a provider, refresh credentials, or attest a remote deployment. Normal managed allocation checks the actual profile and worker pins."}},

	{path: []string{"chatgpt"}, summary: "Connect a local ChatGPT account.", usage: []string{"kd chatgpt sign-in|status|disconnect --state-root <path> --owner-id <uuid> --master-key-file <path> [--registration-id <uuid>]"}, notes: []string{"Uses Kingdom's own local registration. Shared hosted connections require an approved client."}},
	{
		path: []string{"connections"}, summary: "Manage local integration credentials.", usage: []string{"kd connections github key provision --connection-id <id> --from <pem>", "kd connections github key rotate --connection-id <id> --from <pem>"}, subcommands: [][]string{{"connections", "github", "key", "provision"}, {"connections", "github", "key", "rotate"}}, includeGlobalFlags: true,
	},
	{
		path: []string{"connections", "github", "key", "provision"}, summary: "Provision a missing GitHub App private key.", usage: []string{"kd connections github key provision --connection-id <id> --from <pem>"}, flags: bindGitHubKeyHelpFlags, flagOrder: []string{"connection-id", "from"}, includeGlobalFlags: true, notes: []string{"Requires a live saved connection and refuses to overwrite an existing key."},
	},
	{
		path: []string{"connections", "github", "key", "rotate"}, summary: "Atomically rotate an existing GitHub App private key.", usage: []string{"kd connections github key rotate --connection-id <id> --from <pem>"}, flags: bindGitHubKeyHelpFlags, flagOrder: []string{"connection-id", "from"}, includeGlobalFlags: true, notes: []string{"Requires a live saved connection and refuses to create a missing key."},
	},
	{
		path:    []string{"init"},
		summary: "Initialize Kingdom in the current project.",
		usage:   []string{"kd init [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindInitFlags(flags, &initCommandOptions{})
		},
		notes: []string{"Use `--force` to overwrite generated scaffold files."},
	},
	{
		path:    []string{"clean"},
		summary: "Remove the local Kingdom scaffold.",
		usage:   []string{"kd clean [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindCleanFlags(flags, &cleanCommandOptions{})
		},
		notes: []string{"`kd clean` stops a running local summon instance before removing `.kingdom/`."},
	},
	{
		path:        []string{"summon"},
		summary:     "Start or manage the local runtime.",
		usage:       []string{"kd summon [flags]", "kd summon status [flags]", "kd summon logs [flags]", "kd summon stop [flags]"},
		subcommands: [][]string{{"summon"}, {"summon", "status"}, {"summon", "logs"}, {"summon", "stop"}},
		flags: func(flags *flag.FlagSet) {
			bindSummonFlags(flags, &summonOptions{})
		},
		flagAliases: [][]string{{"d", "detach"}},
		notes:       []string{"`kd summon` uses `.kingdom/config.yaml` by default and keeps the runtime attached to the current terminal. Use `-d` or `--detach` to start it in the background. Use `--listen` to fully override the listen address for this summon process only, or `--host` and `--port` to override one side of the configured address. `--listen` cannot be combined with `--host` or `--port`."},
	},
	{
		path:    []string{"summon", "status"},
		summary: "Show the local summon process status.",
		usage:   []string{"kd summon status [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindSummonConfigFlags(flags, &summonConfigOptions{})
		},
	},
	{
		path:    []string{"summon", "logs"},
		summary: "Show the local summon logs.",
		usage:   []string{"kd summon logs [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindSummonLogsFlags(flags, &summonLogsOptions{})
		},
	},
	{
		path:    []string{"summon", "stop"},
		summary: "Stop the local summon process.",
		usage:   []string{"kd summon stop [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindSummonConfigFlags(flags, &summonConfigOptions{})
		},
	},
	{
		path:               []string{"create"},
		summary:            "Create Kingdom resources.",
		usage:              []string{"kd create work order --title <title> --text <text> [flags]"},
		subcommands:        [][]string{{"create", "work-order"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"create", "work-order"},
		summary: "Create a work order and immediately start a run.",
		usage:   []string{"kd create work order --title <title> --text <text> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindCreateWorkOrderFlags(flags, &createWorkOrderOptions{})
		},
		flagOrder: []string{"title", "text", "global-model", "agent-model-overrides", "publish-mode", "autonomous-approvals", "metadata-json", "skip-reviews-and-validations", "run-until-all-approve", "max-execution-review-cycles", "max-validation-rework-cycles"},
		notes: []string{
			"`--global-model` is required when provider.engine=opencode and `--server` is omitted.",
			"`--global-model` example: `github-copilot/gpt-5.4-mini`; `--agent-model-overrides` example: `builder=github-copilot/gpt-5.4-mini,reviewer=github-copilot/gpt-5.4-mini`.",
			"`--max-execution-review-cycles=0` and `--max-validation-rework-cycles=0` auto-succeed review or validation at the cap boundary.",
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"run"},
		summary:            "Start, inspect, and export work order runs.",
		usage:              []string{"kd run start --work-order-id <id> [flags]", "kd run status --run-id <id>", "kd run resume --run-id <id> --reason <text>", "kd run artifacts export --run-id <id> [flags]"},
		subcommands:        [][]string{{"run", "start"}, {"run", "status"}, {"run", "resume"}, {"run", "artifacts", "export"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"run", "start"},
		summary: "Start a run for an existing work order.",
		usage:   []string{"kd run start --work-order-id <id> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindRunStartFlags(flags, &runStartOptions{})
		},
		flagOrder: []string{"work-order-id", "global-model", "agent-model-overrides", "publish-mode", "autonomous-approvals", "skip-reviews-and-validations", "run-until-all-approve", "max-execution-review-cycles", "max-validation-rework-cycles"},
		notes: []string{
			"`--global-model` is required when provider.engine=opencode and `--server` is omitted.",
			"`--global-model` example: `github-copilot/gpt-5.4-mini`; `--agent-model-overrides` example: `builder=github-copilot/gpt-5.4-mini,reviewer=github-copilot/gpt-5.4-mini`.",
			"`--max-execution-review-cycles=0` and `--max-validation-rework-cycles=0` auto-succeed review or validation at the cap boundary.",
		},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"run", "status"},
		summary: "Show the current run state.",
		usage:   []string{"kd run status --run-id <id>"},
		flags: func(flags *flag.FlagSet) {
			bindRunStatusFlags(flags, &runStatusOptions{})
		},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"run", "resume"},
		summary: "Resume the run's interrupted steps from their saved checkpoints.",
		usage:   []string{"kd run resume --run-id <id> --reason <text>"},
		flags: func(flags *flag.FlagSet) {
			bindRunResumeFlags(flags, &runResumeOptions{})
		},
		notes: []string{
			"Resume continues each interrupted step's saved conversation and partial files in a new attempt. When no valid checkpoint exists, use `kd node rerun-from`.",
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"run", "artifacts"},
		summary:            "Export run artifacts.",
		usage:              []string{"kd run artifacts export --run-id <id> [flags]"},
		subcommands:        [][]string{{"run", "artifacts", "export"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"run", "artifacts", "export"},
		summary: "Export run artifacts as a zip archive.",
		usage:   []string{"kd run artifacts export --run-id <id> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindRunArtifactsExportFlags(flags, &runArtifactsExportOptions{})
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"graph"},
		summary:            "Render the workflow graph for a run.",
		usage:              []string{"kd graph show --run-id <id> [--ascii | --mermaid]"},
		subcommands:        [][]string{{"graph", "show"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"graph", "show"},
		summary: "Render the workflow graph for a run.",
		usage:   []string{"kd graph show --run-id <id> [--ascii | --mermaid]"},
		flags: func(flags *flag.FlagSet) {
			bindGraphShowFlags(flags, &graphShowOptions{})
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"node"},
		summary:            "Control run execution at the node level.",
		usage:              []string{"kd node rerun-from --run-id <id> --node-key <key> --reason <text>"},
		subcommands:        [][]string{{"node", "rerun-from"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"node", "rerun-from"},
		summary: "Requeue execution starting from a specific node.",
		usage:   []string{"kd node rerun-from --run-id <id> --node-key <key> --reason <text>"},
		flags: func(flags *flag.FlagSet) {
			bindNodeMutationFlags(flags, &nodeMutationOptions{}, "rerun reason")
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"timeline"},
		summary:            "Stream run timeline events.",
		usage:              []string{"kd timeline follow --run-id <id>"},
		subcommands:        [][]string{{"timeline", "follow"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"timeline", "follow"},
		summary: "Stream run timeline events.",
		usage:   []string{"kd timeline follow --run-id <id>"},
		flags: func(flags *flag.FlagSet) {
			bindTimelineFollowFlags(flags, &timelineFollowOptions{})
		},
		includeGlobalFlags: true,
	},
	{
		path:               []string{"approvals"},
		summary:            "List approval events and record plan or final decisions.",
		usage:              []string{"kd approvals list --run-id <id> [flags]", "kd approvals plan-decide --run-id <id> --action <confirm|rework> [flags]", "kd approvals decide --run-id <id> --action <confirm|rework> [flags]"},
		subcommands:        [][]string{{"approvals", "list"}, {"approvals", "plan-decide"}, {"approvals", "decide"}},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"approvals", "list"},
		summary: "List approval events from the run timeline.",
		usage:   []string{"kd approvals list --run-id <id> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindApprovalsListFlags(flags, &approvalsListOptions{})
		},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"approvals", "plan-decide"},
		summary: "Record the plan approval decision.",
		usage:   []string{"kd approvals plan-decide --run-id <id> --action <confirm|rework> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindApprovalDecisionFlags(flags, &approvalDecisionOptions{}, "plan decision `action`: confirm or rework")
		},
		notes:              []string{"`--reason` is required when `--action` is `rework`."},
		includeGlobalFlags: true,
	},
	{
		path:    []string{"approvals", "decide"},
		summary: "Record the final approval decision.",
		usage:   []string{"kd approvals decide --run-id <id> --action <confirm|rework> [flags]"},
		flags: func(flags *flag.FlagSet) {
			bindApprovalDecisionFlags(flags, &approvalDecisionOptions{}, "final decision `action`: confirm or rework")
		},
		notes:              []string{"`--reason` is required when `--action` is `rework`."},
		includeGlobalFlags: true,
	},
}

func bindGitHubKeyHelpFlags(flags *flag.FlagSet) {
	flags.String("connection-id", "", "saved connection id")
	flags.String("from", "", "RSA PKCS#1 or PKCS#8 PEM file")
}

func writeCommandHelp(stdout io.Writer, topic []string) {
	normalized := normalizeHelpTopic(topic)
	if len(normalized) == 0 {
		writeRootHelp(stdout)
		return
	}

	for len(normalized) > 0 {
		if entry := lookupHelpTopic(normalized); entry != nil {
			writeHelpTopic(stdout, entry)
			return
		}
		normalized = normalized[:len(normalized)-1]
	}

	writeRootHelp(stdout)
}

func writeRootHelp(stdout io.Writer) {
	_, _ = io.WriteString(stdout, "Usage:\n")
	_, _ = io.WriteString(stdout, "  kd <command>\n")
	_, _ = io.WriteString(stdout, "  kd [global flags] <api-command>\n")

	for _, section := range rootHelpSections {
		_, _ = fmt.Fprintf(stdout, "\n%s:\n", section.title)
		if section.description != "" {
			_, _ = fmt.Fprintf(stdout, "  %s\n", section.description)
		}
		writeCommandSummaries(stdout, section.commands)
	}

	writeGlobalFlagsSection(stdout)
	_, _ = io.WriteString(stdout, "\nUse `kd help <command>` for command details.\n")
}

func writeHelpTopic(stdout io.Writer, topic *helpTopic) {
	if topic == nil {
		writeRootHelp(stdout)
		return
	}

	_, _ = fmt.Fprintf(stdout, "%s\n", strings.Join(topic.path, " "))
	if topic.summary != "" {
		_, _ = fmt.Fprintf(stdout, "%s\n", topic.summary)
	}

	_, _ = io.WriteString(stdout, "\nUsage:\n")
	for _, line := range topic.usage {
		_, _ = fmt.Fprintf(stdout, "  %s\n", line)
	}

	if len(topic.subcommands) > 0 {
		_, _ = io.WriteString(stdout, "\nCommands:\n")
		writeCommandSummaries(stdout, topic.subcommands)
	}

	writeBoundFlagsSection(stdout, topic.flags, topic.flagOrder, topic.flagAliases)
	if topic.includeGlobalFlags {
		writeGlobalFlagsSection(stdout)
	}
	if len(topic.notes) > 0 {
		_, _ = io.WriteString(stdout, "\nNotes:\n")
		for _, note := range topic.notes {
			_, _ = fmt.Fprintf(stdout, "  - %s\n", note)
		}
	}
}

func normalizeHelpTopic(topic []string) []string {
	normalized := make([]string, 0, len(topic))
	for _, part := range topic {
		trimmed := strings.TrimSpace(strings.ToLower(part))
		if trimmed == "" || trimmed == "help" {
			continue
		}
		normalized = append(normalized, trimmed)
	}
	return normalized
}

func lookupHelpTopic(path []string) *helpTopic {
	if len(path) == 0 {
		return nil
	}
	for _, topic := range helpTopics {
		if sameHelpPath(topic.path, path) {
			return topic
		}
	}
	return nil
}

func nearestHelpTopic(path []string) []string {
	normalized := normalizeHelpTopic(path)
	for len(normalized) > 0 {
		if lookupHelpTopic(normalized) != nil {
			return normalized
		}
		normalized = normalized[:len(normalized)-1]
	}
	return nil
}

func sameHelpPath(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

func writeCommandSummaries(stdout io.Writer, commands [][]string) {
	type renderedCommand struct {
		label   string
		summary string
	}

	rendered := make([]renderedCommand, 0, len(commands))
	maxLabelWidth := 0
	for _, commandPath := range commands {
		topic := lookupHelpTopic(commandPath)
		if topic == nil {
			continue
		}
		label := strings.Join(commandPath, " ")
		rendered = append(rendered, renderedCommand{label: label, summary: topic.summary})
		if len(label) > maxLabelWidth {
			maxLabelWidth = len(label)
		}
	}

	for _, item := range rendered {
		_, _ = fmt.Fprintf(stdout, "  %-*s  %s\n", maxLabelWidth, item.label, item.summary)
	}
}

func writeGlobalFlagsSection(stdout io.Writer) {
	_, _ = io.WriteString(stdout, "\nGlobal Flags for API Commands:\n")
	maxWidth := 0
	for _, item := range globalHelpFlags {
		if len(item.syntax) > maxWidth {
			maxWidth = len(item.syntax)
		}
	}
	for _, item := range globalHelpFlags {
		_, _ = fmt.Fprintf(stdout, "  %-*s  %s\n", maxWidth, item.syntax, item.description)
	}
	_, _ = io.WriteString(stdout, "  Server defaults come from `.kingdom/config.yaml` when `--server` is omitted.\n")
}

func writeBoundFlagsSection(stdout io.Writer, binder flagBinder, order []string, aliasGroups [][]string) {
	if binder == nil {
		return
	}
	flags := newFlagSet("help")
	binder(flags)
	rendered := renderFlagLines(flags, order, aliasGroups)
	if len(rendered) == 0 {
		return
	}

	_, _ = io.WriteString(stdout, "\nFlags:\n")
	maxWidth := 0
	for _, line := range rendered {
		if len(line.syntax) > maxWidth {
			maxWidth = len(line.syntax)
		}
	}
	for _, line := range rendered {
		_, _ = fmt.Fprintf(stdout, "  %-*s  %s\n", maxWidth, line.syntax, line.description)
	}
}

type renderedFlagLine struct {
	syntax      string
	description string
}

func renderFlagLines(flags *flag.FlagSet, order []string, aliasGroups [][]string) []renderedFlagLine {
	if flags == nil {
		return nil
	}
	lines := make([]renderedFlagLine, 0)
	consumed := make(map[string]struct{})
	for _, group := range aliasGroups {
		line, ok := renderFlagAliasGroup(flags, group)
		if !ok {
			continue
		}
		lines = append(lines, line)
		for _, name := range group {
			consumed[name] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(order))
	for _, name := range order {
		if _, ok := consumed[name]; ok {
			seen[name] = struct{}{}
			continue
		}
		entry := flags.Lookup(name)
		if entry == nil {
			continue
		}
		lines = append(lines, renderFlagLine(entry))
		seen[name] = struct{}{}
	}
	flags.VisitAll(func(f *flag.Flag) {
		if _, ok := consumed[f.Name]; ok {
			return
		}
		if _, ok := seen[f.Name]; ok {
			return
		}
		lines = append(lines, renderFlagLine(f))
	})
	return lines
}

func renderFlagAliasGroup(flags *flag.FlagSet, names []string) (renderedFlagLine, bool) {
	if flags == nil || len(names) == 0 {
		return renderedFlagLine{}, false
	}
	entries := make([]*flag.Flag, 0, len(names))
	for _, name := range names {
		entry := flags.Lookup(name)
		if entry == nil {
			return renderedFlagLine{}, false
		}
		entries = append(entries, entry)
	}
	placeholder, usage := flag.UnquoteUsage(entries[0])
	defaultValue, hasDefault := flagDefaultValue(entries[0])
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		prefix := "--"
		if len(entry.Name) == 1 {
			prefix = "-"
		}
		parts = append(parts, prefix+entry.Name)
	}
	syntax := strings.Join(parts, ", ")
	if placeholder != "" {
		syntax += " <" + placeholder + ">"
	}
	description := usage
	if hasDefault {
		description += fmt.Sprintf(" (default: %s)", defaultValue)
	}
	return renderedFlagLine{syntax: syntax, description: description}, true
}

func renderFlagLine(f *flag.Flag) renderedFlagLine {
	placeholder, usage := flag.UnquoteUsage(f)
	prefix := "--"
	if len(f.Name) == 1 {
		prefix = "-"
	}
	syntax := prefix + f.Name
	if placeholder != "" {
		syntax += " <" + placeholder + ">"
	}
	description := usage
	if defaultValue, ok := flagDefaultValue(f); ok {
		description += fmt.Sprintf(" (default: %s)", defaultValue)
	}
	return renderedFlagLine{syntax: syntax, description: description}
}

func flagDefaultValue(f *flag.Flag) (string, bool) {
	if f == nil {
		return "", false
	}
	getter, ok := f.Value.(flag.Getter)
	if !ok {
		if f.DefValue == "" {
			return `""`, true
		}
		return f.DefValue, true
	}
	value := getter.Get()
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return `""`, true
		}
	}
	if f.DefValue == "" {
		return `""`, true
	}
	return f.DefValue, true
}
