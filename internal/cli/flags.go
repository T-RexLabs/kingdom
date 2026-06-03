package cli

import (
	"flag"
	"path/filepath"
	"time"

	kingdom "kingdom/internal/orchestration"
)

type initCommandOptions struct {
	projectPath            string
	force                  bool
	publishMode            string
	autonomousApprovalsRaw string
	providerEngine         string
	providerStrictRaw      string
	listen                 string
	basePath               string
}

type cleanCommandOptions struct {
	projectPath string
}

type summonServeOptions struct {
	configPath   string
	detach       bool
	readyTimeout time.Duration
}

type summonConfigOptions struct {
	configPath string
}

type createDecreeOptions struct {
	title                     string
	text                      string
	metadataRaw               string
	skipReviewsAndValidations bool
	autonomousApprovalsRaw    string
	runUntilAllApprove        bool
	maxExecutionReviewCycles  int
	maxValidationReworkCycles int
	publishModeRaw            string
	globalModel               string
	agentModelOverridesRaw    string
}

type runStartOptions struct {
	decreeID                  string
	skipReviewsAndValidations bool
	autonomousApprovalsRaw    string
	runUntilAllApprove        bool
	maxExecutionReviewCycles  int
	maxValidationReworkCycles int
	publishModeRaw            string
	globalModel               string
	agentModelOverridesRaw    string
}

type runStatusOptions struct {
	runID string
}

type runArtifactsExportOptions struct {
	runID      string
	outputPath string
}

type graphShowOptions struct {
	runID     string
	asMermaid bool
	asASCII   bool
}

type nodeMutationOptions struct {
	runID   string
	nodeKey string
	reason  string
}

type timelineFollowOptions struct {
	runID string
}

type approvalsListOptions struct {
	runID  string
	limit  int
	cursor string
}

type approvalDecisionOptions struct {
	runID  string
	action string
	reason string
}

func bindInitFlags(flags *flag.FlagSet, opts *initCommandOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.projectPath, "path", ".", "project root `path`")
	flags.BoolVar(&opts.force, "force", false, "overwrite generated files")
	flags.StringVar(&opts.publishMode, "publish-mode", kingdom.PublishModeDirect, "publish `mode`: direct or git_worktree")
	flags.StringVar(&opts.autonomousApprovalsRaw, "autonomous-approvals", "true", "default autonomous approvals for new runs: `true` or `false`")
	flags.StringVar(&opts.providerEngine, "provider-engine", "opencode", "provider `engine`: opencode")
	flags.StringVar(&opts.providerStrictRaw, "provider-strict", "true", "enforce provider startup health checks: `true` or `false`")
	flags.StringVar(&opts.listen, "listen", "127.0.0.1:8080", "summon API listen `address`")
	flags.StringVar(&opts.basePath, "base-path", "/api/v1", "summon API base `path`")
}

func bindCleanFlags(flags *flag.FlagSet, opts *cleanCommandOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.projectPath, "path", ".", "project root `path`")
}

func bindSummonServeFlags(flags *flag.FlagSet, opts *summonServeOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.configPath, "config", filepath.Join(defaultInstallDirName, defaultConfigFileName), "summon config `path`")
	flags.BoolVar(&opts.detach, "detach", false, "run summon in the background")
	flags.DurationVar(&opts.readyTimeout, "ready-timeout", defaultSummonReadyTimeout, "maximum readiness wait `duration`")
}

func bindSummonConfigFlags(flags *flag.FlagSet, opts *summonConfigOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.configPath, "config", filepath.Join(defaultInstallDirName, defaultConfigFileName), "summon config `path`")
}

func bindCreateDecreeFlags(flags *flag.FlagSet, opts *createDecreeOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.title, "title", "", "decree `title`")
	flags.StringVar(&opts.text, "text", "", "decree `text`")
	flags.StringVar(&opts.metadataRaw, "metadata-json", "", "optional decree metadata JSON `object`")
	flags.BoolVar(&opts.skipReviewsAndValidations, "skip-reviews-and-validations", false, "skip execution review and validation")
	flags.StringVar(&opts.autonomousApprovalsRaw, "autonomous-approvals", "inherit", "run approvals `mode`: inherit, true, or false")
	flags.BoolVar(&opts.runUntilAllApprove, "run-until-all-approve", false, "continue reviewer and validator loops until pass")
	flags.IntVar(&opts.maxExecutionReviewCycles, "max-execution-review-cycles", -1, "max execution review `cycles` (0 or greater when provided; 0 uses workflow defaults)")
	flags.IntVar(&opts.maxValidationReworkCycles, "max-validation-rework-cycles", -1, "max validation rework `cycles` (0 or greater when provided; 0 uses workflow defaults)")
	flags.StringVar(&opts.publishModeRaw, "publish-mode", defaultPublishModeForCLI(), "publish `mode`: direct or git_worktree")
	flags.StringVar(&opts.globalModel, "global-model", "", "global model override `provider/model`; required when provider.engine=opencode")
	flags.StringVar(&opts.agentModelOverridesRaw, "agent-model-overrides", "", "optional `role=model` overrides (comma-separated)")
}

func bindRunStartFlags(flags *flag.FlagSet, opts *runStartOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.decreeID, "decree-id", "", "decree `id`")
	flags.BoolVar(&opts.skipReviewsAndValidations, "skip-reviews-and-validations", false, "skip execution review and validation")
	flags.StringVar(&opts.autonomousApprovalsRaw, "autonomous-approvals", "inherit", "run approvals `mode`: inherit, true, or false")
	flags.BoolVar(&opts.runUntilAllApprove, "run-until-all-approve", false, "continue reviewer and validator loops until pass")
	flags.IntVar(&opts.maxExecutionReviewCycles, "max-execution-review-cycles", -1, "max execution review `cycles` (0 or greater when provided; 0 uses workflow defaults)")
	flags.IntVar(&opts.maxValidationReworkCycles, "max-validation-rework-cycles", -1, "max validation rework `cycles` (0 or greater when provided; 0 uses workflow defaults)")
	flags.StringVar(&opts.publishModeRaw, "publish-mode", defaultPublishModeForCLI(), "publish `mode`: direct or git_worktree")
	flags.StringVar(&opts.globalModel, "global-model", "", "global model override `provider/model`; required when provider.engine=opencode")
	flags.StringVar(&opts.agentModelOverridesRaw, "agent-model-overrides", "", "optional `role=model` overrides (comma-separated)")
}

func bindRunStatusFlags(flags *flag.FlagSet, opts *runStatusOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
}

func bindRunArtifactsExportFlags(flags *flag.FlagSet, opts *runArtifactsExportOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
	flags.StringVar(&opts.outputPath, "output", "", "output zip file `path` (default: <run-id>-artifacts.zip)")
}

func bindGraphShowFlags(flags *flag.FlagSet, opts *graphShowOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
	flags.BoolVar(&opts.asMermaid, "mermaid", false, "print Mermaid graph output")
	flags.BoolVar(&opts.asASCII, "ascii", false, "print ASCII graph output")
}

func bindNodeMutationFlags(flags *flag.FlagSet, opts *nodeMutationOptions, reasonLabel string) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
	flags.StringVar(&opts.nodeKey, "node-key", "", "node `key`")
	flags.StringVar(&opts.reason, "reason", "", reasonLabel+" `text`")
}

func bindTimelineFollowFlags(flags *flag.FlagSet, opts *timelineFollowOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
}

func bindApprovalsListFlags(flags *flag.FlagSet, opts *approvalsListOptions) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
	flags.IntVar(&opts.limit, "limit", 50, "timeline page `size`")
	flags.StringVar(&opts.cursor, "cursor", "", "timeline pagination `cursor`")
}

func bindApprovalDecisionFlags(flags *flag.FlagSet, opts *approvalDecisionOptions, actionDescription string) {
	if flags == nil || opts == nil {
		return
	}
	flags.StringVar(&opts.runID, "run-id", "", "run `id`")
	flags.StringVar(&opts.action, "action", "", actionDescription)
	flags.StringVar(&opts.reason, "reason", "", "decision rationale `text`")
}
