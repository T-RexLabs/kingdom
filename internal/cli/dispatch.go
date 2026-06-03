package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/workspace"
)

const (
	ExitSuccess = 0
	ExitUsage   = 2
	ExitRuntime = 3
	ExitAPI     = 4
	ExitSignal  = 130
)

const (
	defaultServerAddress = "http://127.0.0.1:8080"
	defaultActorID       = "cli-user"
	defaultRoleID        = "allfather"
	defaultTimeout       = 30 * time.Second
)

var loadInstallConfigForCLI = installConfigForCLI

type Config struct {
	ServerAddress  string
	ServerExplicit bool
	ActorID        string
	RoleID         string
	JSONOutput     bool
	Timeout        time.Duration
}

type Runtime struct {
	config Config
	stdout io.Writer
	stderr io.Writer
}

func Execute(ctx context.Context, stdout io.Writer, stderr io.Writer, args []string) int {
	if len(args) == 0 {
		writeUsageError(stderr, errors.New("missing command"))
		return ExitUsage
	}

	if args[0] == "help" {
		writeCommandHelp(stdout, args[1:])
		return ExitSuccess
	}
	if isHelpToken(args[0]) {
		writeCommandHelp(stdout, nil)
		return ExitSuccess
	}

	switch args[0] {
	case "init":
		if hasHelpToken(args[1:]) {
			writeCommandHelp(stdout, []string{"init"})
			return ExitSuccess
		}
		if err := runInitCommand(stdout, args[1:]); err != nil {
			return (&Runtime{config: Config{}, stdout: stdout, stderr: stderr}).writeError(err)
		}
		return ExitSuccess
	case "clean":
		if hasHelpToken(args[1:]) {
			writeCommandHelp(stdout, []string{"clean"})
			return ExitSuccess
		}
		if err := runCleanCommand(stdout, args[1:]); err != nil {
			return (&Runtime{config: Config{}, stdout: stdout, stderr: stderr}).writeError(err)
		}
		return ExitSuccess
	case "summon":
		if hasHelpToken(args[1:]) {
			writeCommandHelp(stdout, append([]string{"summon"}, args[1:]...))
			return ExitSuccess
		}
		if err := runSummonCommand(ctx, stdout, stderr, args[1:]); err != nil {
			return (&Runtime{config: Config{}, stdout: stdout, stderr: stderr}).writeError(err)
		}
		return ExitSuccess
	}

	cfg, remaining, err := parseGlobalFlags(args)
	if err != nil {
		writeUsageError(stderr, err)
		return ExitUsage
	}

	if len(remaining) == 0 {
		writeUsageError(stderr, errors.New("missing command"))
		return ExitUsage
	}
	if isHelpToken(remaining[0]) {
		writeCommandHelp(stdout, nil)
		return ExitSuccess
	}
	if len(remaining) >= 2 && isHelpToken(remaining[1]) {
		writeCommandHelp(stdout, []string{remaining[0]})
		return ExitSuccess
	}

	runtime := &Runtime{config: cfg, stdout: stdout, stderr: stderr}
	if err := runtime.run(ctx, remaining); err != nil {
		return runtime.writeError(err)
	}

	return ExitSuccess
}

func parseGlobalFlags(args []string) (Config, []string, error) {
	cfg := Config{
		ServerAddress: defaultServerAddress,
		ActorID:       defaultActorID,
		RoleID:        defaultRoleID,
		Timeout:       defaultTimeout,
	}

	remaining := make([]string, 0, len(args))
	serverExplicit := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "--") {
			remaining = append(remaining, args[i:]...)
			break
		}

		switch {
		case arg == "--json":
			cfg.JSONOutput = true
		case arg == "--server":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--server requires a value")
			}
			i++
			cfg.ServerAddress = strings.TrimSpace(args[i])
			serverExplicit = true
		case strings.HasPrefix(arg, "--server="):
			cfg.ServerAddress = strings.TrimSpace(strings.TrimPrefix(arg, "--server="))
			serverExplicit = true
		case arg == "--actor-id":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--actor-id requires a value")
			}
			i++
			cfg.ActorID = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--actor-id="):
			cfg.ActorID = strings.TrimSpace(strings.TrimPrefix(arg, "--actor-id="))
		case arg == "--role-id":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--role-id requires a value")
			}
			i++
			cfg.RoleID = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--role-id="):
			cfg.RoleID = strings.TrimSpace(strings.TrimPrefix(arg, "--role-id="))
		case arg == "--timeout":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--timeout requires a value")
			}
			i++
			timeout, err := time.ParseDuration(strings.TrimSpace(args[i]))
			if err != nil {
				return Config{}, nil, fmt.Errorf("invalid --timeout value: %w", err)
			}
			cfg.Timeout = timeout
		case strings.HasPrefix(arg, "--timeout="):
			timeoutRaw := strings.TrimSpace(strings.TrimPrefix(arg, "--timeout="))
			timeout, err := time.ParseDuration(timeoutRaw)
			if err != nil {
				return Config{}, nil, fmt.Errorf("invalid --timeout value: %w", err)
			}
			cfg.Timeout = timeout
		default:
			return Config{}, nil, fmt.Errorf("unknown global flag %q", arg)
		}
	}

	if cfg.ServerAddress == "" {
		return Config{}, nil, errors.New("--server cannot be empty")
	}
	if cfg.ActorID == "" {
		return Config{}, nil, errors.New("--actor-id cannot be empty")
	}
	if cfg.RoleID == "" {
		return Config{}, nil, errors.New("--role-id cannot be empty")
	}
	if cfg.Timeout <= 0 {
		return Config{}, nil, errors.New("--timeout must be greater than zero")
	}
	cfg.ServerExplicit = serverExplicit

	applyInstallModeDefaults(&cfg)

	return cfg, remaining, nil
}

func (r *Runtime) run(ctx context.Context, args []string) error {
	if len(args) == 1 {
		return usageError{err: errors.New("incomplete command")}
	}

	if len(args) >= 2 && (isHelpToken(args[1]) || args[1] == "help") {
		writeCommandHelp(r.stdout, []string{args[0]})
		return nil
	}

	if len(args) >= 3 && isHelpToken(args[2]) {
		writeCommandHelp(r.stdout, args[:2])
		return nil
	}
	if len(args) >= 4 && isHelpToken(args[3]) {
		writeCommandHelp(r.stdout, args[:3])
		return nil
	}

	switch args[0] {
	case "create":
		if args[1] != "decree" {
			return usageError{err: fmt.Errorf("unsupported create command %q", args[1])}
		}
		return r.handleCreateDecree(ctx, args[2:])
	case "run":
		switch args[1] {
		case "start":
			return r.handleRunStart(ctx, args[2:])
		case "status":
			return r.handleRunStatus(ctx, args[2:])
		case "artifacts":
			if len(args) < 3 || args[2] != "export" {
				return usageError{err: fmt.Errorf("unsupported run artifacts command %q", strings.Join(args[2:], " "))}
			}
			return r.handleRunArtifactsExport(ctx, args[3:])
		default:
			return usageError{err: fmt.Errorf("unsupported run command %q", args[1])}
		}
	case "graph":
		if args[1] != "show" {
			return usageError{err: fmt.Errorf("unsupported graph command %q", args[1])}
		}
		return r.handleGraphShow(ctx, args[2:])
	case "node":
		switch args[1] {
		case "retry":
			return r.handleNodeRetry(ctx, args[2:])
		case "rerun-from":
			return r.handleNodeRerunFrom(ctx, args[2:])
		default:
			return usageError{err: fmt.Errorf("unsupported node command %q", args[1])}
		}
	case "timeline":
		if args[1] != "follow" {
			return usageError{err: fmt.Errorf("unsupported timeline command %q", args[1])}
		}
		return r.handleTimelineFollow(ctx, args[2:])
	case "approvals":
		switch args[1] {
		case "list":
			return r.handleApprovalsList(ctx, args[2:])
		case "plan-decide":
			return r.handleApprovalsPlanDecide(ctx, args[2:])
		case "decide":
			return r.handleApprovalsDecide(ctx, args[2:])
		default:
			return usageError{err: fmt.Errorf("unsupported approvals command %q", args[1])}
		}
	default:
		return usageError{err: fmt.Errorf("unsupported command %q", args[0])}
	}
}

func (r *Runtime) newResponsesClient() (*gen.ClientWithResponses, error) {
	httpClient := &http.Client{Timeout: r.config.Timeout}
	return gen.NewClientWithResponses(r.config.ServerAddress, gen.WithHTTPClient(httpClient))
}

func (r *Runtime) newRawClient() (*gen.Client, error) {
	httpClient := &http.Client{Timeout: 0}
	return gen.NewClient(r.config.ServerAddress, gen.WithHTTPClient(httpClient))
}

func optionalReason(action, rawReason string) (*string, error) {
	trimmedAction := strings.ToLower(strings.TrimSpace(action))
	trimmedReason := strings.TrimSpace(rawReason)
	if trimmedAction == string(gen.CreateFinalApprovalRequestActionRework) || trimmedAction == string(gen.CreatePlanApprovalRequestActionRework) {
		if trimmedReason == "" {
			return nil, errors.New("--reason is required when --action is rework")
		}
	}
	if trimmedReason == "" {
		return nil, nil
	}
	return &trimmedReason, nil
}

func (r *Runtime) currentPlanRevisionID(ctx context.Context, client *gen.ClientWithResponses, runID string) (string, error) {
	response, err := client.GetRunPlanWithResponse(ctx, runID, &gen.GetRunPlanParams{
		XActorId: r.config.ActorID,
		XRoleId:  r.config.RoleID,
	})
	if err != nil {
		return "", err
	}
	if response.JSON200 == nil {
		return "", unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}
	if string(response.JSON200.RevisionStatus) != "ready" || response.JSON200.PlanRevisionId == nil {
		return "", fmt.Errorf("current plan revision is not available yet for run %s", strings.TrimSpace(runID))
	}
	planRevisionID := strings.TrimSpace(*response.JSON200.PlanRevisionId)
	if planRevisionID == "" {
		return "", fmt.Errorf("current plan revision is not available yet for run %s", strings.TrimSpace(runID))
	}
	return planRevisionID, nil
}

func (r *Runtime) handleCreateDecree(ctx context.Context, args []string) error {
	var title string
	var text string
	var metadataRaw string
	var skipReviewsAndValidations bool
	var autonomousApprovalsRaw string
	var runUntilAllApprove bool
	var maxExecutionReviewCycles int
	var maxValidationReworkCycles int
	var publishModeRaw string
	var globalModel string
	var agentModelOverridesRaw string

	flags := newFlagSet("create decree")
	flags.StringVar(&title, "title", "", "decree title")
	flags.StringVar(&text, "text", "", "decree text")
	flags.StringVar(&metadataRaw, "metadata-json", "", "optional decree metadata JSON object")
	flags.BoolVar(&skipReviewsAndValidations, "skip-reviews-and-validations", false, "skip execution review and validation")
	flags.StringVar(&autonomousApprovalsRaw, "autonomous-approvals", "inherit", "run approvals mode: inherit, true, or false")
	flags.BoolVar(&runUntilAllApprove, "run-until-all-approve", false, "continue reviewer/validator loops until pass")
	flags.IntVar(&maxExecutionReviewCycles, "max-execution-review-cycles", -1, "max execution review cycles (>= 0 when provided; 0 uses workflow defaults)")
	flags.IntVar(&maxValidationReworkCycles, "max-validation-rework-cycles", -1, "max validation rework cycles (>= 0 when provided; 0 uses workflow defaults)")
	flags.StringVar(&publishModeRaw, "publish-mode", defaultPublishModeForCLI(), "publish mode: direct or git_worktree")
	flags.StringVar(&globalModel, "global-model", "", "global model override (provider/model); required when provider.engine=opencode")
	flags.StringVar(&agentModelOverridesRaw, "agent-model-overrides", "", "optional role=model overrides (comma-separated)")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if title == "" || text == "" {
		return usageError{err: errors.New("--title and --text are required")}
	}
	if err := requireGlobalModelForInstallMode(globalModel, r.config); err != nil {
		return usageError{err: err}
	}

	decreeRequest := gen.CreateDecreeRequest{DecreeTitle: title, DecreeText: text}
	if strings.TrimSpace(metadataRaw) != "" {
		metadata, err := parseJSONObject(metadataRaw, "--metadata-json")
		if err != nil {
			return usageError{err: err}
		}
		decreeRequest.Metadata = &metadata
	}
	publishMode, err := parsePublishModeFlag(publishModeRaw)
	if err != nil {
		return usageError{err: err}
	}
	if err := requirePublishModeSupportForInstallMode(publishMode, r.config); err != nil {
		return usageError{err: err}
	}
	runRequest := gen.CreateRunRequest{PublishMode: publishMode}
	autonomousApprovals, err := parseAutonomousApprovalsOverride(autonomousApprovalsRaw)
	if err != nil {
		return usageError{err: err}
	}
	if autonomousApprovals != nil {
		runRequest.AutonomousApprovals = autonomousApprovals
	}
	if err := applyRunWorkflowOptions(&runRequest, skipReviewsAndValidations, runUntilAllApprove, maxExecutionReviewCycles, maxValidationReworkCycles); err != nil {
		return usageError{err: err}
	}
	modelSelection, err := buildRunModelSelection(globalModel, agentModelOverridesRaw)
	if err != nil {
		return usageError{err: err}
	}
	if modelSelection != nil {
		runRequest.ModelSelection = *modelSelection
	}

	decreeResponse, err := r.createDecree(ctx, decreeRequest)
	if err != nil {
		return err
	}
	runRequest.DecreeId = decreeResponse.DecreeId
	runResponse, err := r.createRun(ctx, runRequest)
	if err != nil {
		return &decreeIssueError{DecreeID: decreeResponse.DecreeId, SkipReviewsAndValidations: skipReviewsAndValidations, Err: err}
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, map[string]any{
			"decree_id":   decreeResponse.DecreeId,
			"decree_at":   decreeResponse.CreatedAt,
			"run_id":      runResponse.RunId,
			"workflow_id": runResponse.WorkflowId,
			"status":      runResponse.Status,
			"created_at":  runResponse.CreatedAt,
			"updated_at":  runResponse.UpdatedAt,
		})
	}

	_, err = fmt.Fprintf(r.stdout, "decree issued: %s run=%s status=%s workflow=%s\n", decreeResponse.DecreeId, runResponse.RunId, runResponse.Status, runResponse.WorkflowId)
	return err
}

func (r *Runtime) createDecree(ctx context.Context, request gen.CreateDecreeRequest) (*gen.CreateDecreeResult, error) {
	client, err := r.newResponsesClient()
	if err != nil {
		return nil, err
	}

	response, err := client.CreateDecreeWithResponse(ctx, &gen.CreateDecreeParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("decree-create"),
	}, request)
	if err != nil {
		return nil, err
	}
	if response.JSON201 == nil {
		return nil, unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}
	return response.JSON201, nil
}

func (r *Runtime) createRun(ctx context.Context, request gen.CreateRunRequest) (*gen.RunSummary, error) {
	client, err := r.newResponsesClient()
	if err != nil {
		return nil, err
	}

	response, err := client.CreateRunWithResponse(ctx, &gen.CreateRunParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("run-start"),
	}, request)
	if err != nil {
		return nil, err
	}
	if response.JSON201 == nil {
		return nil, unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}
	return response.JSON201, nil
}

func (r *Runtime) handleRunStart(ctx context.Context, args []string) error {
	var decreeID string
	var skipReviewsAndValidations bool
	var autonomousApprovalsRaw string
	var runUntilAllApprove bool
	var maxExecutionReviewCycles int
	var maxValidationReworkCycles int
	var publishModeRaw string
	var globalModel string
	var agentModelOverridesRaw string

	flags := newFlagSet("run start")
	flags.StringVar(&decreeID, "decree-id", "", "decree identifier")
	flags.BoolVar(&skipReviewsAndValidations, "skip-reviews-and-validations", false, "skip execution review and validation")
	flags.StringVar(&autonomousApprovalsRaw, "autonomous-approvals", "inherit", "run approvals mode: inherit, true, or false")
	flags.BoolVar(&runUntilAllApprove, "run-until-all-approve", false, "continue reviewer/validator loops until pass")
	flags.IntVar(&maxExecutionReviewCycles, "max-execution-review-cycles", -1, "max execution review cycles (>= 0 when provided; 0 uses workflow defaults)")
	flags.IntVar(&maxValidationReworkCycles, "max-validation-rework-cycles", -1, "max validation rework cycles (>= 0 when provided; 0 uses workflow defaults)")
	flags.StringVar(&publishModeRaw, "publish-mode", defaultPublishModeForCLI(), "publish mode: direct or git_worktree")
	flags.StringVar(&globalModel, "global-model", "", "global model override (provider/model); required when provider.engine=opencode")
	flags.StringVar(&agentModelOverridesRaw, "agent-model-overrides", "", "optional role=model overrides (comma-separated)")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if decreeID == "" {
		return usageError{err: errors.New("--decree-id is required")}
	}
	if err := requireGlobalModelForInstallMode(globalModel, r.config); err != nil {
		return usageError{err: err}
	}

	publishMode, err := parsePublishModeFlag(publishModeRaw)
	if err != nil {
		return usageError{err: err}
	}
	if err := requirePublishModeSupportForInstallMode(publishMode, r.config); err != nil {
		return usageError{err: err}
	}
	request := gen.CreateRunRequest{DecreeId: decreeID, PublishMode: publishMode}
	autonomousApprovals, err := parseAutonomousApprovalsOverride(autonomousApprovalsRaw)
	if err != nil {
		return usageError{err: err}
	}
	if autonomousApprovals != nil {
		request.AutonomousApprovals = autonomousApprovals
	}
	if err := applyRunWorkflowOptions(&request, skipReviewsAndValidations, runUntilAllApprove, maxExecutionReviewCycles, maxValidationReworkCycles); err != nil {
		return usageError{err: err}
	}
	modelSelection, err := buildRunModelSelection(globalModel, agentModelOverridesRaw)
	if err != nil {
		return usageError{err: err}
	}
	if modelSelection != nil {
		request.ModelSelection = *modelSelection
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.CreateRunWithResponse(ctx, &gen.CreateRunParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("run-start"),
	}, request)
	if err != nil {
		return err
	}
	if response.JSON201 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON201)
	}

	_, err = fmt.Fprintf(r.stdout, "run started: %s status=%s workflow=%s\n", response.JSON201.RunId, response.JSON201.Status, response.JSON201.WorkflowId)
	return err
}

func (r *Runtime) handleRunStatus(ctx context.Context, args []string) error {
	var runID string

	flags := newFlagSet("run status")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" {
		return usageError{err: errors.New("--run-id is required")}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunWithResponse(ctx, runID, &gen.GetRunParams{XActorId: r.config.ActorID, XRoleId: r.config.RoleID})
	if err != nil {
		return err
	}
	if response.JSON200 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON200)
	}

	_, err = fmt.Fprintf(
		r.stdout,
		"run %s\n  status: %s\n  workflow: %s\n  created: %s\n  updated: %s\n",
		response.JSON200.RunId,
		response.JSON200.Status,
		response.JSON200.WorkflowId,
		response.JSON200.CreatedAt.UTC().Format(time.RFC3339),
		response.JSON200.UpdatedAt.UTC().Format(time.RFC3339),
	)
	return err
}

func (r *Runtime) handleRunArtifactsExport(ctx context.Context, args []string) error {
	var runID string
	var outputPath string

	flags := newFlagSet("run artifacts export")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.StringVar(&outputPath, "output", "", "output zip file path (default: <run-id>-artifacts.zip)")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" {
		return usageError{err: errors.New("--run-id is required")}
	}
	if strings.TrimSpace(outputPath) == "" {
		outputPath = fmt.Sprintf("%s-artifacts.zip", runID)
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunArtifactsArchiveWithResponse(ctx, runID, &gen.GetRunArtifactsArchiveParams{XActorId: r.config.ActorID, XRoleId: r.config.RoleID})
	if err != nil {
		return err
	}

	if response.StatusCode() != http.StatusOK {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}
	body := response.Body

	if err := os.WriteFile(outputPath, body, 0o644); err != nil {
		return fmt.Errorf("write artifact archive: %w", err)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, map[string]any{
			"run_id":      runID,
			"output_path": outputPath,
			"size_bytes":  len(body),
		})
	}

	_, err = fmt.Fprintf(r.stdout, "artifacts exported: run=%s file=%s size=%d\n", runID, outputPath, len(body))
	return err
}

func (r *Runtime) handleGraphShow(ctx context.Context, args []string) error {
	var runID string
	var asMermaid bool
	var asASCII bool

	flags := newFlagSet("graph show")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.BoolVar(&asMermaid, "mermaid", false, "print mermaid graph output")
	flags.BoolVar(&asASCII, "ascii", false, "print ASCII graph output")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" {
		return usageError{err: errors.New("--run-id is required")}
	}
	if asMermaid && asASCII {
		return usageError{err: errors.New("choose one of --ascii or --mermaid")}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunGraphWithResponse(ctx, runID, &gen.GetRunGraphParams{XActorId: r.config.ActorID, XRoleId: r.config.RoleID})
	if err != nil {
		return err
	}
	if response.JSON200 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON200)
	}

	if asMermaid {
		_, err = io.WriteString(r.stdout, renderMermaidGraph(response.JSON200))
		return err
	}
	_, err = io.WriteString(r.stdout, renderASCIIGraph(response.JSON200))
	return err
}

func (r *Runtime) handleNodeRetry(ctx context.Context, args []string) error {
	var runID string
	var nodeKey string
	var reason string

	flags := newFlagSet("node retry")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.StringVar(&nodeKey, "node-key", "", "node identifier")
	flags.StringVar(&reason, "reason", "", "retry reason")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" || nodeKey == "" || reason == "" {
		return usageError{err: errors.New("--run-id, --node-key, and --reason are required")}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.RetryNodeWithResponse(ctx, runID, nodeKey, &gen.RetryNodeParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("node-retry"),
	}, gen.RetryNodeRequest{Reason: reason})
	if err != nil {
		return err
	}
	if response.JSON202 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON202)
	}

	_, err = fmt.Fprintf(r.stdout, "retry enqueued: run=%s node=%s\n", response.JSON202.RunId, response.JSON202.NodeKey)
	return err
}

func (r *Runtime) handleNodeRerunFrom(ctx context.Context, args []string) error {
	var runID string
	var nodeKey string
	var reason string

	flags := newFlagSet("node rerun-from")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.StringVar(&nodeKey, "node-key", "", "node identifier")
	flags.StringVar(&reason, "reason", "", "rerun reason")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" || nodeKey == "" || reason == "" {
		return usageError{err: errors.New("--run-id, --node-key, and --reason are required")}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.RerunFromNodeWithResponse(ctx, runID, nodeKey, &gen.RerunFromNodeParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("node-rerun-from"),
	}, gen.RerunFromNodeRequest{Reason: reason})
	if err != nil {
		return err
	}
	if response.JSON202 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON202)
	}

	_, err = fmt.Fprintf(r.stdout, "rerun enqueued: run=%s from=%s\n", response.JSON202.RunId, response.JSON202.FromNode)
	return err
}

func (r *Runtime) handleTimelineFollow(ctx context.Context, args []string) error {
	var runID string

	flags := newFlagSet("timeline follow")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" {
		return usageError{err: errors.New("--run-id is required")}
	}

	client, err := r.newRawClient()
	if err != nil {
		return err
	}

	response, err := client.StreamEvents(ctx, &gen.StreamEventsParams{RunId: runID, XActorId: r.config.ActorID, XRoleId: r.config.RoleID})
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return readErr
		}

		var envelope gen.ErrorResponse
		if unmarshalErr := json.Unmarshal(body, &envelope); unmarshalErr == nil {
			return &apiError{
				statusCode: response.StatusCode,
				code:       string(envelope.Error.Code),
				message:    envelope.Error.Message,
				retryable:  envelope.Error.Retryable,
			}
		}

		return &apiError{statusCode: response.StatusCode, message: strings.TrimSpace(string(body))}
	}

	return r.consumeSSE(response.Body)
}

func (r *Runtime) handleApprovalsList(ctx context.Context, args []string) error {
	var runID string
	var limit int
	var cursor string

	flags := newFlagSet("approvals list")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.IntVar(&limit, "limit", 50, "timeline page limit")
	flags.StringVar(&cursor, "cursor", "", "timeline cursor")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if runID == "" {
		return usageError{err: errors.New("--run-id is required")}
	}
	if limit <= 0 {
		return usageError{err: errors.New("--limit must be greater than zero")}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	limitValue := gen.Limit(limit)
	params := &gen.GetRunTimelineParams{XActorId: r.config.ActorID, XRoleId: r.config.RoleID, Limit: &limitValue}
	if cursor != "" {
		cursorValue := gen.Cursor(cursor)
		params.Cursor = &cursorValue
	}

	response, err := client.GetRunTimelineWithResponse(ctx, runID, params)
	if err != nil {
		return err
	}
	if response.JSON200 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	filtered := make([]gen.TimelineEvent, 0, len(response.JSON200.Items))
	for _, item := range response.JSON200.Items {
		if strings.HasPrefix(item.EventType, "approval.") {
			filtered = append(filtered, item)
		}
	}

	payload := struct {
		RunID      string              `json:"run_id"`
		Approvals  []gen.TimelineEvent `json:"approvals"`
		NextCursor *string             `json:"next_cursor,omitempty"`
	}{
		RunID:      response.JSON200.RunId,
		Approvals:  filtered,
		NextCursor: response.JSON200.NextCursor,
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, payload)
	}

	if len(filtered) == 0 {
		_, err := io.WriteString(r.stdout, "no approval events found in timeline\n")
		return err
	}

	for _, item := range filtered {
		_, err := fmt.Fprintf(
			r.stdout,
			"run_event_seq=%d attempt_seq=%d type=%s emitted_at=%s event_id=%s\n",
			item.RunEventSeq,
			item.AttemptSeq,
			item.EventType,
			item.EmittedAt.UTC().Format(time.RFC3339),
			item.EventId,
		)
		if err != nil {
			return err
		}
	}

	if payload.NextCursor != nil && *payload.NextCursor != "" {
		_, err := fmt.Fprintf(r.stdout, "next_cursor=%s\n", *payload.NextCursor)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Runtime) handleApprovalsDecide(ctx context.Context, args []string) error {
	var runID string
	var action string
	var reason string

	flags := newFlagSet("approvals decide")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.StringVar(&action, "action", "", "final decision action: confirm or rework")
	flags.StringVar(&reason, "reason", "", "decision rationale")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if strings.TrimSpace(runID) == "" {
		return usageError{err: errors.New("--run-id is required")}
	}
	action = strings.ToLower(strings.TrimSpace(action))
	if action != string(gen.CreateFinalApprovalRequestActionConfirm) && action != string(gen.CreateFinalApprovalRequestActionRework) {
		return usageError{err: errors.New("--action must be confirm or rework")}
	}
	reasonPtr, err := optionalReason(action, reason)
	if err != nil {
		return usageError{err: err}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.CreateFinalApprovalWithResponse(ctx, runID, &gen.CreateFinalApprovalParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("approval-decide"),
	}, gen.CreateFinalApprovalRequest{
		Action: gen.CreateFinalApprovalRequestAction(action),
		Reason: reasonPtr,
	})
	if err != nil {
		return err
	}
	if response.JSON201 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON201)
	}

	_, err = fmt.Fprintf(r.stdout, "final decision recorded: run=%s status=%s approval_id=%s\n", response.JSON201.RunId, response.JSON201.Status, response.JSON201.ApprovalId)
	return err
}

func (r *Runtime) handleApprovalsPlanDecide(ctx context.Context, args []string) error {
	var runID string
	var action string
	var reason string

	flags := newFlagSet("approvals plan-decide")
	flags.StringVar(&runID, "run-id", "", "run identifier")
	flags.StringVar(&action, "action", "", "plan decision action: confirm or rework")
	flags.StringVar(&reason, "reason", "", "decision rationale")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err}
	}
	if strings.TrimSpace(runID) == "" {
		return usageError{err: errors.New("--run-id is required")}
	}
	action = strings.ToLower(strings.TrimSpace(action))
	if action != string(gen.CreatePlanApprovalRequestActionConfirm) && action != string(gen.CreatePlanApprovalRequestActionRework) {
		return usageError{err: errors.New("--action must be confirm or rework")}
	}
	reasonPtr, err := optionalReason(action, reason)
	if err != nil {
		return usageError{err: err}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	planRevisionID, err := r.currentPlanRevisionID(ctx, client, runID)
	if err != nil {
		return err
	}

	response, err := client.CreatePlanApprovalWithResponse(ctx, runID, &gen.CreatePlanApprovalParams{
		XActorId:       r.config.ActorID,
		XRoleId:        r.config.RoleID,
		IdempotencyKey: newIdempotencyKey("approval-plan-decide"),
	}, gen.CreatePlanApprovalRequest{
		Action:         gen.CreatePlanApprovalRequestAction(action),
		PlanRevisionId: planRevisionID,
		Reason:         reasonPtr,
	})
	if err != nil {
		return err
	}
	if response.JSON201 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON201)
	}

	_, err = fmt.Fprintf(r.stdout, "plan decision recorded: run=%s status=%s approval_id=%s\n", response.JSON201.RunId, response.JSON201.Status, response.JSON201.ApprovalId)
	return err
}

func (r *Runtime) consumeSSE(stream io.Reader) error {
	scanner := bufio.NewScanner(stream)
	var eventType string
	var eventID string
	var dataLines []string

	flush := func() error {
		if len(dataLines) == 0 {
			eventType = ""
			eventID = ""
			return nil
		}

		data := strings.Join(dataLines, "\n")
		if r.config.JSONOutput {
			payload := map[string]string{"event": eventType, "id": eventID, "data": data}
			if err := writeJSON(r.stdout, payload); err != nil {
				return err
			}
		} else {
			if eventType == "" {
				eventType = "message"
			}
			if _, err := fmt.Fprintf(r.stdout, "[%s] %s %s\n", eventType, eventID, data); err != nil {
				return err
			}
		}

		eventType = ""
		eventID = ""
		dataLines = dataLines[:0]
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "id:") {
			eventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return flush()
}

func renderASCIIGraph(graph *gen.RunGraphResponse) string {
	if graph == nil {
		return ""
	}

	nodes := append([]gen.RunGraphNode(nil), graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})

	edges := append([]gen.RunGraphEdge(nil), graph.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("run graph: %s\n", graph.RunId))
	builder.WriteString("nodes:\n")
	for _, node := range nodes {
		builder.WriteString(fmt.Sprintf("  - [%s] %s (role=%s)\n", node.State, node.NodeId, node.RequiredRole))
	}
	builder.WriteString("edges:\n")
	for _, edge := range edges {
		condition := ""
		if edge.On != nil && strings.TrimSpace(*edge.On) != "" {
			condition = " [on=" + strings.TrimSpace(*edge.On) + "]"
		}
		builder.WriteString(fmt.Sprintf("  - %s -> %s%s\n", edge.From, edge.To, condition))
	}

	return builder.String()
}

func renderMermaidGraph(graph *gen.RunGraphResponse) string {
	if graph == nil {
		return ""
	}

	nodes := append([]gen.RunGraphNode(nil), graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})

	edges := append([]gen.RunGraphEdge(nil), graph.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})

	var builder strings.Builder
	builder.WriteString("graph TD\n")
	for _, node := range nodes {
		builder.WriteString(fmt.Sprintf("  %s[\"%s\\n%s\"]\n", toMermaidID(node.NodeId), node.NodeId, node.State))
	}
	for _, edge := range edges {
		if edge.On != nil && strings.TrimSpace(*edge.On) != "" {
			builder.WriteString(fmt.Sprintf("  %s -->|%s| %s\n", toMermaidID(edge.From), strings.TrimSpace(*edge.On), toMermaidID(edge.To)))
			continue
		}
		builder.WriteString(fmt.Sprintf("  %s --> %s\n", toMermaidID(edge.From), toMermaidID(edge.To)))
	}

	return builder.String()
}

func toMermaidID(raw string) string {
	replacer := strings.NewReplacer("-", "_", ".", "_", "/", "_", " ", "_")
	value := replacer.Replace(strings.TrimSpace(raw))
	if value == "" {
		return "node"
	}
	if value[0] >= '0' && value[0] <= '9' {
		return "n_" + value
	}
	return value
}

func newFlagSet(name string) *flag.FlagSet {
	set := flag.NewFlagSet(name, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	return set
}

func isHelpToken(value string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	return trimmed == "--help" || trimmed == "-h"
}

func hasHelpToken(values []string) bool {
	for _, value := range values {
		if value == "help" || isHelpToken(value) {
			return true
		}
	}
	return false
}

func writeCommandHelp(stdout io.Writer, topic []string) {
	if len(topic) == 0 {
		_, _ = io.WriteString(stdout, "usage: kd [--server URL] [--actor-id ID] [--role-id ROLE] [--json] [--timeout DURATION] <command>\n")
		_, _ = io.WriteString(stdout, "commands: init | clean | summon [serve|status|stop] | create decree | run status|artifacts export | graph show --ascii|--mermaid | node retry|rerun-from | timeline follow | approvals list|plan-decide|decide\n")
		return
	}

	primary := strings.TrimSpace(strings.ToLower(topic[0]))
	switch primary {
	case "init":
		_, _ = io.WriteString(stdout, "usage: kd init [--path <project-root>] [--force] [--publish-mode <direct|git_worktree>] [--autonomous-approvals <bool>] [--provider-engine <opencode>] [--provider-strict <bool>] [--listen <addr>] [--base-path <path>]\n")
	case "clean":
		_, _ = io.WriteString(stdout, "usage: kd clean [--path <project-root>]\n")
	case "summon":
		if len(topic) >= 2 {
			sub := strings.TrimSpace(strings.ToLower(topic[1]))
			switch sub {
			case "status":
				_, _ = io.WriteString(stdout, "usage: kd summon status [--config <path>]\n")
				return
			case "stop":
				_, _ = io.WriteString(stdout, "usage: kd summon stop [--config <path>]\n")
				return
			}
		}
		_, _ = io.WriteString(stdout, "usage: kd summon [serve] [--config <path>] [--ready-timeout <duration>] [--detach]\n")
		_, _ = io.WriteString(stdout, "       kd summon status [--config <path>]\n")
		_, _ = io.WriteString(stdout, "       kd summon stop [--config <path>]\n")
	case "create":
		_, _ = io.WriteString(stdout, "usage: kd create decree --title <title> --text <text> [--metadata-json <json>] [--skip-reviews-and-validations] [--autonomous-approvals <inherit|true|false>] [--run-until-all-approve] [--max-execution-review-cycles <n>] [--max-validation-rework-cycles <n>] --global-model <provider/model> [--agent-model-overrides <role=model,...>]\n")
	case "run":
		if len(topic) >= 2 && strings.TrimSpace(strings.ToLower(topic[1])) == "status" {
			_, _ = io.WriteString(stdout, "usage: kd run status --run-id <id>\n")
			return
		}
		if len(topic) >= 2 && strings.TrimSpace(strings.ToLower(topic[1])) == "artifacts" {
			_, _ = io.WriteString(stdout, "usage: kd run artifacts export --run-id <id> [--output <path>]\n")
			return
		}
		_, _ = io.WriteString(stdout, "usage: kd run start --decree-id <id> [--skip-reviews-and-validations] [--autonomous-approvals <inherit|true|false>] [--run-until-all-approve] [--max-execution-review-cycles <n>] [--max-validation-rework-cycles <n>] --global-model <provider/model> [--agent-model-overrides <role=model,...>]\n")
		_, _ = io.WriteString(stdout, "       kd run status --run-id <id>\n")
		_, _ = io.WriteString(stdout, "       kd run artifacts export --run-id <id> [--output <path>]\n")
	case "graph":
		_, _ = io.WriteString(stdout, "usage: kd graph show --run-id <id> [--ascii|--mermaid]\n")
	case "node":
		if len(topic) >= 2 && strings.TrimSpace(strings.ToLower(topic[1])) == "rerun-from" {
			_, _ = io.WriteString(stdout, "usage: kd node rerun-from --run-id <id> --node-key <node> --reason <text>\n")
			return
		}
		_, _ = io.WriteString(stdout, "usage: kd node retry --run-id <id> --node-key <node> --reason <text>\n")
		_, _ = io.WriteString(stdout, "       kd node rerun-from --run-id <id> --node-key <node> --reason <text>\n")
	case "timeline":
		_, _ = io.WriteString(stdout, "usage: kd timeline follow --run-id <id>\n")
	case "approvals":
		_, _ = io.WriteString(stdout, "usage: kd approvals list --run-id <id> [--limit <n>] [--cursor <cursor>]\n")
		_, _ = io.WriteString(stdout, "usage: kd approvals plan-decide --run-id <id> --action <confirm|rework> [--reason <text>]\n")
		_, _ = io.WriteString(stdout, "       note: --reason is required when --action is rework\n")
		_, _ = io.WriteString(stdout, "usage: kd approvals decide --run-id <id> --action <confirm|rework> [--reason <text>]\n")
		_, _ = io.WriteString(stdout, "       note: --reason is required when --action is rework\n")
	default:
		_, _ = io.WriteString(stdout, "usage: kd [--server URL] [--actor-id ID] [--role-id ROLE] [--json] [--timeout DURATION] <command>\n")
	}
}

func parseJSONObject(raw string, flagName string) (map[string]interface{}, error) {
	var value map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%s must be valid JSON object: %w", flagName, err)
	}
	if value == nil {
		return nil, fmt.Errorf("%s must be JSON object", flagName)
	}
	return value, nil
}

func installConfigForCLI() (installConfig, bool) {
	configPath := filepath.Join(defaultInstallDirName, defaultConfigFileName)
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		return installConfig{}, false
	}
	return cfg, true
}

func defaultPublishModeForCLI() string {
	configPath := filepath.Join(defaultInstallDirName, defaultConfigFileName)
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		return workspace.PublishModeDirect
	}
	return workspace.NormalizePublishMode(cfg.Publish.Mode)
}

func applyInstallModeDefaults(cfg *Config) {
	if cfg == nil || cfg.ServerExplicit || strings.TrimSpace(cfg.ServerAddress) != defaultServerAddress {
		return
	}
	installCfg, ok := loadInstallConfigForCLI()
	if !ok {
		return
	}
	// installCfg comes from readInstallConfigFromStruct, which
	// already canonicalizes Server.BasePath. No local re-trim needed.
	listen := strings.TrimSpace(installCfg.Server.Listen)
	if listen == "" || installCfg.Server.BasePath == "" {
		return
	}
	cfg.ServerAddress = fmt.Sprintf("http://%s%s", listen, installCfg.Server.BasePath)
}

func requireGlobalModelForInstallMode(globalModel string, cfg Config) error {
	if cfg.ServerExplicit {
		return nil
	}
	installCfg, ok := loadInstallConfigForCLI()
	if !ok {
		return nil
	}
	if strings.TrimSpace(installCfg.Provider.Engine) != "opencode" {
		return nil
	}
	if strings.TrimSpace(globalModel) != "" {
		return nil
	}
	return errors.New("--global-model is required when provider.engine=opencode")
}

func requirePublishModeSupportForInstallMode(publishMode gen.RunPublishMode, cfg Config) error {
	if cfg.ServerExplicit || publishMode != gen.GitWorktree {
		return nil
	}
	installCfg, ok := loadInstallConfigForCLI()
	if !ok {
		return nil
	}
	topology := workspace.ResolveGitProjectTopology(publishOutputRootFromConfig(installCfg), nil)
	if topology.WorktreeAvailable {
		return nil
	}
	if strings.TrimSpace(topology.FailureReason) == "" {
		return errors.New("git worktree is not available for the configured project root")
	}
	return errors.New(topology.FailureReason)
}

func parsePublishModeFlag(raw string) (gen.RunPublishMode, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		trimmed = workspace.PublishModeDirect
	}
	if !workspace.IsSupportedPublishMode(trimmed) {
		return "", errors.New("--publish-mode must be one of: direct, git_worktree")
	}
	return gen.RunPublishMode(workspace.NormalizePublishMode(trimmed)), nil
}

func parseAutonomousApprovalsOverride(raw string) (*bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "inherit", "default":
		return nil, nil
	case "true", "on", "auto", "autonomous":
		value := true
		return &value, nil
	case "false", "off", "manual":
		value := false
		return &value, nil
	default:
		return nil, errors.New("--autonomous-approvals must be one of: inherit, true, false")
	}
}

func parseAgentModelOverridesFlag(raw string) (map[string]string, error) {
	result := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(pair)
		if trimmed == "" {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("--agent-model-overrides must use role=model format")
		}
		roleID := strings.TrimSpace(parts[0])
		modelID := strings.TrimSpace(parts[1])
		if roleID == "" || modelID == "" {
			return nil, fmt.Errorf("--agent-model-overrides contains empty role or model")
		}
		result[roleID] = modelID
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("--agent-model-overrides must include at least one role=model pair")
	}
	return result, nil
}

func buildRunModelSelection(globalModel string, agentModelOverridesRaw string) (*gen.RunModelSelection, error) {
	selection := &gen.RunModelSelection{}
	if trimmedGlobalModel := strings.TrimSpace(globalModel); trimmedGlobalModel != "" {
		selection.Global = gen.RunModelSelectionEntry{Model: trimmedGlobalModel}
	}
	if strings.TrimSpace(agentModelOverridesRaw) != "" {
		overrides, err := parseAgentModelOverridesFlag(agentModelOverridesRaw)
		if err != nil {
			return nil, err
		}
		entries := make(map[string]gen.RunModelSelectionEntry, len(overrides))
		for roleID, modelID := range overrides {
			entries[roleID] = gen.RunModelSelectionEntry{Model: modelID}
		}
		selection.RoleOverrides = &entries
	}
	if strings.TrimSpace(selection.Global.Model) == "" && (selection.RoleOverrides == nil || len(*selection.RoleOverrides) == 0) {
		return nil, nil
	}
	return selection, nil
}

func assignLoopOverrides(request *gen.CreateRunRequest, maxExecutionReviewCycles int, maxValidationReworkCycles int) error {
	if maxExecutionReviewCycles < -1 {
		return errors.New("--max-execution-review-cycles must be greater than or equal to zero when provided")
	}
	if maxValidationReworkCycles < -1 {
		return errors.New("--max-validation-rework-cycles must be greater than or equal to zero when provided")
	}
	if maxExecutionReviewCycles >= 0 {
		request.MaxExecutionReviewCycles = &maxExecutionReviewCycles
	}
	if maxValidationReworkCycles >= 0 {
		request.MaxValidationReworkCycles = &maxValidationReworkCycles
	}
	return nil
}

func applyRunWorkflowOptions(request *gen.CreateRunRequest, skipReviewsAndValidations bool, runUntilAllApprove bool, maxExecutionReviewCycles int, maxValidationReworkCycles int) error {
	if skipReviewsAndValidations {
		request.SkipReviewsAndValidations = &skipReviewsAndValidations
	}
	request.RunUntilAllApprove = &runUntilAllApprove
	return assignLoopOverrides(request, maxExecutionReviewCycles, maxValidationReworkCycles)
}

func newIdempotencyKey(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func writeJSON(writer io.Writer, value interface{}) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "%s\n", encoded)
	return err
}

type usageError struct {
	err error
}

func (e usageError) Error() string {
	if e.err == nil {
		return "invalid command usage"
	}
	return e.err.Error()
}

type apiError struct {
	statusCode int
	code       string
	message    string
	retryable  bool
}

type decreeIssueError struct {
	DecreeID                  string
	SkipReviewsAndValidations bool
	Err                       error
}

func (e *decreeIssueError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("decree created (%s) but run start failed: %v", e.DecreeID, e.Err)
}

func (e *apiError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("status=%d", e.statusCode)}
	if e.code != "" {
		parts = append(parts, "code="+e.code)
	}
	if e.message != "" {
		parts = append(parts, "message="+e.message)
	}
	parts = append(parts, "retryable="+strconv.FormatBool(e.retryable))
	return strings.Join(parts, " ")
}

func unwrapAPIError(status int, badRequest *gen.ErrorResponse, fallback *gen.ErrorResponse, body []byte) error {
	if badRequest != nil {
		return &apiError{statusCode: status, code: string(badRequest.Error.Code), message: badRequest.Error.Message, retryable: badRequest.Error.Retryable}
	}
	if fallback != nil {
		return &apiError{statusCode: status, code: string(fallback.Error.Code), message: fallback.Error.Message, retryable: fallback.Error.Retryable}
	}
	return &apiError{statusCode: status, message: strings.TrimSpace(string(body))}
}

func (r *Runtime) writeError(err error) int {
	switch typed := err.(type) {
	case usageError:
		writeUsageError(r.stderr, typed.err)
		return ExitUsage
	case *apiError:
		r.writeStructuredError(typed)
		return ExitAPI
	case *decreeIssueError:
		r.writeDecreeIssueError(typed)
		if _, ok := typed.Err.(*apiError); ok {
			return ExitAPI
		}
		return ExitRuntime
	default:
		if errors.Is(err, errSummonForcedShutdown) {
			_, _ = fmt.Fprintln(r.stderr, "summon forced shutdown")
			return ExitSignal
		}
		if errors.Is(err, context.Canceled) {
			_, _ = fmt.Fprintln(r.stderr, "command canceled")
			return ExitRuntime
		}
		_, _ = fmt.Fprintf(r.stderr, "runtime error: %v\n", err)
		return ExitRuntime
	}
}

func (r *Runtime) writeDecreeIssueError(err *decreeIssueError) {
	recoveryCommand := fmt.Sprintf("kd run start --decree-id %s", strings.TrimSpace(err.DecreeID))
	if err.SkipReviewsAndValidations {
		recoveryCommand += " --skip-reviews-and-validations"
	}
	workflowID := workflow.BuiltInWorkflowID(err.SkipReviewsAndValidations)
	if r.config.JSONOutput {
		payload := map[string]any{
			"error": map[string]any{
				"message": err.Error(),
			},
			"decree_id":                    err.DecreeID,
			"skip_reviews_and_validations": err.SkipReviewsAndValidations,
			"workflow_id":                  workflowID,
			"recovery_command":             recoveryCommand,
		}
		_ = writeJSON(r.stderr, payload)
		return
	}

	_, _ = fmt.Fprintf(r.stderr, "api error: decree created (%s) but run start failed\n", err.DecreeID)
	if err.Err != nil {
		_, _ = fmt.Fprintf(r.stderr, "  cause: %v\n", err.Err)
	}
	_, _ = fmt.Fprintf(r.stderr, "  recovery: %s\n", recoveryCommand)
}

func (r *Runtime) writeStructuredError(err *apiError) {
	if r.config.JSONOutput {
		payload := map[string]interface{}{
			"error": map[string]interface{}{
				"status":    err.statusCode,
				"code":      err.code,
				"message":   err.message,
				"retryable": err.retryable,
			},
		}
		_ = writeJSON(r.stderr, payload)
		return
	}

	_, _ = fmt.Fprintf(r.stderr, "api error: %s\n", err.Error())
}

func writeUsageError(stderr io.Writer, err error) {
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "usage error: %v\n", err)
	}
	_, _ = io.WriteString(stderr, "usage: kd [--server URL] [--actor-id ID] [--role-id ROLE] [--json] [--timeout DURATION] <command>\n")
	_, _ = io.WriteString(stderr, "commands: init | summon [serve|status|stop] | create decree | run status|artifacts export | graph show --ascii|--mermaid | node retry|rerun-from | timeline follow | approvals list|plan-decide|decide\n")
}
