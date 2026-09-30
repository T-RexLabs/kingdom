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
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/workflow"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/idempotency"
	"kingdom/internal/localauth"
	"kingdom/internal/state"
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
	defaultTimeout       = 30 * time.Second
)

var loadInstallConfigForCLI = installConfigForCLI
var findActiveSummonForCLI = findActiveInstallModeSummon

type Config struct {
	ServerAddress      string
	ServerExplicit     bool
	JSONOutput         bool
	Timeout            time.Duration
	CredentialFile     string
	CredentialExplicit bool
	IdempotencyKey     string
}

type Runtime struct {
	config Config
	stdout io.Writer
	stderr io.Writer
}

func Execute(ctx context.Context, stdout io.Writer, stderr io.Writer, args []string) int {
	if len(args) == 0 {
		writeUsageError(stderr, errors.New("missing command"), nil)
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
		writeUsageError(stderr, err, nil)
		return ExitUsage
	}

	if len(remaining) == 0 {
		writeUsageError(stderr, errors.New("missing command"), nil)
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
	if routeErr := validateCLIRoute(remaining); routeErr != nil {
		writeUsageError(stderr, routeErr.err, routeErr.helpTopic)
		return ExitUsage
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
		case arg == "--credential-file":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--credential-file requires a value")
			}
			i++
			cfg.CredentialFile = strings.TrimSpace(args[i])
			cfg.CredentialExplicit = true
		case strings.HasPrefix(arg, "--credential-file="):
			cfg.CredentialFile = strings.TrimSpace(strings.TrimPrefix(arg, "--credential-file="))
			cfg.CredentialExplicit = true
		case arg == "--idempotency-key":
			if i+1 >= len(args) {
				return Config{}, nil, fmt.Errorf("--idempotency-key requires a value")
			}
			i++
			cfg.IdempotencyKey = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--idempotency-key="):
			cfg.IdempotencyKey = strings.TrimSpace(strings.TrimPrefix(arg, "--idempotency-key="))
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
	if cfg.Timeout <= 0 {
		return Config{}, nil, errors.New("--timeout must be greater than zero")
	}
	if cfg.IdempotencyKey != "" {
		key, err := idempotency.Parse(cfg.IdempotencyKey)
		if err != nil {
			return Config{}, nil, fmt.Errorf("invalid --idempotency-key: %w", err)
		}
		if err := key.ValidateAt(time.Now()); err != nil {
			return Config{}, nil, fmt.Errorf("invalid --idempotency-key: %w", err)
		}
		cfg.IdempotencyKey = key.Raw
	}
	cfg.ServerExplicit = serverExplicit

	applyInstallModeDefaults(&cfg)
	normalizedServer, err := normalizeAPIServerAddress(cfg.ServerAddress)
	if err != nil {
		return Config{}, nil, fmt.Errorf("invalid --server value: %w", err)
	}
	cfg.ServerAddress = normalizedServer
	if err := validateCLIAPIAuthentication(cfg); err != nil {
		return Config{}, nil, err
	}

	return cfg, remaining, nil
}

func validateCLIAPIAuthentication(cfg Config) error {
	parsed, err := url.Parse(strings.TrimSpace(cfg.ServerAddress))
	if err != nil {
		return fmt.Errorf("parse API server: %w", err)
	}
	if isLoopbackAPIHost(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme != "https" {
		return errors.New("non-loopback API servers require HTTPS")
	}
	if !cfg.CredentialExplicit || strings.TrimSpace(cfg.CredentialFile) == "" {
		return errors.New("non-loopback API servers require an explicit --credential-file")
	}
	return nil
}

func isLoopbackAPIHost(host string) bool {
	if strings.EqualFold(strings.TrimSpace(host), "localhost") {
		return true
	}
	address, err := netip.ParseAddr(strings.TrimSpace(host))
	return err == nil && address.Unmap().IsLoopback()
}

func normalizeAPIServerAddress(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("must use http or https")
	}
	if parsed.Host == "" {
		return "", errors.New("must be an absolute URL with a host")
	}
	if parsed.User != nil {
		return "", errors.New("must not include user information")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("must not include a query or fragment")
	}
	if parsed.RawPath != "" {
		return "", errors.New("must not use an encoded API path")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if parsed.Path == "" {
		parsed.Path = "/api/v1"
	}
	return parsed.String(), nil
}

func (r *Runtime) run(ctx context.Context, args []string) error {
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

	switch {
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"create", "decree"}):
		return r.handleCreateDecree(ctx, args[2:])
	case len(args) >= 4 && sameHelpPath(args[:4], []string{"connections", "github", "key", "provision"}):
		return r.handleGitHubKeyProvision(args[4:], false)
	case len(args) >= 4 && sameHelpPath(args[:4], []string{"connections", "github", "key", "rotate"}):
		return r.handleGitHubKeyProvision(args[4:], true)
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"run", "start"}):
		return r.handleRunStart(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"run", "status"}):
		return r.handleRunStatus(ctx, args[2:])
	case len(args) >= 3 && sameHelpPath(args[:3], []string{"run", "artifacts", "export"}):
		return r.handleRunArtifactsExport(ctx, args[3:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"graph", "show"}):
		return r.handleGraphShow(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"node", "retry"}):
		return r.handleNodeRetry(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"node", "rerun-from"}):
		return r.handleNodeRerunFrom(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"timeline", "follow"}):
		return r.handleTimelineFollow(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"approvals", "list"}):
		return r.handleApprovalsList(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"approvals", "plan-decide"}):
		return r.handleApprovalsPlanDecide(ctx, args[2:])
	case len(args) >= 2 && sameHelpPath(args[:2], []string{"approvals", "decide"}):
		return r.handleApprovalsDecide(ctx, args[2:])
	default:
		return usageError{err: errors.New("invalid command"), helpTopic: nearestHelpTopic(args)}
	}
}

type commandRoute struct {
	path []string
}

var cliCommandRoutes = []commandRoute{
	{path: []string{"init"}},
	{path: []string{"clean"}},
	{path: []string{"summon"}},
	{path: []string{"summon", "status"}},
	{path: []string{"summon", "logs"}},
	{path: []string{"summon", "stop"}},
	{path: []string{"connections", "github", "key", "provision"}},
	{path: []string{"connections", "github", "key", "rotate"}},
	{path: []string{"create", "decree"}},
	{path: []string{"run", "start"}},
	{path: []string{"run", "status"}},
	{path: []string{"run", "artifacts", "export"}},
	{path: []string{"graph", "show"}},
	{path: []string{"node", "retry"}},
	{path: []string{"node", "rerun-from"}},
	{path: []string{"timeline", "follow"}},
	{path: []string{"approvals", "list"}},
	{path: []string{"approvals", "plan-decide"}},
	{path: []string{"approvals", "decide"}},
}

func validateCLIRoute(args []string) *usageError {
	trimmed := commandPathPrefix(args)
	if len(trimmed) == 0 {
		return nil
	}
	if longestLeafRoutePrefix(trimmed) != nil {
		return nil
	}
	longestParent := nearestKnownHelpPath(trimmed)
	if len(longestParent) == 0 {
		return &usageError{err: fmt.Errorf("invalid command %q", trimmed[0])}
	}
	if len(longestParent) == len(trimmed) {
		return &usageError{err: errors.New("incomplete command"), helpTopic: longestParent}
	}
	next := trimmed[len(longestParent)]
	return &usageError{err: fmt.Errorf("invalid command %q", next), helpTopic: longestParent}
}

func commandPathPrefix(args []string) []string {
	trimmed := make([]string, 0, len(args))
	for _, arg := range args {
		normalized := strings.TrimSpace(strings.ToLower(arg))
		if normalized == "" || normalized == "help" {
			continue
		}
		if strings.HasPrefix(normalized, "-") {
			break
		}
		trimmed = append(trimmed, normalized)
	}
	return trimmed
}

func longestLeafRoutePrefix(path []string) []string {
	var longest []string
	for _, route := range cliCommandRoutes {
		if len(route.path) > len(path) {
			continue
		}
		matched := true
		for idx := range route.path {
			if route.path[idx] != path[idx] {
				matched = false
				break
			}
		}
		if matched && len(route.path) > len(longest) {
			longest = route.path
		}
	}
	if len(longest) == 0 {
		return nil
	}
	resolved := make([]string, len(longest))
	copy(resolved, longest)
	return resolved
}

func nearestKnownHelpPath(path []string) []string {
	for length := len(path); length > 0; length-- {
		candidate := path[:length]
		if lookupHelpTopic(candidate) != nil {
			resolved := make([]string, len(candidate))
			copy(resolved, candidate)
			return resolved
		}
	}
	return nil
}

func (r *Runtime) newResponsesClient() (*gen.ClientWithResponses, error) {
	httpClient, err := r.authenticatedHTTPClient(r.config.Timeout)
	if err != nil {
		return nil, err
	}
	return gen.NewClientWithResponses(r.config.ServerAddress, gen.WithHTTPClient(httpClient))
}

func (r *Runtime) newRawClient() (*gen.Client, error) {
	httpClient, err := r.authenticatedHTTPClient(0)
	if err != nil {
		return nil, err
	}
	return gen.NewClient(r.config.ServerAddress, gen.WithHTTPClient(httpClient))
}

type bearerTransport struct {
	token         []byte
	allowedOrigin string
	base          http.RoundTripper
}

type ambiguousMutationError struct {
	Key   string
	Cause error
}

func (e *ambiguousMutationError) Error() string {
	return fmt.Sprintf("mutation outcome remains ambiguous; retry with --idempotency-key %s: %v", e.Key, e.Cause)
}

func (e *ambiguousMutationError) Unwrap() error { return e.Cause }

func (t bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if httpOrigin(req.URL) != t.allowedOrigin {
		return nil, fmt.Errorf("refusing to send operator credential to %s", httpOrigin(req.URL))
	}
	canReplayBody := req.GetBody != nil || req.Body == nil || req.Body == http.NoBody
	for attempt := 0; ; attempt++ {
		if req.Context().Err() != nil {
			if attempt > 0 && req.Header.Get("Idempotency-Key") != "" {
				return nil, &ambiguousMutationError{Key: req.Header.Get("Idempotency-Key"), Cause: req.Context().Err()}
			}
			return nil, req.Context().Err()
		}
		body := req.Body
		if attempt > 0 {
			body = http.NoBody
			if req.GetBody != nil {
				var bodyErr error
				body, bodyErr = req.GetBody()
				if bodyErr != nil {
					return nil, fmt.Errorf("reopen idempotent request body: %w", bodyErr)
				}
			}
		}
		response, err := t.base.RoundTrip(t.authenticatedClone(req, body))
		if err == nil && req.Header.Get("Idempotency-Key") != "" {
			err = spoolHTTPResponseBody(response)
			if err != nil && response != nil && response.Body != nil {
				_ = response.Body.Close()
				response = nil
			}
		}
		pending := false
		var inspectErr error
		if req.Header.Get("Idempotency-Key") != "" && err == nil {
			pending, inspectErr = responseHasCanonicalErrorCode(response, errorcatalog.CodeIdempotencyPending)
		}
		if inspectErr != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			return nil, &ambiguousMutationError{Key: req.Header.Get("Idempotency-Key"), Cause: inspectErr}
		}
		shouldRetry := req.Header.Get("Idempotency-Key") != "" && canReplayBody && (err != nil || pending)
		if !shouldRetry {
			return response, err
		}
		if attempt == 2 {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			cause := err
			if cause == nil {
				cause = errors.New("idempotency request remains pending")
			}
			return nil, &ambiguousMutationError{Key: req.Header.Get("Idempotency-Key"), Cause: cause}
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return nil, &ambiguousMutationError{Key: req.Header.Get("Idempotency-Key"), Cause: req.Context().Err()}
		case <-timer.C:
		}
	}
}

func spoolHTTPResponseBody(response *http.Response) error {
	if response == nil || response.Body == nil {
		return nil
	}
	original := response.Body
	defer original.Close()
	body, err := os.CreateTemp("", "kingdom-mutation-response-*")
	if err != nil {
		return err
	}
	spool := &temporaryResponseBody{File: body}
	_, err = io.Copy(body, original)
	if err == nil {
		_, err = body.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = spool.Close()
		return fmt.Errorf("read idempotent mutation response: %w", err)
	}
	response.Body = spool
	return nil
}

type temporaryResponseBody struct{ *os.File }

func (b *temporaryResponseBody) Close() error {
	err := b.File.Close()
	removeErr := os.Remove(b.Name())
	if err != nil {
		return err
	}
	return removeErr
}

func responseHasCanonicalErrorCode(response *http.Response, code errorcatalog.Code) (bool, error) {
	if response == nil || response.StatusCode != http.StatusConflict || response.Body == nil {
		return false, nil
	}
	body, ok := response.Body.(*temporaryResponseBody)
	if !ok {
		if err := spoolHTTPResponseBody(response); err != nil {
			return false, err
		}
		body = response.Body.(*temporaryResponseBody)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	decodeErr := json.NewDecoder(body).Decode(&envelope)
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	// A non-canonical response is returned unchanged, as before.
	return decodeErr == nil && envelope.Error.Code == string(code), nil
}

func (t bearerTransport) authenticatedClone(req *http.Request, body io.ReadCloser) *http.Request {
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+string(t.token))
	clone.Body = body
	return clone
}

func (r *Runtime) authenticatedHTTPClient(timeout time.Duration) (*http.Client, error) {
	transport := http.DefaultTransport
	credentialFile := strings.TrimSpace(r.config.CredentialFile)
	if credentialFile == "" {
		return nil, errors.New("local operator credential was not found; run the command from an initialized Kingdom workspace or pass --credential-file")
	}
	if err := validateCLIAPIAuthentication(r.config); err != nil {
		return nil, err
	}
	readCredential := localauth.ReadCredential
	if r.config.CredentialExplicit {
		readCredential = localauth.ReadCredentialFile
	}
	token, err := readCredential(credentialFile)
	if err != nil {
		return nil, err
	}
	serverURL, err := url.Parse(r.config.ServerAddress)
	if err != nil {
		return nil, fmt.Errorf("parse server address: %w", err)
	}
	allowedOrigin := httpOrigin(serverURL)
	return &http.Client{
		Timeout:   timeout,
		Transport: bearerTransport{token: token, allowedOrigin: allowedOrigin, base: transport},
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			if httpOrigin(req.URL) != allowedOrigin {
				return fmt.Errorf("refusing authenticated redirect to %s", httpOrigin(req.URL))
			}
			return nil
		},
	}, nil
}

func httpOrigin(value *url.URL) string {
	if value == nil {
		return ""
	}
	return strings.ToLower(value.Scheme) + "://" + strings.ToLower(value.Host)
}

func (r *Runtime) handleGitHubKeyProvision(args []string, rotate bool) error {
	flags := newFlagSet("connections github key")
	var connectionID, from string
	flags.StringVar(&connectionID, "connection-id", "", "saved connection id")
	flags.StringVar(&from, "from", "", "RSA PKCS#1 or PKCS#8 PEM file")
	if err := flags.Parse(args); err != nil {
		return usageError{err: err, helpTopic: []string{"connections"}}
	}
	if flags.NArg() != 0 || strings.TrimSpace(connectionID) == "" || strings.TrimSpace(from) == "" {
		return usageError{err: errors.New("--connection-id and --from are required"), helpTopic: []string{"connections"}}
	}
	credentialPath := strings.TrimSpace(r.config.CredentialFile)
	if credentialPath == "" {
		return errors.New("GitHub key provisioning requires an initialized Kingdom workspace")
	}
	secretsRoot := filepath.Dir(credentialPath)
	dbPath := filepath.Join(filepath.Dir(secretsRoot), "kingdom.db")
	db, err := openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		return fmt.Errorf("open Kingdom state: %w", err)
	}
	defer db.Close()
	externalStore := state.NewExternalStore(db)
	connection, err := externalStore.GetConnectionForCredentialMutation(context.Background(), connectionID)
	if err != nil {
		return fmt.Errorf("validate saved connection: %w", err)
	}
	if connection.CredentialStrategy != "app_private_key" {
		return errors.New("GitHub private-key provision and rotate are unavailable for shared GitHub connections; use Reconnect")
	}
	store, err := githubexternal.NewKeyStore(secretsRoot)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Clean(from))
	if err != nil {
		return fmt.Errorf("read GitHub App private key: %w", err)
	}
	action := "provisioned"
	if rotate {
		action = "rotated"
	}
	eventType := "credential.provisioned"
	if rotate {
		eventType = "credential.rotated"
	}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	targetGeneration := connection.CredentialGeneration + 1
	if _, err = store.InstallAudited(connectionID, targetGeneration, raw, rotate, func() error {
		return externalStore.RecordCredentialEvent(context.Background(), connectionID, connection.RowVersion, targetGeneration, "local-operator", fmt.Sprintf("external_audit_%d", time.Now().UTC().UnixNano()), eventType, at)
	}); err != nil {
		return fmt.Errorf("provision and record GitHub App private key: %w", err)
	}
	if r.config.JSONOutput {
		return writeJSON(r.stdout, map[string]string{"connection_id": connectionID, "credential_state": "present", "action": action})
	}
	_, err = fmt.Fprintf(r.stdout, "GitHub App private key %s for connection %s\n", action, connectionID)
	return err
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
	response, err := client.GetRunPlanWithResponse(ctx, runID)
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
	opts := createDecreeOptions{}
	commandUsage := func(err error) error {
		return usageError{err: err, helpTopic: []string{"create", "decree"}}
	}
	flags := newFlagSet("create decree")
	bindCreateDecreeFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"create", "decree"}); err != nil {
		return commandUsage(err)
	}
	if opts.title == "" || opts.text == "" {
		return commandUsage(errors.New("--title and --text are required"))
	}
	if err := requireGlobalModelForInstallMode(opts.globalModel, r.config); err != nil {
		return commandUsage(err)
	}

	decreeRequest := gen.CreateDecreeRequest{DecreeTitle: opts.title, DecreeText: opts.text}
	if strings.TrimSpace(opts.metadataRaw) != "" {
		metadata, err := parseJSONObject(opts.metadataRaw, "--metadata-json")
		if err != nil {
			return commandUsage(err)
		}
		decreeRequest.Metadata = &metadata
	}
	publishMode, err := parsePublishModeFlag(opts.publishModeRaw)
	if err != nil {
		return commandUsage(err)
	}
	if err := requirePublishModeSupportForInstallMode(publishMode, r.config); err != nil {
		return commandUsage(err)
	}
	runRequest := gen.CreateRunRequest{PublishMode: publishMode}
	autonomousApprovals, err := parseAutonomousApprovalsOverride(opts.autonomousApprovalsRaw)
	if err != nil {
		return commandUsage(err)
	}
	if autonomousApprovals != nil {
		runRequest.AutonomousApprovals = autonomousApprovals
	}
	if err := applyRunWorkflowOptions(&runRequest, opts.skipReviewsAndValidations, opts.runUntilAllApprove, opts.maxExecutionReviewCycles, opts.maxValidationReworkCycles); err != nil {
		return commandUsage(err)
	}
	modelSelection, err := buildRunModelSelection(opts.globalModel, opts.agentModelOverridesRaw)
	if err != nil {
		return commandUsage(err)
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
		return &decreeIssueError{DecreeID: decreeResponse.DecreeId, SkipReviewsAndValidations: opts.skipReviewsAndValidations, RunRequest: runRequest, Err: err}
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
	key, err := r.newIdempotencyKey("decree-create")
	if err != nil {
		return nil, err
	}

	response, err := client.CreateDecreeWithResponse(ctx, &gen.CreateDecreeParams{
		IdempotencyKey: key,
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
	key, err := r.newIdempotencyKey("run-start")
	if err != nil {
		return nil, err
	}

	response, err := client.CreateRunWithResponse(ctx, &gen.CreateRunParams{
		IdempotencyKey: key,
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
	opts := runStartOptions{}
	commandUsage := func(err error) error {
		return usageError{err: err, helpTopic: []string{"run", "start"}}
	}
	flags := newFlagSet("run start")
	bindRunStartFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"run", "start"}); err != nil {
		return commandUsage(err)
	}
	if opts.decreeID == "" {
		return commandUsage(errors.New("--decree-id is required"))
	}
	if err := requireGlobalModelForInstallMode(opts.globalModel, r.config); err != nil {
		return commandUsage(err)
	}

	publishMode, err := parsePublishModeFlag(opts.publishModeRaw)
	if err != nil {
		return commandUsage(err)
	}
	if err := requirePublishModeSupportForInstallMode(publishMode, r.config); err != nil {
		return commandUsage(err)
	}
	request := gen.CreateRunRequest{DecreeId: opts.decreeID, PublishMode: publishMode}
	autonomousApprovals, err := parseAutonomousApprovalsOverride(opts.autonomousApprovalsRaw)
	if err != nil {
		return commandUsage(err)
	}
	if autonomousApprovals != nil {
		request.AutonomousApprovals = autonomousApprovals
	}
	if err := applyRunWorkflowOptions(&request, opts.skipReviewsAndValidations, opts.runUntilAllApprove, opts.maxExecutionReviewCycles, opts.maxValidationReworkCycles); err != nil {
		return commandUsage(err)
	}
	modelSelection, err := buildRunModelSelection(opts.globalModel, opts.agentModelOverridesRaw)
	if err != nil {
		return commandUsage(err)
	}
	if modelSelection != nil {
		request.ModelSelection = *modelSelection
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	key, err := r.newIdempotencyKey("run-start")
	if err != nil {
		return err
	}

	response, err := client.CreateRunWithResponse(ctx, &gen.CreateRunParams{
		IdempotencyKey: key,
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
	opts := runStatusOptions{}
	flags := newFlagSet("run status")
	bindRunStatusFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"run", "status"}); err != nil {
		return err
	}
	if opts.runID == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"run", "status"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunWithResponse(ctx, opts.runID)
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
	opts := runArtifactsExportOptions{}
	flags := newFlagSet("run artifacts export")
	bindRunArtifactsExportFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"run", "artifacts", "export"}); err != nil {
		return err
	}
	if opts.runID == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"run", "artifacts", "export"}}
	}
	if strings.TrimSpace(opts.outputPath) == "" {
		opts.outputPath = fmt.Sprintf("%s-artifacts.zip", opts.runID)
	}

	client, err := r.newRawClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunArtifactsArchive(ctx, opts.runID)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		var envelope gen.ErrorResponse
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			return fmt.Errorf("artifact archive HTTP %d: %w", response.StatusCode, err)
		}
		return unwrapAPIError(response.StatusCode, &envelope, nil, nil)
	}
	size, err := saveArtifactDownload(response.Body, response.ContentLength, opts.outputPath)
	if err != nil {
		return fmt.Errorf("write artifact archive: %w", err)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, map[string]any{
			"run_id":      opts.runID,
			"output_path": opts.outputPath,
			"size_bytes":  size,
		})
	}

	_, err = fmt.Fprintf(r.stdout, "artifacts exported: run=%s file=%s size=%d\n", opts.runID, opts.outputPath, size)
	return err
}

func saveArtifactDownload(body io.Reader, expectedSize int64, destination string) (int64, error) {
	f, err := os.CreateTemp(filepath.Dir(destination), ".kingdom-download-*")
	if err != nil {
		return 0, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	size, err := io.Copy(f, body)
	if err != nil {
		return 0, err
	}
	if expectedSize >= 0 && size != expectedSize {
		return 0, io.ErrUnexpectedEOF
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(f.Name(), destination); err != nil {
		return 0, err
	}
	return size, nil
}

func (r *Runtime) handleGraphShow(ctx context.Context, args []string) error {
	opts := graphShowOptions{}
	flags := newFlagSet("graph show")
	bindGraphShowFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"graph", "show"}); err != nil {
		return err
	}
	if opts.runID == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"graph", "show"}}
	}
	if opts.asMermaid && opts.asASCII {
		return usageError{err: errors.New("choose one of --ascii or --mermaid"), helpTopic: []string{"graph", "show"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	response, err := client.GetRunGraphWithResponse(ctx, opts.runID)
	if err != nil {
		return err
	}
	if response.JSON200 == nil {
		return unwrapAPIError(response.StatusCode(), response.JSON400, response.JSONDefault, response.Body)
	}

	if r.config.JSONOutput {
		return writeJSON(r.stdout, response.JSON200)
	}

	if opts.asMermaid {
		_, err = io.WriteString(r.stdout, renderMermaidGraph(response.JSON200))
		return err
	}
	_, err = io.WriteString(r.stdout, renderASCIIGraph(response.JSON200))
	return err
}

func (r *Runtime) handleNodeRetry(ctx context.Context, args []string) error {
	opts := nodeMutationOptions{}
	flags := newFlagSet("node retry")
	bindNodeMutationFlags(flags, &opts, "retry reason")
	if err := parseLeafFlags(flags, args, []string{"node", "retry"}); err != nil {
		return err
	}
	if opts.runID == "" || opts.nodeKey == "" || opts.reason == "" {
		return usageError{err: errors.New("--run-id, --node-key, and --reason are required"), helpTopic: []string{"node", "retry"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	key, err := r.newIdempotencyKey("node-retry")
	if err != nil {
		return err
	}

	response, err := client.RetryNodeWithResponse(ctx, opts.runID, opts.nodeKey, &gen.RetryNodeParams{
		IdempotencyKey: key,
	}, gen.RetryNodeRequest{Reason: opts.reason})
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
	opts := nodeMutationOptions{}
	flags := newFlagSet("node rerun-from")
	bindNodeMutationFlags(flags, &opts, "rerun reason")
	if err := parseLeafFlags(flags, args, []string{"node", "rerun-from"}); err != nil {
		return err
	}
	if opts.runID == "" || opts.nodeKey == "" || opts.reason == "" {
		return usageError{err: errors.New("--run-id, --node-key, and --reason are required"), helpTopic: []string{"node", "rerun-from"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	key, err := r.newIdempotencyKey("node-rerun-from")
	if err != nil {
		return err
	}

	response, err := client.RerunFromNodeWithResponse(ctx, opts.runID, opts.nodeKey, &gen.RerunFromNodeParams{
		IdempotencyKey: key,
	}, gen.RerunFromNodeRequest{Reason: opts.reason})
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
	opts := timelineFollowOptions{}
	flags := newFlagSet("timeline follow")
	bindTimelineFollowFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"timeline", "follow"}); err != nil {
		return err
	}
	if opts.runID == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"timeline", "follow"}}
	}

	client, err := r.newRawClient()
	if err != nil {
		return err
	}

	follow := true
	response, err := client.StreamEvents(ctx, &gen.StreamEventsParams{RunId: opts.runID, Follow: &follow})
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
	opts := approvalsListOptions{}
	flags := newFlagSet("approvals list")
	bindApprovalsListFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"approvals", "list"}); err != nil {
		return err
	}
	if opts.runID == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"approvals", "list"}}
	}
	if opts.limit <= 0 {
		return usageError{err: errors.New("--limit must be greater than zero"), helpTopic: []string{"approvals", "list"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}

	limitValue := gen.Limit(opts.limit)
	params := &gen.GetRunTimelineParams{Limit: &limitValue}
	if opts.cursor != "" {
		cursorValue := gen.Cursor(opts.cursor)
		params.Cursor = &cursorValue
	}

	response, err := client.GetRunTimelineWithResponse(ctx, opts.runID, params)
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
	opts := approvalDecisionOptions{}
	flags := newFlagSet("approvals decide")
	bindApprovalDecisionFlags(flags, &opts, "final decision `action`: confirm or rework")
	if err := parseLeafFlags(flags, args, []string{"approvals", "decide"}); err != nil {
		return err
	}
	if strings.TrimSpace(opts.runID) == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"approvals", "decide"}}
	}
	opts.action = strings.ToLower(strings.TrimSpace(opts.action))
	action := opts.action
	if action != string(gen.CreateFinalApprovalRequestActionConfirm) && action != string(gen.CreateFinalApprovalRequestActionRework) {
		return usageError{err: errors.New("--action must be confirm or rework"), helpTopic: []string{"approvals", "decide"}}
	}
	reasonPtr, err := optionalReason(action, opts.reason)
	if err != nil {
		return usageError{err: err, helpTopic: []string{"approvals", "decide"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	key, err := r.newIdempotencyKey("approval-decide")
	if err != nil {
		return err
	}

	response, err := client.CreateFinalApprovalWithResponse(ctx, opts.runID, &gen.CreateFinalApprovalParams{
		IdempotencyKey: key,
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
	opts := approvalDecisionOptions{}
	flags := newFlagSet("approvals plan-decide")
	bindApprovalDecisionFlags(flags, &opts, "plan decision `action`: confirm or rework")
	if err := parseLeafFlags(flags, args, []string{"approvals", "plan-decide"}); err != nil {
		return err
	}
	if strings.TrimSpace(opts.runID) == "" {
		return usageError{err: errors.New("--run-id is required"), helpTopic: []string{"approvals", "plan-decide"}}
	}
	opts.action = strings.ToLower(strings.TrimSpace(opts.action))
	action := opts.action
	if action != string(gen.CreatePlanApprovalRequestActionConfirm) && action != string(gen.CreatePlanApprovalRequestActionRework) {
		return usageError{err: errors.New("--action must be confirm or rework"), helpTopic: []string{"approvals", "plan-decide"}}
	}
	reasonPtr, err := optionalReason(action, opts.reason)
	if err != nil {
		return usageError{err: err, helpTopic: []string{"approvals", "plan-decide"}}
	}

	client, err := r.newResponsesClient()
	if err != nil {
		return err
	}
	planRevisionID, err := r.currentPlanRevisionID(ctx, client, opts.runID)
	if err != nil {
		return err
	}
	key, err := r.newIdempotencyKey("approval-plan-decide")
	if err != nil {
		return err
	}

	response, err := client.CreatePlanApprovalWithResponse(ctx, opts.runID, &gen.CreatePlanApprovalParams{
		IdempotencyKey: key,
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

func parseLeafFlags(flags *flag.FlagSet, args []string, helpTopic []string) error {
	if err := flags.Parse(args); err != nil {
		return usageError{err: err, helpTopic: helpTopic}
	}
	if flags.NArg() == 0 {
		return nil
	}
	return usageError{err: fmt.Errorf("unexpected argument(s): %s", strings.Join(flags.Args(), " ")), helpTopic: helpTopic}
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
	if cfg == nil {
		return
	}
	mayReplaceServer := !cfg.ServerExplicit && strings.TrimSpace(cfg.ServerAddress) == defaultServerAddress
	if mayReplaceServer {
		if record, ok := findActiveSummonForCLI(); ok {
			if strings.TrimSpace(cfg.CredentialFile) == "" && strings.TrimSpace(record.KingdomHome) != "" {
				candidate := localauth.CredentialPath(record.KingdomHome)
				if _, err := os.Stat(candidate); err == nil {
					cfg.CredentialFile = candidate
				}
			}
			listen := strings.TrimSpace(record.Listen)
			basePath := strings.TrimSpace(record.BasePath)
			if listen != "" && basePath != "" {
				cfg.ServerAddress = fmt.Sprintf("http://%s%s", summonConnectAddress(listen), basePath)
				return
			}
		}
	}
	installCfg, ok := loadInstallConfigForCLI()
	if !ok {
		return
	}
	if !cfg.ServerExplicit && strings.TrimSpace(cfg.CredentialFile) == "" && localCredentialMayAuthenticate(cfg.ServerAddress) {
		candidate := localauth.CredentialPath(installCfg.KingdomHome)
		if _, err := os.Stat(candidate); err == nil {
			cfg.CredentialFile = candidate
		}
	}
	if !mayReplaceServer {
		return
	}
	// installCfg comes from readInstallConfigFromStruct, which
	// already canonicalizes Server.BasePath. No local re-trim needed.
	listen := strings.TrimSpace(installCfg.Server.Listen)
	if listen == "" || installCfg.Server.BasePath == "" {
		return
	}
	cfg.ServerAddress = fmt.Sprintf("http://%s%s", summonConnectAddress(listen), installCfg.Server.BasePath)
}

func localCredentialMayAuthenticate(serverAddress string) bool {
	parsed, err := url.Parse(strings.TrimSpace(serverAddress))
	if err != nil {
		return false
	}
	return (parsed.Scheme == "http" || parsed.Scheme == "https") && isLoopbackAPIHost(parsed.Hostname())
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
		return nil, errors.New("--autonomous-approvals must be true or false")
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

func newIdempotencyKey(scope string) (string, error) {
	key, err := idempotency.Generate(scope, time.Now())
	if err != nil {
		return "", fmt.Errorf("generate idempotency key: %w", err)
	}
	return key, nil
}

func (r *Runtime) newIdempotencyKey(scope string) (string, error) {
	if key := strings.TrimSpace(r.config.IdempotencyKey); key != "" {
		r.config.IdempotencyKey = ""
		return key, nil
	}
	return newIdempotencyKey(scope)
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
	err       error
	helpTopic []string
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
	RunRequest                gen.CreateRunRequest
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
		writeUsageError(r.stderr, typed.err, typed.helpTopic)
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
	recoveryCommand := buildRunRecoveryCommand(err.RunRequest, err.Err)
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

func buildRunRecoveryCommand(request gen.CreateRunRequest, cause error) string {
	parts := []string{"kd"}
	var ambiguous *ambiguousMutationError
	if errors.As(cause, &ambiguous) && strings.TrimSpace(ambiguous.Key) != "" {
		parts = append(parts, "--idempotency-key", shellQuote(ambiguous.Key))
	}
	parts = append(parts, "run", "start", "--decree-id", shellQuote(request.DecreeId))
	if request.ModelSelection.Global.Model != "" {
		parts = append(parts, "--global-model", shellQuote(request.ModelSelection.Global.Model))
	}
	if request.ModelSelection.RoleOverrides != nil && len(*request.ModelSelection.RoleOverrides) > 0 {
		roles := make([]string, 0, len(*request.ModelSelection.RoleOverrides))
		for role := range *request.ModelSelection.RoleOverrides {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		overrides := make([]string, 0, len(roles))
		for _, role := range roles {
			overrides = append(overrides, role+"="+(*request.ModelSelection.RoleOverrides)[role].Model)
		}
		parts = append(parts, "--agent-model-overrides", shellQuote(strings.Join(overrides, ",")))
	}
	parts = append(parts, "--publish-mode", shellQuote(string(request.PublishMode)))
	if request.AutonomousApprovals != nil {
		parts = append(parts, "--autonomous-approvals="+strconv.FormatBool(*request.AutonomousApprovals))
	}
	if request.SkipReviewsAndValidations != nil && *request.SkipReviewsAndValidations {
		parts = append(parts, "--skip-reviews-and-validations")
	}
	if request.RunUntilAllApprove != nil && *request.RunUntilAllApprove {
		parts = append(parts, "--run-until-all-approve")
	}
	if request.MaxExecutionReviewCycles != nil {
		parts = append(parts, "--max-execution-review-cycles", strconv.Itoa(*request.MaxExecutionReviewCycles))
	}
	if request.MaxValidationReworkCycles != nil {
		parts = append(parts, "--max-validation-rework-cycles", strconv.Itoa(*request.MaxValidationReworkCycles))
	}
	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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

func writeUsageError(stderr io.Writer, err error, topic []string) {
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "usage error: %v\n", err)
	}
	_, _ = io.WriteString(stderr, "\n")
	writeCommandHelp(stderr, topic)
}
