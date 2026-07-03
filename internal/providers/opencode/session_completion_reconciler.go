package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type SessionExporter interface {
	Export(ctx context.Context, req sessionExportRequest) (sessionExportResult, error)
}

type sessionExportResult struct {
	Stdout []byte
	Stderr string
}

type sessionExportMode string

const (
	sessionExportModeRaw       sessionExportMode = "raw"
	sessionExportModeSanitized sessionExportMode = "sanitized"
)

type sessionExportRequest struct {
	ProviderSessionID string
	WorkDir           string
	ConfigDir         string
	DatabasePath      string
	Mode              sessionExportMode
}

type sessionCompletionReconcilerConfig struct {
	Exporter        SessionExporter
	TotalTimeout    time.Duration
	BackoffSchedule []time.Duration
	FallbackWorkDir string
}

type sessionCompletionReconciler struct {
	exporter        SessionExporter
	totalTimeout    time.Duration
	backoffSchedule []time.Duration
	fallbackWorkDir string
}

type sessionCompletionReconcileRequest struct {
	ProviderSessionID   string
	WorkDir             string
	ConfigDir           string
	DatabasePath        string
	StreamOutputText    string
	Tokens              TokenUsage
	AttemptID           string
	InvocationID        string
	InvocationStartedAt time.Time
	ReceiptRequired     bool
}

type sessionCompletionReconcileStatus string

const (
	sessionCompletionReconcileStatusResolved        sessionCompletionReconcileStatus = "resolved"
	sessionCompletionReconcileStatusUnresolved      sessionCompletionReconcileStatus = "unresolved"
	sessionCompletionReconcileStatusMalformedExport sessionCompletionReconcileStatus = "malformed_export"
	sessionCompletionReconcileStatusExporterError   sessionCompletionReconcileStatus = "exporter_error"
)

type sessionCompletionReconcileOutcome struct {
	Result         attemptCompletionResult
	Status         sessionCompletionReconcileStatus
	Diagnostic     string
	ExportMode     sessionExportMode
	ExportAttempts int
	Elapsed        time.Duration
	Checks         []ExportCheck
}

type sessionCompletionReconcileError struct {
	Err    error
	Checks []ExportCheck
}

func (e *sessionCompletionReconcileError) Error() string {
	if e == nil || e.Err == nil {
		return "session completion reconcile error"
	}
	return e.Err.Error()
}

func (e *sessionCompletionReconcileError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type exportedAssistantTurn struct {
	Finish           stepFinishReasonEvidence
	CompletedAt      string
	CompletedAtTime  time.Time
	CompletedAtValid bool
	HasError         bool
	ErrorText        string
	OutputText       string
	ToolStates       []string
}

type exportAssistantScanResult struct {
	Turn              exportedAssistantTurn
	Found             bool
	OwnedByInvocation bool
	Salvageable       bool
	Err               error
}

type exportInvocationBoundary struct {
	AttemptID    string
	InvocationID string
}

type cliSessionExporter struct {
	binaryPath string
}

func defaultSessionCompletionReconcileBackoffSchedule() []time.Duration {
	return []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
		5 * time.Second,
	}
}

func (e cliSessionExporter) Export(ctx context.Context, req sessionExportRequest) (sessionExportResult, error) {
	providerSessionID := strings.TrimSpace(req.ProviderSessionID)
	if providerSessionID == "" {
		return sessionExportResult{}, fmt.Errorf("provider session id is required")
	}
	args := []string{"export"}
	if req.Mode == sessionExportModeSanitized {
		args = append(args, "--sanitize")
	}
	args = append(args, providerSessionID)
	cmd := exec.CommandContext(ctx, e.binaryPath, args...)
	if strings.TrimSpace(req.WorkDir) != "" {
		cmd.Dir = strings.TrimSpace(req.WorkDir)
	}
	cmd.Env = opencodeProcessEnvironment(ConfigDirEnvironment(req.ConfigDir), DatabasePathEnvironment(req.DatabasePath))
	stdoutFile, err := os.CreateTemp("", "opencode-export-*.json")
	if err != nil {
		return sessionExportResult{}, fmt.Errorf("create temp export file: %w", err)
	}
	defer func() {
		_ = stdoutFile.Close()
		_ = os.Remove(stdoutFile.Name())
	}()
	var stderr bytes.Buffer
	cmd.Stdout = stdoutFile
	cmd.Stderr = &stderr
	err = cmd.Run()
	if _, seekErr := stdoutFile.Seek(0, 0); seekErr != nil {
		return sessionExportResult{}, fmt.Errorf("rewind temp export file: %w", seekErr)
	}
	stdout, readErr := io.ReadAll(stdoutFile)
	if readErr != nil {
		return sessionExportResult{}, fmt.Errorf("read temp export file: %w", readErr)
	}
	result := sessionExportResult{
		Stdout: append([]byte(nil), stdout...),
		Stderr: strings.TrimSpace(stderr.String()),
	}
	if err != nil {
		diagnostic := firstNonEmpty(result.Stderr, strings.TrimSpace(string(result.Stdout)))
		if diagnostic != "" {
			return result, fmt.Errorf("opencode export %q: %w: %s", providerSessionID, err, diagnostic)
		}
		return result, fmt.Errorf("opencode export %q: %w", providerSessionID, err)
	}
	return result, nil
}

func newSessionCompletionReconciler(cfg sessionCompletionReconcilerConfig) *sessionCompletionReconciler {
	totalTimeout := cfg.TotalTimeout
	if totalTimeout <= 0 {
		totalTimeout = 20 * time.Second
	}
	backoffSchedule := normalizeSessionCompletionReconcileBackoffSchedule(cfg.BackoffSchedule)
	return &sessionCompletionReconciler{
		exporter:        cfg.Exporter,
		totalTimeout:    totalTimeout,
		backoffSchedule: backoffSchedule,
		fallbackWorkDir: strings.TrimSpace(cfg.FallbackWorkDir),
	}
}

func (r *sessionCompletionReconciler) Reconcile(ctx context.Context, req sessionCompletionReconcileRequest) (sessionCompletionReconcileOutcome, error) {
	if r == nil || r.exporter == nil {
		return newSessionCompletionReconcileOutcome(req, sessionExportModeRaw), nil
	}
	providerSessionID := strings.TrimSpace(req.ProviderSessionID)
	if providerSessionID == "" {
		return newSessionCompletionReconcileOutcome(req, sessionExportModeRaw), nil
	}
	startedAt := time.Now()
	deadline := startedAt.Add(r.totalTimeout)
	lastOutcome := newSessionCompletionReconcileOutcome(req, sessionExportModeRaw)
	totalExportAttempts := 0
	allChecks := make([]ExportCheck, 0)
	for cycle := 0; ; cycle++ {
		if cycle > 0 && !time.Now().Before(deadline) {
			lastOutcome.ExportAttempts = totalExportAttempts
			lastOutcome.Elapsed = time.Since(startedAt)
			lastOutcome.Checks = append([]ExportCheck(nil), allChecks...)
			return lastOutcome, nil
		}
		outcome, err := r.reconcileCycle(ctx, req, deadline)
		if err != nil {
			var wrapped *sessionCompletionReconcileError
			if errors.As(err, &wrapped) {
				return sessionCompletionReconcileOutcome{}, &sessionCompletionReconcileError{Err: wrapped.Err, Checks: append(append([]ExportCheck(nil), allChecks...), wrapped.Checks...)}
			}
			return sessionCompletionReconcileOutcome{}, err
		}
		totalExportAttempts += outcome.ExportAttempts
		allChecks = append(allChecks, outcome.Checks...)
		lastOutcome = outcome
		lastOutcome.ExportAttempts = totalExportAttempts
		lastOutcome.Elapsed = time.Since(startedAt)
		lastOutcome.Checks = append([]ExportCheck(nil), allChecks...)
		if lastOutcome.Status == sessionCompletionReconcileStatusResolved {
			return lastOutcome, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return lastOutcome, nil
		}
		waitFor := r.retryDelay(cycle)
		if waitFor > remaining {
			waitFor = remaining
		}
		select {
		case <-ctx.Done():
			return sessionCompletionReconcileOutcome{}, ctx.Err()
		case <-time.After(waitFor):
		}
	}
}

func (r *sessionCompletionReconciler) reconcileCycle(ctx context.Context, req sessionCompletionReconcileRequest, deadline time.Time) (sessionCompletionReconcileOutcome, error) {
	rawOutcome, err := r.reconcileExportModeOnce(ctx, req, sessionExportModeRaw, deadline)
	if err != nil {
		return sessionCompletionReconcileOutcome{}, err
	}
	if rawOutcome.Status != sessionCompletionReconcileStatusMalformedExport {
		return rawOutcome, nil
	}
	sanitizedOutcome, err := r.reconcileExportModeOnce(ctx, req, sessionExportModeSanitized, deadline)
	if err != nil {
		var wrapped *sessionCompletionReconcileError
		if errors.As(err, &wrapped) {
			return sessionCompletionReconcileOutcome{}, &sessionCompletionReconcileError{Err: wrapped.Err, Checks: append(append([]ExportCheck(nil), rawOutcome.Checks...), wrapped.Checks...)}
		}
		return sessionCompletionReconcileOutcome{}, err
	}
	return combineMalformedExportOutcomes(rawOutcome, sanitizedOutcome), nil
}

func (r *sessionCompletionReconciler) reconcileExportModeOnce(ctx context.Context, req sessionCompletionReconcileRequest, mode sessionExportMode, deadline time.Time) (sessionCompletionReconcileOutcome, error) {
	outcome := newSessionCompletionReconcileOutcome(req, mode)
	workDir := firstNonEmpty(strings.TrimSpace(req.WorkDir), r.fallbackWorkDir)
	outcome.ExportAttempts = 1
	checkStartedAt := time.Now().UTC()
	exportCtx, cancel := boundedReconcileExportContext(ctx, deadline)
	defer cancel()
	exportResult, err := r.exporter.Export(exportCtx, sessionExportRequest{
		ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
		WorkDir:           workDir,
		ConfigDir:         strings.TrimSpace(req.ConfigDir),
		DatabasePath:      strings.TrimSpace(req.DatabasePath),
		Mode:              mode,
	})
	if err != nil {
		if ctxErr := exportCtx.Err(); ctxErr != nil {
			check := buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusDeadlineExceeded, sessionCompletionReconcileStatusExporterError, ctxErr.Error(), sessionExportResult{}, exportAssistantScanResult{}, false)
			if errors.Is(ctxErr, context.Canceled) {
				check.Status = ExportCheckStatusCancelled
			}
			return sessionCompletionReconcileOutcome{}, &sessionCompletionReconcileError{Err: ctxErr, Checks: []ExportCheck{check}}
		}
		outcome.Status = sessionCompletionReconcileStatusExporterError
		outcome.Diagnostic = strings.TrimSpace(err.Error())
		outcome.Result.FailureText = outcome.Diagnostic
		outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusExporterError, outcome.Status, outcome.Diagnostic, exportResult, exportAssistantScanResult{}, false)}
		return outcome, nil
	}
	scan := parseLatestAssistantTurnForInvocation(exportResult.Stdout, req)
	assistant, found := scan.Turn, scan.Found
	if found {
		if !assistantOwnedByInvocation(scan, req) {
			outcome.Status = sessionCompletionReconcileStatusUnresolved
			outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusUnresolved, outcome.Status, outcome.Diagnostic, exportResult, scan, false)}
			return outcome, nil
		}
		if isPersistedSuccess(assistant) {
			resolvedOutput := preferredOutputForExportMode(mode, assistant.OutputText, req.StreamOutputText)
			if req.ReceiptRequired {
				outcome.Result.State = attemptCompletionStateIncompleteTurn
				outcome.Result.ReasonCode = "provider_incomplete_turn"
			} else {
				outcome.Result.State = attemptCompletionStateSuccess
				outcome.Result.ReasonCode = "execution_completed"
			}
			outcome.Result.OutputText = resolvedOutput
			outcome.Result.SuppressEmptyOutputRetry = true
			outcome.Status = sessionCompletionReconcileStatusResolved
			outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusResolvedSuccess, outcome.Status, outcome.Diagnostic, exportResult, scan, false)}
			return outcome, nil
		}
		if isPersistedFailure(assistant) {
			resolvedFailureDiagnostic := firstNonEmpty(strings.TrimSpace(assistant.ErrorText), fmt.Sprintf("opencode export reported terminal failure finish %q", assistant.Finish.Normalized))
			if req.ReceiptRequired {
				outcome.Result.State = attemptCompletionStateIncompleteTurn
				outcome.Result.ReasonCode = "provider_incomplete_turn"
			} else {
				outcome.Result.State = attemptCompletionStateFailed
				outcome.Result.ReasonCode = "provider_terminal_failure"
				outcome.Result.FailureText = resolvedFailureDiagnostic
			}
			outcome.Status = sessionCompletionReconcileStatusResolved
			outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusResolvedFailure, outcome.Status, resolvedFailureDiagnostic, exportResult, scan, false)}
			return outcome, nil
		}
	}
	if scan.Err != nil {
		if scan.Salvageable && found && assistantOwnedByInvocation(scan, req) {
			if isPersistedSuccess(assistant) {
				resolvedOutput := preferredOutputForExportMode(mode, assistant.OutputText, req.StreamOutputText)
				if req.ReceiptRequired {
					outcome.Result.State = attemptCompletionStateIncompleteTurn
					outcome.Result.ReasonCode = "provider_incomplete_turn"
				} else {
					outcome.Result.State = attemptCompletionStateSuccess
					outcome.Result.ReasonCode = "execution_completed"
				}
				outcome.Result.OutputText = resolvedOutput
				outcome.Result.SuppressEmptyOutputRetry = true
				outcome.Status = sessionCompletionReconcileStatusResolved
				outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusResolvedSuccess, outcome.Status, outcome.Diagnostic, exportResult, scan, true)}
				return outcome, nil
			}
			if isPersistedFailure(assistant) {
				resolvedFailureDiagnostic := firstNonEmpty(strings.TrimSpace(assistant.ErrorText), fmt.Sprintf("opencode export reported terminal failure finish %q", assistant.Finish.Normalized))
				if req.ReceiptRequired {
					outcome.Result.State = attemptCompletionStateIncompleteTurn
					outcome.Result.ReasonCode = "provider_incomplete_turn"
				} else {
					outcome.Result.State = attemptCompletionStateFailed
					outcome.Result.ReasonCode = "provider_terminal_failure"
					outcome.Result.FailureText = resolvedFailureDiagnostic
				}
				outcome.Status = sessionCompletionReconcileStatusResolved
				outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusResolvedFailure, outcome.Status, resolvedFailureDiagnostic, exportResult, scan, true)}
				return outcome, nil
			}
		}
		outcome.Status = sessionCompletionReconcileStatusMalformedExport
		outcome.Diagnostic = strings.TrimSpace(formatExportParseDiagnostic(scan.Err, exportResult.Stderr))
		outcome.Result.FailureText = outcome.Diagnostic
		outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusMalformedExport, outcome.Status, outcome.Diagnostic, exportResult, scan, false)}
		return outcome, nil
	}
	outcome.Status = sessionCompletionReconcileStatusUnresolved
	outcome.Checks = []ExportCheck{buildExportCheck(req, mode, checkStartedAt, time.Now().UTC(), ExportCheckStatusUnresolved, outcome.Status, outcome.Diagnostic, exportResult, scan, false)}
	return outcome, nil
}

func boundedReconcileExportContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if deadline.IsZero() {
		return context.WithCancel(ctx)
	}
	if time.Until(deadline) <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline)
}

func newSessionCompletionReconcileOutcome(req sessionCompletionReconcileRequest, mode sessionExportMode) sessionCompletionReconcileOutcome {
	return sessionCompletionReconcileOutcome{
		Result: attemptCompletionResult{
			Tokens:            req.Tokens,
			ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
			EvidenceSource:    evidenceSourceForExportMode(mode),
		},
		ExportMode: mode,
	}
}

func combineMalformedExportOutcomes(rawOutcome, sanitizedOutcome sessionCompletionReconcileOutcome) sessionCompletionReconcileOutcome {
	sanitizedOutcome.ExportAttempts += rawOutcome.ExportAttempts
	sanitizedOutcome.Checks = append(append([]ExportCheck(nil), rawOutcome.Checks...), sanitizedOutcome.Checks...)
	if sanitizedOutcome.Status == sessionCompletionReconcileStatusResolved {
		return sanitizedOutcome
	}
	if strings.TrimSpace(rawOutcome.Diagnostic) == "" {
		return sanitizedOutcome
	}
	if strings.TrimSpace(sanitizedOutcome.Diagnostic) == "" {
		sanitizedOutcome.Diagnostic = rawOutcome.Diagnostic
		sanitizedOutcome.Result.FailureText = firstNonEmpty(strings.TrimSpace(sanitizedOutcome.Result.FailureText), strings.TrimSpace(rawOutcome.Diagnostic))
		return sanitizedOutcome
	}
	combined := fmt.Sprintf("raw export malformed: %s; sanitized export status=%s: %s", strings.TrimSpace(rawOutcome.Diagnostic), sanitizedOutcome.Status, strings.TrimSpace(sanitizedOutcome.Diagnostic))
	sanitizedOutcome.Diagnostic = combined
	sanitizedOutcome.Result.FailureText = firstNonEmpty(strings.TrimSpace(sanitizedOutcome.Result.FailureText), combined)
	return sanitizedOutcome
}

func buildExportCheck(req sessionCompletionReconcileRequest, mode sessionExportMode, startedAt, finishedAt time.Time, status ExportCheckStatus, reconcileStatus sessionCompletionReconcileStatus, diagnostic string, result sessionExportResult, scan exportAssistantScanResult, salvageUsed bool) ExportCheck {
	check := ExportCheck{
		ProviderSessionID:          strings.TrimSpace(req.ProviderSessionID),
		ProviderInvocationID:       strings.TrimSpace(req.InvocationID),
		ExportMode:                 strings.TrimSpace(string(mode)),
		Status:                     status,
		SalvageUsed:                salvageUsed,
		AssistantFound:             scan.Found,
		AssistantOwnedByInvocation: scan.OwnedByInvocation,
		AssistantCompletedAt:       strings.TrimSpace(scan.Turn.CompletedAt),
		AssistantFinishReason:      strings.TrimSpace(scan.Turn.Finish.Normalized),
		AssistantHasError:          scan.Turn.HasError,
		StdoutBytes:                len(result.Stdout),
		StderrBytes:                len(result.Stderr),
		DiagnosticText:             firstNonEmpty(strings.TrimSpace(diagnostic), strings.TrimSpace(result.Stderr)),
		StartedAt:                  startedAt.UTC().Format(time.RFC3339),
		FinishedAt:                 finishedAt.UTC().Format(time.RFC3339),
		DurationMillis:             finishedAt.Sub(startedAt).Milliseconds(),
	}
	if shouldPersistExportCheckPayload(status, salvageUsed) {
		check.StdoutCapture = strings.TrimSpace(string(result.Stdout))
		check.StderrCapture = strings.TrimSpace(result.Stderr)
	}
	if check.DiagnosticText == "" && reconcileStatus != "" {
		check.DiagnosticText = strings.TrimSpace(string(reconcileStatus))
	}
	return check
}

func shouldPersistExportCheckPayload(status ExportCheckStatus, salvageUsed bool) bool {
	if salvageUsed {
		return true
	}
	switch status {
	case ExportCheckStatusMalformedExport, ExportCheckStatusExporterError, ExportCheckStatusDeadlineExceeded, ExportCheckStatusCancelled:
		return true
	default:
		return false
	}
}

func normalizeSessionCompletionReconcileBackoffSchedule(schedule []time.Duration) []time.Duration {
	if len(schedule) == 0 {
		return defaultSessionCompletionReconcileBackoffSchedule()
	}
	normalized := make([]time.Duration, 0, len(schedule))
	for _, delay := range schedule {
		if delay > 0 {
			normalized = append(normalized, delay)
		}
	}
	if len(normalized) == 0 {
		return defaultSessionCompletionReconcileBackoffSchedule()
	}
	return normalized
}

func (r *sessionCompletionReconciler) retryDelay(cycle int) time.Duration {
	if len(r.backoffSchedule) == 0 {
		return 0
	}
	if cycle < 0 {
		cycle = 0
	}
	if cycle >= len(r.backoffSchedule) {
		return r.backoffSchedule[len(r.backoffSchedule)-1]
	}
	return r.backoffSchedule[cycle]
}

func evidenceSourceForExportMode(mode sessionExportMode) completionEvidenceSource {
	if mode == sessionExportModeSanitized {
		return completionEvidenceSourceExportSanitized
	}
	return completionEvidenceSourceExport
}

func preferredOutputForExportMode(mode sessionExportMode, exportOutput, streamOutput string) string {
	if mode == sessionExportModeSanitized {
		return firstNonEmpty(strings.TrimSpace(streamOutput), strings.TrimSpace(exportOutput))
	}
	return firstNonEmpty(strings.TrimSpace(exportOutput), strings.TrimSpace(streamOutput))
}

func formatExportParseDiagnostic(parseErr error, stderr string) string {
	if strings.TrimSpace(stderr) != "" {
		return fmt.Sprintf("%s: stderr: %s", parseErr.Error(), strings.TrimSpace(stderr))
	}
	return parseErr.Error()
}

func isPersistedSuccess(turn exportedAssistantTurn) bool {
	if !turn.CompletedAtValid || turn.HasError {
		return false
	}
	if turn.Finish.Class != stepFinishTerminalSuccessCandidate {
		return false
	}
	for _, state := range turn.ToolStates {
		if state == "pending" || state == "running" {
			return false
		}
	}
	return true
}

func isPersistedFailure(turn exportedAssistantTurn) bool {
	if !turn.CompletedAtValid {
		return false
	}
	if turn.HasError {
		return true
	}
	return turn.Finish.Class == stepFinishTerminalFailure
}

func parseLatestAssistantTurnForInvocation(raw []byte, req sessionCompletionReconcileRequest) exportAssistantScanResult {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	result := exportAssistantScanResult{}
	trimmedInvocationID := strings.TrimSpace(req.InvocationID)
	currentBoundary := exportInvocationBoundary{}
	haveBoundary := false
	lastOwnedTurn := exportedAssistantTurn{}
	lastOwnedFound := false

	if start, err := decoder.Token(); err != nil {
		maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
		result.Err = fmt.Errorf("decode opencode export: %w", err)
		return result
	} else if delim, ok := start.(json.Delim); !ok || delim != '{' {
		result.Err = fmt.Errorf("decode opencode export: root object not found")
		return result
	}

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
			result.Err = fmt.Errorf("decode opencode export: %w", err)
			return result
		}
		key, ok := keyToken.(string)
		if !ok {
			result.Err = fmt.Errorf("decode opencode export: expected object key")
			return result
		}
		if key != "messages" {
			if err := skipExportValue(decoder); err != nil {
				maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
				result.Err = fmt.Errorf("decode opencode export: %w", err)
				return result
			}
			continue
		}

		messagesStart, err := decoder.Token()
		if err != nil {
			maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
			result.Err = fmt.Errorf("decode opencode export: %w", err)
			return result
		}
		if delim, ok := messagesStart.(json.Delim); !ok || delim != '[' {
			result.Err = fmt.Errorf("decode opencode export: messages array not found")
			return result
		}

		for decoder.More() {
			var rawMessage json.RawMessage
			if err := decoder.Decode(&rawMessage); err != nil {
				maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
				result.Err = fmt.Errorf("decode opencode export: %w", err)
				return result
			}
			role, err := messageRoleFromRawMessage(rawMessage)
			if err != nil {
				maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
				result.Err = err
				return result
			}

			if trimmedInvocationID != "" && role == "user" {
				boundary, foundBoundary, err := rawMessageInvocationBoundary(rawMessage)
				if err != nil {
					result.Err = err
					return result
				}
				if foundBoundary {
					currentBoundary = boundary
					haveBoundary = true
					result.Found = false
					result.OwnedByInvocation = false
					result.Turn = exportedAssistantTurn{}
				}
				continue
			}

			if role != "assistant" {
				continue
			}
			if trimmedInvocationID != "" && !haveBoundary {
				continue
			}
			turn, found, err := assistantTurnFromRawMessage(rawMessage)
			if err != nil {
				maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
				result.Err = err
				return result
			}
			if found {
				ownedByInvocation := boundaryMatchesReconcileRequest(currentBoundary, req)
				result.Turn = turn
				result.Found = true
				result.OwnedByInvocation = ownedByInvocation
				if ownedByInvocation {
					lastOwnedTurn = turn
					lastOwnedFound = true
				}
			}
		}
		if end, err := decoder.Token(); err != nil {
			maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
			result.Err = fmt.Errorf("decode opencode export: %w", err)
			return result
		} else if delim, ok := end.(json.Delim); !ok || delim != ']' {
			result.Err = fmt.Errorf("decode opencode export: messages array not terminated")
			return result
		}
	}

	if end, err := decoder.Token(); err != nil {
		maybeApplySalvageCandidate(&result, err, lastOwnedTurn, lastOwnedFound)
		result.Err = fmt.Errorf("decode opencode export: %w", err)
		return result
	} else if delim, ok := end.(json.Delim); !ok || delim != '}' {
		result.Err = fmt.Errorf("decode opencode export: root object not terminated")
		return result
	}

	return result
}

func maybeApplySalvageCandidate(result *exportAssistantScanResult, err error, lastOwnedTurn exportedAssistantTurn, lastOwnedFound bool) {
	if result == nil || !isSalvageableExportParseError(err) {
		return
	}
	result.Salvageable = true
	if !result.Found && lastOwnedFound {
		result.Turn = lastOwnedTurn
		result.Found = true
		result.OwnedByInvocation = true
	}
}

func isSalvageableExportParseError(err error) bool {
	return errorsIsEOF(err)
}

func errorsIsEOF(err error) bool {
	return err != nil && (err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(strings.ToLower(err.Error()), "unexpected eof") || strings.Contains(strings.ToLower(err.Error()), " eof"))
}

func skipExportValue(decoder *json.Decoder) error {
	var discarded json.RawMessage
	return decoder.Decode(&discarded)
}

func messageRoleFromRawMessage(raw json.RawMessage) (string, error) {
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		return "", fmt.Errorf("decode opencode export message: %w", err)
	}
	metadata, _ := exportMessageMetadata(node)
	return strings.ToLower(strings.TrimSpace(stringValue(metadata["role"]))), nil
}

func assistantTurnFromRawMessage(raw json.RawMessage) (exportedAssistantTurn, bool, error) {
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		return exportedAssistantTurn{}, false, fmt.Errorf("decode opencode export message: %w", err)
	}
	turn, ok := assistantTurnFromMap(node)
	return turn, ok, nil
}

func rawMessageInvocationBoundary(raw json.RawMessage) (exportInvocationBoundary, bool, error) {
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		return exportInvocationBoundary{}, false, fmt.Errorf("decode opencode export message: %w", err)
	}
	metadata, _ := exportMessageMetadata(node)
	if strings.ToLower(strings.TrimSpace(stringValue(metadata["role"]))) != "user" {
		return exportInvocationBoundary{}, false, nil
	}
	boundary, found := invocationBoundaryFromMap(node)
	return boundary, found, nil
}

func invocationBoundaryFromMap(node map[string]any) (exportInvocationBoundary, bool) {
	metadata, metadataSeparate := exportMessageMetadata(node)
	segments := extractMessageTextSegments(assistantMessageContentNodes(node, metadata, metadataSeparate))
	for _, candidate := range invocationBoundaryTextCandidates(segments) {
		if boundary, found := parseInvocationBoundary(candidate); found {
			return boundary, true
		}
	}
	return exportInvocationBoundary{}, false
}

func invocationBoundaryTextCandidates(segments []string) []string {
	if len(segments) == 0 {
		return nil
	}
	joinedWithNewlines := strings.TrimSpace(strings.Join(segments, "\n"))
	joinedCompact := strings.TrimSpace(strings.Join(segments, ""))
	if joinedWithNewlines == "" {
		return nil
	}
	if joinedCompact == joinedWithNewlines {
		return []string{joinedWithNewlines}
	}
	return []string{joinedWithNewlines, joinedCompact}
}

func parseInvocationBoundary(text string) (exportInvocationBoundary, bool) {
	lines := strings.Split(text, "\n")
	current := exportInvocationBoundary{}
	insideMetadata := false
	found := false
	for _, rawLine := range lines {
		line := normalizeInvocationMetadataLine(rawLine)
		if line == "" {
			continue
		}
		if line == providerInvocationMetadataHeader {
			insideMetadata = true
			current = exportInvocationBoundary{}
			continue
		}
		if !insideMetadata {
			continue
		}
		switch {
		case strings.HasPrefix(line, providerInvocationAttemptLabel):
			current.AttemptID = normalizeInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationAttemptLabel))
		case strings.HasPrefix(line, providerInvocationIDLabel):
			current.InvocationID = normalizeInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationIDLabel))
		}
		if current.AttemptID != "" && current.InvocationID != "" {
			found = true
		}
	}
	return current, found
}

func normalizeInvocationMetadataLine(line string) string {
	normalized := strings.TrimSpace(line)
	for strings.HasPrefix(normalized, ">") {
		normalized = strings.TrimSpace(strings.TrimPrefix(normalized, ">"))
	}
	return normalizeInvocationMetadataValue(normalized)
}

func normalizeInvocationMetadataValue(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.TrimSpace(strings.Trim(normalized, `"'`+"`"))
	return normalized
}

func boundaryMatchesReconcileRequest(boundary exportInvocationBoundary, req sessionCompletionReconcileRequest) bool {
	trimmedInvocationID := strings.TrimSpace(req.InvocationID)
	if trimmedInvocationID == "" {
		return false
	}
	if normalizeInvocationMetadataValue(boundary.InvocationID) != trimmedInvocationID {
		return false
	}
	trimmedAttemptID := strings.TrimSpace(req.AttemptID)
	if trimmedAttemptID == "" {
		return true
	}
	return normalizeInvocationMetadataValue(boundary.AttemptID) == trimmedAttemptID
}

func assistantTurnFromMap(node map[string]any) (exportedAssistantTurn, bool) {
	metadata, metadataSeparate := exportMessageMetadata(node)
	role := strings.ToLower(strings.TrimSpace(stringValue(metadata["role"])))
	if role != "assistant" {
		return exportedAssistantTurn{}, false
	}
	contentNodes := assistantMessageContentNodes(node, metadata, metadataSeparate)
	textSegments := make([]string, 0)
	toolStates := make([]string, 0)
	for _, contentNode := range contentNodes {
		textSegments = append(textSegments, extractMessageTextSegments(contentNode)...)
		toolStates = append(toolStates, extractToolStates(contentNode)...)
	}
	finishRaw, finishPresent := exportFinishValue(metadata, contentNodes)
	completedAt := strings.TrimSpace(nestedStringValue(metadata, "time", "completed"))
	completedAtTime, completedAtValid := parseExportedTimestamp(completedAt)
	turn := exportedAssistantTurn{
		Finish:           buildStepFinishReasonEvidence(finishRaw, finishPresent),
		CompletedAt:      completedAt,
		CompletedAtTime:  completedAtTime,
		CompletedAtValid: completedAtValid,
		OutputText:       strings.TrimSpace(strings.Join(textSegments, "\n")),
		ToolStates:       toolStates,
	}
	if errorText, hasError := exportErrorText(node, metadata); hasError {
		turn.HasError = true
		turn.ErrorText = errorText
	}
	return turn, true
}

func exportMessageMetadata(node map[string]any) (map[string]any, bool) {
	if info, ok := node["info"].(map[string]any); ok {
		return info, true
	}
	return node, false
}

func assistantMessageContentNodes(node map[string]any, metadata map[string]any, metadataSeparate bool) []any {
	nodes := make([]any, 0, 2)
	if parts, ok := node["parts"]; ok {
		nodes = append(nodes, parts)
	}
	if content, ok := node["content"]; ok {
		nodes = append(nodes, content)
	}
	if metadataSeparate {
		if parts, ok := metadata["parts"]; ok {
			nodes = append(nodes, parts)
		}
		if content, ok := metadata["content"]; ok {
			nodes = append(nodes, content)
		}
	}
	return nodes
}

func exportFinishValue(metadata map[string]any, contentNodes []any) (string, bool) {
	if metadata != nil && mapHasKey(metadata, "finish") {
		return stringValue(metadata["finish"]), true
	}
	for _, contentNode := range contentNodes {
		if finish, found := finishFromContentNode(contentNode); found {
			return finish, true
		}
	}
	return "", false
}

func assistantOwnedByInvocation(scan exportAssistantScanResult, req sessionCompletionReconcileRequest) bool {
	if strings.TrimSpace(req.InvocationID) != "" {
		return scan.Found && scan.OwnedByInvocation
	}
	return assistantBelongsToInvocation(scan.Turn, req.InvocationStartedAt)
}

func assistantBelongsToInvocation(turn exportedAssistantTurn, invocationStartedAt time.Time) bool {
	if invocationStartedAt.IsZero() {
		return true
	}
	if !turn.CompletedAtValid {
		return false
	}
	return !turn.CompletedAtTime.Before(invocationStartedAt.UTC())
}

func parseExportedTimestamp(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed.UTC(), true
		}
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	absValue := value
	if absValue < 0 {
		absValue = -absValue
	}
	switch {
	case absValue >= 1_000_000_000_000_000_000:
		return time.Unix(0, value).UTC(), true
	case absValue >= 1_000_000_000_000_000:
		return time.UnixMicro(value).UTC(), true
	case absValue >= 1_000_000_000_000:
		return time.UnixMilli(value).UTC(), true
	default:
		return time.Unix(value, 0).UTC(), true
	}
}

func finishFromContentNode(node any) (string, bool) {
	switch typed := node.(type) {
	case []any:
		for idx := len(typed) - 1; idx >= 0; idx-- {
			if finish, found := finishFromContentNode(typed[idx]); found {
				return finish, true
			}
		}
	case map[string]any:
		partType := strings.ToLower(strings.TrimSpace(stringValue(typed["type"])))
		if partType == "step-finish" {
			if finish := stringValue(typed["reason"]); finish != "" {
				return finish, true
			}
		}
		if content, ok := typed["content"]; ok {
			if finish, found := finishFromContentNode(content); found {
				return finish, true
			}
		}
		if parts, ok := typed["parts"]; ok {
			if finish, found := finishFromContentNode(parts); found {
				return finish, true
			}
		}
	}
	return "", false
}

func mapHasKey(node map[string]any, key string) bool {
	_, ok := node[key]
	return ok
}

func nestedStringValue(node map[string]any, keys ...string) string {
	current := any(node)
	for _, key := range keys {
		mapping, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = mapping[key]
	}
	return stringValue(current)
}

func exportErrorText(node map[string]any, metadata map[string]any) (string, bool) {
	for _, candidate := range []map[string]any{metadata, node} {
		if candidate == nil {
			continue
		}
		value, ok := candidate["error"]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			return strings.TrimSpace(typed), true
		case map[string]any:
			return firstNonEmpty(stringValue(typed["message"]), stringValue(typed["text"]), "provider export reported assistant error"), true
		default:
			return "provider export reported assistant error", true
		}
	}
	return "", false
}

func extractMessageTextSegments(node any) []string {
	segments := make([]string, 0)
	switch typed := node.(type) {
	case string:
		if strings.TrimSpace(typed) != "" {
			segments = append(segments, strings.TrimSpace(typed))
		}
	case []any:
		for _, item := range typed {
			segments = append(segments, extractMessageTextSegments(item)...)
		}
	case map[string]any:
		partType := strings.ToLower(strings.TrimSpace(stringValue(typed["type"])))
		if shouldIgnoreMessageTextPart(partType) {
			return segments
		}
		if shouldIncludeMessageTextPart(partType) {
			if text := strings.TrimSpace(stringValue(typed["text"])); text != "" {
				segments = append(segments, text)
			}
		} else if partType == "" {
			if text := strings.TrimSpace(stringValue(typed["text"])); text != "" {
				segments = append(segments, text)
			}
		}
		if content, ok := typed["content"]; ok {
			segments = append(segments, extractMessageTextSegments(content)...)
		}
		if parts, ok := typed["parts"]; ok {
			segments = append(segments, extractMessageTextSegments(parts)...)
		}
	}
	return segments
}

func shouldIgnoreMessageTextPart(partType string) bool {
	if strings.Contains(partType, "tool") {
		return true
	}
	switch partType {
	case "reasoning", "step-start", "step-finish":
		return true
	default:
		return false
	}
}

func shouldIncludeMessageTextPart(partType string) bool {
	switch partType {
	case "text", "input_text", "output_text":
		return true
	default:
		return false
	}
}

func extractToolState(node map[string]any) string {
	state := node["state"]
	switch typed := state.(type) {
	case string:
		return strings.ToLower(strings.TrimSpace(typed))
	case map[string]any:
		return strings.ToLower(strings.TrimSpace(stringValue(typed["status"])))
	default:
		return ""
	}
}

func extractToolStates(node any) []string {
	states := make([]string, 0)
	switch typed := node.(type) {
	case []any:
		for _, item := range typed {
			states = append(states, extractToolStates(item)...)
		}
	case map[string]any:
		state := extractToolState(typed)
		partType := strings.ToLower(strings.TrimSpace(stringValue(typed["type"])))
		if state != "" && strings.Contains(partType, "tool") {
			states = append(states, state)
		}
		if parts, ok := typed["parts"]; ok {
			states = append(states, extractToolStates(parts)...)
		}
		if content, ok := typed["content"]; ok {
			states = append(states, extractToolStates(content)...)
		}
	}
	return states
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}
