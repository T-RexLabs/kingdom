package opencode

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type SessionAdapterConfig struct {
	BinaryPath               string
	WorkDir                  string
	ConfigDir                string
	DatabasePath             string
	InlineConfigContent      string
	Model                    string
	Variant                  string
	Agent                    string
	AttachURL                string
	SkipPermissions          bool
	MaxContinuationTurns     int
	FirstEventTimeout        time.Duration
	StreamInactivityTimeout  time.Duration
	SessionExporter          SessionExporter
	ReconcileTotalTimeout    time.Duration
	ReconcileBackoffSchedule []time.Duration
}

type SessionAdapter struct {
	mu sync.Mutex

	binaryPath              string
	workDir                 string
	configDir               string
	databasePath            string
	inlineConfigContent     string
	model                   string
	variant                 string
	agent                   string
	attachURL               string
	skipPermissions         bool
	maxContinuationTurns    int
	firstEventTimeout       time.Duration
	streamInactivityTimeout time.Duration
	reconciler              *sessionCompletionReconciler

	logicalToActual map[string]string
	attempts        map[string]*sessionAttemptRecord
	inFlight        map[string]*exec.Cmd
	attemptCancels  map[string]context.CancelFunc
}

type sessionAttemptRecord struct {
	Events []AttemptStreamEvent
}

type opencodeRunEvent struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionID"`
	ErrorText string `json:"-"`
	Part      struct {
		Text          string  `json:"text"`
		Reason        string  `json:"reason"`
		ReasonPresent bool    `json:"-"`
		Cost          float64 `json:"cost"`
		Tokens        *struct {
			Total     int64 `json:"total"`
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens,omitempty"`
	} `json:"part"`
}

type streamDiagnostics struct {
	ProcessStartedAt  string
	FirstEventAt      string
	LastEventAt       string
	ProcessExitedAt   string
	FirstEventLatency time.Duration
	MaxInterEventGap  time.Duration
	EventCount        int
}

type executionPathDiagnostics struct {
	AttemptID                 string
	SessionID                 string
	CmdStartedAt              string
	CmdWaitStartedAt          string
	CmdWaitFinishedAt         string
	WaitReturned              bool
	SucceededEventAppended    bool
	FailedEventAppended       bool
	CancelledEventAppended    bool
	StepFinishSeen            bool
	LastStepFinishReason      string
	LastStepFinishReasonKind  string
	TerminalStateClass        string
	UpstreamErrorText         string
	CompletionGatePassed      bool
	StdoutEOFSeen             bool
	StderrEOFSeen             bool
	StdoutEventCount          int
	StdoutMalformedLineCount  int
	StderrLineCount           int
	LastStdoutEventAt         string
	FirstStdoutMalformedAt    string
	LastStdoutMalformedAt     string
	FirstStdoutMalformedSHA   string
	LastStdoutMalformedSHA    string
	LastStderrLineAt          string
	LastProgressTextLen       int
	LastProgressTextSHA       string
	ExportCheckPolicy         string
	ExportCheckReason         string
	ExportCheckAttempted      bool
	ExportCheckStatus         string
	ExportCheckEvidenceSource string
	ExportCheckLastError      string
	ExportCheckAttempts       int
	ExportCheckElapsedMs      int64
	ContinuationTurnsElapsed  int
	ContinuationTurnsMax      int
	ExitCode                  *int
	ExitSignal                *int
	WaitError                 string
}

type stderrCapture struct {
	raw          []byte
	lineCount    int
	lastLineAt   string
	eofSeen      bool
	scannerError string
}

type providerTimeoutError struct {
	TimeoutType string
	Timeout     time.Duration
}

const (
	providerInvocationMetadataHeader = "Runtime Invocation Metadata:"
	providerInvocationAttemptLabel   = "AttemptID:"
	providerInvocationIDLabel        = "KingdomProviderInvocationID:"
)

type IncompleteTerminalStateError struct {
	Reason string
}

type continuationTurnResult struct {
	TextChunks        []string
	Tokens            TokenUsage
	LastReason        stepFinishReasonEvidence
	ProviderSessionID string
	UpstreamErrorText string
}

type startedOpencodeRun struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
}

const completionReceiptObservationGracePeriod = 750 * time.Millisecond

const missingCompletionReceiptRecoveryPrompt = "Kingdom has not yet received the required completion receipt for this active invocation.\n\nDo not change your prior answer.\nDo not perform any additional analysis.\nDo not create, edit, rename, or delete any files.\nDo not call any tool except `kingdom_complete_attempt`.\nCall `kingdom_complete_attempt` now.\nReuse your exact final response body in `final_text`."

func (e *IncompleteTerminalStateError) Error() string {
	return fmt.Sprintf("incomplete terminal state (reason=%q)", strings.TrimSpace(e.Reason))
}

func (e *providerTimeoutError) Error() string {
	return fmt.Sprintf("provider %s exceeded after %s", strings.TrimSpace(e.TimeoutType), e.Timeout)
}

func executeResponseForCompletionResult(result attemptCompletionResult) ExecuteResponse {
	return ExecuteResponse{
		Accepted:                        result.State == attemptCompletionStateSuccess,
		OutputText:                      strings.TrimSpace(result.OutputText),
		Tokens:                          result.Tokens,
		ProviderSessionID:               strings.TrimSpace(result.ProviderSessionID),
		SuppressEmptyOutputRetry:        result.SuppressEmptyOutputRetry,
		ProviderInvocationID:            strings.TrimSpace(result.ProviderInvocationID),
		ProviderInvocationAt:            strings.TrimSpace(result.ProviderInvocationAt),
		CompletionReceiptAccepted:       result.CompletionReceiptAccepted,
		CompletionReceiptEvidenceID:     strings.TrimSpace(result.CompletionReceiptEvidenceID),
		CompletionStructuredPayloadJSON: strings.TrimSpace(result.CompletionStructuredPayloadJSON),
		Outcome:                         executionOutcomeFromAttemptCompletionResult(result),
	}
}

func executeResponseForObservedState(outputText string, tokens TokenUsage, providerSessionID string) ExecuteResponse {
	return ExecuteResponse{
		OutputText:        strings.TrimSpace(outputText),
		Tokens:            tokens,
		ProviderSessionID: strings.TrimSpace(providerSessionID),
	}
}

func withExecuteExportChecks(resp ExecuteResponse, checks []ExportCheck) ExecuteResponse {
	if len(checks) == 0 {
		return resp
	}
	resp.ExportChecks = append([]ExportCheck(nil), checks...)
	return resp
}

func withExecuteProviderDiagnostics(resp ExecuteResponse, diagnosticsJSON string) ExecuteResponse {
	resp.ProviderDiagnosticsJSON = firstNonEmpty(strings.TrimSpace(diagnosticsJSON), "{}")
	return resp
}

func appendPhasedExportChecks(dst []ExportCheck, phase ExportCheckPhase, outcome sessionCompletionReconcileOutcome, err error) []ExportCheck {
	checks := append([]ExportCheck(nil), outcome.Checks...)
	var wrapped *sessionCompletionReconcileError
	if errors.As(err, &wrapped) {
		checks = append(checks, wrapped.Checks...)
	}
	if len(checks) == 0 {
		return dst
	}
	for idx := range checks {
		checks[idx].Phase = phase
	}
	return append(dst, checks...)
}

func executeErrorForCompletionResult(result attemptCompletionResult) error {
	switch result.State {
	case attemptCompletionStateCancelled:
		return context.Canceled
	case attemptCompletionStateIncompleteTurn:
		reason := firstNonEmpty(strings.TrimSpace(result.ReasonCode), strings.TrimSpace(result.FailureText), "provider_incomplete_turn")
		return fmt.Errorf("opencode run failed: %w", &IncompleteTerminalStateError{Reason: reason})
	case attemptCompletionStateFailed:
		failureText := firstNonEmpty(strings.TrimSpace(result.FailureText), strings.TrimSpace(result.ReasonCode), "opencode run failed")
		return fmt.Errorf("opencode run failed: %s", failureText)
	default:
		return nil
	}
}

func executionResultForTimeout(timeoutErr error, providerSessionID string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: strings.TrimSpace(providerSessionID),
		Timeout:           true,
		FailureText:       firstNonEmpty(strings.TrimSpace(errorString(timeoutErr)), "opencode run timed out"),
	}).Result
}

func executionResultForProcessError(err error, providerSessionID, failureText string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: strings.TrimSpace(providerSessionID),
		ProcessFailed:     true,
		FailureText:       firstNonEmpty(strings.TrimSpace(failureText), strings.TrimSpace(errorString(err)), "opencode run failed"),
	}).Result
}

func executionResultForCancellation(err error, providerSessionID, failureText string) attemptCompletionResult {
	return attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: strings.TrimSpace(providerSessionID),
		Cancelled:         true,
		FailureText:       firstNonEmpty(strings.TrimSpace(failureText), strings.TrimSpace(errorString(err)), "opencode run cancelled"),
	}).Result
}

func canonicalSettledOutputText(observedText, latestContinuationText string, usedContinuation bool) string {
	trimmedObserved := strings.TrimSpace(observedText)
	if !usedContinuation {
		return trimmedObserved
	}
	return firstNonEmpty(strings.TrimSpace(latestContinuationText), trimmedObserved)
}

func completionResultForAcceptedReceipt(accepted CompletionReceiptAcceptance, outputText string, tokens TokenUsage, providerSessionID, invocationID, invocationAt string) attemptCompletionResult {
	return attemptCompletionResult{
		State:                           attemptCompletionStateSuccess,
		ReasonCode:                      "execution_completed",
		OutputText:                      firstNonEmpty(strings.TrimSpace(accepted.FinalText), strings.TrimSpace(outputText)),
		Tokens:                          tokens,
		ProviderSessionID:               strings.TrimSpace(providerSessionID),
		ProviderInvocationID:            strings.TrimSpace(invocationID),
		ProviderInvocationAt:            strings.TrimSpace(invocationAt),
		EvidenceSource:                  completionEvidenceSourceReceipt,
		CompletionReceiptAccepted:       true,
		CompletionReceiptEvidenceID:     strings.TrimSpace(accepted.EvidenceID),
		CompletionStructuredPayloadJSON: strings.TrimSpace(accepted.StructuredPayloadJSON),
	}
}

func completionReceiptSettlement(req ExecuteRequest, outputText string, tokens TokenUsage, providerSessionID string) (attemptCompletionResult, bool, bool) {
	if !completionReceiptModeEnabled(req) {
		return attemptCompletionResult{}, false, false
	}
	processedReceipts, err := processCompletionReceipts(req)
	if err != nil {
		return attemptCompletionResult{
			State:                attemptCompletionStateIncompleteTurn,
			ReasonCode:           "provider_incomplete_turn",
			FailureText:          firstNonEmpty(strings.TrimSpace(err.Error()), "provider completion receipt validation failed"),
			OutputText:           strings.TrimSpace(outputText),
			Tokens:               tokens,
			ProviderSessionID:    strings.TrimSpace(providerSessionID),
			ProviderInvocationID: strings.TrimSpace(req.InvocationID),
			ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
			EvidenceSource:       completionEvidenceSourceStream,
		}, true, false
	}
	if processedReceipts.TerminalFailure {
		return attemptCompletionResult{
			State:                attemptCompletionStateIncompleteTurn,
			ReasonCode:           "provider_incomplete_turn",
			FailureText:          firstNonEmpty(strings.TrimSpace(processedReceipts.FailureReason), "provider completion receipt validation failed"),
			OutputText:           strings.TrimSpace(outputText),
			Tokens:               tokens,
			ProviderSessionID:    strings.TrimSpace(providerSessionID),
			ProviderInvocationID: strings.TrimSpace(req.InvocationID),
			ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
			EvidenceSource:       completionEvidenceSourceStream,
		}, true, false
	}
	if !processedReceipts.Accepted {
		return attemptCompletionResult{}, false, false
	}
	return completionResultForAcceptedReceipt(processedReceipts.Acceptance, outputText, tokens, providerSessionID, req.InvocationID, req.InvocationIssuedAt), true, true
}

func waitForCompletionReceiptSettlement(req ExecuteRequest, outputText string, tokens TokenUsage, providerSessionID string, maxWait time.Duration) (attemptCompletionResult, bool, bool) {
	if !completionReceiptModeEnabled(req) || maxWait <= 0 {
		return completionReceiptSettlement(req, outputText, tokens, providerSessionID)
	}
	firstReceiptDeadline := time.Now().Add(maxWait)
	quiescenceDeadline := time.Time{}
	acceptedResult := attemptCompletionResult{}
	for {
		result, settled, accepted := completionReceiptSettlement(req, outputText, tokens, providerSessionID)
		if settled {
			if !accepted {
				return result, true, false
			}
			acceptedResult = result
			quiescenceDeadline = time.Now().Add(maxWait)
		}
		now := time.Now()
		if !quiescenceDeadline.IsZero() {
			if !now.Before(quiescenceDeadline) {
				return acceptedResult, true, true
			}
		} else if !now.Before(firstReceiptDeadline) {
			return attemptCompletionResult{}, false, false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func receiptRequiredDecision(decision attemptCompletionDecision, providerSessionID, outputText, invocationID, invocationAt string) attemptCompletionDecision {
	if decision.Result.State != attemptCompletionStateSuccess {
		return decision
	}
	decision.Result.State = attemptCompletionStateIncompleteTurn
	decision.Result.ReasonCode = "provider_incomplete_turn"
	decision.Result.FailureText = ""
	decision.Result.ProviderSessionID = strings.TrimSpace(providerSessionID)
	decision.Result.ProviderInvocationID = strings.TrimSpace(invocationID)
	decision.Result.ProviderInvocationAt = strings.TrimSpace(invocationAt)
	decision.Result.OutputText = firstNonEmpty(strings.TrimSpace(decision.Result.OutputText), strings.TrimSpace(outputText))
	decision.Result.EvidenceSource = completionEvidenceSourceStream
	decision.ExportCheckPolicy = exportCheckPolicyRequired
	decision.ExportCheckReason = firstNonEmpty(strings.TrimSpace(decision.ExportCheckReason), "missing_completion_receipt")
	return decision
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func accumulateTokenUsage(total *TokenUsage, delta TokenUsage) {
	if total == nil {
		return
	}
	total.InputTokens += delta.InputTokens
	total.OutputTokens += delta.OutputTokens
	total.TotalTokens += delta.TotalTokens
	total.ReasoningTokens += delta.ReasoningTokens
	total.CacheReadTokens += delta.CacheReadTokens
	total.CacheWriteTokens += delta.CacheWriteTokens
}

func (a *SessionAdapter) startOpencodeRun(ctx context.Context, req ExecuteRequest, sessionID, prompt string) (startedOpencodeRun, error) {
	args := []string{"run", "--format", "json"}
	if a.skipPermissions {
		args = append(args, "--dangerously-skip-permissions")
	}
	if strings.TrimSpace(sessionID) != "" {
		args = append(args, "--session", strings.TrimSpace(sessionID))
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = a.model
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	variant := strings.TrimSpace(req.Variant)
	if variant == "" {
		variant = a.variant
	}
	if variant != "" {
		args = append(args, "--variant", variant)
	}
	agent := strings.TrimSpace(req.Agent)
	if agent == "" {
		agent = a.agent
	}
	if agent != "" {
		args = append(args, "--agent", agent)
	}
	if strings.TrimSpace(req.WorkspacePath) != "" {
		args = append(args, "--dir", strings.TrimSpace(req.WorkspacePath))
	}
	if a.attachURL != "" {
		args = append(args, "--attach", a.attachURL)
	}
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, a.binaryPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if strings.TrimSpace(req.WorkspacePath) != "" {
		cmd.Dir = strings.TrimSpace(req.WorkspacePath)
	} else if a.workDir != "" {
		cmd.Dir = a.workDir
	}
	cmd.Env = append(os.Environ(), opencodeCommandEnvironment(req, a.configDir, a.databasePath, a.inlineConfigContent)...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return startedOpencodeRun{}, fmt.Errorf("open opencode stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return startedOpencodeRun{}, fmt.Errorf("open opencode stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return startedOpencodeRun{}, fmt.Errorf("start opencode run: %w", err)
	}
	return startedOpencodeRun{cmd: cmd, stdout: stdout, stderr: stderr}, nil
}

func readStderrCapture(stream io.Reader) stderrCapture {
	capture := stderrCapture{}
	reader := bufio.NewReader(stream)
	lines := make([]string, 0)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			lines = append(lines, strings.TrimRight(line, "\r\n"))
			capture.lineCount++
			capture.lastLineAt = time.Now().UTC().Format(time.RFC3339)
		}
		if err != nil {
			if err != io.EOF {
				capture.scannerError = err.Error()
			}
			break
		}
	}
	capture.eofSeen = true
	capture.raw = []byte(strings.Join(lines, "\n"))
	return capture
}

type completionReceiptFilePayload struct {
	ProtocolVersion      string          `json:"protocol_version"`
	AttemptID            string          `json:"attempt_id"`
	InvocationID         string          `json:"invocation_id"`
	CompletionCapability string          `json:"completion_capability"`
	FinalText            string          `json:"final_text"`
	StructuredPayload    json.RawMessage `json:"structured_payload"`
}

type completionReceiptFile struct {
	Path    string
	Receipt CompletionReceipt
}

type completionReceiptProcessingResult struct {
	Seen            bool
	Accepted        bool
	Acceptance      CompletionReceiptAcceptance
	TerminalFailure bool
	FailureReason   string
}

type completionReceiptDisposition string

const (
	completionReceiptDispositionAccepted       completionReceiptDisposition = "accepted"
	completionReceiptDispositionRejected       completionReceiptDisposition = "rejected"
	completionReceiptDispositionTerminalReject completionReceiptDisposition = "terminal_rejected"
)

func completionReceiptModeEnabled(req ExecuteRequest) bool {
	return req.OnCompletionReceipt != nil || strings.TrimSpace(req.CompletionReceiptDir) != "" || strings.TrimSpace(req.CompletionCapability) != ""
}

func completionProtocolVersionForRequest(req ExecuteRequest) string {
	return firstNonEmpty(strings.TrimSpace(req.CompletionProtocolVersion), CompletionProtocolVersion)
}

func completionReceiptEnvironment(req ExecuteRequest) []string {
	if !completionReceiptModeEnabled(req) {
		return nil
	}
	return []string{
		"KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION=" + completionProtocolVersionForRequest(req),
		"KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID=" + strings.TrimSpace(req.AttemptID),
		"KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID=" + strings.TrimSpace(req.InvocationID),
		"KINGDOM_COMPLETE_ATTEMPT_CAPABILITY=" + strings.TrimSpace(req.CompletionCapability),
		"KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR=" + strings.TrimSpace(req.CompletionReceiptDir),
	}
}

func ConfigDirEnvironment(configDir string) []string {
	trimmed := strings.TrimSpace(configDir)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_CONFIG_DIR=" + trimmed}
}

func DatabasePathEnvironment(databasePath string) []string {
	trimmed := strings.TrimSpace(databasePath)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_DB=" + trimmed}
}

func configContentEnvironment(configContent string) []string {
	trimmed := strings.TrimSpace(configContent)
	if trimmed == "" {
		return nil
	}
	return []string{"OPENCODE_CONFIG_CONTENT=" + trimmed}
}

func opencodeCommandEnvironment(req ExecuteRequest, defaultConfigDir string, defaultDatabasePath string, defaultInlineConfigContent string) []string {
	env := completionReceiptEnvironment(req)
	configDir := strings.TrimSpace(req.ConfigDir)
	if configDir == "" {
		configDir = strings.TrimSpace(defaultConfigDir)
	}
	databasePath := strings.TrimSpace(req.DatabasePath)
	if databasePath == "" {
		databasePath = strings.TrimSpace(defaultDatabasePath)
	}
	env = append(env, ConfigDirEnvironment(configDir)...)
	env = append(env, DatabasePathEnvironment(databasePath)...)
	return append(env, configContentEnvironment(defaultInlineConfigContent)...)
}

func loadCompletionReceipts(req ExecuteRequest) ([]completionReceiptFile, error) {
	receiptDir := strings.TrimSpace(req.CompletionReceiptDir)
	if receiptDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(receiptDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read completion receipt dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	receipts := make([]completionReceiptFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		receiptPath := filepath.Join(receiptDir, entry.Name())
		raw, err := os.ReadFile(receiptPath)
		if err != nil {
			return nil, fmt.Errorf("read completion receipt file %q: %w", receiptPath, err)
		}
		var payload completionReceiptFilePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("decode completion receipt file %q: %w", receiptPath, err)
		}
		structuredPayloadJSON := "{}"
		if trimmed := strings.TrimSpace(string(payload.StructuredPayload)); trimmed != "" {
			structuredPayloadJSON = trimmed
		}
		receipts = append(receipts, completionReceiptFile{
			Path: receiptPath,
			Receipt: CompletionReceipt{
				ProtocolVersion:       firstNonEmpty(strings.TrimSpace(payload.ProtocolVersion), completionProtocolVersionForRequest(req)),
				AttemptID:             strings.TrimSpace(payload.AttemptID),
				InvocationID:          strings.TrimSpace(payload.InvocationID),
				CompletionCapability:  strings.TrimSpace(payload.CompletionCapability),
				FinalText:             strings.TrimSpace(payload.FinalText),
				StructuredPayloadJSON: structuredPayloadJSON,
			},
		})
	}
	return receipts, nil
}

func removeCompletionReceiptFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove completion receipt file %q: %w", path, err)
	}
	return nil
}

func completionReceiptDispositionForHandlerResult(result CompletionReceiptAcceptance, err error) (completionReceiptDisposition, error) {
	if err != nil {
		return "", err
	}
	if result.Rejected {
		if strings.TrimSpace(result.EvidenceID) != "" || result.Idempotent {
			return "", fmt.Errorf("completion receipt handler cannot both accept and reject the same receipt")
		}
		if result.InvalidatesInvocation {
			return completionReceiptDispositionTerminalReject, nil
		}
		return completionReceiptDispositionRejected, nil
	}
	if result.InvalidatesInvocation {
		return "", fmt.Errorf("completion receipt handler cannot invalidate an invocation without rejecting the receipt")
	}
	if strings.TrimSpace(result.EvidenceID) != "" || result.Idempotent {
		return completionReceiptDispositionAccepted, nil
	}
	return "", fmt.Errorf("completion receipt handler must explicitly accept or reject the receipt")
}

func processCompletionReceipts(req ExecuteRequest) (completionReceiptProcessingResult, error) {
	if !completionReceiptModeEnabled(req) {
		return completionReceiptProcessingResult{}, nil
	}
	if req.OnCompletionReceipt == nil {
		return completionReceiptProcessingResult{}, fmt.Errorf("completion receipt handler is required")
	}
	receipts, err := loadCompletionReceipts(req)
	if err != nil {
		return completionReceiptProcessingResult{}, err
	}
	if len(receipts) == 0 {
		return completionReceiptProcessingResult{}, nil
	}
	processed := completionReceiptProcessingResult{Seen: true}
	for _, receipt := range receipts {
		result, err := req.OnCompletionReceipt(receipt.Receipt)
		disposition, dispositionErr := completionReceiptDispositionForHandlerResult(result, err)
		if dispositionErr != nil {
			return processed, dispositionErr
		}
		switch disposition {
		case completionReceiptDispositionRejected:
			if err := removeCompletionReceiptFile(receipt.Path); err != nil {
				return processed, err
			}
			continue
		case completionReceiptDispositionTerminalReject:
			if err := removeCompletionReceiptFile(receipt.Path); err != nil {
				return processed, err
			}
			processed.Accepted = false
			processed.Acceptance = CompletionReceiptAcceptance{}
			processed.TerminalFailure = true
			processed.FailureReason = firstNonEmpty(strings.TrimSpace(result.RejectedReason), "provider completion receipt invalidated the invocation")
			return processed, nil
		case completionReceiptDispositionAccepted:
			if err := removeCompletionReceiptFile(receipt.Path); err != nil {
				return processed, err
			}
			processed.Acceptance = result
			processed.Accepted = true
			continue
		default:
			return processed, fmt.Errorf("unsupported completion receipt disposition: %q", disposition)
		}
	}
	return processed, nil
}

func NewSessionAdapter(cfg SessionAdapterConfig) (*SessionAdapter, error) {
	binaryPath := strings.TrimSpace(cfg.BinaryPath)
	if binaryPath == "" {
		binaryPath = "opencode"
	}
	if _, err := exec.LookPath(binaryPath); err != nil {
		return nil, fmt.Errorf("resolve opencode binary %q: %w", binaryPath, err)
	}
	exporter := firstNonNilSessionExporter(cfg.SessionExporter, cliSessionExporter{binaryPath: binaryPath})

	return &SessionAdapter{
		binaryPath:              binaryPath,
		workDir:                 strings.TrimSpace(cfg.WorkDir),
		configDir:               strings.TrimSpace(cfg.ConfigDir),
		databasePath:            strings.TrimSpace(cfg.DatabasePath),
		inlineConfigContent:     strings.TrimSpace(cfg.InlineConfigContent),
		model:                   strings.TrimSpace(cfg.Model),
		variant:                 strings.TrimSpace(cfg.Variant),
		agent:                   strings.TrimSpace(cfg.Agent),
		attachURL:               strings.TrimSpace(cfg.AttachURL),
		skipPermissions:         cfg.SkipPermissions,
		maxContinuationTurns:    normalizeMaxContinuationTurns(cfg.MaxContinuationTurns),
		firstEventTimeout:       cfg.FirstEventTimeout,
		streamInactivityTimeout: cfg.StreamInactivityTimeout,
		reconciler:              newSessionCompletionReconciler(sessionCompletionReconcilerConfig{Exporter: exporter, TotalTimeout: cfg.ReconcileTotalTimeout, BackoffSchedule: cfg.ReconcileBackoffSchedule, FallbackWorkDir: strings.TrimSpace(cfg.WorkDir)}),
		logicalToActual:         make(map[string]string),
		attempts:                make(map[string]*sessionAttemptRecord),
		inFlight:                make(map[string]*exec.Cmd),
		attemptCancels:          make(map[string]context.CancelFunc),
	}, nil
}

func normalizeMaxContinuationTurns(value int) int {
	if value <= 0 {
		return 3
	}
	return value
}

func (a *SessionAdapter) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	if err := validateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	a.setAttemptCancel(req.AttemptID, cancelAttempt)
	defer a.clearAttemptCancel(req.AttemptID)

	a.appendEvent(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeStarted,
		PayloadJSON: `{}`,
		OccurredAt:  req.OccurredAt,
	})

	invocationID := resolveProviderInvocationID(req)
	startAt := time.Time{}
	lastEventAt := time.Time{}
	diagnostics := streamDiagnostics{}

	actualSessionID := ""
	providerSessionObserved := false
	if !req.ForceNewSession {
		actualSessionID = firstNonEmpty(strings.TrimSpace(req.ProviderSessionID), a.actualSessionID(req.SessionID))
	}
	execDiag := executionPathDiagnostics{
		AttemptID: strings.TrimSpace(req.AttemptID),
		SessionID: strings.TrimSpace(req.SessionID),
	}
	startedRun, err := a.startOpencodeRun(attemptCtx, req, actualSessionID, promptWithProviderInvocationMetadata(req.Prompt, req.AttemptID, invocationID))
	if err != nil {
		return ExecuteResponse{}, err
	}
	cmd := startedRun.cmd
	stdout := startedRun.stdout
	stderr := startedRun.stderr
	startAt = time.Now().UTC()
	a.setInFlight(req.AttemptID, cmd)
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}
	diagnostics.ProcessStartedAt = startAt.Format(time.RFC3339)
	execDiag.CmdStartedAt = diagnostics.ProcessStartedAt

	ctxCancelDone := make(chan struct{})
	go func() {
		select {
		case <-attemptCtx.Done():
			_ = terminateCommandProcessTree(cmd)
		case <-ctxCancelDone:
		}
	}()
	defer close(ctxCancelDone)
	watchdog := startCommandWatchdog(cmd, a.firstEventTimeout, a.streamInactivityTimeout)

	stderrDone := make(chan stderrCapture, 1)
	go func() {
		stderrDone <- readStderrCapture(stderr)
	}()

	textChunks := make([]string, 0)
	var accumulatedTokens TokenUsage
	exportChecks := make([]ExportCheck, 0)
	stderrSnapshot := stderrCapture{}
	finish := func(resp ExecuteResponse) ExecuteResponse {
		resp = withExecuteExportChecks(resp, exportChecks)
		return withExecuteProviderDiagnostics(resp, buildProviderDiagnosticsJSON(req, diagnostics, execDiag, stderrSnapshot, exportChecks))
	}
	lastStepFinish := buildStepFinishReasonEvidence("", false)
	upstreamErrorText := ""
	terminalSuccessSeen := false
	terminalFailureSeen := false
	conflictingTerminalEvidence := false
	if err := decodeOpencodeRunEventStream(stdout, func(event opencodeRunEvent) error {
		now := time.Now().UTC()
		diagnostics.EventCount++
		if lastEventAt.IsZero() {
			diagnostics.FirstEventLatency = now.Sub(startAt)
			diagnostics.FirstEventAt = now.Format(time.RFC3339)
		} else {
			gap := now.Sub(lastEventAt)
			if gap > diagnostics.MaxInterEventGap {
				diagnostics.MaxInterEventGap = gap
			}
		}
		lastEventAt = now
		diagnostics.LastEventAt = now.Format(time.RFC3339)

		watchdog.Signal()

		if strings.TrimSpace(event.SessionID) != "" {
			actualSessionID = a.observeProviderSession(req, actualSessionID, event.SessionID, &providerSessionObserved)
			execDiag.SessionID = actualSessionID
		}
		execDiag.LastStdoutEventAt = now.Format(time.RFC3339)
		execDiag.StdoutEventCount++
		if strings.TrimSpace(event.ErrorText) != "" && strings.TrimSpace(event.Type) == "error" {
			upstreamErrorText = strings.TrimSpace(event.ErrorText)
			execDiag.UpstreamErrorText = upstreamErrorText
		}

		if event.Type == "step_finish" {
			execDiag.StepFinishSeen = true
			lastStepFinish = buildStepFinishReasonEvidence(event.Part.Reason, event.Part.ReasonPresent)
			execDiag.LastStepFinishReason = strings.TrimSpace(lastStepFinish.Normalized)
			execDiag.LastStepFinishReasonKind = string(lastStepFinish.Kind)
			execDiag.TerminalStateClass = string(lastStepFinish.Class)
			switch lastStepFinish.Class {
			case stepFinishTerminalSuccessCandidate:
				if terminalFailureSeen {
					conflictingTerminalEvidence = true
				}
				terminalSuccessSeen = true
			case stepFinishTerminalFailure:
				if terminalSuccessSeen {
					conflictingTerminalEvidence = true
				}
				terminalFailureSeen = true
			}
		}
		if event.Type == "step_finish" && event.Part.Tokens != nil {
			accumulatedTokens.InputTokens += event.Part.Tokens.Input
			accumulatedTokens.OutputTokens += event.Part.Tokens.Output
			accumulatedTokens.TotalTokens += event.Part.Tokens.Total
			accumulatedTokens.ReasoningTokens += event.Part.Tokens.Reasoning
			accumulatedTokens.CacheReadTokens += event.Part.Tokens.Cache.Read
			accumulatedTokens.CacheWriteTokens += event.Part.Tokens.Cache.Write
		}

		if event.Type != "text" || strings.TrimSpace(event.Part.Text) == "" {
			return nil
		}
		execDiag.LastProgressTextLen = len(event.Part.Text)
		sum := sha256.Sum256([]byte(event.Part.Text))
		execDiag.LastProgressTextSHA = fmt.Sprintf("%x", sum[:])

		textChunks = append(textChunks, event.Part.Text)
		if req.OnProgress != nil {
			req.OnProgress(event.Part.Text)
		}
		payload, _ := json.Marshal(map[string]any{
			"text":                                event.Part.Text,
			"execution_path_diagnostics_snapshot": execDiag.progressSnapshot(),
		})
		a.appendEvent(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeProgress,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return nil
	}, func(line string, err error) {
		recordMalformedStdoutLine(&execDiag, line)
	}); err != nil {
		watchdog.Stop()
		a.clearInFlight(req.AttemptID)
		if timeoutErr := watchdog.Err(); timeoutErr != nil {
			if result, settled, _ := waitForCompletionReceiptSettlement(req, strings.TrimSpace(strings.Join(textChunks, "\n")), accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
				return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
			}
			result := executionResultForTimeout(decorateTimeoutError(timeoutErr, diagnostics), actualSessionID)
			result.OutputText = strings.TrimSpace(strings.Join(textChunks, "\n"))
			result.Tokens = accumulatedTokens
			payload, _ := json.Marshal(buildTimeoutFailurePayload(timeoutErr, diagnostics, execDiag))
			execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeFailed,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		if result, settled, _ := waitForCompletionReceiptSettlement(req, strings.TrimSpace(strings.Join(textChunks, "\n")), accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		return finish(executeResponseForObservedState(strings.TrimSpace(strings.Join(textChunks, "\n")), accumulatedTokens, actualSessionID)), fmt.Errorf("read opencode stream: %w", err)
	}
	execDiag.StdoutEOFSeen = true

	execDiag.CmdWaitStartedAt = time.Now().UTC().Format(time.RFC3339)
	waitErr := cmd.Wait()
	execDiag.CmdWaitFinishedAt = time.Now().UTC().Format(time.RFC3339)
	execDiag.WaitReturned = true
	if waitErr != nil {
		execDiag.WaitError = waitErr.Error()
	}
	if cmd.ProcessState != nil {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			exitCode := status.ExitStatus()
			execDiag.ExitCode = &exitCode
			if status.Signaled() {
				exitSignal := int(status.Signal())
				execDiag.ExitSignal = &exitSignal
			}
		}
	}
	diagnostics.ProcessExitedAt = time.Now().UTC().Format(time.RFC3339)
	watchdog.Stop()
	a.clearInFlight(req.AttemptID)
	stderrSnapshot = <-stderrDone
	execDiag.StderrLineCount = stderrSnapshot.lineCount
	execDiag.LastStderrLineAt = stderrSnapshot.lastLineAt
	execDiag.StderrEOFSeen = stderrSnapshot.eofSeen
	if strings.TrimSpace(stderrSnapshot.scannerError) != "" {
		if strings.TrimSpace(execDiag.WaitError) == "" {
			execDiag.WaitError = stderrSnapshot.scannerError
		} else {
			execDiag.WaitError = execDiag.WaitError + "; stderr_scan_error=" + stderrSnapshot.scannerError
		}
	}
	stderrText := strings.TrimSpace(string(stderrSnapshot.raw))

	if actualSessionID != "" {
		a.bindActualSession(req.SessionID, actualSessionID)
	}
	partialCombinedText := strings.TrimSpace(strings.Join(textChunks, "\n"))
	continuationUsed := false
	latestContinuationText := ""
	decision := attemptCompletionDecision{}
	result := attemptCompletionResult{}
	settledBeforeContinuation := false
	receiptAccepted := false
	receiptRequired := completionReceiptModeEnabled(req)
	if receiptResult, settled, accepted := waitForCompletionReceiptSettlement(req, partialCombinedText, accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
		result = receiptResult
		receiptAccepted = accepted
		settledBeforeContinuation = true
	}

	if !settledBeforeContinuation && watchdog.Err() != nil {
		if result, settled, accepted := waitForCompletionReceiptSettlement(req, partialCombinedText, accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
			receiptAccepted = accepted
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		timeoutErr := watchdog.Err()
		result := executionResultForTimeout(decorateTimeoutError(timeoutErr, diagnostics), actualSessionID)
		result.OutputText = partialCombinedText
		result.Tokens = accumulatedTokens
		execDiag.FailedEventAppended = true
		payload, _ := json.Marshal(buildTimeoutFailurePayload(timeoutErr, diagnostics, execDiag))
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeFailed,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
	}

	if !settledBeforeContinuation && waitErr != nil {
		if !isExecutionCancelled(waitErr) && !isExecutionCancelled(attemptCtx.Err()) {
			if result, settled, accepted := waitForCompletionReceiptSettlement(req, partialCombinedText, accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
				receiptAccepted = accepted
				return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
			}
		}
		if isExecutionCancelled(waitErr) || isExecutionCancelled(attemptCtx.Err()) {
			result := executionResultForCancellation(waitErr, actualSessionID, firstNonEmpty(waitErr.Error(), "opencode run cancelled"))
			result.OutputText = partialCombinedText
			result.Tokens = accumulatedTokens
			reason := firstNonEmpty(waitErr.Error(), "opencode run cancelled")
			payload, _ := json.Marshal(map[string]any{"reason": reason})
			execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeCancelled,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		if isExecutionTimedOut(waitErr) || isExecutionTimedOut(attemptCtx.Err()) {
			timeoutErr := firstNonNilError(waitErr, attemptCtx.Err())
			result := executionResultForTimeout(timeoutErr, actualSessionID)
			result.OutputText = partialCombinedText
			result.Tokens = accumulatedTokens
			payload, _ := json.Marshal(buildTimeoutFailurePayload(timeoutErr, diagnostics, execDiag))
			execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeFailed,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
		}
		result := executionResultForProcessError(waitErr, actualSessionID, firstNonEmpty(stderrText, waitErr.Error()))
		result.OutputText = partialCombinedText
		result.Tokens = accumulatedTokens
		payload, _ := json.Marshal(map[string]any{
			"error":                      firstNonEmpty(stderrText, waitErr.Error()),
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		})
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
			AttemptID:   req.AttemptID,
			EventType:   AttemptEventTypeFailed,
			PayloadJSON: string(payload),
			OccurredAt:  time.Now().UTC().Format(time.RFC3339),
		})
		return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
	}

	if execDiag.TerminalStateClass == "non_terminal" && !settledBeforeContinuation && !receiptRequired {
		reconcileInvocationID := invocationID
		if strings.TrimSpace(actualSessionID) != "" {
			preContinuationOutcome, preContinuationErr := a.reconciler.Reconcile(attemptCtx, sessionCompletionReconcileRequest{
				ProviderSessionID:   actualSessionID,
				WorkDir:             firstNonEmpty(strings.TrimSpace(req.WorkspacePath), a.workDir),
				ConfigDir:           firstNonEmpty(strings.TrimSpace(req.ConfigDir), a.configDir),
				DatabasePath:        firstNonEmpty(strings.TrimSpace(req.DatabasePath), a.databasePath),
				StreamOutputText:    partialCombinedText,
				Tokens:              accumulatedTokens,
				AttemptID:           strings.TrimSpace(req.AttemptID),
				InvocationID:        reconcileInvocationID,
				InvocationStartedAt: startAt,
				ReceiptRequired:     false,
			})
			exportChecks = appendPhasedExportChecks(exportChecks, ExportCheckPhaseForegroundReconcile, preContinuationOutcome, preContinuationErr)
			if preContinuationErr != nil {
				if isExecutionCancelled(preContinuationErr) {
					result := executionResultForCancellation(preContinuationErr, actualSessionID, "opencode run cancelled during continuation precheck")
					result.OutputText = partialCombinedText
					result.Tokens = accumulatedTokens
					execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeCancelled,
						PayloadJSON: `{"reason":"opencode run cancelled during continuation precheck"}`,
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				if isExecutionTimedOut(preContinuationErr) {
					result := executionResultForTimeout(preContinuationErr, actualSessionID)
					result.OutputText = partialCombinedText
					result.Tokens = accumulatedTokens
					payload, _ := json.Marshal(buildTimeoutFailurePayload(preContinuationErr, diagnostics, execDiag))
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeFailed,
						PayloadJSON: string(payload),
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				return finish(executeResponseForObservedState(partialCombinedText, accumulatedTokens, actualSessionID)), preContinuationErr
			}
			if preContinuationOutcome.Status == sessionCompletionReconcileStatusResolved {
				execDiag.ExportCheckAttempted = true
				execDiag.ExportCheckStatus = strings.TrimSpace(string(preContinuationOutcome.Status))
				execDiag.ExportCheckEvidenceSource = strings.TrimSpace(string(preContinuationOutcome.Result.EvidenceSource))
				execDiag.ExportCheckLastError = strings.TrimSpace(firstNonEmpty(preContinuationOutcome.Diagnostic, preContinuationOutcome.Result.FailureText))
				execDiag.ExportCheckAttempts = preContinuationOutcome.ExportAttempts
				execDiag.ExportCheckElapsedMs = preContinuationOutcome.Elapsed.Milliseconds()
				result = preContinuationOutcome.Result
				settledBeforeContinuation = true
			}
		}
	}

	if execDiag.TerminalStateClass == "non_terminal" && !settledBeforeContinuation {
		execDiag.ContinuationTurnsMax = a.maxContinuationTurns - 1
		remainingTurns := a.maxContinuationTurns - 1
		for remainingTurns > 0 {
			if attemptCtx.Err() != nil {
				if isExecutionTimedOut(attemptCtx.Err()) {
					result := executionResultForTimeout(attemptCtx.Err(), actualSessionID)
					result.OutputText = strings.TrimSpace(strings.Join(textChunks, "\n"))
					result.Tokens = accumulatedTokens
					payload, _ := json.Marshal(buildTimeoutFailurePayload(attemptCtx.Err(), diagnostics, execDiag))
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeFailed,
						PayloadJSON: string(payload),
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				result := executionResultForCancellation(attemptCtx.Err(), actualSessionID, "opencode run cancelled during continuation")
				result.OutputText = strings.TrimSpace(strings.Join(textChunks, "\n"))
				result.Tokens = accumulatedTokens
				execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeCancelled,
					PayloadJSON: `{"reason":"opencode run cancelled during continuation"}`,
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
			}
			remainingTurns--
			execDiag.ContinuationTurnsElapsed++
			continuationPayload, _ := json.Marshal(map[string]any{
				"reason":                              "continuation_turn_started",
				"continuation_turn":                   execDiag.ContinuationTurnsElapsed,
				"continuation_turn_max":               execDiag.ContinuationTurnsMax,
				"prior_finish_reason":                 strings.TrimSpace(execDiag.LastStepFinishReason),
				"prior_terminal_class":                strings.TrimSpace(execDiag.TerminalStateClass),
				"execution_path_diagnostics_snapshot": execDiag.progressSnapshot(),
			})
			a.appendEvent(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeProgress,
				PayloadJSON: string(continuationPayload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
			continuationResult, continuationErr := a.executeContinuationTurn(attemptCtx, req, actualSessionID, invocationID)
			continuationUsed = true
			if continuationResult.ProviderSessionID != "" {
				actualSessionID = continuationResult.ProviderSessionID
				a.bindActualSession(req.SessionID, actualSessionID)
				execDiag.SessionID = actualSessionID
			}
			textChunks = append(textChunks, continuationResult.TextChunks...)
			if continuationText := strings.TrimSpace(strings.Join(continuationResult.TextChunks, "\n")); continuationText != "" {
				latestContinuationText = continuationText
			}
			accumulateTokenUsage(&accumulatedTokens, continuationResult.Tokens)
			combinedContinuationText := strings.TrimSpace(strings.Join(textChunks, "\n"))
			if receiptResult, settled, accepted := waitForCompletionReceiptSettlement(req, canonicalSettledOutputText(combinedContinuationText, latestContinuationText, continuationUsed), accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
				result = receiptResult
				receiptAccepted = accepted
				settledBeforeContinuation = true
				break
			}
			if continuationErr != nil {
				if !isExecutionCancelled(continuationErr) {
					if receiptResult, settled, accepted := waitForCompletionReceiptSettlement(req, canonicalSettledOutputText(combinedContinuationText, latestContinuationText, continuationUsed), accumulatedTokens, actualSessionID, completionReceiptObservationGracePeriod); settled {
						result = receiptResult
						receiptAccepted = accepted
						settledBeforeContinuation = true
						break
					}
				}
				if isExecutionCancelled(continuationErr) {
					result := executionResultForCancellation(continuationErr, actualSessionID, "opencode continuation cancelled")
					result.OutputText = combinedContinuationText
					result.Tokens = accumulatedTokens
					execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeCancelled,
						PayloadJSON: `{"reason":"opencode continuation cancelled"}`,
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				if isExecutionTimedOut(continuationErr) {
					result := executionResultForTimeout(continuationErr, actualSessionID)
					result.OutputText = combinedContinuationText
					result.Tokens = accumulatedTokens
					payload, _ := json.Marshal(buildTimeoutFailurePayload(continuationErr, diagnostics, execDiag))
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
						AttemptID:   req.AttemptID,
						EventType:   AttemptEventTypeFailed,
						PayloadJSON: string(payload),
						OccurredAt:  time.Now().UTC().Format(time.RFC3339),
					})
					return finish(executeResponseForCompletionResult(result)), executeErrorForCompletionResult(result)
				}
				failureEvidence := attemptCompletionEvidence{
					ProviderSessionID: actualSessionID,
					OutputText:        combinedContinuationText,
					Tokens:            accumulatedTokens,
					UpstreamErrorText: strings.TrimSpace(continuationResult.UpstreamErrorText),
					FailureText:       continuationErr.Error(),
				}
				var timeoutTyped *providerTimeoutError
				if errors.As(continuationErr, &timeoutTyped) {
					failureEvidence.Timeout = true
				} else if strings.TrimSpace(failureEvidence.UpstreamErrorText) == "" {
					failureEvidence.ProcessFailed = true
				}
				failureResult := attemptCompletionAdjudicator{}.Adjudicate(failureEvidence).Result
				payloadMap := map[string]any{
					"error":                      firstNonEmpty(strings.TrimSpace(failureResult.FailureText), continuationErr.Error()),
					"stream_diagnostics":         diagnostics.toMap(),
					"execution_path_diagnostics": execDiag.toMap(),
				}
				if errors.As(continuationErr, &timeoutTyped) {
					payloadMap["timeout_type"] = strings.TrimSpace(timeoutTyped.TimeoutType)
					payloadMap["timeout_ms"] = timeoutTyped.Timeout.Milliseconds()
				}
				payload, _ := json.Marshal(payloadMap)
				execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
					AttemptID:   req.AttemptID,
					EventType:   AttemptEventTypeFailed,
					PayloadJSON: string(payload),
					OccurredAt:  time.Now().UTC().Format(time.RFC3339),
				})
				return finish(executeResponseForCompletionResult(failureResult)), executeErrorForCompletionResult(failureResult)
			}

			lastStepFinish = continuationResult.LastReason
			execDiag.LastStepFinishReason = strings.TrimSpace(lastStepFinish.Normalized)
			execDiag.LastStepFinishReasonKind = string(lastStepFinish.Kind)
			execDiag.TerminalStateClass = string(lastStepFinish.Class)
			execDiag.StepFinishSeen = true
			if execDiag.TerminalStateClass == "terminal_success_candidate" {
				if terminalFailureSeen {
					conflictingTerminalEvidence = true
				}
				terminalSuccessSeen = true
				break
			}
			if execDiag.TerminalStateClass == "terminal_failure" {
				if terminalSuccessSeen {
					conflictingTerminalEvidence = true
				}
				terminalFailureSeen = true
				break
			}
		}
	}

	observedCombinedText := strings.TrimSpace(strings.Join(textChunks, "\n"))
	canonicalCombinedText := canonicalSettledOutputText(observedCombinedText, latestContinuationText, continuationUsed)
	if !settledBeforeContinuation {
		decision = attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
			ProviderSessionID:           actualSessionID,
			OutputText:                  canonicalCombinedText,
			Tokens:                      accumulatedTokens,
			StepFinishSeen:              execDiag.StepFinishSeen,
			LastStepFinish:              lastStepFinish,
			UpstreamErrorText:           upstreamErrorText,
			ConflictingTerminalEvidence: conflictingTerminalEvidence,
			CleanEOF:                    waitErr == nil,
		})
		if receiptRequired && !receiptAccepted {
			decision = receiptRequiredDecision(decision, actualSessionID, canonicalCombinedText, req.InvocationID, req.InvocationIssuedAt)
		}
		result = decision.Result
		execDiag.ExportCheckPolicy = strings.TrimSpace(string(decision.ExportCheckPolicy))
		execDiag.ExportCheckReason = strings.TrimSpace(decision.ExportCheckReason)
	}
	if !settledBeforeContinuation && decision.ExportCheckPolicy != exportCheckPolicyNone {
		if strings.TrimSpace(actualSessionID) == "" {
			missingSessionText := fmt.Sprintf("missing provider session id after ambiguous completion: %s", decision.ExportCheckReason)
			execDiag.ExportCheckStatus = "missing_provider_session_id"
			execDiag.ExportCheckLastError = strings.TrimSpace(firstNonEmpty(stderrText, missingSessionText))
			result = attemptCompletionResult{
				State:                attemptCompletionStateIncompleteTurn,
				ReasonCode:           "provider_incomplete_turn",
				FailureText:          firstNonEmpty(stderrText, missingSessionText),
				OutputText:           canonicalCombinedText,
				Tokens:               accumulatedTokens,
				ProviderSessionID:    actualSessionID,
				ProviderInvocationID: strings.TrimSpace(req.InvocationID),
				ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
				EvidenceSource:       completionEvidenceSourceStream,
			}
		} else {
			execDiag.ExportCheckAttempted = true
			reconcileInvocationID := invocationID
			emitExecuteLifecycleObservation(req, ExecuteLifecycleObservation{
				Phase:             ExecuteLifecyclePhaseProviderExportReconciliationStarted,
				ProviderSessionID: actualSessionID,
				AmbiguityReason:   decision.ExportCheckReason,
			})
			reconcileOutcome, reconcileErr := a.reconciler.Reconcile(attemptCtx, sessionCompletionReconcileRequest{
				ProviderSessionID:   actualSessionID,
				WorkDir:             firstNonEmpty(strings.TrimSpace(req.WorkspacePath), a.workDir),
				ConfigDir:           firstNonEmpty(strings.TrimSpace(req.ConfigDir), a.configDir),
				DatabasePath:        firstNonEmpty(strings.TrimSpace(req.DatabasePath), a.databasePath),
				StreamOutputText:    canonicalCombinedText,
				Tokens:              accumulatedTokens,
				AttemptID:           strings.TrimSpace(req.AttemptID),
				InvocationID:        reconcileInvocationID,
				InvocationStartedAt: startAt,
				ReceiptRequired:     receiptRequired,
			})
			exportChecks = appendPhasedExportChecks(exportChecks, ExportCheckPhaseForegroundReconcile, reconcileOutcome, reconcileErr)
			if reconcileErr != nil {
				execDiag.ExportCheckStatus = "reconcile_error"
				execDiag.ExportCheckLastError = strings.TrimSpace(reconcileErr.Error())
				emitExecuteLifecycleObservation(req, ExecuteLifecycleObservation{
					Phase:             ExecuteLifecyclePhaseProviderExportReconciliationFailed,
					ProviderSessionID: actualSessionID,
					AmbiguityReason:   decision.ExportCheckReason,
					Error:             reconcileErr.Error(),
				})
				if isExecutionCancelled(attemptCtx.Err()) || (!receiptRequired && isExecutionCancelled(reconcileErr)) {
					cancelledResult := attemptCompletionResult{
						State:             attemptCompletionStateCancelled,
						ReasonCode:        "provider_cancelled",
						FailureText:       firstNonEmpty(strings.TrimSpace(reconcileErr.Error()), "opencode run cancelled"),
						ProviderSessionID: actualSessionID,
						OutputText:        observedCombinedText,
						Tokens:            accumulatedTokens,
						EvidenceSource:    completionEvidenceSourceExport,
					}
					payload, _ := json.Marshal(map[string]any{
						"error":                      cancelledResult.FailureText,
						"stream_diagnostics":         diagnostics.toMap(),
						"execution_path_diagnostics": execDiag.toMap(),
					})
					execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: AttemptEventTypeCancelled, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
					return finish(executeResponseForCompletionResult(cancelledResult)), context.Canceled
				}
				if isExecutionTimedOut(attemptCtx.Err()) || (!receiptRequired && isExecutionTimedOut(reconcileErr)) {
					timeoutResult := executionResultForTimeout(firstNonNilError(attemptCtx.Err(), reconcileErr), actualSessionID)
					timeoutResult.OutputText = observedCombinedText
					timeoutResult.Tokens = accumulatedTokens
					payload, _ := json.Marshal(buildTimeoutFailurePayload(firstNonNilError(attemptCtx.Err(), reconcileErr), diagnostics, execDiag))
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: AttemptEventTypeFailed, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
					return finish(executeResponseForCompletionResult(timeoutResult)), executeErrorForCompletionResult(timeoutResult)
				}
				if receiptRequired {
					result = attemptCompletionResult{
						State:                attemptCompletionStateIncompleteTurn,
						ReasonCode:           "provider_incomplete_turn",
						FailureText:          firstNonEmpty(strings.TrimSpace(result.FailureText), stderrText, "provider exited without an accepted kingdom_complete_attempt receipt"),
						ProviderSessionID:    actualSessionID,
						ProviderInvocationID: strings.TrimSpace(req.InvocationID),
						ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
						OutputText:           firstNonEmpty(strings.TrimSpace(result.OutputText), canonicalCombinedText),
						Tokens:               accumulatedTokens,
						EvidenceSource:       completionEvidenceSource(firstNonEmpty(strings.TrimSpace(string(result.EvidenceSource)), string(completionEvidenceSourceStream))),
					}
				} else {
					payload, _ := json.Marshal(map[string]any{
						"error":                      reconcileErr.Error(),
						"stream_diagnostics":         diagnostics.toMap(),
						"execution_path_diagnostics": execDiag.toMap(),
					})
					execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: AttemptEventTypeFailed, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
					return finish(executeResponseForObservedState(observedCombinedText, accumulatedTokens, actualSessionID)), reconcileErr
				}
			} else {
				execDiag.ExportCheckStatus = strings.TrimSpace(string(reconcileOutcome.Status))
				execDiag.ExportCheckEvidenceSource = strings.TrimSpace(string(reconcileOutcome.Result.EvidenceSource))
				execDiag.ExportCheckLastError = strings.TrimSpace(firstNonEmpty(reconcileOutcome.Diagnostic, reconcileOutcome.Result.FailureText))
				execDiag.ExportCheckAttempts = reconcileOutcome.ExportAttempts
				execDiag.ExportCheckElapsedMs = reconcileOutcome.Elapsed.Milliseconds()
				if reconcileOutcome.Status == sessionCompletionReconcileStatusResolved {
					phase := ExecuteLifecyclePhaseProviderExportReconciliationSucceeded
					if !receiptRequired && reconcileOutcome.Result.State != attemptCompletionStateSuccess {
						phase = ExecuteLifecyclePhaseProviderExportReconciliationFailed
					}
					emitExecuteLifecycleObservation(req, ExecuteLifecycleObservation{
						Phase:              phase,
						ProviderSessionID:  actualSessionID,
						AmbiguityReason:    decision.ExportCheckReason,
						ResolvedState:      string(reconcileOutcome.Result.State),
						ResolvedReasonCode: strings.TrimSpace(reconcileOutcome.Result.ReasonCode),
						EvidenceSource:     string(reconcileOutcome.Result.EvidenceSource),
					})
					result = reconcileOutcome.Result
					result.ProviderInvocationID = strings.TrimSpace(req.InvocationID)
					result.ProviderInvocationAt = strings.TrimSpace(req.InvocationIssuedAt)
				} else {
					if reconcileOutcome.Status == sessionCompletionReconcileStatusUnresolved {
						emitExecuteLifecycleObservation(req, ExecuteLifecycleObservation{
							Phase:             ExecuteLifecyclePhaseProviderExportReconciliationUnresolved,
							ProviderSessionID: actualSessionID,
							AmbiguityReason:   decision.ExportCheckReason,
						})
					} else {
						emitExecuteLifecycleObservation(req, ExecuteLifecycleObservation{
							Phase:             ExecuteLifecyclePhaseProviderExportReconciliationFailed,
							ProviderSessionID: actualSessionID,
							AmbiguityReason:   decision.ExportCheckReason,
							Error:             firstNonEmpty(reconcileOutcome.Diagnostic, reconcileOutcome.Result.FailureText),
						})
					}
					unresolvedText := fmt.Sprintf("incomplete terminal state after reconciliation: %s", decision.ExportCheckReason)
					if reconcileOutcome.Status != sessionCompletionReconcileStatusUnresolved {
						unresolvedText = firstNonEmpty(reconcileOutcome.Diagnostic, reconcileOutcome.Result.FailureText, unresolvedText)
					}
					result = attemptCompletionResult{
						State:                attemptCompletionStateIncompleteTurn,
						ReasonCode:           "provider_incomplete_turn",
						FailureText:          firstNonEmpty(strings.TrimSpace(reconcileOutcome.Result.FailureText), strings.TrimSpace(reconcileOutcome.Diagnostic), stderrText, unresolvedText),
						ProviderSessionID:    actualSessionID,
						ProviderInvocationID: strings.TrimSpace(req.InvocationID),
						ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
						OutputText:           canonicalCombinedText,
						Tokens:               accumulatedTokens,
						EvidenceSource:       completionEvidenceSourceExport,
					}
				}
			}
		}
	}
	if completionReceiptModeEnabled(req) && !receiptAccepted {
		if result.State == attemptCompletionStateSuccess || result.State == "" {
			result.State = attemptCompletionStateIncompleteTurn
			result.ReasonCode = "provider_incomplete_turn"
			result.FailureText = firstNonEmpty(strings.TrimSpace(result.FailureText), "provider exited without an accepted kingdom_complete_attempt receipt")
			result.OutputText = firstNonEmpty(strings.TrimSpace(result.OutputText), canonicalCombinedText)
			result.Tokens = accumulatedTokens
			result.ProviderSessionID = actualSessionID
			result.ProviderInvocationID = strings.TrimSpace(req.InvocationID)
			result.ProviderInvocationAt = strings.TrimSpace(req.InvocationIssuedAt)
		}
	}

	if result.State != attemptCompletionStateSuccess {
		reason := firstNonEmpty(result.ReasonCode, execDiag.LastStepFinishReasonKind, execDiag.LastStepFinishReason)
		if result.State == attemptCompletionStateIncompleteTurn {
			if receiptRequired {
				reason = firstNonEmpty(result.ReasonCode, reason)
			} else {
				reason = firstNonEmpty(execDiag.LastStepFinishReason, execDiag.LastStepFinishReasonKind, decision.ExportCheckReason, result.ReasonCode)
			}
		}
		errorText := firstNonEmpty(result.FailureText, stderrText, fmt.Sprintf("incomplete terminal state: %s", reason))
		payload, _ := json.Marshal(map[string]any{
			"error":                      errorText,
			"stream_diagnostics":         diagnostics.toMap(),
			"execution_path_diagnostics": execDiag.toMap(),
		})
		eventType := AttemptEventTypeFailed
		var returnErr error
		if result.State == attemptCompletionStateCancelled {
			eventType = AttemptEventTypeCancelled
			returnErr = context.Canceled
		} else if result.State == attemptCompletionStateIncompleteTurn {
			returnErr = fmt.Errorf("opencode run failed: %w", &IncompleteTerminalStateError{Reason: reason})
		} else {
			returnErr = fmt.Errorf("opencode run failed: %s", errorText)
		}
		if eventType == AttemptEventTypeCancelled {
			execDiag.CancelledEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: eventType, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
			return finish(executeResponseForCompletionResult(result)), returnErr
		}
		execDiag.FailedEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{AttemptID: req.AttemptID, EventType: eventType, PayloadJSON: string(payload), OccurredAt: time.Now().UTC().Format(time.RFC3339)})
		return finish(executeResponseForCompletionResult(result)), returnErr
	}

	combinedText := strings.TrimSpace(result.OutputText)
	execDiag.CompletionGatePassed = true
	payload, _ := json.Marshal(map[string]any{
		"session_id":                 actualSessionID,
		"output":                     combinedText,
		"stream_diagnostics":         diagnostics.toMap(),
		"execution_path_diagnostics": execDiag.toMap(),
	})
	execDiag.SucceededEventAppended = a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeSucceeded,
		PayloadJSON: string(payload),
		OccurredAt:  time.Now().UTC().Format(time.RFC3339),
	})

	return finish(executeResponseForCompletionResult(result)), nil
}

func buildProviderDiagnosticsJSON(req ExecuteRequest, diagnostics streamDiagnostics, execDiagnostics executionPathDiagnostics, stderr stderrCapture, exportChecks []ExportCheck) string {
	payload := map[string]any{
		"completion_receipt_mode_enabled": completionReceiptModeEnabled(req),
		"stream_diagnostics":              diagnostics.toMap(),
		"execution_path_diagnostics":      execDiagnostics.toMap(),
	}
	if stderr.lineCount > 0 {
		payload["stderr_line_count"] = stderr.lineCount
	}
	if strings.TrimSpace(stderr.lastLineAt) != "" {
		payload["stderr_last_line_at"] = strings.TrimSpace(stderr.lastLineAt)
	}
	if stderr.eofSeen {
		payload["stderr_eof_seen"] = true
	}
	if tail := tailString(string(stderr.raw), 4096); tail != "" {
		payload["stderr_tail"] = tail
	}
	if strings.TrimSpace(stderr.scannerError) != "" {
		payload["stderr_scan_error"] = strings.TrimSpace(stderr.scannerError)
	}
	if checks := providerDiagnosticsExportChecks(exportChecks); len(checks) > 0 {
		payload["export_checks"] = checks
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func providerDiagnosticsExportChecks(checks []ExportCheck) []map[string]any {
	if len(checks) == 0 {
		return nil
	}
	result := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		result = append(result, map[string]any{
			"provider_session_id":           strings.TrimSpace(check.ProviderSessionID),
			"provider_invocation_id":        strings.TrimSpace(check.ProviderInvocationID),
			"phase":                         strings.TrimSpace(string(check.Phase)),
			"export_mode":                   strings.TrimSpace(check.ExportMode),
			"status":                        strings.TrimSpace(string(check.Status)),
			"salvage_used":                  check.SalvageUsed,
			"assistant_found":               check.AssistantFound,
			"assistant_owned_by_invocation": check.AssistantOwnedByInvocation,
			"assistant_completed_at":        strings.TrimSpace(check.AssistantCompletedAt),
			"assistant_finish_reason":       strings.TrimSpace(check.AssistantFinishReason),
			"assistant_has_error":           check.AssistantHasError,
			"stdout_bytes":                  check.StdoutBytes,
			"stderr_bytes":                  check.StderrBytes,
			"diagnostic_text":               strings.TrimSpace(check.DiagnosticText),
			"stdout_capture":                strings.TrimSpace(check.StdoutCapture),
			"stderr_capture":                strings.TrimSpace(check.StderrCapture),
			"started_at":                    strings.TrimSpace(check.StartedAt),
			"finished_at":                   strings.TrimSpace(check.FinishedAt),
			"duration_millis":               check.DurationMillis,
		})
	}
	return result
}

func tailString(raw string, maxBytes int) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || maxBytes <= 0 || len(trimmed) <= maxBytes {
		return trimmed
	}
	return trimmed[len(trimmed)-maxBytes:]
}

func emitExecuteLifecycleObservation(req ExecuteRequest, observation ExecuteLifecycleObservation) {
	if req.OnLifecycle == nil {
		return
	}
	phase := strings.TrimSpace(string(observation.Phase))
	if phase == "" {
		return
	}
	if strings.TrimSpace(observation.OccurredAt) == "" {
		observation.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	req.OnLifecycle(observation)
}

func (a *SessionAdapter) Cancel(_ context.Context, req CancelRequest) error {
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("attempt id is required")
	}
	if strings.TrimSpace(req.OccurredAt) == "" {
		return fmt.Errorf("occurred_at is required")
	}

	a.mu.Lock()
	_, knownAttempt := a.attempts[req.AttemptID]
	cmd := a.inFlight[req.AttemptID]
	cancel := a.attemptCancels[req.AttemptID]
	a.mu.Unlock()

	if !knownAttempt {
		return &AttemptNotFoundError{AttemptID: req.AttemptID}
	}

	if cancel != nil {
		cancel()
	}

	if cmd != nil && cmd.Process != nil {
		if err := terminateCommandProcessTree(cmd); err != nil {
			return fmt.Errorf("kill opencode process: %w", err)
		}
	}

	payload, _ := json.Marshal(map[string]any{"reason": req.Reason})
	a.appendTerminalEventOnce(req.AttemptID, AttemptStreamEvent{
		AttemptID:   req.AttemptID,
		EventType:   AttemptEventTypeCancelled,
		PayloadJSON: string(payload),
		OccurredAt:  req.OccurredAt,
	})

	return nil
}

func (a *SessionAdapter) executeContinuationTurn(ctx context.Context, req ExecuteRequest, sessionID, invocationID string) (continuationTurnResult, error) {
	startedRun, err := a.startOpencodeRun(ctx, req, sessionID, promptWithProviderInvocationMetadata("Continue from the pending tool-call state and finish the response.", req.AttemptID, invocationID))
	if err != nil {
		return continuationTurnResult{}, err
	}
	cmd := startedRun.cmd
	stdout := startedRun.stdout
	stderr := startedRun.stderr
	a.setInFlight(req.AttemptID, cmd)
	defer terminateCommandProcessTree(cmd)
	defer a.clearInFlight(req.AttemptID)

	ctxCancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = terminateCommandProcessTree(cmd)
		case <-ctxCancelDone:
		}
	}()
	defer close(ctxCancelDone)

	watchdog := startCommandWatchdog(cmd, a.firstEventTimeout, a.streamInactivityTimeout)
	defer watchdog.Stop()

	result := continuationTurnResult{
		TextChunks:        make([]string, 0),
		LastReason:        buildStepFinishReasonEvidence("", false),
		ProviderSessionID: strings.TrimSpace(sessionID),
	}
	providerSessionObserved := false

	if err := decodeOpencodeRunEventStream(stdout, func(event opencodeRunEvent) error {
		watchdog.Signal()
		if strings.TrimSpace(event.SessionID) != "" {
			result.ProviderSessionID = a.observeProviderSession(req, result.ProviderSessionID, event.SessionID, &providerSessionObserved)
		}
		if strings.TrimSpace(event.ErrorText) != "" && strings.TrimSpace(event.Type) == "error" {
			result.UpstreamErrorText = strings.TrimSpace(event.ErrorText)
		}
		if event.Type == "step_finish" {
			result.LastReason = buildStepFinishReasonEvidence(event.Part.Reason, event.Part.ReasonPresent)
			if event.Part.Tokens != nil {
				result.Tokens.InputTokens += event.Part.Tokens.Input
				result.Tokens.OutputTokens += event.Part.Tokens.Output
				result.Tokens.TotalTokens += event.Part.Tokens.Total
				result.Tokens.ReasoningTokens += event.Part.Tokens.Reasoning
				result.Tokens.CacheReadTokens += event.Part.Tokens.Cache.Read
				result.Tokens.CacheWriteTokens += event.Part.Tokens.Cache.Write
			}
		}
		if event.Type == "text" && strings.TrimSpace(event.Part.Text) != "" {
			result.TextChunks = append(result.TextChunks, event.Part.Text)
			sum := sha256.Sum256([]byte(event.Part.Text))
			payload, _ := json.Marshal(map[string]any{
				"text": event.Part.Text,
				"execution_path_diagnostics_snapshot": map[string]any{
					"last_progress_text_len": len(event.Part.Text),
					"last_progress_text_sha": fmt.Sprintf("%x", sum[:]),
				},
			})
			a.appendEvent(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeProgress,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}
		return nil
	}, nil); err != nil {
		if timeoutErr := watchdog.Err(); timeoutErr != nil {
			return result, decorateTimeoutError(timeoutErr, streamDiagnostics{})
		}
		return result, fmt.Errorf("read opencode continuation stream: %w", err)
	}

	stderrRaw, _ := io.ReadAll(stderr)
	if waitErr := cmd.Wait(); waitErr != nil {
		if timeoutErr := watchdog.Err(); timeoutErr != nil {
			return result, decorateTimeoutError(timeoutErr, streamDiagnostics{})
		}
		if isExecutionCancelled(waitErr) || isExecutionCancelled(ctx.Err()) {
			return result, context.Canceled
		}
		if isExecutionTimedOut(waitErr) || isExecutionTimedOut(ctx.Err()) {
			return result, context.DeadlineExceeded
		}
		trimmed := strings.TrimSpace(string(stderrRaw))
		if trimmed == "" {
			trimmed = waitErr.Error()
		}
		return result, fmt.Errorf("opencode continuation failed: %s", trimmed)
	}
	if timeoutErr := watchdog.Err(); timeoutErr != nil {
		return result, decorateTimeoutError(timeoutErr, streamDiagnostics{})
	}
	if strings.TrimSpace(result.UpstreamErrorText) != "" {
		return result, fmt.Errorf("opencode continuation failed: %s", result.UpstreamErrorText)
	}

	if !result.LastReason.Present {
		result.LastReason = buildStepFinishReasonEvidence("", false)
	}
	return result, nil
}

func (a *SessionAdapter) RecoverMissingCompletionReceipt(ctx context.Context, req ExecuteRequest, finalText string) (ExecuteResponse, error) {
	if err := validateExecuteRequest(req); err != nil {
		return ExecuteResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	expectedFinalText := strings.TrimSpace(finalText)
	if expectedFinalText == "" {
		result := attemptCompletionResult{
			State:                attemptCompletionStateIncompleteTurn,
			ReasonCode:           "provider_incomplete_turn",
			FailureText:          "completion recovery requires non-empty final_text",
			ProviderSessionID:    strings.TrimSpace(req.ProviderSessionID),
			ProviderInvocationID: strings.TrimSpace(req.InvocationID),
			ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
			EvidenceSource:       completionEvidenceSourceStream,
		}
		return executeResponseForCompletionResult(result), executeErrorForCompletionResult(result)
	}

	actualSessionID := firstNonEmpty(strings.TrimSpace(req.ProviderSessionID), a.actualSessionID(req.SessionID))
	if actualSessionID == "" {
		result := attemptCompletionResult{
			State:                attemptCompletionStateIncompleteTurn,
			ReasonCode:           "provider_incomplete_turn",
			FailureText:          "completion recovery requires a provider session id",
			OutputText:           expectedFinalText,
			ProviderInvocationID: strings.TrimSpace(req.InvocationID),
			ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
			EvidenceSource:       completionEvidenceSourceStream,
		}
		return executeResponseForCompletionResult(result), executeErrorForCompletionResult(result)
	}

	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	a.setAttemptCancel(req.AttemptID, cancelAttempt)
	defer a.clearAttemptCancel(req.AttemptID)

	wrappedReq := req
	wrappedReq.ProviderSessionID = actualSessionID
	wrappedReq.OnCompletionReceipt = expectedFinalTextRecoveryHandler(expectedFinalText, req.OnCompletionReceipt)

	startedRun, err := a.startOpencodeRun(attemptCtx, wrappedReq, actualSessionID, promptWithProviderInvocationMetadata(missingCompletionReceiptRecoveryPrompt, req.AttemptID, req.InvocationID))
	if err != nil {
		return ExecuteResponse{}, err
	}
	cmd := startedRun.cmd
	stdout := startedRun.stdout
	stderr := startedRun.stderr
	a.setInFlight(req.AttemptID, cmd)
	defer terminateCommandProcessTree(cmd)
	defer a.clearInFlight(req.AttemptID)

	ctxCancelDone := make(chan struct{})
	go func() {
		select {
		case <-attemptCtx.Done():
			_ = terminateCommandProcessTree(cmd)
		case <-ctxCancelDone:
		}
	}()
	defer close(ctxCancelDone)

	watchdog := startCommandWatchdog(cmd, a.firstEventTimeout, a.streamInactivityTimeout)
	defer watchdog.Stop()

	result := continuationTurnResult{
		TextChunks:        make([]string, 0),
		LastReason:        buildStepFinishReasonEvidence("", false),
		ProviderSessionID: actualSessionID,
	}
	providerSessionObserved := false

	if err := decodeOpencodeRunEventStream(stdout, func(event opencodeRunEvent) error {
		watchdog.Signal()
		if strings.TrimSpace(event.SessionID) != "" {
			result.ProviderSessionID = a.observeProviderSession(wrappedReq, result.ProviderSessionID, event.SessionID, &providerSessionObserved)
		}
		if strings.TrimSpace(event.ErrorText) != "" && strings.TrimSpace(event.Type) == "error" {
			result.UpstreamErrorText = strings.TrimSpace(event.ErrorText)
		}
		if event.Type == "step_finish" {
			result.LastReason = buildStepFinishReasonEvidence(event.Part.Reason, event.Part.ReasonPresent)
			if event.Part.Tokens != nil {
				result.Tokens.InputTokens += event.Part.Tokens.Input
				result.Tokens.OutputTokens += event.Part.Tokens.Output
				result.Tokens.TotalTokens += event.Part.Tokens.Total
				result.Tokens.ReasoningTokens += event.Part.Tokens.Reasoning
				result.Tokens.CacheReadTokens += event.Part.Tokens.Cache.Read
				result.Tokens.CacheWriteTokens += event.Part.Tokens.Cache.Write
			}
		}
		if event.Type == "text" && strings.TrimSpace(event.Part.Text) != "" {
			result.TextChunks = append(result.TextChunks, event.Part.Text)
			sum := sha256.Sum256([]byte(event.Part.Text))
			payload, _ := json.Marshal(map[string]any{
				"text": event.Part.Text,
				"execution_path_diagnostics_snapshot": map[string]any{
					"last_progress_text_len": len(event.Part.Text),
					"last_progress_text_sha": fmt.Sprintf("%x", sum[:]),
				},
			})
			a.appendEvent(req.AttemptID, AttemptStreamEvent{
				AttemptID:   req.AttemptID,
				EventType:   AttemptEventTypeProgress,
				PayloadJSON: string(payload),
				OccurredAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}
		return nil
	}, nil); err != nil {
		if timeoutErr := watchdog.Err(); timeoutErr != nil {
			return executeResponseForCompletionResult(executionResultForTimeout(decorateTimeoutError(timeoutErr, streamDiagnostics{}), actualSessionID)), decorateTimeoutError(timeoutErr, streamDiagnostics{})
		}
		return executeResponseForObservedState(expectedFinalText, result.Tokens, result.ProviderSessionID), fmt.Errorf("read opencode completion recovery stream: %w", err)
	}

	stderrRaw, _ := io.ReadAll(stderr)
	if waitErr := cmd.Wait(); waitErr != nil {
		if timeoutErr := watchdog.Err(); timeoutErr != nil {
			decorated := decorateTimeoutError(timeoutErr, streamDiagnostics{})
			return executeResponseForCompletionResult(executionResultForTimeout(decorated, result.ProviderSessionID)), decorated
		}
		if isExecutionCancelled(waitErr) || isExecutionCancelled(attemptCtx.Err()) {
			cancelledResult := executionResultForCancellation(waitErr, result.ProviderSessionID, "opencode completion recovery cancelled")
			cancelledResult.OutputText = expectedFinalText
			cancelledResult.Tokens = result.Tokens
			return executeResponseForCompletionResult(cancelledResult), executeErrorForCompletionResult(cancelledResult)
		}
		if isExecutionTimedOut(waitErr) || isExecutionTimedOut(attemptCtx.Err()) {
			timeoutErr := firstNonNilError(waitErr, attemptCtx.Err())
			timeoutResult := executionResultForTimeout(timeoutErr, result.ProviderSessionID)
			timeoutResult.OutputText = expectedFinalText
			timeoutResult.Tokens = result.Tokens
			return executeResponseForCompletionResult(timeoutResult), executeErrorForCompletionResult(timeoutResult)
		}
		trimmed := strings.TrimSpace(string(stderrRaw))
		if trimmed == "" {
			trimmed = waitErr.Error()
		}
		failedResult := executionResultForProcessError(waitErr, result.ProviderSessionID, trimmed)
		failedResult.OutputText = expectedFinalText
		failedResult.Tokens = result.Tokens
		return executeResponseForCompletionResult(failedResult), executeErrorForCompletionResult(failedResult)
	}
	if timeoutErr := watchdog.Err(); timeoutErr != nil {
		decorated := decorateTimeoutError(timeoutErr, streamDiagnostics{})
		return executeResponseForCompletionResult(executionResultForTimeout(decorated, result.ProviderSessionID)), decorated
	}
	if strings.TrimSpace(result.UpstreamErrorText) != "" {
		failedResult := executionResultForProcessError(errors.New(result.UpstreamErrorText), result.ProviderSessionID, result.UpstreamErrorText)
		failedResult.OutputText = expectedFinalText
		failedResult.Tokens = result.Tokens
		return executeResponseForCompletionResult(failedResult), executeErrorForCompletionResult(failedResult)
	}

	if receiptResult, settled, _ := waitForCompletionReceiptSettlement(wrappedReq, expectedFinalText, result.Tokens, result.ProviderSessionID, completionReceiptObservationGracePeriod); settled {
		if strings.TrimSpace(receiptResult.OutputText) == "" {
			receiptResult.OutputText = expectedFinalText
		}
		return executeResponseForCompletionResult(receiptResult), executeErrorForCompletionResult(receiptResult)
	}

	incompleteResult := attemptCompletionResult{
		State:                attemptCompletionStateIncompleteTurn,
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider exited without an accepted kingdom_complete_attempt receipt after recovery",
		OutputText:           expectedFinalText,
		Tokens:               result.Tokens,
		ProviderSessionID:    result.ProviderSessionID,
		ProviderInvocationID: strings.TrimSpace(req.InvocationID),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
		EvidenceSource:       completionEvidenceSourceStream,
	}
	return executeResponseForCompletionResult(incompleteResult), executeErrorForCompletionResult(incompleteResult)
}

func expectedFinalTextRecoveryHandler(expectedFinalText string, next func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error)) func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
	trimmedExpected := strings.TrimSpace(expectedFinalText)
	return func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
		if strings.TrimSpace(receipt.FinalText) != trimmedExpected {
			return CompletionReceiptAcceptance{Rejected: true, RejectedReason: "completion recovery final_text mismatch"}, nil
		}
		if next == nil {
			return CompletionReceiptAcceptance{}, fmt.Errorf("completion receipt handler is required")
		}
		return next(receipt)
	}
}

func (a *SessionAdapter) Stream(ctx context.Context, req StreamRequest) (<-chan AttemptStreamEvent, <-chan error, error) {
	if strings.TrimSpace(req.AttemptID) == "" {
		return nil, nil, fmt.Errorf("attempt id is required")
	}

	a.mu.Lock()
	record, ok := a.attempts[req.AttemptID]
	if !ok {
		a.mu.Unlock()
		return nil, nil, &AttemptNotFoundError{AttemptID: req.AttemptID}
	}
	snapshot := make([]AttemptStreamEvent, len(record.Events))
	copy(snapshot, record.Events)
	a.mu.Unlock()

	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Seq < snapshot[j].Seq
	})

	eventCh := make(chan AttemptStreamEvent)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)

		for _, event := range snapshot {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case eventCh <- event:
			}
		}
	}()

	return eventCh, errCh, nil
}

func (a *SessionAdapter) InFlightSnapshot(attemptID string) map[string]any {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return map[string]any{"attempt_id": "", "attempt_known": false, "in_flight_present": false}
	}

	a.mu.Lock()
	record, attemptKnown := a.attempts[trimmedAttemptID]
	cmd := a.inFlight[trimmedAttemptID]
	a.mu.Unlock()

	snapshot := map[string]any{
		"attempt_id":        trimmedAttemptID,
		"attempt_known":     attemptKnown,
		"in_flight_present": cmd != nil,
	}
	if attemptKnown {
		snapshot["stream_event_count"] = len(record.Events)
	}
	if cmd == nil || cmd.Process == nil {
		return snapshot
	}

	pid := cmd.Process.Pid
	snapshot["pid"] = pid
	snapshot["process_alive"] = isProcessAlive(pid)
	if cmd.ProcessState != nil {
		snapshot["process_state_known"] = true
		snapshot["wait_returned"] = true
		snapshot["process_exited"] = cmd.ProcessState.Exited()
	} else {
		snapshot["process_state_known"] = false
		snapshot["wait_returned"] = false
	}

	return snapshot
}

func (a *SessionAdapter) setInFlight(attemptID string, cmd *exec.Cmd) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight[attemptID] = cmd
}

func (a *SessionAdapter) clearInFlight(attemptID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.inFlight, attemptID)
}

func (a *SessionAdapter) setAttemptCancel(attemptID string, cancel context.CancelFunc) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attemptCancels[attemptID] = cancel
}

func (a *SessionAdapter) clearAttemptCancel(attemptID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.attemptCancels, attemptID)
}

func (a *SessionAdapter) appendEvent(attemptID string, event AttemptStreamEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.attempts[attemptID]
	if !ok {
		record = &sessionAttemptRecord{}
		a.attempts[attemptID] = record
	}
	event.Seq = len(record.Events) + 1
	if strings.TrimSpace(event.OccurredAt) == "" {
		event.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	record.Events = append(record.Events, event)
}

func (a *SessionAdapter) appendTerminalEventOnce(attemptID string, event AttemptStreamEvent) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.attempts[attemptID]
	if !ok {
		record = &sessionAttemptRecord{}
		a.attempts[attemptID] = record
	}
	for _, existing := range record.Events {
		if isTerminalAttemptEvent(existing.EventType) {
			return false
		}
	}
	event.Seq = len(record.Events) + 1
	if strings.TrimSpace(event.OccurredAt) == "" {
		event.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	record.Events = append(record.Events, event)
	return true
}

func isTerminalAttemptEvent(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case AttemptEventTypeSucceeded, AttemptEventTypeFailed, AttemptEventTypeCancelled:
		return true
	default:
		return false
	}
}

func (a *SessionAdapter) snapshotEvents(attemptID string) ([]AttemptStreamEvent, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.attempts[attemptID]
	if !ok {
		return nil, false
	}
	snapshot := make([]AttemptStreamEvent, len(record.Events))
	copy(snapshot, record.Events)
	return snapshot, true
}

func (a *SessionAdapter) actualSessionID(logicalSessionID string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return strings.TrimSpace(a.logicalToActual[logicalSessionID])
}

func (a *SessionAdapter) bindActualSession(logicalSessionID, actualSessionID string) {
	logicalSessionID = strings.TrimSpace(logicalSessionID)
	actualSessionID = strings.TrimSpace(actualSessionID)
	if logicalSessionID == "" || actualSessionID == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.logicalToActual[logicalSessionID] = actualSessionID
}

func (a *SessionAdapter) observeProviderSession(req ExecuteRequest, currentSessionID, observedSessionID string, reported *bool) string {
	observedSessionID = strings.TrimSpace(observedSessionID)
	if observedSessionID == "" {
		return strings.TrimSpace(currentSessionID)
	}
	a.bindActualSession(req.SessionID, observedSessionID)
	shouldReport := strings.TrimSpace(currentSessionID) != observedSessionID
	if !shouldReport && req.OnProviderSession != nil && strings.TrimSpace(req.ProviderSessionID) == observedSessionID {
		shouldReport = reported == nil || !*reported
	}
	if shouldReport && req.OnProviderSession != nil {
		req.OnProviderSession(observedSessionID)
	}
	if reported != nil {
		*reported = true
	}
	return observedSessionID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func terminateCommandProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if pid <= 0 {
		return cmd.Process.Kill()
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
			return killErr
		}
	}
	return nil
}

type commandWatchdog struct {
	signal chan struct{}
	stop   chan struct{}
	done   chan struct{}

	mu  sync.Mutex
	err error
}

func startCommandWatchdog(cmd *exec.Cmd, firstTimeout, inactivityTimeout time.Duration) *commandWatchdog {
	wd := &commandWatchdog{
		signal: make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}

	go func() {
		defer close(wd.done)

		if firstTimeout <= 0 && inactivityTimeout <= 0 {
			return
		}

		firstEventSeen := false
		var firstTimer *time.Timer
		var firstTimerC <-chan time.Time
		if firstTimeout > 0 {
			firstTimer = time.NewTimer(firstTimeout)
			firstTimerC = firstTimer.C
		}

		var inactivityTimer *time.Timer
		var inactivityTimerC <-chan time.Time
		resetInactivity := func() {
			if inactivityTimeout <= 0 {
				return
			}
			if inactivityTimer == nil {
				inactivityTimer = time.NewTimer(inactivityTimeout)
				inactivityTimerC = inactivityTimer.C
				return
			}
			if !inactivityTimer.Stop() {
				select {
				case <-inactivityTimer.C:
				default:
				}
			}
			inactivityTimer.Reset(inactivityTimeout)
		}

		stopTimer := func(timer *time.Timer) {
			if timer == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		defer stopTimer(firstTimer)
		defer stopTimer(inactivityTimer)

		for {
			select {
			case <-wd.stop:
				return
			case <-wd.signal:
				if !firstEventSeen {
					firstEventSeen = true
					if firstTimer != nil {
						stopTimer(firstTimer)
						firstTimer = nil
						firstTimerC = nil
					}
				}
				resetInactivity()
			case <-firstTimerC:
				wd.setErr(newProviderTimeoutError("first_event_timeout", firstTimeout))
				_ = terminateCommandProcessTree(cmd)
				return
			case <-inactivityTimerC:
				wd.setErr(newProviderTimeoutError("stream_inactivity_timeout", inactivityTimeout))
				_ = terminateCommandProcessTree(cmd)
				return
			}
		}
	}()

	return wd
}

func (w *commandWatchdog) setErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *commandWatchdog) Signal() {
	if w == nil {
		return
	}
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

func (w *commandWatchdog) Stop() {
	if w == nil {
		return
	}
	close(w.stop)
	<-w.done
}

func (w *commandWatchdog) Err() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, syscall.Signal(0))
	if err == nil {
		return true
	}
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	return true
}

type stepFinishTerminalClass string

const (
	stepFinishNonTerminal              stepFinishTerminalClass = "non_terminal"
	stepFinishTerminalSuccessCandidate stepFinishTerminalClass = "terminal_success_candidate"
	stepFinishTerminalFailure          stepFinishTerminalClass = "terminal_failure"
	stepFinishAmbiguous                stepFinishTerminalClass = "ambiguous"
)

func normalizeStepFinishReason(reason string) string {
	trimmed := strings.ToLower(strings.TrimSpace(reason))
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}

func classifyStepFinishReason(reason string) stepFinishTerminalClass {
	switch normalizeStepFinishReason(reason) {
	case "tool-calls", "unknown":
		return stepFinishNonTerminal
	case "error":
		return stepFinishTerminalFailure
	case "stop", "length", "content-filter", "other":
		return stepFinishTerminalSuccessCandidate
	default:
		return stepFinishAmbiguous
	}
}

func firstNonNilSessionExporter(exporter SessionExporter, fallback SessionExporter) SessionExporter {
	if exporter != nil {
		return exporter
	}
	return fallback
}

func newProviderTimeoutError(timeoutType string, timeout time.Duration) error {
	return &providerTimeoutError{
		TimeoutType: strings.TrimSpace(timeoutType),
		Timeout:     timeout,
	}
}

func resolveProviderInvocationID(req ExecuteRequest) string {
	if trimmed := strings.TrimSpace(req.InvocationID); trimmed != "" {
		return trimmed
	}
	return fmt.Sprintf("%s:%d", sanitizeProviderInvocationToken(req.AttemptID), time.Now().UTC().UnixNano())
}

func sanitizeProviderInvocationToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "invocation"
	}
	replacer := strings.NewReplacer(" ", "_", "\t", "_", "\n", "_", "\r", "_")
	return replacer.Replace(trimmed)
}

func buildProviderInvocationPromptMarker(invocationID string) string {
	trimmed := strings.TrimSpace(invocationID)
	if trimmed == "" {
		return ""
	}
	return providerInvocationIDLabel + " " + trimmed
}

func promptWithProviderInvocationMetadata(prompt, attemptID, invocationID string) string {
	trimmedPrompt := strings.TrimRight(prompt, "\n")
	marker := buildProviderInvocationPromptMarker(invocationID)
	if marker == "" {
		return trimmedPrompt
	}
	metadataBlock := strings.Join([]string{
		providerInvocationMetadataHeader,
		providerInvocationAttemptLabel + " " + strings.TrimSpace(attemptID),
		marker,
	}, "\n")
	if trimmedPrompt == "" {
		return metadataBlock
	}
	return trimmedPrompt + "\n\n" + metadataBlock
}

func isExecutionCancelled(err error) bool {
	return err != nil && errors.Is(err, context.Canceled)
}

func isExecutionTimedOut(err error) bool {
	return err != nil && errors.Is(err, context.DeadlineExceeded)
}

func firstNonNilError(errorsToCheck ...error) error {
	for _, err := range errorsToCheck {
		if err != nil {
			return err
		}
	}
	return nil
}

func buildTimeoutFailurePayload(timeoutErr error, diagnostics streamDiagnostics, execDiagnostics executionPathDiagnostics) map[string]any {
	payload := map[string]any{
		"error":                      timeoutErr.Error(),
		"stream_diagnostics":         diagnostics.toMap(),
		"execution_path_diagnostics": execDiagnostics.toMap(),
	}
	var typed *providerTimeoutError
	if errors.As(timeoutErr, &typed) {
		payload["timeout_type"] = strings.TrimSpace(typed.TimeoutType)
		payload["timeout_ms"] = typed.Timeout.Milliseconds()
	}
	return payload
}

func (d executionPathDiagnostics) toMap() map[string]any {
	payload := map[string]any{
		"attempt_id":                   strings.TrimSpace(d.AttemptID),
		"session_id":                   strings.TrimSpace(d.SessionID),
		"cmd_started_at":               strings.TrimSpace(d.CmdStartedAt),
		"cmd_wait_started_at":          strings.TrimSpace(d.CmdWaitStartedAt),
		"cmd_wait_finished_at":         strings.TrimSpace(d.CmdWaitFinishedAt),
		"wait_returned":                d.WaitReturned,
		"step_finish_seen":             d.StepFinishSeen,
		"last_step_finish_reason":      strings.TrimSpace(d.LastStepFinishReason),
		"last_step_finish_reason_kind": strings.TrimSpace(d.LastStepFinishReasonKind),
		"terminal_state_class":         strings.TrimSpace(d.TerminalStateClass),
		"upstream_error_text":          strings.TrimSpace(d.UpstreamErrorText),
		"completion_gate_passed":       d.CompletionGatePassed,
		"stdout_eof_seen":              d.StdoutEOFSeen,
		"stderr_eof_seen":              d.StderrEOFSeen,
		"stdout_event_count":           d.StdoutEventCount,
		"stdout_malformed_line_count":  d.StdoutMalformedLineCount,
		"stderr_line_count":            d.StderrLineCount,
		"last_stdout_event_at":         strings.TrimSpace(d.LastStdoutEventAt),
		"last_stderr_line_at":          strings.TrimSpace(d.LastStderrLineAt),
		"last_progress_text_len":       d.LastProgressTextLen,
		"last_progress_text_sha":       strings.TrimSpace(d.LastProgressTextSHA),
		"export_check_policy":          strings.TrimSpace(d.ExportCheckPolicy),
		"export_check_reason":          strings.TrimSpace(d.ExportCheckReason),
		"export_check_attempted":       d.ExportCheckAttempted,
		"export_check_status":          strings.TrimSpace(d.ExportCheckStatus),
		"export_check_evidence_source": strings.TrimSpace(d.ExportCheckEvidenceSource),
		"export_check_attempts":        d.ExportCheckAttempts,
		"export_check_elapsed_ms":      d.ExportCheckElapsedMs,
		"continuation_turns_elapsed":   d.ContinuationTurnsElapsed,
		"continuation_turns_max":       d.ContinuationTurnsMax,
		"succeeded_event_appended":     d.SucceededEventAppended,
		"failed_event_appended":        d.FailedEventAppended,
		"cancelled_event_appended":     d.CancelledEventAppended,
	}
	if strings.TrimSpace(d.ExportCheckLastError) != "" {
		payload["export_check_last_error"] = strings.TrimSpace(d.ExportCheckLastError)
	}
	if strings.TrimSpace(d.FirstStdoutMalformedAt) != "" {
		payload["first_stdout_malformed_at"] = strings.TrimSpace(d.FirstStdoutMalformedAt)
	}
	if strings.TrimSpace(d.LastStdoutMalformedAt) != "" {
		payload["last_stdout_malformed_at"] = strings.TrimSpace(d.LastStdoutMalformedAt)
	}
	if strings.TrimSpace(d.FirstStdoutMalformedSHA) != "" {
		payload["first_stdout_malformed_sha"] = strings.TrimSpace(d.FirstStdoutMalformedSHA)
	}
	if strings.TrimSpace(d.LastStdoutMalformedSHA) != "" {
		payload["last_stdout_malformed_sha"] = strings.TrimSpace(d.LastStdoutMalformedSHA)
	}
	if d.ExitCode != nil {
		payload["exit_code"] = *d.ExitCode
	}
	if d.ExitSignal != nil {
		payload["exit_signal"] = *d.ExitSignal
	}
	if strings.TrimSpace(d.WaitError) != "" {
		payload["wait_error"] = strings.TrimSpace(d.WaitError)
	}
	return payload
}

func (d executionPathDiagnostics) progressSnapshot() map[string]any {
	payload := map[string]any{
		"step_finish_seen":             d.StepFinishSeen,
		"last_step_finish_reason":      strings.TrimSpace(d.LastStepFinishReason),
		"last_step_finish_reason_kind": strings.TrimSpace(d.LastStepFinishReasonKind),
		"terminal_state_class":         strings.TrimSpace(d.TerminalStateClass),
		"completion_gate_passed":       d.CompletionGatePassed,
		"stdout_event_count":           d.StdoutEventCount,
		"stdout_malformed_line_count":  d.StdoutMalformedLineCount,
		"last_stdout_event_at":         strings.TrimSpace(d.LastStdoutEventAt),
		"last_progress_text_len":       d.LastProgressTextLen,
		"last_progress_text_sha":       strings.TrimSpace(d.LastProgressTextSHA),
		"export_check_attempted":       d.ExportCheckAttempted,
		"export_check_status":          strings.TrimSpace(d.ExportCheckStatus),
		"export_check_attempts":        d.ExportCheckAttempts,
		"export_check_elapsed_ms":      d.ExportCheckElapsedMs,
		"cmd_started_at":               strings.TrimSpace(d.CmdStartedAt),
		"wait_returned":                d.WaitReturned,
		"continuation_turns_elapsed":   d.ContinuationTurnsElapsed,
		"continuation_turns_max":       d.ContinuationTurnsMax,
		"succeeded_event_appended":     d.SucceededEventAppended,
		"failed_event_appended":        d.FailedEventAppended,
	}
	return payload
}

func recordMalformedStdoutLine(d *executionPathDiagnostics, line string) {
	if d == nil {
		return
	}
	d.StdoutMalformedLineCount++
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(d.FirstStdoutMalformedAt) == "" {
		d.FirstStdoutMalformedAt = now
	}
	d.LastStdoutMalformedAt = now
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		trimmed = "<blank>"
	}
	sum := sha256.Sum256([]byte(trimmed))
	hash := fmt.Sprintf("%x", sum[:])
	if strings.TrimSpace(d.FirstStdoutMalformedSHA) == "" {
		d.FirstStdoutMalformedSHA = hash
	}
	d.LastStdoutMalformedSHA = hash
}

func decorateTimeoutError(timeoutErr error, diagnostics streamDiagnostics) error {
	if timeoutErr == nil {
		return nil
	}
	return fmt.Errorf("%w (first_event_latency_ms=%d max_inter_event_gap_ms=%d event_count=%d)",
		timeoutErr,
		diagnostics.FirstEventLatency.Milliseconds(),
		diagnostics.MaxInterEventGap.Milliseconds(),
		diagnostics.EventCount,
	)
}

func (d streamDiagnostics) toMap() map[string]any {
	payload := map[string]any{
		"first_event_latency_ms": d.FirstEventLatency.Milliseconds(),
		"max_inter_event_gap_ms": d.MaxInterEventGap.Milliseconds(),
		"event_count":            d.EventCount,
	}
	if strings.TrimSpace(d.ProcessStartedAt) != "" {
		payload["process_started_at"] = strings.TrimSpace(d.ProcessStartedAt)
	}
	if strings.TrimSpace(d.FirstEventAt) != "" {
		payload["first_event_at"] = strings.TrimSpace(d.FirstEventAt)
	}
	if strings.TrimSpace(d.LastEventAt) != "" {
		payload["last_event_at"] = strings.TrimSpace(d.LastEventAt)
	}
	if strings.TrimSpace(d.ProcessExitedAt) != "" {
		payload["process_exited_at"] = strings.TrimSpace(d.ProcessExitedAt)
	}
	return payload
}
