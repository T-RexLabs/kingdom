package opencode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeSessionExporter struct {
	providerSessionIDs []string
	workDirs           []string
	configDirs         []string
	databasePaths      []string
	exportModes        []sessionExportMode
	results            []sessionExportResult
	sanitizedResults   []sessionExportResult
	errs               []error
	sanitizedErrs      []error
	err                error
	sanitizedErr       error
	exportFn           func(context.Context, sessionExportRequest) (sessionExportResult, error)
	index              int
	sanitizedIndex     int
}

func (f *fakeSessionExporter) Export(ctx context.Context, req sessionExportRequest) (sessionExportResult, error) {
	f.providerSessionIDs = append(f.providerSessionIDs, req.ProviderSessionID)
	f.workDirs = append(f.workDirs, req.WorkDir)
	f.configDirs = append(f.configDirs, req.ConfigDir)
	f.databasePaths = append(f.databasePaths, req.DatabasePath)
	f.exportModes = append(f.exportModes, req.Mode)
	if f.exportFn != nil {
		return f.exportFn(ctx, req)
	}
	if req.Mode == sessionExportModeSanitized {
		idx := f.sanitizedIndex
		if len(f.sanitizedErrs) > 0 {
			if idx >= len(f.sanitizedErrs) {
				idx = len(f.sanitizedErrs) - 1
			}
			if f.sanitizedErrs[idx] != nil {
				f.sanitizedIndex++
				return sessionExportResult{}, f.sanitizedErrs[idx]
			}
		}
		if f.sanitizedErr != nil {
			f.sanitizedIndex++
			return sessionExportResult{}, f.sanitizedErr
		}
		if len(f.sanitizedResults) > 0 {
			if idx >= len(f.sanitizedResults) {
				idx = len(f.sanitizedResults) - 1
			}
			f.sanitizedIndex++
			return f.sanitizedResults[idx], nil
		}
	}
	idx := f.index
	if len(f.errs) > 0 {
		if idx >= len(f.errs) {
			idx = len(f.errs) - 1
		}
		if f.errs[idx] != nil {
			f.index++
			return sessionExportResult{}, f.errs[idx]
		}
	}
	if f.err != nil {
		f.index++
		return sessionExportResult{}, f.err
	}
	if len(f.results) == 0 {
		f.index++
		return sessionExportResult{}, fmt.Errorf("no export fixture configured")
	}
	if idx >= len(f.results) {
		idx = len(f.results) - 1
	}
	f.index++
	return f.results[idx], nil
}

func exportResultFixture(stdout string) sessionExportResult {
	return sessionExportResult{Stdout: []byte(stdout)}
}

func exportResultFixtureWithStderr(stdout, stderr string) sessionExportResult {
	return sessionExportResult{Stdout: []byte(stdout), Stderr: stderr}
}

func singlePassReconcilerConfig(exporter SessionExporter) sessionCompletionReconcilerConfig {
	return sessionCompletionReconcilerConfig{
		Exporter:     exporter,
		TotalTimeout: time.Nanosecond,
	}
}

func shortRetryBudgetReconcilerConfig(exporter SessionExporter) sessionCompletionReconcilerConfig {
	return sessionCompletionReconcilerConfig{
		Exporter:        exporter,
		TotalTimeout:    20 * time.Millisecond,
		BackoffSchedule: []time.Duration{1 * time.Millisecond},
	}
}

func TestSessionCompletionReconcilerUsesProviderSessionIDForExport(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("Recovered from export"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_real_123",
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved {
		t.Fatal("expected reconciliation to resolve")
	}
	if len(exporter.providerSessionIDs) != 1 || exporter.providerSessionIDs[0] != "ses_real_123" {
		t.Fatalf("expected export to use provider session id, got %#v", exporter.providerSessionIDs)
	}
	if len(exporter.configDirs) != 1 || exporter.configDirs[0] != "" {
		t.Fatalf("expected empty config dir by default, got %#v", exporter.configDirs)
	}
	if len(exporter.databasePaths) != 1 || exporter.databasePaths[0] != "" {
		t.Fatalf("expected empty database path by default, got %#v", exporter.databasePaths)
	}
	if outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected success result, got %q", outcome.Result.State)
	}
	if outcome.Result.OutputText != "Recovered from export" {
		t.Fatalf("expected recovered output text, got %q", outcome.Result.OutputText)
	}
	if !outcome.Result.SuppressEmptyOutputRetry {
		t.Fatal("expected reconciled success to suppress empty output retry")
	}
}

func TestSessionCompletionReconcilerPassesManagedPathsToExporter(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("Recovered from export"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))
	workDir := filepath.Join(t.TempDir(), "workspace")
	configDir := filepath.Join(workDir, ".kingdom", "opencode")
	databasePath := filepath.Join(configDir, "opencode.db")

	_, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_real_123",
		WorkDir:           workDir,
		ConfigDir:         configDir,
		DatabasePath:      databasePath,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(exporter.workDirs) != 1 || exporter.workDirs[0] != workDir {
		t.Fatalf("expected export workdir %q, got %#v", workDir, exporter.workDirs)
	}
	if len(exporter.configDirs) != 1 || exporter.configDirs[0] != configDir {
		t.Fatalf("expected export config dir %q, got %#v", configDir, exporter.configDirs)
	}
	if len(exporter.databasePaths) != 1 || exporter.databasePaths[0] != databasePath {
		t.Fatalf("expected export database path %q, got %#v", databasePath, exporter.databasePaths)
	}
}

func TestCLISessionExporterSetsManagedOpenCodeEnvironment(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	t.Setenv("FAKE_OPENCODE_EXPORT_JSON", successExportFixture("Recovered from export"))

	exporter := cliSessionExporter{binaryPath: binaryPath}
	workDir := t.TempDir()
	configDir := filepath.Join(t.TempDir(), ".kingdom", "opencode")
	databasePath := filepath.Join(configDir, "opencode.db")
	_, err := exporter.Export(context.Background(), sessionExportRequest{
		ProviderSessionID: "ses_export_env",
		WorkDir:           workDir,
		ConfigDir:         configDir,
		DatabasePath:      databasePath,
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(raw))
	if envLog["OPENCODE_CONFIG_DIR"] != configDir {
		t.Fatalf("expected OPENCODE_CONFIG_DIR %q, got %q", configDir, envLog["OPENCODE_CONFIG_DIR"])
	}
	if envLog["OPENCODE_DB"] != databasePath {
		t.Fatalf("expected OPENCODE_DB %q, got %q", databasePath, envLog["OPENCODE_DB"])
	}
}

func TestSessionCompletionReconcilerParsesStdoutWhenExportWritesBannerToStderr(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixtureWithStderr(successExportFixture("Recovered despite stderr banner"), "Exporting session: ses_real_123")}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_real_123",
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "Recovered despite stderr banner" {
		t.Fatalf("expected recovered output text, got %q", outcome.Result.OutputText)
	}
}

func TestSessionCompletionReconcilerReconcilesCleanEOFToSuccess(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("Recovered response"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_success",
		StreamOutputText:  "",
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "Recovered response" {
		t.Fatalf("expected export output recovery, got %q", outcome.Result.OutputText)
	}
}

func TestSessionCompletionReconcilerReceiptRequiredRecoversOutputWithoutSettlingSuccess(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("Recovered response"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_success",
		ReceiptRequired:   true,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved {
		t.Fatalf("expected resolved diagnostic recovery, got status=%q", outcome.Status)
	}
	if outcome.Result.State != attemptCompletionStateIncompleteTurn || outcome.Result.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected diagnostic-only incomplete outcome, got %#v", outcome.Result)
	}
	if outcome.Result.OutputText != "Recovered response" {
		t.Fatalf("expected export output recovery, got %q", outcome.Result.OutputText)
	}
	if outcome.Result.EvidenceSource != completionEvidenceSourceExport {
		t.Fatalf("expected export evidence source for recovered diagnostics, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerDoesNotResolveFromAssistantTurnBeforeInvocationStart(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 3, 2, 10, 22, 1, 0, time.UTC)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixtureAt("stale retry output", completedAt.UnixMilli()))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_stale_retry",
		InvocationStartedAt: completedAt.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected stale assistant turn to remain unresolved, got status=%q", outcome.Status)
	}
	if outcome.Result.State != "" {
		t.Fatalf("expected no resolved state from stale assistant turn, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerDoesNotResolveLatePersistedAssistantFromPriorInvocation(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 3, 2, 10, 23, 30, 0, time.UTC)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationOwnershipExportFixture("invocation_old", completedAt.UnixMilli(), "stale prior output", "invocation_current", ""))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_stale_late_persist",
		InvocationID:        "invocation_current",
		InvocationStartedAt: completedAt.Add(-15 * time.Second),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected unresolved reconciliation for late persisted prior invocation, got status=%q", outcome.Status)
	}
	if outcome.Result.State != "" {
		t.Fatalf("expected no resolved state from prior invocation output, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerResolvesAssistantAfterMatchingInvocationAnchor(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationOwnershipExportFixture("invocation_old", completedAt.Add(-time.Minute).UnixMilli(), "older output", "invocation_current", "current output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_current_invocation",
		InvocationID:        "invocation_current",
		InvocationStartedAt: completedAt.Add(-15 * time.Second),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success for matching invocation anchor, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "current output" {
		t.Fatalf("expected output from current invocation, got %q", outcome.Result.OutputText)
	}
}

func TestSessionCompletionReconcilerResolvesMarkerOwnedAssistantDespiteEarlierCompletedTimestamp(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationOwnershipExportFixtureWithCurrentCompletedAt("invocation_old", completedAt.Add(-time.Minute).UnixMilli(), "older output", "invocation_current", "current output", completedAt.Format(time.RFC3339)))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_current_invocation_coarse",
		InvocationID:        "invocation_current",
		InvocationStartedAt: completedAt.Add(400 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success for marker-owned invocation despite earlier completed timestamp, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "current output" {
		t.Fatalf("expected output from current invocation, got %q", outcome.Result.OutputText)
	}
	if outcome.Result.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed reason, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerResolvesQuotedInvocationMetadataWrapper(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(quotedInvocationOwnershipExportFixture("invocation_current", "current output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_quoted_wrapper",
		AttemptID:           "attempt_current",
		InvocationID:        "invocation_current",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success for quoted metadata wrapper, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "current output" {
		t.Fatalf("expected output from quoted-wrapper invocation, got %q", outcome.Result.OutputText)
	}
}

func TestSessionCompletionReconcilerResolvesSplitInvocationMarkerAcrossTextParts(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(splitInvocationMarkerExportFixture("invocation_current", "current output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_split_marker",
		AttemptID:           "attempt_current",
		InvocationID:        "invocation_current",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success for split invocation marker, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
}

func TestSessionCompletionReconcilerKeepsInvocationBoundaryAcrossWrapperUserMessage(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationBoundaryWithWrapperUserFixture("invocation_current", "current output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_wrapper_user",
		AttemptID:           "attempt_current",
		InvocationID:        "invocation_current",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected wrapper user message to preserve invocation boundary, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
}

func TestSessionCompletionReconcilerUsesLatestBoundaryWhenLaterPromptQuotesPriorMetadata(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(quotedPriorBoundaryThenLaterBoundaryExportFixture("invocation_target", "invocation_later", "later output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_later_boundary",
		AttemptID:           "attempt_target",
		InvocationID:        "invocation_target",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected unresolved reconciliation once a later boundary supersedes quoted prior metadata, got status=%q", outcome.Status)
	}
	if outcome.Result.State != "" {
		t.Fatalf("expected no resolved state from superseded boundary, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerDoesNotResolveAssistantFromLaterInvocationWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationWindowBleedExportFixture("invocation_target", "", "invocation_later", "later invocation output", "stop"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_later_invocation_output",
		InvocationID:        "invocation_target",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected unresolved reconciliation for later invocation output, got status=%q", outcome.Status)
	}
	if outcome.Result.State != "" {
		t.Fatalf("expected no resolved state from later invocation output, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerDoesNotResolveFailureFromLaterInvocationWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationWindowBleedExportFixture("invocation_target", "", "invocation_later", "later invocation failed", "error"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_later_invocation_failure",
		InvocationID:        "invocation_target",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected unresolved reconciliation for later invocation failure, got status=%q", outcome.Status)
	}
	if outcome.Result.State != "" {
		t.Fatalf("expected no resolved state from later invocation failure, got %#v", outcome.Result)
	}
}

func TestSessionCompletionReconcilerResolvesAssistantAfterContinuationPromptInSameInvocation(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(continuedInvocationOwnershipExportFixture("invocation_current", "tool work pending", "continued current output"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_current_invocation_continued",
		InvocationID:        "invocation_current",
		InvocationStartedAt: time.Date(2026, 3, 2, 10, 24, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success for same-invocation continuation, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "continued current output" {
		t.Fatalf("expected continuation output from current invocation, got %q", outcome.Result.OutputText)
	}
}

func TestSessionCompletionReconcilerReconcilesCleanEOFToFailure(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(failureExportFixture())}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_failure"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateFailed {
		t.Fatalf("expected resolved failure, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.FailureText == "" {
		t.Fatal("expected failure text from export")
	}
}

func TestSessionCompletionReconcilerReportsMalformedExportWithoutHardError(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixtureWithStderr(`{"messages":[`, "Exporting session: ses_broken")}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_broken"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusMalformedExport {
		t.Fatalf("expected malformed export status, got %q", outcome.Status)
	}
	if !strings.Contains(outcome.Diagnostic, "decode opencode export") {
		t.Fatalf("expected decode error in diagnostic, got %q", outcome.Diagnostic)
	}
	if !strings.Contains(outcome.Diagnostic, "Exporting session: ses_broken") {
		t.Fatalf("expected stderr banner in diagnostic, got %q", outcome.Diagnostic)
	}
}

func TestSessionCompletionReconcilerRetriesMalformedExportWithinBoundedWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{
		results: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
			exportResultFixture(`{"messages":[`),
			exportResultFixture(successExportFixture("recovered after malformed export")),
		},
		sanitizedResults: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
			exportResultFixture(`{"messages":[`),
		},
	}
	reconciler := newSessionCompletionReconciler(shortRetryBudgetReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_retry_malformed"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.ExportAttempts != 5 {
		t.Fatalf("expected five export attempts before recovery, got %d", outcome.ExportAttempts)
	}
	if len(exporter.providerSessionIDs) != 5 {
		t.Fatalf("expected five export attempts, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerFallsBackToSanitizedExportWhenRawMalformed(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{
		results:          []sessionExportResult{exportResultFixture(`{"messages":[`)},
		sanitizedResults: []sessionExportResult{exportResultFixture(successExportFixture("sanitized recovered output"))},
	}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_sanitized_fallback",
		StreamOutputText:  "stream fallback output",
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected sanitized fallback success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.ExportMode != sessionExportModeSanitized {
		t.Fatalf("expected sanitized export mode, got %q", outcome.ExportMode)
	}
	if outcome.Result.EvidenceSource != completionEvidenceSourceExportSanitized {
		t.Fatalf("expected sanitized evidence source, got %q", outcome.Result.EvidenceSource)
	}
	if outcome.Result.OutputText != "stream fallback output" {
		t.Fatalf("expected stream text to win over sanitized output, got %q", outcome.Result.OutputText)
	}
	if len(exporter.exportModes) != 2 || exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized {
		t.Fatalf("expected raw then sanitized export modes, got %#v", exporter.exportModes)
	}
	if len(exporter.providerSessionIDs) < 2 {
		t.Fatalf("expected at least two export attempts, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerSalvagesResolvedAssistantFromMalformedExportTail(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(malformedTailAfterResolvedAssistantFixture("invocation_current", "recovered from malformed tail"))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_malformed_tail",
		InvocationID:        "invocation_current",
		InvocationStartedAt: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected malformed tail salvage to resolve success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.ExportMode != sessionExportModeRaw {
		t.Fatalf("expected raw export mode after salvage, got %q", outcome.ExportMode)
	}
	if outcome.Result.EvidenceSource != completionEvidenceSourceExport {
		t.Fatalf("expected raw export evidence source after salvage, got %q", outcome.Result.EvidenceSource)
	}
	if outcome.Result.OutputText != "recovered from malformed tail" {
		t.Fatalf("expected recovered output text from salvaged assistant turn, got %q", outcome.Result.OutputText)
	}
	if len(exporter.exportModes) != 1 || exporter.exportModes[0] != sessionExportModeRaw {
		t.Fatalf("expected raw export only, got %#v", exporter.exportModes)
	}
	if len(exporter.providerSessionIDs) != 1 {
		t.Fatalf("expected one raw export attempt, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerSalvagesTest2StyleReviewVerdictFromMalformedTail(t *testing.T) {
	t.Parallel()

	const reviewVerdict = "VERDICT: PASS\n\nCaptured reviewer response."
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(test2MalformedReviewExportFixture("invocation_review_current", "invocation_review_current_later", reviewVerdict))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID:   "ses_test2_review",
		InvocationID:        "invocation_review_current",
		InvocationStartedAt: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected malformed tail salvage to resolve success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != reviewVerdict {
		t.Fatalf("expected recovered review verdict text, got %q", outcome.Result.OutputText)
	}
	if outcome.Result.EvidenceSource != completionEvidenceSourceExport {
		t.Fatalf("expected raw export evidence source, got %q", outcome.Result.EvidenceSource)
	}
	if len(exporter.exportModes) != 1 || exporter.exportModes[0] != sessionExportModeRaw {
		t.Fatalf("expected raw export only, got %#v", exporter.exportModes)
	}
}

func TestSessionCompletionReconcilerBlankRecoveredOutputSuppressesRetry(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture(""))}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_blank"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "" {
		t.Fatalf("expected blank recovered output, got %q", outcome.Result.OutputText)
	}
	if !outcome.Result.SuppressEmptyOutputRetry {
		t.Fatal("expected blank recovered output to suppress empty output retry")
	}
}

func TestSessionCompletionReconcilerReturnsAmbiguousWhenExportStillIncomplete(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(ambiguousExportFixture())}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_ambiguous"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected ambiguous export to remain unresolved, got status=%q", outcome.Status)
	}
}

func TestSessionCompletionReconcilerUsesLatestCanonicalAssistantMessage(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(latestAssistantAmbiguousFixture())}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_latest_only"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusUnresolved {
		t.Fatalf("expected unresolved reconciliation when the latest canonical assistant message is ambiguous, got status=%q", outcome.Status)
	}
}

func TestSessionCompletionReconcilerIgnoresToolResultTextWhenRecoveringOutput(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(toolResultOnlyTextExportFixture())}}
	reconciler := newSessionCompletionReconciler(singlePassReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{
		ProviderSessionID: "ses_tool_text",
		StreamOutputText:  "stream fallback output",
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected resolved success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if outcome.Result.OutputText != "stream fallback output" {
		t.Fatalf("expected tool-result text to be ignored in favor of stream fallback, got %q", outcome.Result.OutputText)
	}
}

func successExportFixture(text string) string {
	return successExportFixtureAt(text, defaultExportFixtureCompletedAt())
}

func successExportFixtureAt(text string, completedAt int64) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"step-start"},{"type":"reasoning","text":"internal draft"},{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`, completedAt, text)
}

func invocationSuccessExportFixture(invocationID, text string) string {
	return invocationSuccessExportFixtureAtAndAttempt("attempt_current", invocationID, text, defaultExportFixtureCompletedAt())
}

func invocationSuccessExportFixtureWithAttempt(attemptID, invocationID, text string) string {
	return invocationSuccessExportFixtureAtAndAttempt(attemptID, invocationID, text, defaultExportFixtureCompletedAt())
}

func invocationSuccessExportFixtureAt(invocationID, text string, completedAt int64) string {
	return invocationSuccessExportFixtureAtAndAttempt("attempt_current", invocationID, text, completedAt)
}

func invocationSuccessExportFixtureAtAndAttempt(attemptID, invocationID, text string, completedAt int64) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		promptWithProviderInvocationMetadata("current invocation prompt", attemptID, invocationID),
		completedAt,
		text,
	)
}

func invocationFailureExportFixture(invocationID string) string {
	return invocationFailureExportFixtureWithAttempt("attempt_current", invocationID)
}

func invocationFailureExportFixtureWithAttempt(attemptID, invocationID string) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"error","error":{"message":"exported failure"},"time":{"completed":%d}},"parts":[{"type":"text","text":"failed"}]}]}`,
		promptWithProviderInvocationMetadata("current invocation prompt", attemptID, invocationID),
		defaultExportFixtureCompletedAt(),
	)
}

func failureExportFixture() string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"assistant","finish":"error","error":{"message":"exported failure"},"time":{"completed":%d}},"parts":[{"type":"text","text":"failed"}]}]}`,
		defaultExportFixtureCompletedAt(),
	)
}

func ambiguousExportFixture() string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"assistant","finish":"tool-calls","time":{"completed":%d}},"parts":[{"type":"tool_call","state":{"status":"running"}}]}]}`,
		defaultExportFixtureCompletedAt(),
	)
}

func latestAssistantAmbiguousFixture() string {
	completedAt := defaultExportFixtureCompletedAt()
	return fmt.Sprintf(`{"messages":[{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":"older success"}]},{"info":{"role":"assistant","finish":"tool-calls","time":{"completed":%d}},"parts":[{"type":"tool_call","state":{"status":"running"}}],"shadow":{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":"stale nested success"}]}}]}`,
		completedAt-60000,
		completedAt,
		completedAt-300000,
	)
}

func toolResultOnlyTextExportFixture() string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"tool_result","state":{"status":"completed"},"content":[{"type":"text","text":"tool leaked text"}]}]}]}`,
		defaultExportFixtureCompletedAt(),
	)
}

func invocationOwnershipExportFixture(priorInvocationID string, priorCompletedAt int64, priorOutput, currentInvocationID, currentOutput string) string {
	return invocationOwnershipExportFixtureWithAttempts("attempt_old", priorInvocationID, priorCompletedAt, priorOutput, "attempt_current", currentInvocationID, currentOutput)
}

func invocationOwnershipExportFixtureWithAttempts(priorAttemptID, priorInvocationID string, priorCompletedAt int64, priorOutput, currentAttemptID, currentInvocationID, currentOutput string) string {
	currentAssistant := ""
	if strings.TrimSpace(currentOutput) != "" {
		currentAssistant = fmt.Sprintf(`,{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}`,
			defaultExportFixtureCompletedAt(),
			currentOutput,
		)
	}
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}%s]}`,
		promptWithProviderInvocationMetadata("prior invocation prompt", priorAttemptID, priorInvocationID),
		priorCompletedAt,
		priorOutput,
		promptWithProviderInvocationMetadata("current invocation prompt", currentAttemptID, currentInvocationID),
		currentAssistant,
	)
}

func invocationOwnershipExportFixtureWithCurrentCompletedAt(priorInvocationID string, priorCompletedAt int64, priorOutput, currentInvocationID, currentOutput, currentCompletedAt string) string {
	return invocationOwnershipExportFixtureWithCurrentCompletedAtAndAttempts("attempt_old", priorInvocationID, priorCompletedAt, priorOutput, "attempt_current", currentInvocationID, currentOutput, currentCompletedAt)
}

func invocationOwnershipExportFixtureWithCurrentCompletedAtAndAttempts(priorAttemptID, priorInvocationID string, priorCompletedAt int64, priorOutput, currentAttemptID, currentInvocationID, currentOutput, currentCompletedAt string) string {
	currentAssistant := ""
	if strings.TrimSpace(currentOutput) != "" {
		currentAssistant = fmt.Sprintf(`,{"info":{"role":"assistant","finish":"stop","time":{"completed":%q}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}`,
			currentCompletedAt,
			currentOutput,
		)
	}
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}%s]}`,
		promptWithProviderInvocationMetadata("prior invocation prompt", priorAttemptID, priorInvocationID),
		priorCompletedAt,
		priorOutput,
		promptWithProviderInvocationMetadata("current invocation prompt", currentAttemptID, currentInvocationID),
		currentAssistant,
	)
}

func quotedInvocationOwnershipExportFixture(invocationID, output string) string {
	return quotedInvocationOwnershipExportFixtureWithAttempt("attempt_current", invocationID, output)
}

func quotedInvocationOwnershipExportFixtureWithAttempt(attemptID, invocationID, output string) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		`"`+promptWithProviderInvocationMetadata("current invocation prompt", attemptID, invocationID)+`"`,
		defaultExportFixtureCompletedAt(),
		output,
	)
}

func splitInvocationMarkerExportFixture(invocationID, output string) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q},{"type":"text","text":%q},{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		"current invocation prompt\n\n"+providerInvocationMetadataHeader+"\n"+providerInvocationAttemptLabel+" attempt_current\n"+providerInvocationIDLabel,
		" ",
		invocationID,
		defaultExportFixtureCompletedAt(),
		output,
	)
}

func invocationBoundaryWithWrapperUserFixture(invocationID, output string) string {
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"user"},"parts":[{"type":"text","text":"wrapper message without runtime metadata"}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		promptWithProviderInvocationMetadata("current invocation prompt", "attempt_current", invocationID),
		defaultExportFixtureCompletedAt(),
		output,
	)
}

func quotedPriorBoundaryThenLaterBoundaryExportFixture(targetInvocationID, laterInvocationID, laterOutput string) string {
	quotedPrior := `"` + promptWithProviderInvocationMetadata("prior invocation prompt", "attempt_target", targetInvocationID) + `"`
	laterPrompt := promptWithProviderInvocationMetadata("later invocation prompt", "attempt_later", laterInvocationID)
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":"target output"},{"type":"tool_result","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		promptWithProviderInvocationMetadata("target invocation prompt", "attempt_target", targetInvocationID),
		defaultExportFixtureCompletedAt()-60000,
		quotedPrior+"\n\n"+laterPrompt,
		defaultExportFixtureCompletedAt(),
		laterOutput,
	)
}

func invocationWindowBleedExportFixture(targetInvocationID, targetOutput, laterInvocationID, laterOutput, laterFinish string) string {
	targetAssistant := ""
	if strings.TrimSpace(targetOutput) != "" {
		targetAssistant = fmt.Sprintf(`,{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}`,
			defaultExportFixtureCompletedAt()-60000,
			targetOutput,
		)
	}
	laterError := ""
	if strings.TrimSpace(laterFinish) == "error" {
		laterError = `,"error":{"message":"later invocation failed"}`
	}
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}%s,{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":%q%s,"time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}]}`,
		promptWithProviderInvocationMetadata("target invocation prompt", "attempt_target", targetInvocationID),
		targetAssistant,
		promptWithProviderInvocationMetadata("later invocation prompt", "attempt_later", laterInvocationID),
		laterFinish,
		laterError,
		defaultExportFixtureCompletedAt(),
		laterOutput,
	)
}

func continuedInvocationOwnershipExportFixture(invocationID, initialOutput, continuationOutput string) string {
	return continuedInvocationOwnershipExportFixtureWithAttempt("attempt_target", invocationID, initialOutput, continuationOutput)
}

func continuedInvocationOwnershipExportFixtureWithAttempt(attemptID, invocationID, initialOutput, continuationOutput string) string {
	initialAssistant := ""
	if strings.TrimSpace(initialOutput) != "" {
		initialAssistant = fmt.Sprintf(`,{"info":{"role":"assistant","finish":"tool-calls","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_call","state":{"status":"completed"}}]}`,
			defaultExportFixtureCompletedAt()-60000,
			initialOutput,
		)
	}
	continuationAssistant := ""
	if strings.TrimSpace(continuationOutput) != "" {
		continuationAssistant = fmt.Sprintf(`,{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]}`,
			defaultExportFixtureCompletedAt(),
			continuationOutput,
		)
	}
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}%s,{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}%s]}`,
		promptWithProviderInvocationMetadata("initial invocation prompt", attemptID, invocationID),
		initialAssistant,
		promptWithProviderInvocationMetadata("Continue from the pending tool-call state and finish the response.", attemptID, invocationID),
		continuationAssistant,
	)
}

func malformedTailAfterResolvedAssistantFixture(invocationID, recoveredOutput string) string {
	completedAt := defaultExportFixtureCompletedAt()
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"tool-calls","time":{"completed":%d}},"parts":[{"type":"text","text":"partial work"},{"type":"tool_call","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]},{"info":{"role":"assistant"`,
		promptWithProviderInvocationMetadata("initial invocation prompt", "attempt_target", invocationID),
		completedAt-60000,
		promptWithProviderInvocationMetadata("Continue from the pending tool-call state and finish the response.", "attempt_target", invocationID),
		completedAt,
		recoveredOutput,
	)
}

func test2MalformedReviewExportFixture(invocationID, laterInvocationID, recoveredOutput string) string {
	return test2MalformedReviewExportFixtureWithAttempts("attempt_review_current", invocationID, "attempt_review_later", laterInvocationID, recoveredOutput)
}

func test2MalformedReviewExportFixtureWithAttempts(currentAttemptID, invocationID, laterAttemptID, laterInvocationID, recoveredOutput string) string {
	completedAt := defaultExportFixtureCompletedAt()
	return fmt.Sprintf(`{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"tool-calls","time":{"completed":%d}},"parts":[{"type":"text","text":"running final review checks"},{"type":"tool_call","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]},{"info":{"role":"assistant","finish":"stop","time":{"completed":%d}},"parts":[{"type":"text","text":%q},{"type":"tool_result","state":{"status":"completed"}}]},{"info":{"role":"user"},"parts":[{"type":"text","text":%q}]}`,
		promptWithProviderInvocationMetadata("review invocation prompt", currentAttemptID, invocationID),
		completedAt-60000,
		promptWithProviderInvocationMetadata("continue and issue the final review verdict now", currentAttemptID, invocationID),
		completedAt,
		recoveredOutput,
		promptWithProviderInvocationMetadata("later invocation prompt", laterAttemptID, laterInvocationID),
	)
}

func defaultExportFixtureCompletedAt() int64 {
	return time.Now().UTC().Add(5 * time.Second).UnixMilli()
}

func TestSessionCompletionReconcilerRetriesWithinBoundedWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(ambiguousExportFixture()), exportResultFixture(successExportFixture("recovered later"))}}
	reconciler := newSessionCompletionReconciler(shortRetryBudgetReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_retry"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected bounded retry to recover success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if len(exporter.providerSessionIDs) < 2 {
		t.Fatalf("expected at least two export attempts, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerRetriesExporterErrorsWithinBoundedWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{
		errs:    []error{fmt.Errorf("session not found yet"), nil},
		results: []sessionExportResult{exportResultFixture(successExportFixture("recovered after export miss"))},
	}
	reconciler := newSessionCompletionReconciler(shortRetryBudgetReconcilerConfig(exporter))

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_retry_error"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusResolved || outcome.Result.State != attemptCompletionStateSuccess {
		t.Fatalf("expected export retry to recover success, got status=%q state=%q", outcome.Status, outcome.Result.State)
	}
	if len(exporter.providerSessionIDs) != 2 {
		t.Fatalf("expected two export attempts, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerReturnsUnresolvedAfterExporterErrorsExhaustWindow(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{errs: []error{fmt.Errorf("session not found yet"), fmt.Errorf("session not found yet")}}
	reconciler := newSessionCompletionReconciler(sessionCompletionReconcilerConfig{
		Exporter:        exporter,
		TotalTimeout:    12 * time.Millisecond,
		BackoffSchedule: []time.Duration{5 * time.Millisecond},
	})

	outcome, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_retry_error_exhausted"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if outcome.Status != sessionCompletionReconcileStatusExporterError {
		t.Fatalf("expected exporter errors to report exporter_error status, got %q", outcome.Status)
	}
	if !strings.Contains(outcome.Result.FailureText, "session not found yet") {
		t.Fatalf("expected last export error in failure text, got %q", outcome.Result.FailureText)
	}
	if len(exporter.providerSessionIDs) < 2 {
		t.Fatalf("expected at least two export attempts, got %d", len(exporter.providerSessionIDs))
	}
}

func TestSessionCompletionReconcilerBoundsExportAttemptByDeadline(t *testing.T) {
	t.Parallel()

	exporter := &fakeSessionExporter{exportFn: func(ctx context.Context, req sessionExportRequest) (sessionExportResult, error) {
		<-ctx.Done()
		return sessionExportResult{}, ctx.Err()
	}}
	reconciler := newSessionCompletionReconciler(sessionCompletionReconcilerConfig{
		Exporter:     exporter,
		TotalTimeout: 25 * time.Millisecond,
	})

	startedAt := time.Now()
	_, err := reconciler.Reconcile(context.Background(), sessionCompletionReconcileRequest{ProviderSessionID: "ses_deadline_bound"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("expected bounded export to return promptly, elapsed=%s", elapsed)
	}
	if len(exporter.providerSessionIDs) != 1 || exporter.providerSessionIDs[0] != "ses_deadline_bound" {
		t.Fatalf("expected single deadline-bound export attempt, got %#v", exporter.providerSessionIDs)
	}
}
