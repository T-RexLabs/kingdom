package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"

	gen "kingdom/internal/apigen"

	"golang.org/x/sync/singleflight"
)

type ProviderModelCatalog interface {
	Snapshot(ctx context.Context, refresh bool) (ProviderModelCatalogSnapshot, error)
	List(ctx context.Context, refresh bool) ([]gen.ProviderModel, error)
}

type ProviderModelCatalogStatus string

const (
	ProviderModelCatalogStatusFresh       ProviderModelCatalogStatus = "fresh"
	ProviderModelCatalogStatusStale       ProviderModelCatalogStatus = "stale"
	ProviderModelCatalogStatusUnavailable ProviderModelCatalogStatus = "unavailable"
)

type ProviderModelCatalogFailureReason string

const (
	ProviderModelCatalogFailureReasonNone          ProviderModelCatalogFailureReason = ""
	ProviderModelCatalogFailureReasonTimeout       ProviderModelCatalogFailureReason = "timeout"
	ProviderModelCatalogFailureReasonBinaryMissing ProviderModelCatalogFailureReason = "binary_missing"
	ProviderModelCatalogFailureReasonCommandFailed ProviderModelCatalogFailureReason = "command_failed"
	ProviderModelCatalogFailureReasonParseFailed   ProviderModelCatalogFailureReason = "parse_failed"
)

type ProviderModelCatalogSnapshot struct {
	Items          []gen.ProviderModel
	Status         ProviderModelCatalogStatus
	LastSuccessAt  *time.Time
	FailureReason  ProviderModelCatalogFailureReason
	FailureMessage string
	UnavailableErr error
}

type openCodeProviderModelCatalog struct {
	binaryPath   string
	now          func() time.Time
	ttl          time.Duration
	fetchTimeout time.Duration
	execVerbose  func(ctx context.Context, binaryPath string, refresh bool) ([]byte, error)

	mu         sync.Mutex
	cachedAt   time.Time
	cachedRows []gen.ProviderModel
	// lastFailure records why the most recent refresh attempt failed while a
	// usable cache was already held. It is what turns the next read into a
	// `stale` snapshot, so a background refresh that fails still reaches the
	// operator instead of dying silently. Cleared on the next success.
	lastFailure *providerModelCatalogUnavailableError
	refreshing  bool
	flight      singleflight.Group

	// refreshed fires after every completed background refresh. Only tests
	// read it; production has nothing to wait for.
	refreshed chan struct{}
}

type providerModelCatalogUnavailableError struct {
	reason ProviderModelCatalogFailureReason
	cause  error
}

func (e *providerModelCatalogUnavailableError) Error() string {
	if e == nil {
		return "provider model catalog unavailable"
	}
	reason := strings.TrimSpace(string(e.reason))
	if reason == "" {
		reason = "provider model catalog unavailable"
	}
	if e.cause == nil {
		return reason
	}
	return fmt.Sprintf("%s: %v", reason, e.cause)
}

func (e *providerModelCatalogUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func isProviderModelCatalogUnavailable(err error) bool {
	var unavailableErr *providerModelCatalogUnavailableError
	return errors.As(err, &unavailableErr)
}

func NewOpenCodeProviderModelCatalog(binaryPath string, now func() time.Time, ttl time.Duration) ProviderModelCatalog {
	resolvedBinary := strings.TrimSpace(binaryPath)
	if resolvedBinary == "" {
		resolvedBinary = "opencode"
	}
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &openCodeProviderModelCatalog{
		binaryPath:   resolvedBinary,
		now:          now,
		ttl:          ttl,
		fetchTimeout: 5 * time.Second,
		execVerbose:  executeOpenCodeVerboseModels,
	}
}

func (c *openCodeProviderModelCatalog) List(ctx context.Context, refresh bool) ([]gen.ProviderModel, error) {
	snapshot, err := c.Snapshot(ctx, refresh)
	if err != nil {
		return nil, err
	}
	if snapshot.Status == ProviderModelCatalogStatusUnavailable {
		if snapshot.UnavailableErr != nil {
			return nil, snapshot.UnavailableErr
		}
		return nil, &providerModelCatalogUnavailableError{reason: snapshot.FailureReason}
	}
	return cloneProviderModels(snapshot.Items), nil
}

// Snapshot serves the model catalog without ever making an operator wait on
// OpenCode more than once per process.
//
// Listing models means spawning `opencode models --verbose`, which costs
// seconds. Blocking every read whose cache had just aged past the TTL put that
// cost on the request path forever: the planner page polled on the same period
// as the TTL, so a browser tab guaranteed a subprocess spawn a minute, and each
// one disabled the model selectors while it ran. Reads are now
// stale-while-revalidate — a populated cache answers immediately and an aged
// one refreshes in the background — so only the very first read of a process
// can block, and `kd summon` pays that one through Warm.
//
// An explicit refresh stays synchronous: it exists so the operator can press
// Retry and be told what happened.
func (c *openCodeProviderModelCatalog) Snapshot(ctx context.Context, refresh bool) (ProviderModelCatalogSnapshot, error) {
	if !refresh {
		if snapshot, ok := c.cachedSnapshot(); ok {
			return snapshot, nil
		}
	}
	return c.fetchSnapshot(ctx, refresh)
}

// cachedSnapshot answers from cache when there is one, scheduling a background
// refresh if the rows have aged out. The second return reports whether the
// cache could answer at all.
func (c *openCodeProviderModelCatalog) cachedSnapshot() (ProviderModelCatalogSnapshot, bool) {
	c.mu.Lock()
	if c.cachedAt.IsZero() {
		c.mu.Unlock()
		return ProviderModelCatalogSnapshot{}, false
	}
	rows := cloneProviderModels(c.cachedRows)
	cachedAt := c.cachedAt
	failure := c.lastFailure
	stale := c.now().Sub(cachedAt) > c.ttl
	scheduleRefresh := stale && !c.refreshing
	if scheduleRefresh {
		c.refreshing = true
	}
	c.mu.Unlock()

	if scheduleRefresh {
		go c.refreshInBackground()
	}
	if failure != nil {
		return ProviderModelCatalogSnapshot{
			Items:          rows,
			Status:         ProviderModelCatalogStatusStale,
			LastSuccessAt:  &cachedAt,
			FailureReason:  failure.reason,
			FailureMessage: operatorCatalogFailureMessage(failure.reason, true),
		}, true
	}
	return ProviderModelCatalogSnapshot{Items: rows, Status: ProviderModelCatalogStatusFresh, LastSuccessAt: &cachedAt}, true
}

// refreshInBackground revalidates an aged cache off the request path. Its
// deadline is the fetch timeout rather than any caller's context, because the
// caller it was scheduled for has already been answered and may be long gone.
func (c *openCodeProviderModelCatalog) refreshInBackground() {
	defer func() {
		c.mu.Lock()
		c.refreshing = false
		refreshed := c.refreshed
		c.mu.Unlock()
		if refreshed != nil {
			select {
			case refreshed <- struct{}{}:
			default:
			}
		}
	}()
	_, _ = c.fetchSnapshot(context.Background(), false)
}

// fetchSnapshot runs the provider fetch, collapsing concurrent callers onto one
// subprocess, and records the outcome for later reads.
func (c *openCodeProviderModelCatalog) fetchSnapshot(ctx context.Context, refresh bool) (ProviderModelCatalogSnapshot, error) {
	fetchKey := "cached"
	if refresh {
		fetchKey = "refresh"
	}
	resolvedRows, err, _ := c.flight.Do(fetchKey, func() (any, error) {
		c.mu.Lock()
		cachedRows := cloneProviderModels(c.cachedRows)
		cachedAt := c.cachedAt
		c.mu.Unlock()

		rows, fetchErr := c.fetch(ctx, refresh)
		if fetchErr != nil {
			unavailableErr := classifyProviderModelCatalogError(fetchErr)
			servingCachedModels := !cachedAt.IsZero()
			if servingCachedModels {
				c.mu.Lock()
				c.lastFailure = unavailableErr
				c.mu.Unlock()
				return ProviderModelCatalogSnapshot{
					Items:          cachedRows,
					Status:         ProviderModelCatalogStatusStale,
					LastSuccessAt:  &cachedAt,
					FailureReason:  unavailableErr.reason,
					FailureMessage: operatorCatalogFailureMessage(unavailableErr.reason, true),
				}, nil
			}
			return ProviderModelCatalogSnapshot{
				Items:          []gen.ProviderModel{},
				Status:         ProviderModelCatalogStatusUnavailable,
				FailureReason:  unavailableErr.reason,
				FailureMessage: operatorCatalogFailureMessage(unavailableErr.reason, false),
				UnavailableErr: unavailableErr,
			}, nil
		}

		c.mu.Lock()
		c.cachedRows = cloneProviderModels(rows)
		c.cachedAt = c.now()
		c.lastFailure = nil
		resolvedCachedAt := c.cachedAt
		c.mu.Unlock()

		return ProviderModelCatalogSnapshot{Items: cloneProviderModels(rows), Status: ProviderModelCatalogStatusFresh, LastSuccessAt: &resolvedCachedAt}, nil
	})
	if err != nil {
		return ProviderModelCatalogSnapshot{}, err
	}
	snapshot, ok := resolvedRows.(ProviderModelCatalogSnapshot)
	if !ok {
		return ProviderModelCatalogSnapshot{}, fmt.Errorf("provider model catalog snapshot: unexpected result type %T", resolvedRows)
	}
	return snapshot, nil
}

func (c *openCodeProviderModelCatalog) fetch(ctx context.Context, refresh bool) ([]gen.ProviderModel, error) {
	fetchCtx := ctx
	cancel := func() {}
	if c.fetchTimeout > 0 {
		fetchCtx, cancel = context.WithTimeout(ctx, c.fetchTimeout)
	}
	defer cancel()

	output, err := c.execVerbose(fetchCtx, c.binaryPath, refresh)
	if err != nil {
		if errors.Is(fetchCtx.Err(), context.DeadlineExceeded) {
			return nil, &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonTimeout, cause: context.DeadlineExceeded}
		}
		if errors.Is(fetchCtx.Err(), context.Canceled) {
			return nil, fmt.Errorf("list provider models canceled: %w", err)
		}
		if errors.Is(err, exec.ErrNotFound) {
			return nil, &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonBinaryMissing, cause: err}
		}
		return nil, &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonCommandFailed, cause: err}
	}
	rows, parseErr := parseOpenCodeVerboseModels(output)
	if parseErr != nil {
		return nil, &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonParseFailed, cause: parseErr}
	}
	return rows, nil
}

// operatorCatalogFailureMessage turns a catalog failure into something an
// operator can act on.
//
// `FailureMessage` reaches the model selector verbatim, so it must never carry
// a Go error string: "timeout: context deadline exceeded" tells the operator
// nothing about what Kingdom was doing or whether their work is affected. The
// two outcomes differ enough to deserve different words — with a cached list
// the planner keeps working and the refresh is the only casualty, without one
// no model can be chosen at all. The underlying error stays in FailureReason
// for programmatic callers and in the process log for diagnosis.
func operatorCatalogFailureMessage(reason ProviderModelCatalogFailureReason, servingCachedModels bool) string {
	if servingCachedModels {
		switch reason {
		case ProviderModelCatalogFailureReasonTimeout:
			return "Showing the models Kingdom already had. The refresh from OpenCode timed out."
		case ProviderModelCatalogFailureReasonBinaryMissing:
			return "Showing the models Kingdom already had. The opencode binary is no longer on PATH."
		case ProviderModelCatalogFailureReasonParseFailed:
			return "Showing the models Kingdom already had. Kingdom could not read the model list OpenCode returned."
		default:
			return "Showing the models Kingdom already had. The refresh from OpenCode did not complete."
		}
	}
	switch reason {
	case ProviderModelCatalogFailureReasonTimeout:
		return "OpenCode did not return a model list in time. Retry once it responds."
	case ProviderModelCatalogFailureReasonBinaryMissing:
		return "Kingdom could not find the opencode binary on PATH. Install or configure OpenCode, then retry."
	case ProviderModelCatalogFailureReasonParseFailed:
		return "Kingdom could not read the model list OpenCode returned. Check the installed OpenCode version, then retry."
	default:
		return "Kingdom could not list OpenCode models. Check the OpenCode installation, then retry."
	}
}

func classifyProviderModelCatalogError(err error) *providerModelCatalogUnavailableError {
	if err == nil {
		return &providerModelCatalogUnavailableError{}
	}
	var unavailableErr *providerModelCatalogUnavailableError
	if errors.As(err, &unavailableErr) {
		return unavailableErr
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonTimeout, cause: err}
	}
	return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonCommandFailed, cause: err}
}

func executeOpenCodeVerboseModels(ctx context.Context, binaryPath string, refresh bool) ([]byte, error) {
	args := []string{"models", "--verbose"}
	if refresh {
		args = append(args, "--refresh")
	}
	runner, err := processguardian.NewRunner(processguardian.Config{})
	if err != nil {
		return nil, err
	}
	handle, err := runner.Start(ctx, processguardian.Command{Path: binaryPath, Args: args, Env: os.Environ()}, nil)
	if err != nil {
		return nil, err
	}
	return collectOpenCodeVerboseModelOutput(ctx, handle)
}

func collectOpenCodeVerboseModelOutput(ctx context.Context, handle processguardian.Handle) ([]byte, error) {
	if handle == nil {
		return nil, fmt.Errorf("model catalog process handle is required")
	}
	type streamResult struct {
		raw []byte
		err error
	}
	stdoutDone := make(chan streamResult, 1)
	stderrDone := make(chan streamResult, 1)
	stdoutStream, stderrStream := handle.Stdout(), handle.Stderr()
	var cancelOnce sync.Once
	cancel := func() { cancelOnce.Do(func() { _ = handle.Cancel(context.Background()) }) }
	go func() {
		defer stdoutStream.Close()
		raw, err := providerlimits.ReadBoundedOnLimit(stdoutStream, providerlimits.GuardedCommandStreamBytes, "model catalog stdout", cancel)
		stdoutDone <- streamResult{raw: raw, err: err}
	}()
	go func() {
		defer stderrStream.Close()
		raw, err := providerlimits.ReadBoundedOnLimit(stderrStream, providerlimits.GuardedCommandStreamBytes, "model catalog stderr", cancel)
		stderrDone <- streamResult{raw: raw, err: err}
	}()
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-cancelDone:
		}
	}()
	result, waitErr := handle.Wait(context.Background())
	close(cancelDone)
	stdout, stderr := <-stdoutDone, <-stderrDone
	if stdout.err != nil {
		return stdout.raw, stdout.err
	}
	if stderr.err != nil {
		return stdout.raw, stderr.err
	}
	if waitErr != nil {
		return stdout.raw, waitErr
	}
	if !result.Quiescent || result.ExitCode != 0 || result.ExitSignal != 0 || result.FailureCode != "" {
		return stdout.raw, fmt.Errorf("guarded model catalog command failed: %s", strings.TrimSpace(string(stderr.raw)))
	}
	return stdout.raw, nil
}

func parseOpenCodeVerboseModels(output []byte) ([]gen.ProviderModel, error) {
	models := make([]gen.ProviderModel, 0)
	seen := make(map[string]struct{})
	outputText := string(output)
	rawOutput := strings.TrimSpace(outputText)
	for lineStart := 0; lineStart < len(outputText); {
		lineEnd := len(outputText)
		if newline := strings.IndexByte(outputText[lineStart:], '\n'); newline >= 0 {
			lineEnd = lineStart + newline
		}
		line := outputText[lineStart:lineEnd]
		trimmedLine := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmedLine, "{") {
			lineStart = lineEnd + 1
			continue
		}
		objectStart := lineStart + strings.Index(line, trimmedLine)
		var row struct {
			ID         string `json:"id"`
			ProviderID string `json:"providerID"`
			Name       string `json:"name"`
			Variants   map[string]struct {
				ReasoningEffort string `json:"reasoningEffort"`
			} `json:"variants"`
		}
		decoder := json.NewDecoder(strings.NewReader(outputText[objectStart:]))
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode provider model row: %w", err)
		}
		objectEnd := objectStart + int(decoder.InputOffset())
		objectLineEnd := len(outputText)
		if newline := strings.IndexByte(outputText[objectEnd:], '\n'); newline >= 0 {
			objectLineEnd = objectEnd + newline
		}
		if strings.TrimSpace(outputText[objectEnd:objectLineEnd]) != "" {
			return nil, fmt.Errorf("decode provider model row: invalid content after json object")
		}
		lineStart = objectLineEnd + 1
		providerID := strings.TrimSpace(row.ProviderID)
		modelID := strings.TrimSpace(row.ID)
		if providerID == "" || modelID == "" {
			continue
		}
		id := providerID + "/" + modelID
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		modelName := strings.TrimSpace(row.Name)
		if modelName == "" {
			modelName = modelID
		}
		thinkingEffortOptions, thinkingEffortDefault := parseThinkingEffortOptions(row.Variants)
		models = append(models, gen.ProviderModel{
			Id:                    id,
			ProviderId:            providerID,
			ProviderName:          formatProviderName(providerID),
			ModelName:             modelName,
			ThinkingEffortDefault: thinkingEffortDefault,
			ThinkingEffortOptions: thinkingEffortOptions,
		})
	}

	sort.Slice(models, func(i, j int) bool {
		left := strings.ToLower(models[i].ProviderName + "\x00" + models[i].ModelName)
		right := strings.ToLower(models[j].ProviderName + "\x00" + models[j].ModelName)
		return left < right
	})

	if rawOutput != "" && len(models) == 0 {
		return nil, fmt.Errorf("decode provider model rows: non-empty output did not yield any valid rows")
	}

	return models, nil
}

func parseThinkingEffortOptions(variants map[string]struct {
	ReasoningEffort string `json:"reasoningEffort"`
}) ([]gen.ThinkingEffortOption, *string) {
	if len(variants) == 0 {
		return []gen.ThinkingEffortOption{}, nil
	}
	type effortOption struct {
		value string
		label string
	}
	preferredOrder := []string{"minimal", "none", "low", "medium", "high", "xhigh", "max"}
	seen := make(map[string]struct{})
	options := make([]effortOption, 0, len(variants))
	for _, key := range preferredOrder {
		variant, ok := variants[key]
		if !ok {
			continue
		}
		value := strings.TrimSpace(variant.ReasoningEffort)
		if value == "" {
			value = strings.TrimSpace(key)
		}
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		options = append(options, effortOption{value: value, label: formatThinkingEffortLabel(value)})
	}
	additionalKeys := make([]string, 0, len(variants))
	for key := range variants {
		additionalKeys = append(additionalKeys, key)
	}
	sort.Strings(additionalKeys)
	for _, key := range additionalKeys {
		variant := variants[key]
		value := strings.TrimSpace(variant.ReasoningEffort)
		if value == "" {
			value = strings.TrimSpace(key)
		}
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		options = append(options, effortOption{value: value, label: formatThinkingEffortLabel(value)})
	}
	result := make([]gen.ThinkingEffortOption, 0, len(options))
	for _, option := range options {
		result = append(result, gen.ThinkingEffortOption{Value: option.value, Label: option.label})
	}
	defaultValue := thinkingEffortDefaultForOptions(result)
	return result, defaultValue
}

func thinkingEffortDefaultForOptions(options []gen.ThinkingEffortOption) *string {
	if len(options) == 0 {
		return nil
	}
	for _, preferred := range []string{"medium", "low", "minimal", "none", "high", "xhigh", "max"} {
		for _, option := range options {
			if option.Value == preferred {
				value := option.Value
				return &value
			}
		}
	}
	value := options[0].Value
	return &value
}

func formatThinkingEffortLabel(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "Custom"
	}
	if strings.EqualFold(trimmed, "xhigh") {
		return "X-High"
	}
	words := strings.Fields(strings.NewReplacer("-", " ", "_", " ").Replace(strings.ToLower(trimmed)))
	for i := range words {
		words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
	}
	if len(words) == 0 {
		return "Custom"
	}
	return strings.Join(words, " ")
}

func cloneProviderModels(rows []gen.ProviderModel) []gen.ProviderModel {
	cloned := make([]gen.ProviderModel, len(rows))
	copy(cloned, rows)
	return cloned
}

func formatProviderName(providerID string) string {
	trimmed := strings.TrimSpace(providerID)
	if trimmed == "" {
		return "Unknown"
	}
	known := map[string]string{
		"github-copilot": "GitHub Copilot",
		"github-models":  "GitHub Models",
		"openai":         "OpenAI",
		"opencode":       "OpenCode",
	}
	if label, ok := known[strings.ToLower(trimmed)]; ok {
		return label
	}
	replacer := strings.NewReplacer("-", " ", "_", " ")
	words := strings.Fields(replacer.Replace(trimmed))
	for i := range words {
		w := strings.ToLower(words[i])
		if w == "ai" {
			words[i] = "AI"
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	if len(words) == 0 {
		return "Unknown"
	}
	return strings.Join(words, " ")
}
