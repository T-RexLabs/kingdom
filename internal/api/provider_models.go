package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

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
	flight     singleflight.Group
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

func (c *openCodeProviderModelCatalog) Snapshot(ctx context.Context, refresh bool) (ProviderModelCatalogSnapshot, error) {
	c.mu.Lock()
	if !refresh && len(c.cachedRows) > 0 && c.now().Sub(c.cachedAt) <= c.ttl {
		rows := cloneProviderModels(c.cachedRows)
		cachedAt := c.cachedAt
		c.mu.Unlock()
		return ProviderModelCatalogSnapshot{Items: rows, Status: ProviderModelCatalogStatusFresh, LastSuccessAt: &cachedAt}, nil
	}
	c.mu.Unlock()

	fetchKey := "cached"
	if refresh {
		fetchKey = "refresh"
	}
	resolvedRows, err, _ := c.flight.Do(fetchKey, func() (any, error) {
		c.mu.Lock()
		if !refresh && len(c.cachedRows) > 0 && c.now().Sub(c.cachedAt) <= c.ttl {
			rows := cloneProviderModels(c.cachedRows)
			cachedAt := c.cachedAt
			c.mu.Unlock()
			return ProviderModelCatalogSnapshot{Items: rows, Status: ProviderModelCatalogStatusFresh, LastSuccessAt: &cachedAt}, nil
		}
		cachedRows := cloneProviderModels(c.cachedRows)
		cachedAt := c.cachedAt
		c.mu.Unlock()

		rows, fetchErr := c.fetch(ctx, refresh)
		if fetchErr != nil {
			unavailableErr := classifyProviderModelCatalogError(fetchErr)
			if len(cachedRows) > 0 && !cachedAt.IsZero() {
				return ProviderModelCatalogSnapshot{
					Items:          cachedRows,
					Status:         ProviderModelCatalogStatusStale,
					LastSuccessAt:  &cachedAt,
					FailureReason:  unavailableErr.reason,
					FailureMessage: unavailableErr.Error(),
				}, nil
			}
			return ProviderModelCatalogSnapshot{
				Items:          []gen.ProviderModel{},
				Status:         ProviderModelCatalogStatusUnavailable,
				FailureReason:  unavailableErr.reason,
				FailureMessage: unavailableErr.Error(),
				UnavailableErr: unavailableErr,
			}, nil
		}

		c.mu.Lock()
		c.cachedRows = cloneProviderModels(rows)
		c.cachedAt = c.now()
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
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	return cmd.Output()
}

func parseOpenCodeVerboseModels(output []byte) ([]gen.ProviderModel, error) {
	models := make([]gen.ProviderModel, 0)
	seen := make(map[string]struct{})
	rawOutput := strings.TrimSpace(string(output))
	lines := strings.Split(string(output), "\n")
	var (
		collecting bool
		depth      int
		builder    strings.Builder
	)
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if !collecting {
			if !strings.HasPrefix(trimmedLine, "{") {
				continue
			}
			collecting = true
			depth = 0
			builder.Reset()
		}
		builder.WriteString(line)
		builder.WriteByte('\n')
		depth += strings.Count(line, "{")
		depth -= strings.Count(line, "}")
		if collecting && depth > 0 {
			continue
		}
		collecting = false
		trimmed := strings.TrimSpace(builder.String())
		if trimmed == "" {
			continue
		}
		var row struct {
			ID         string `json:"id"`
			ProviderID string `json:"providerID"`
			Name       string `json:"name"`
			Variants   map[string]struct {
				ReasoningEffort string `json:"reasoningEffort"`
			} `json:"variants"`
		}
		if err := json.Unmarshal([]byte(trimmed), &row); err != nil {
			return nil, fmt.Errorf("decode provider model row: %w", err)
		}
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

	if collecting || depth != 0 {
		return nil, fmt.Errorf("decode provider model row: incomplete json object")
	}
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
