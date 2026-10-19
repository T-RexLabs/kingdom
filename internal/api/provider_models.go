package api

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"kingdom/internal/providers/opencode"

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
	ProviderModelCatalogFailureReasonNone                 ProviderModelCatalogFailureReason = ""
	ProviderModelCatalogFailureReasonTimeout              ProviderModelCatalogFailureReason = "timeout"
	ProviderModelCatalogFailureReasonBinaryMissing        ProviderModelCatalogFailureReason = "binary_missing"
	ProviderModelCatalogFailureReasonServiceUnavailable   ProviderModelCatalogFailureReason = "service_unavailable"
	ProviderModelCatalogFailureReasonParseFailed          ProviderModelCatalogFailureReason = "parse_failed"
	ProviderModelCatalogFailureReasonConfigurationInvalid ProviderModelCatalogFailureReason = "configuration_invalid"
)

type ProviderModelCatalogSnapshot struct {
	Items          []gen.ProviderModel
	Status         ProviderModelCatalogStatus
	LastSuccessAt  *time.Time
	FailureReason  ProviderModelCatalogFailureReason
	FailureMessage string
	UnavailableErr error
}

// OpenCodeModelSource lists the models the shared OpenCode server can use.
type OpenCodeModelSource interface {
	Models(context.Context) ([]opencode.Model, error)
}

// ProviderModelCatalogTTL is how long a listed catalog stays fresh. Models
// change only when the operator changes their OpenCode installation, so a long
// window keeps listing off the request path; the explicit refresh bypasses it.
const ProviderModelCatalogTTL = 10 * time.Minute

type openCodeProviderModelCatalog struct {
	source       OpenCodeModelSource
	now          func() time.Time
	ttl          time.Duration
	fetchTimeout time.Duration

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

func NewOpenCodeProviderModelCatalog(source OpenCodeModelSource, now func() time.Time, ttl time.Duration) ProviderModelCatalog {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = ProviderModelCatalogTTL
	}
	return &openCodeProviderModelCatalog{
		source:       source,
		now:          now,
		ttl:          ttl,
		fetchTimeout: 10 * time.Second,
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

// Snapshot serves cached models immediately and revalidates expired snapshots
// in the background. The first read and an explicit refresh wait for OpenCode's
// shared service. A successful empty list is cached like any other snapshot.
func (c *openCodeProviderModelCatalog) Snapshot(ctx context.Context, refresh bool) (ProviderModelCatalogSnapshot, error) {
	if !refresh {
		if snapshot, ok := c.cachedSnapshot(); ok {
			return snapshot, nil
		}
	}
	return c.fetchSnapshot(ctx)
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
	_, _ = c.fetchSnapshot(context.Background())
}

// fetchSnapshot coalesces concurrent reads from the same provider service and
// records the outcome for later reads.
func (c *openCodeProviderModelCatalog) fetchSnapshot(ctx context.Context) (ProviderModelCatalogSnapshot, error) {
	resolvedRows, err, _ := c.flight.Do("models", func() (any, error) {
		c.mu.Lock()
		cachedRows := cloneProviderModels(c.cachedRows)
		cachedAt := c.cachedAt
		c.mu.Unlock()

		rows, fetchErr := c.fetch(ctx)
		if fetchErr != nil {
			unavailableErr := classifyProviderModelCatalogError(fetchErr)
			c.mu.Lock()
			c.lastFailure = unavailableErr
			c.mu.Unlock()
			servingCachedModels := !cachedAt.IsZero()
			if servingCachedModels {
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
	snapshot.Items = cloneProviderModels(snapshot.Items)
	if snapshot.LastSuccessAt != nil {
		value := *snapshot.LastSuccessAt
		snapshot.LastSuccessAt = &value
	}
	return snapshot, nil
}

func (c *openCodeProviderModelCatalog) fetch(ctx context.Context) ([]gen.ProviderModel, error) {
	if c.source == nil {
		return nil, errors.New("OpenCode model service is unavailable")
	}
	if c.fetchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.fetchTimeout)
		defer cancel()
	}
	models, err := c.source.Models(ctx)
	if err != nil {
		return nil, err
	}
	return providerModels(models), nil
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
		case ProviderModelCatalogFailureReasonConfigurationInvalid:
			return "Showing the models Kingdom already had. A higher-priority OpenCode configuration enables automatic formatting, which Kingdom requires to be off."
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
	case ProviderModelCatalogFailureReasonConfigurationInvalid:
		return "A higher-priority OpenCode configuration, such as an administrator or managed one, enables automatic formatting, which Kingdom requires to be off. Disable it there, then retry."
	case ProviderModelCatalogFailureReasonParseFailed:
		return "Kingdom could not read the model list OpenCode returned. Check the installed OpenCode version, then retry."
	default:
		return "Kingdom could not list OpenCode models. Kingdom starts OpenCode itself, so check that opencode --version reports 2.x and that your providers are configured, then retry."
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
	var configurationErr *opencode.ConfigurationError
	if errors.As(err, &configurationErr) {
		return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonConfigurationInvalid, cause: err}
	}
	var protocolErr *opencode.ProtocolError
	if errors.As(err, &protocolErr) {
		return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonParseFailed, cause: err}
	}
	if errors.Is(err, exec.ErrNotFound) {
		return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonBinaryMissing, cause: err}
	}
	return &providerModelCatalogUnavailableError{reason: ProviderModelCatalogFailureReasonServiceUnavailable, cause: err}
}

// providerModels projects the provider's configured catalog without interpreting
// account entitlement or maintaining a second model allowlist.
func providerModels(models []opencode.Model) []gen.ProviderModel {
	rows := make([]gen.ProviderModel, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		id := model.ProviderID + "/" + model.ID
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		providerName := model.ProviderName
		if providerName == "" {
			providerName = formatProviderName(model.ProviderID)
		}
		name := model.Name
		if name == "" {
			name = model.ID
		}
		options := modelVariantOptions(model.Variants)
		rows = append(rows, gen.ProviderModel{
			Id:                    id,
			ProviderId:            model.ProviderID,
			ProviderName:          providerName,
			ModelName:             name,
			ThinkingEffortOptions: options,
			ThinkingEffortDefault: thinkingEffortDefaultForOptions(options),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		left := strings.ToLower(rows[i].ProviderName + "\x00" + rows[i].ModelName)
		right := strings.ToLower(rows[j].ProviderName + "\x00" + rows[j].ModelName)
		if left == right {
			return rows[i].Id < rows[j].Id
		}
		return left < right
	})
	return rows
}

func modelVariantOptions(variants []opencode.ModelVariant) []gen.ThinkingEffortOption {
	ids := make([]string, 0, len(variants))
	seen := make(map[string]bool, len(variants))
	for _, variant := range variants {
		if variant.ID == "" || seen[variant.ID] {
			continue
		}
		seen[variant.ID] = true
		ids = append(ids, variant.ID)
	}
	preferred := map[string]int{"minimal": 0, "none": 1, "low": 2, "medium": 3, "high": 4, "xhigh": 5, "max": 6}
	sort.Slice(ids, func(i, j int) bool {
		left, leftKnown := preferred[ids[i]]
		right, rightKnown := preferred[ids[j]]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown {
			return left < right
		}
		return ids[i] < ids[j]
	})
	options := make([]gen.ThinkingEffortOption, 0, len(ids))
	for _, id := range ids {
		options = append(options, gen.ThinkingEffortOption{Value: id, Label: formatThinkingEffortLabel(id)})
	}
	return options
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
	for i, row := range rows {
		cloned[i] = row
		cloned[i].ThinkingEffortOptions = append([]gen.ThinkingEffortOption{}, row.ThinkingEffortOptions...)
		if row.ThinkingEffortDefault != nil {
			value := *row.ThinkingEffortDefault
			cloned[i].ThinkingEffortDefault = &value
		}
	}
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
