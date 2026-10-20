package api

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/providers/opencode"
)

type modelSourceFunc func(context.Context) ([]opencode.Model, error)

func (f modelSourceFunc) Models(ctx context.Context) ([]opencode.Model, error) { return f(ctx) }

func newTestModelCatalog(source modelSourceFunc) *openCodeProviderModelCatalog {
	return NewOpenCodeProviderModelCatalog(source, time.Now, time.Minute).(*openCodeProviderModelCatalog)
}

func TestProviderModelsPreservesCustomIDsAndExactVariants(t *testing.T) {
	t.Parallel()
	models := []opencode.Model{
		{ProviderID: "local-lab", ProviderName: "My Local Endpoint", ID: "organization/model-a", Name: "Local Model", Variants: []opencode.ModelVariant{{ID: "careful-mode"}, {ID: "medium"}, {ID: "low"}, {ID: "careful-mode"}, {ID: ""}}},
		{ProviderID: "openai", ID: "model-b", Name: "Model B"},
		{ProviderID: "local-lab", ID: "organization/model-a", Name: "Duplicate"},
	}
	rows := providerModels(models)
	if len(rows) != 2 {
		t.Fatalf("rows=%#v", rows)
	}
	row := rows[0]
	if row.Id != "local-lab/organization/model-a" || row.ProviderId != "local-lab" || row.ProviderName != "My Local Endpoint" || row.ModelName != "Local Model" {
		t.Fatalf("custom model changed: %#v", row)
	}
	want := []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}, {Value: "medium", Label: "Medium"}, {Value: "careful-mode", Label: "Careful Mode"}}
	if !reflect.DeepEqual(row.ThinkingEffortOptions, want) {
		t.Fatalf("variant IDs changed: %#v", row.ThinkingEffortOptions)
	}
	if row.ThinkingEffortDefault == nil || *row.ThinkingEffortDefault != "medium" {
		t.Fatalf("default=%v", row.ThinkingEffortDefault)
	}
	if rows[1].ProviderName != "OpenAI" || rows[1].ThinkingEffortDefault != nil || len(rows[1].ThinkingEffortOptions) != 0 {
		t.Fatalf("nonvariant model=%#v", rows[1])
	}
}

func TestProviderModelsSortsDeterministicallyAndDefaultsNames(t *testing.T) {
	t.Parallel()
	rows := providerModels([]opencode.Model{{ProviderID: "custom", ID: "z", Name: "Same"}, {ProviderID: "custom", ID: "a", Name: "Same"}, {ProviderID: "github-copilot", ID: "unlabelled"}})
	if rows[0].Id != "custom/a" || rows[1].Id != "custom/z" || rows[2].ModelName != "unlabelled" || rows[2].ProviderName != "GitHub Copilot" {
		t.Fatalf("rows=%#v", rows)
	}
}

func TestModelVariantOptionsOrdersKnownAndCustomIDs(t *testing.T) {
	t.Parallel()
	variants := []opencode.ModelVariant{{ID: "zeta"}, {ID: "max"}, {ID: "none"}, {ID: "xhigh"}, {ID: "minimal"}, {ID: "alpha"}, {ID: "none"}}
	options := modelVariantOptions(variants)
	var ids []string
	for _, option := range options {
		ids = append(ids, option.Value)
	}
	if !reflect.DeepEqual(ids, []string{"minimal", "none", "xhigh", "max", "alpha", "zeta"}) {
		t.Fatalf("options=%#v", options)
	}
	if selected := thinkingEffortDefaultForOptions(modelVariantOptions([]opencode.ModelVariant{{ID: "custom-profile"}})); selected == nil || *selected != "custom-profile" {
		t.Fatalf("custom default=%v", selected)
	}
}

func TestOpenCodeProviderModelCatalogListDeduplicatesConcurrentMisses(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	catalog := newTestModelCatalog(func(ctx context.Context) ([]opencode.Model, error) {
		calls.Add(1)
		select {
		case <-time.After(75 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []opencode.Model{{ProviderID: "openai", ID: "ready"}}, nil
	})
	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := catalog.List(context.Background(), false)
			if err != nil {
				errs <- err
				return
			}
			if len(rows) != 1 {
				errs <- fmt.Errorf("rows=%d", len(rows))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("service calls=%d", calls.Load())
	}
}

func TestOpenCodeProviderModelCatalogListTimesOutBoundedFetch(t *testing.T) {
	t.Parallel()
	catalog := newTestModelCatalog(func(ctx context.Context) ([]opencode.Model, error) { <-ctx.Done(); return nil, ctx.Err() })
	catalog.fetchTimeout = 25 * time.Millisecond
	_, err := catalog.List(context.Background(), true)
	if !isProviderModelCatalogUnavailable(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout=%v", err)
	}
}

func TestOpenCodeProviderModelCatalogCachesSuccessfulEmptySnapshot(t *testing.T) {
	t.Parallel()
	var calls int
	catalog := newTestModelCatalog(func(context.Context) ([]opencode.Model, error) { calls++; return nil, nil })
	for range 2 {
		snapshot, err := catalog.Snapshot(context.Background(), false)
		if err != nil || snapshot.Status != ProviderModelCatalogStatusFresh || snapshot.Items == nil || len(snapshot.Items) != 0 || snapshot.LastSuccessAt == nil {
			t.Fatalf("snapshot=%#v error=%v", snapshot, err)
		}
	}
	if calls != 1 {
		t.Fatalf("empty snapshot was fetched %d times", calls)
	}
}

func TestOpenCodeProviderModelCatalogSnapshotServesCacheWhileRevalidating(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	catalog := newTestModelCatalog(func(ctx context.Context) ([]opencode.Model, error) {
		select {
		case <-release:
			return []opencode.Model{{ProviderID: "openai", ID: "new"}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	catalog.cachedAt = time.Now().Add(-2 * time.Minute)
	catalog.cachedRows = []gen.ProviderModel{cachedProviderModel()}
	catalog.refreshed = make(chan struct{}, 1)
	snapshot, err := catalog.Snapshot(context.Background(), false)
	if err != nil || snapshot.Status != ProviderModelCatalogStatusFresh || len(snapshot.Items) != 1 || snapshot.Items[0].Id != "github-copilot/gpt-5.3-codex" {
		t.Fatalf("cached snapshot=%#v error=%v", snapshot, err)
	}
	close(release)
	awaitBackgroundRefresh(t, catalog)
	snapshot, err = catalog.Snapshot(context.Background(), false)
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Id != "openai/new" {
		t.Fatalf("refreshed snapshot=%#v error=%v", snapshot, err)
	}
}

func TestOpenCodeProviderModelCatalogFailedRevalidationPreservesCache(t *testing.T) {
	t.Parallel()
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			catalog := newTestModelCatalog(func(context.Context) ([]opencode.Model, error) { return nil, context.DeadlineExceeded })
			catalog.cachedAt = time.Now().Add(-2 * time.Minute)
			catalog.cachedRows = []gen.ProviderModel{}
			if !empty {
				catalog.cachedRows = append(catalog.cachedRows, cachedProviderModel())
			}
			catalog.refreshed = make(chan struct{}, 1)
			if _, err := catalog.Snapshot(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			awaitBackgroundRefresh(t, catalog)
			snapshot, err := catalog.Snapshot(context.Background(), false)
			if err != nil || snapshot.Status != ProviderModelCatalogStatusStale || snapshot.FailureReason != ProviderModelCatalogFailureReasonTimeout || snapshot.LastSuccessAt == nil {
				t.Fatalf("snapshot=%#v error=%v", snapshot, err)
			}
			want := 1
			if empty {
				want = 0
			}
			if len(snapshot.Items) != want || !strings.Contains(snapshot.FailureMessage, "already had") {
				t.Fatalf("cached rows/message=%#v", snapshot)
			}
		})
	}
}

func TestOpenCodeProviderModelCatalogExplicitRefreshAndRecovery(t *testing.T) {
	t.Parallel()
	var calls int
	catalog := newTestModelCatalog(func(context.Context) ([]opencode.Model, error) {
		calls++
		if calls == 2 {
			return nil, &opencode.ServiceError{Operation: "models", StatusCode: 503}
		}
		return []opencode.Model{{ProviderID: "openai", ID: fmt.Sprint(calls)}}, nil
	})
	first, err := catalog.Snapshot(context.Background(), false)
	if err != nil || first.Status != ProviderModelCatalogStatusFresh {
		t.Fatalf("first=%#v error=%v", first, err)
	}
	stale, err := catalog.Snapshot(context.Background(), true)
	if err != nil || stale.Status != ProviderModelCatalogStatusStale || stale.Items[0].Id != "openai/1" || stale.FailureReason != ProviderModelCatalogFailureReasonServiceUnavailable {
		t.Fatalf("stale=%#v error=%v", stale, err)
	}
	recovered, err := catalog.Snapshot(context.Background(), true)
	if err != nil || recovered.Status != ProviderModelCatalogStatusFresh || recovered.Items[0].Id != "openai/3" || recovered.FailureReason != "" {
		t.Fatalf("recovered=%#v error=%v", recovered, err)
	}
}

func TestOpenCodeProviderModelCatalogSnapshotExplainsUnavailableWithoutRawError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		err    error
		reason ProviderModelCatalogFailureReason
	}{
		{"timeout", context.DeadlineExceeded, ProviderModelCatalogFailureReasonTimeout},
		{"response body timeout", &opencode.ProtocolError{Operation: "models", Cause: context.DeadlineExceeded}, ProviderModelCatalogFailureReasonTimeout},
		{"missing binary", exec.ErrNotFound, ProviderModelCatalogFailureReasonBinaryMissing},
		{"service", &opencode.ServiceError{Operation: "models", StatusCode: 503}, ProviderModelCatalogFailureReasonServiceUnavailable},
		{"protocol", &opencode.ProtocolError{Operation: "models", Cause: errors.New("fake sensitive body")}, ProviderModelCatalogFailureReasonParseFailed},
		{"configuration", &opencode.ConfigurationError{}, ProviderModelCatalogFailureReasonConfigurationInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalog := newTestModelCatalog(func(context.Context) ([]opencode.Model, error) { return nil, tc.err })
			snapshot, err := catalog.Snapshot(context.Background(), true)
			if err != nil || snapshot.Status != ProviderModelCatalogStatusUnavailable || snapshot.FailureReason != tc.reason || snapshot.FailureMessage == "" || !errors.Is(snapshot.UnavailableErr, tc.err) {
				t.Fatalf("snapshot=%#v error=%v", snapshot, err)
			}
			for _, leak := range []string{"context deadline exceeded", "executable file not found", "HTTP 503", "fake sensitive body"} {
				if strings.Contains(snapshot.FailureMessage, leak) {
					t.Fatalf("raw diagnostics exposed: %q", snapshot.FailureMessage)
				}
			}
		})
	}
}

func TestOpenCodeProviderModelCatalogSnapshotsDoNotShareMutableOptions(t *testing.T) {
	t.Parallel()
	catalog := newTestModelCatalog(func(context.Context) ([]opencode.Model, error) {
		return []opencode.Model{{ProviderID: "openai", ID: "model", Variants: []opencode.ModelVariant{{ID: "medium"}}}}, nil
	})
	first, err := catalog.Snapshot(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	first.Items[0].ThinkingEffortOptions[0].Value = "changed"
	*first.Items[0].ThinkingEffortDefault = "changed"
	second, err := catalog.List(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if second[0].ThinkingEffortOptions[0].Value != "medium" || *second[0].ThinkingEffortDefault != "medium" {
		t.Fatalf("caller mutated cache: %#v", second[0])
	}
}

func cachedProviderModel() gen.ProviderModel {
	return gen.ProviderModel{Id: "github-copilot/gpt-5.3-codex", ProviderId: "github-copilot", ProviderName: "GitHub Copilot", ModelName: "GPT-5.3-Codex"}
}
func awaitBackgroundRefresh(t *testing.T, catalog *openCodeProviderModelCatalog) {
	t.Helper()
	select {
	case <-catalog.refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("background model refresh did not complete")
	}
}
