package api

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
)

func TestParseOpenCodeVerboseModels(t *testing.T) {
	t.Parallel()

	raw := []byte(`opencode/gpt-5.3-codex
{
  "id": "gpt-5.3-codex",
  "providerID": "github-copilot",
  "name": "GPT-5.3-Codex"
}
opencode/claude-sonnet-4.6
{
  "id": "claude-sonnet-4.6",
  "providerID": "github-copilot",
  "name": "Claude Sonnet 4.6"
}
`)

	items, err := parseOpenCodeVerboseModels(raw)
	if err != nil {
		t.Fatalf("parse models: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 models, got %d", len(items))
	}
	if items[0].ProviderName != "GitHub Copilot" {
		t.Fatalf("expected provider label formatting, got %q", items[0].ProviderName)
	}
	if items[0].ModelName == "" || items[1].ModelName == "" {
		t.Fatalf("expected model names to be populated, got %#v", items)
	}
}

func TestParseOpenCodeVerboseModelsFailsOnMalformedRow(t *testing.T) {
	t.Parallel()

	_, err := parseOpenCodeVerboseModels([]byte(`
opencode/gpt-5.3-codex
{
  "id": "gpt-5.3-codex",
  "providerID": "github-copilot",
  "name": "GPT-5.3-Codex",
}
`))
	if err == nil {
		t.Fatal("expected parse error for malformed provider model row")
	}
}

func TestParseOpenCodeVerboseModelsFailsOnNonEmptyUnparsableOutput(t *testing.T) {
	t.Parallel()

	_, err := parseOpenCodeVerboseModels([]byte("warning: provider model cache refresh failed\n"))
	if err == nil {
		t.Fatal("expected parse error when output is non-empty but no rows parsed")
	}
}

func TestOpenCodeProviderModelCatalogListDeduplicatesConcurrentMisses(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	catalog := &openCodeProviderModelCatalog{
		binaryPath:   "opencode",
		now:          time.Now,
		ttl:          60 * time.Second,
		fetchTimeout: 2 * time.Second,
		execVerbose: func(_ context.Context, _ string, _ bool) ([]byte, error) {
			calls.Add(1)
			time.Sleep(75 * time.Millisecond)
			return []byte(`
{
  "id": "gpt-5.3-codex",
  "providerID": "github-copilot",
  "name": "GPT-5.3-Codex"
}
`), nil
		},
	}

	const workers = 8
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := catalog.List(context.Background(), false)
			if err != nil {
				errCh <- err
				return
			}
			if len(items) != 1 {
				errCh <- fmt.Errorf("expected 1 item, got %d", len(items))
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("list provider models concurrently: %v", err)
	}

	if calls.Load() != 1 {
		t.Fatalf("expected one provider command invocation, got %d", calls.Load())
	}
}

func TestOpenCodeProviderModelCatalogListTimesOutBoundedFetch(t *testing.T) {
	t.Parallel()

	catalog := &openCodeProviderModelCatalog{
		binaryPath:   "opencode",
		now:          time.Now,
		ttl:          60 * time.Second,
		fetchTimeout: 25 * time.Millisecond,
		execVerbose: func(ctx context.Context, _ string, _ bool) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	_, err := catalog.List(context.Background(), true)
	if err == nil {
		t.Fatal("expected timeout error for provider model catalog fetch")
	}
	if !isProviderModelCatalogUnavailable(err) {
		t.Fatalf("expected provider catalog unavailable classification, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestOpenCodeProviderModelCatalogSnapshotReturnsStaleCacheOnFetchFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	catalog := &openCodeProviderModelCatalog{
		binaryPath: "opencode",
		now: func() time.Time {
			return now.Add(2 * time.Minute)
		},
		ttl:          60 * time.Second,
		fetchTimeout: 25 * time.Millisecond,
		cachedAt:     now,
		cachedRows: []gen.ProviderModel{{
			Id:           "github-copilot/gpt-5.3-codex",
			ProviderId:   "github-copilot",
			ProviderName: "GitHub Copilot",
			ModelName:    "GPT-5.3-Codex",
		}},
		execVerbose: func(ctx context.Context, _ string, _ bool) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	snapshot, err := catalog.Snapshot(context.Background(), false)
	if err != nil {
		t.Fatalf("snapshot provider models: %v", err)
	}
	if snapshot.Status != ProviderModelCatalogStatusStale {
		t.Fatalf("expected stale provider model catalog status, got %q", snapshot.Status)
	}
	if len(snapshot.Items) != 1 {
		t.Fatalf("expected stale snapshot to reuse cached rows, got %#v", snapshot.Items)
	}
	if snapshot.FailureReason != ProviderModelCatalogFailureReasonTimeout {
		t.Fatalf("expected timeout failure reason, got %q", snapshot.FailureReason)
	}
	// The message reaches the model selector verbatim, so it has to read as
	// operator guidance rather than as the Go error that caused it.
	if strings.Contains(snapshot.FailureMessage, "context deadline exceeded") {
		t.Fatalf("expected operator guidance, got raw error text: %q", snapshot.FailureMessage)
	}
	if !strings.Contains(snapshot.FailureMessage, "already had") {
		t.Fatalf("expected the message to say cached models are still in use, got %q", snapshot.FailureMessage)
	}
}

func TestOpenCodeProviderModelCatalogSnapshotExplainsUnavailableWithoutRawError(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name      string
		execError error
		reason    ProviderModelCatalogFailureReason
	}{
		{"timeout", context.DeadlineExceeded, ProviderModelCatalogFailureReasonTimeout},
		{"binary missing", exec.ErrNotFound, ProviderModelCatalogFailureReasonBinaryMissing},
		{"command failed", errors.New("exit status 1"), ProviderModelCatalogFailureReasonCommandFailed},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			catalog := &openCodeProviderModelCatalog{
				binaryPath:   "opencode",
				now:          time.Now,
				ttl:          60 * time.Second,
				fetchTimeout: 25 * time.Millisecond,
				execVerbose: func(ctx context.Context, _ string, _ bool) ([]byte, error) {
					if errors.Is(testCase.execError, context.DeadlineExceeded) {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, testCase.execError
				},
			}

			snapshot, err := catalog.Snapshot(context.Background(), true)
			if err != nil {
				t.Fatalf("snapshot provider models: %v", err)
			}
			if snapshot.Status != ProviderModelCatalogStatusUnavailable {
				t.Fatalf("expected unavailable status, got %q", snapshot.Status)
			}
			if snapshot.FailureReason != testCase.reason {
				t.Fatalf("expected %q failure reason, got %q", testCase.reason, snapshot.FailureReason)
			}
			if snapshot.FailureMessage == "" {
				t.Fatal("expected an operator-facing failure message")
			}
			// Raw causes are diagnostics, not operator copy. They must not be
			// what the model selector shows.
			for _, leak := range []string{"context deadline exceeded", "executable file not found", "exit status"} {
				if strings.Contains(snapshot.FailureMessage, leak) {
					t.Fatalf("expected operator guidance, got raw error text %q in %q", leak, snapshot.FailureMessage)
				}
			}
		})
	}
}
