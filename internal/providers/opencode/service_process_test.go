package opencode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// This opt-in check runs stock OpenCode with entirely disposable account,
// configuration, and project directories. It discovers local models without
// invoking a provider or reading the developer's OpenCode accounts.
func TestServiceStockOpenCodeDiscovery(t *testing.T) {
	binary := os.Getenv("KINGDOM_OPENCODE_V2_TEST_BINARY")
	root := os.Getenv("KINGDOM_OPENCODE_V2_TEST_ROOT")
	if binary == "" || root == "" {
		t.Skip("set KINGDOM_OPENCODE_V2_TEST_BINARY and KINGDOM_OPENCODE_V2_TEST_ROOT for isolated stock verification")
	}
	for _, directory := range []string{"home", "config", "data", "cache", "state", "tmp", "project-a", "project-b", "runtime"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		http.Error(w, "discovery must not invoke the model", http.StatusForbidden)
	}))
	defer provider.Close()
	for _, project := range []string{"project-a", "project-b"} {
		config := map[string]any{"providers": map[string]any{
			project: map[string]any{
				"name":     "Local " + project,
				"package":  "@opencode/ai/providers/openai-compatible",
				"settings": map[string]any{"baseURL": provider.URL + "/v1", "encoding": "none"},
				"models": map[string]any{"custom-model": map[string]any{
					"name":         "Custom " + project,
					"capabilities": map[string]any{"tools": false, "input": []string{"text"}, "output": []string{"text"}},
					"limit":        map[string]any{"context": 32000, "output": 4096},
				}},
			},
		}}
		data, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, project, "opencode.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(root, "home"),
		"TMPDIR=" + filepath.Join(root, "tmp"), "LANG=en_US.UTF-8",
		"XDG_CONFIG_HOME=" + filepath.Join(root, "config"), "XDG_DATA_HOME=" + filepath.Join(root, "data"),
		"XDG_CACHE_HOME=" + filepath.Join(root, "cache"), "XDG_STATE_HOME=" + filepath.Join(root, "state"),
		"OPENCODE_CONFIG_DIR=" + filepath.Join(root, "config"), "OPENCODE_DB=" + filepath.Join(root, "opencode.db"),
		"OPENCODE_DISABLE_MODELS_FETCH=1", "OPENCODE_DISABLE_FFF=1",
		`OPENCODE_CONFIG_CONTENT={/* Existing OpenCode JSONC. */"formatter":false,}`,
	}
	service, err := NewService(ServiceConfig{BinaryPath: binary, WorkDir: filepath.Join(root, "project-a"), RuntimeRoot: filepath.Join(root, "runtime"), Env: env})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.CloseContext(ctx); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := service.Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"project-a", "project-b"} {
		directory, err := filepath.EvalSymlinks(filepath.Join(root, project))
		if err != nil {
			t.Fatal(err)
		}
		models, err := client.Models(ctx, directory)
		if err != nil {
			t.Fatalf("discover %s: %v", project, err)
		}
		found := false
		for _, model := range models {
			if model.ProviderID == project && model.ID == "custom-model" && model.Name == "Custom "+project {
				found = true
			}
		}
		if !found {
			t.Fatalf("custom model missing for %s: %+v", project, models)
		}
		t.Logf("discovered %s/custom-model in %s", project, directory)
	}
	if providerCalls.Load() != 0 {
		t.Fatalf("discovery invoked model endpoint %d times", providerCalls.Load())
	}
	t.Log("stock service readiness, effective formatting, location-specific model discovery, and zero provider calls verified")
}
