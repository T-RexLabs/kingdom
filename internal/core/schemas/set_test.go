package schemas

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestFixtureCorpus(t *testing.T) {
	t.Parallel()

	set := loadDefaultSchemaSet(t)
	fixtureRoot := repoRootPath(t, "contracts", "fixtures")

	for _, descriptor := range Required {
		descriptor := descriptor
		t.Run(descriptor.Name, func(t *testing.T) {
			t.Parallel()

			runFixtureGroup(t, set, descriptor.Name, filepath.Join(fixtureRoot, descriptor.Name, "valid"), true)
			runFixtureGroup(t, set, descriptor.Name, filepath.Join(fixtureRoot, descriptor.Name, "invalid"), false)
		})
	}
}

func TestVersionFieldsAreRequiredAndValidated(t *testing.T) {
	t.Parallel()

	set := loadDefaultSchemaSet(t)

	testCases := []struct {
		name       string
		schemaName string
		json       string
	}{
		{
			name:       "workflow_missing_version",
			schemaName: "workflow",
			json:       `{"workflow_id":"work-order-v1","nodes":[{"node_id":"work_intake","required_role":"governance","node_class":"work_intake"}],"edges":[]}`,
		},
		{
			name:       "workflow_wrong_version",
			schemaName: "workflow",
			json:       `{"version":"v2","workflow_id":"work-order-v1","nodes":[{"node_id":"work_intake","required_role":"governance","node_class":"work_intake"}],"edges":[]}`,
		},
		{
			name:       "event_missing_version",
			schemaName: "event_v1",
			json:       `{"event_id":"evt_1","run_id":"run_1","run_event_seq":1,"node_run_id":"nr_1","attempt_id":"att_1","seq":1,"created_at":"2026-03-01T12:00:00Z","event_type":"attempt.progress","payload_json":{},"actor_id":"actor_1","role_id":"governance","authority_source":"runtime.event_bridge","reason_code":"stream_event","reason_text":"stream event ingested","correlation_id":"corr_1","causation_id":"caus_1"}`,
		},
		{
			name:       "event_wrong_version",
			schemaName: "event_v1",
			json:       `{"event_id":"evt_1","schema_version":"v2","run_id":"run_1","run_event_seq":1,"node_run_id":"nr_1","attempt_id":"att_1","seq":1,"created_at":"2026-03-01T12:00:00Z","event_type":"attempt.progress","payload_json":{},"actor_id":"actor_1","role_id":"governance","authority_source":"runtime.event_bridge","reason_code":"stream_event","reason_text":"stream event ingested","correlation_id":"corr_1","causation_id":"caus_1"}`,
		},
		{
			name:       "retry_policy_missing_version",
			schemaName: "retry_policy",
			json:       `{"policy_id":"retry-policy-mvp-v1","defaults":{"jitter_mode":"full"},"classes":{"non_retryable":{"max_retries":0,"escalation":{"target_role":"coordinator"}}},"error_code_map":{"invalid_output_schema":"non_retryable"}}`,
		},
		{
			name:       "retry_policy_wrong_version",
			schemaName: "retry_policy",
			json:       `{"version":"v2","policy_id":"retry-policy-mvp-v1","defaults":{"jitter_mode":"full"},"classes":{"non_retryable":{"max_retries":0,"escalation":{"target_role":"coordinator"}}},"error_code_map":{"invalid_output_schema":"non_retryable"}}`,
		},
		{
			name:       "worker_output_missing_version",
			schemaName: "worker_output",
			json:       `{"status":"succeeded","summary":"done","payload_json":{},"artifacts":[],"error_reason":null}`,
		},
		{
			name:       "worker_output_wrong_version",
			schemaName: "worker_output",
			json:       `{"schema_version":"v2","status":"succeeded","summary":"done","payload_json":{},"artifacts":[],"error_reason":null}`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := set.ValidateJSONBytes(tc.schemaName, []byte(tc.json))
			if err == nil {
				t.Fatalf("expected validation to fail for %s", tc.name)
			}
		})
	}
}

func TestLoadFailsDeterministicallyOnInvalidSchemaSet(t *testing.T) {
	t.Parallel()

	originalSchemaDir := repoRootPath(t, "contracts", "schemas")
	tempSchemaDir := t.TempDir()

	for _, descriptor := range Required {
		sourcePath := filepath.Join(originalSchemaDir, descriptor.FileName)
		destinationPath := filepath.Join(tempSchemaDir, descriptor.FileName)
		contents, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatalf("read schema %s: %v", sourcePath, err)
		}
		if err := os.WriteFile(destinationPath, contents, 0o644); err != nil {
			t.Fatalf("write schema %s: %v", destinationPath, err)
		}
	}

	brokenFile := filepath.Join(tempSchemaDir, "event_v1.schema.json")
	if err := os.WriteFile(brokenFile, []byte("{ invalid json"), 0o644); err != nil {
		t.Fatalf("write broken schema: %v", err)
	}

	_, err := Load(tempSchemaDir)
	if err == nil {
		t.Fatal("expected schema loading to fail")
	}

	if !strings.Contains(err.Error(), "event_v1") || !strings.Contains(err.Error(), "event_v1.schema.json") {
		t.Fatalf("expected deterministic error to identify event_v1 schema, got: %v", err)
	}
}

func runFixtureGroup(t *testing.T, set *Set, schemaName string, fixtureDir string, shouldPass bool) {
	t.Helper()

	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("read fixture directory %s: %v", fixtureDir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)

	if len(files) == 0 {
		t.Fatalf("no JSON fixtures found in %s", fixtureDir)
	}

	for _, fileName := range files {
		fileName := fileName
		t.Run(fileName, func(t *testing.T) {
			t.Parallel()

			raw, err := os.ReadFile(filepath.Join(fixtureDir, fileName))
			if err != nil {
				t.Fatalf("read fixture %s: %v", fileName, err)
			}

			err = set.ValidateJSONBytes(schemaName, raw)
			if shouldPass && err != nil {
				t.Fatalf("expected fixture to pass, got error: %v", err)
			}
			if !shouldPass && err == nil {
				t.Fatal("expected fixture to fail")
			}
		})
	}
}

func loadDefaultSchemaSet(t *testing.T) *Set {
	t.Helper()

	set, err := Load(repoRootPath(t, "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}

	return set
}

func repoRootPath(t *testing.T, parts ...string) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	allParts := append([]string{root}, parts...)
	return filepath.Join(allParts...)
}
