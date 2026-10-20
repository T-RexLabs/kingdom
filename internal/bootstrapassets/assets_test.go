package bootstrapassets

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/dbmigrations"
)

func newScaffoldPaths(t *testing.T) (projectRoot string, kingdomHome string) {
	t.Helper()
	projectRoot = t.TempDir()
	kingdomHome = filepath.Join(projectRoot, ".kingdom")
	return projectRoot, kingdomHome
}

func TestWriteScaffoldDoesNotOverwriteWithoutForce(t *testing.T) {
	t.Parallel()

	_, kingdomHome := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, false); err != nil {
		t.Fatalf("initial scaffold write: %v", err)
	}

	rolePath := filepath.Join(kingdomHome, "policies", "role_policy.yaml")
	if err := os.WriteFile(rolePath, []byte("custom"), 0o644); err != nil {
		t.Fatalf("write override file: %v", err)
	}

	if err := WriteScaffold(kingdomHome, false); err != nil {
		t.Fatalf("second scaffold write: %v", err)
	}

	raw, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read role policy: %v", err)
	}
	if string(raw) != "custom" {
		t.Fatalf("expected existing policy preserved, got %q", string(raw))
	}

	if err := WriteScaffold(kingdomHome, true); err != nil {
		t.Fatalf("forced scaffold write: %v", err)
	}

	raw, err = os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read forced role policy: %v", err)
	}
	if string(raw) == "custom" {
		t.Fatalf("expected force to overwrite policy")
	}
}

func TestBootstrapCopiesStayInSyncWithCanonicalAssets(t *testing.T) {
	t.Parallel()

	for bootstrapDir, canonicalDir := range canonicalAssetDirs {
		bootstrapDir := bootstrapDir
		canonicalDir := canonicalDir
		t.Run(bootstrapDir, func(t *testing.T) {
			t.Parallel()
			entries, err := listAssetFiles(bootstrapDir, "")
			if err != nil {
				t.Fatalf("list asset files: %v", err)
			}
			for _, name := range entries {
				name := name
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					canonicalPath := filepath.Join("..", "..", canonicalDir, name)
					bootstrapPath := filepath.Join(bootstrapDir, name)

					canonicalRaw, err := os.ReadFile(canonicalPath)
					if err != nil {
						t.Fatalf("read canonical asset: %v", err)
					}
					bootstrapRaw, err := os.ReadFile(bootstrapPath)
					if err != nil {
						t.Fatalf("read bootstrap asset: %v", err)
					}
					if !bytes.Equal(canonicalRaw, bootstrapRaw) {
						t.Fatalf("bootstrap asset %s must stay byte-identical to canonical source %s", bootstrapPath, canonicalPath)
					}
				})
			}
		})
	}
}

func TestScaffoldedProviderPolicyLoadsAgainstScaffoldedSchema(t *testing.T) {
	t.Parallel()

	_, kingdomHome := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	policyPath := filepath.Join(kingdomHome, "policies", "opencode_provider_policy.yaml")
	schemaPath := filepath.Join(kingdomHome, "schemas", "opencode_provider_policy.schema.json")
	if _, err := providerpolicy.LoadPolicyFromFile(policyPath, schemaPath); err != nil {
		t.Fatalf("load scaffolded provider policy: %v", err)
	}
}

func TestWriteScaffoldLeavesOpenCodeConfigurationToOpenCode(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	for _, path := range []string{
		filepath.Join(projectRoot, ".opencode"),
		filepath.Join(kingdomHome, "opencode"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected scaffold not to create %s, stat err=%v", path, err)
		}
	}
}

func TestFreshScaffoldContainsCanonicalMigrations(t *testing.T) {
	t.Parallel()
	_, kingdomHome := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, false); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(kingdomHome, "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"0001_initial_schema.up.sql",
		"0002_events_execution_activity_index.up.sql",
		"0003_external_sources_and_idempotency_retention.up.sql",
		"0004_resumable_workspace_tree_cleanup.up.sql",
		"0005_agent_voice_and_guidance.up.sql",
		"0006_planning_session_owned_cascades.up.sql",
		"0007_provider_request_ownership.up.sql",
	}
	if len(entries) != len(want) {
		t.Fatalf("fresh scaffold has %d migrations, want %d", len(entries), len(want))
	}
	for i, name := range want {
		if entries[i].Name() != name {
			t.Fatalf("migration %d = %q, want %q", i, entries[i].Name(), name)
		}
		got, err := os.ReadFile(filepath.Join(kingdomHome, "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, canonical) {
			t.Fatalf("scaffold migration %s differs from canonical SQL", name)
		}
	}
}
