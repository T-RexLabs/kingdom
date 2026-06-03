package bootstrapassets

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/core/providerpolicy"
)

func newScaffoldPaths(t *testing.T) (projectRoot string, kingdomHome string, overlayRoot string) {
	t.Helper()
	projectRoot = t.TempDir()
	kingdomHome = filepath.Join(projectRoot, kingdomDirName)
	overlayRoot = filepath.Join(kingdomHome, opencodeDirName)
	return projectRoot, kingdomHome, overlayRoot
}

func TestWriteScaffoldDoesNotOverwriteWithoutForce(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("initial scaffold write: %v", err)
	}

	rolePath := filepath.Join(kingdomHome, "policies", "role_policy.yaml")
	if err := os.WriteFile(rolePath, []byte("custom"), 0o644); err != nil {
		t.Fatalf("write override file: %v", err)
	}

	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("second scaffold write: %v", err)
	}

	raw, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read role policy: %v", err)
	}
	if string(raw) != "custom" {
		t.Fatalf("expected existing policy preserved, got %q", string(raw))
	}

	if err := WriteScaffold(kingdomHome, projectRoot, true); err != nil {
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

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	policyPath := filepath.Join(kingdomHome, "policies", "opencode_provider_policy.yaml")
	schemaPath := filepath.Join(kingdomHome, "schemas", "opencode_provider_policy.schema.json")
	if _, err := providerpolicy.LoadPolicyFromFile(policyPath, schemaPath); err != nil {
		t.Fatalf("load scaffolded provider policy: %v", err)
	}
}

func TestWriteScaffoldIncludesOpenCodeAgentProfiles(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	for _, rel := range []string{
		filepath.Join("opencode", "agents", "advisor.md"),
		filepath.Join("opencode", "agents", "builder.md"),
		filepath.Join("opencode", "agents", "chancellor.md"),
		filepath.Join("opencode", "agents", "council.md"),
		filepath.Join("opencode", "agents", "king.md"),
		filepath.Join("opencode", "agents", "nightraven.md"),
		filepath.Join("opencode", "agents", "planner.md"),
		filepath.Join("opencode", "agents", "qa_scout.md"),
		filepath.Join("opencode", "agents", "reviewer.md"),
		filepath.Join("opencode", "agents", "tester.md"),
	} {
		if _, err := os.Stat(filepath.Join(kingdomHome, rel)); err != nil {
			t.Fatalf("expected scaffold file %s: %v", rel, err)
		}
	}
}

func TestWriteScaffoldIncludesManagedOpenCodeCompletionTool(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	toolPath := filepath.Join(kingdomHome, "opencode", "tools", "kingdom_complete_attempt.ts")
	raw, err := os.ReadFile(toolPath)
	if err != nil {
		t.Fatalf("read managed completion tool: %v", err)
	}
	content := string(raw)
	for _, token := range []string{"completion_capability", "KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR", "Request canonical Kingdom completion settlement"} {
		if !strings.Contains(content, token) {
			t.Fatalf("expected managed completion tool to include %q", token)
		}
	}
}

func TestValidateManagedOpenCodeAssetsSucceedsAfterScaffold(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	if err := ValidateManagedOpenCodeAgentProfiles(overlayRoot); err != nil {
		t.Fatalf("validate managed opencode agent profiles: %v", err)
	}
	if err := ValidateManagedOpenCodePlannerAssets(overlayRoot); err != nil {
		t.Fatalf("validate managed opencode planner assets: %v", err)
	}
	if err := ValidateManagedOpenCodeTools(overlayRoot); err != nil {
		t.Fatalf("validate managed opencode tools: %v", err)
	}
}

func TestValidateManagedOpenCodeAssetsFailsWhenManagedFileMissing(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	plannerPath := filepath.Join(overlayRoot, "agents", "planner.md")
	if err := os.Remove(plannerPath); err != nil {
		t.Fatalf("remove planner profile: %v", err)
	}

	err := ValidateManagedOpenCodePlannerAssets(overlayRoot)
	if err == nil {
		t.Fatal("expected planner asset validation to fail when planner profile is missing")
	}
	if !strings.Contains(err.Error(), "kd init --force") {
		t.Fatalf("expected actionable planner asset validation error, got %v", err)
	}
}

func TestValidateManagedOpenCodeAssetsFailsWhenManagedFileDrifts(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	toolPath := filepath.Join(overlayRoot, "tools", "kingdom_complete_attempt.ts")
	if err := os.WriteFile(toolPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("overwrite managed tool: %v", err)
	}

	err := ValidateManagedOpenCodePlannerAssets(overlayRoot)
	if err == nil {
		t.Fatal("expected planner asset validation to fail when managed tool drifts")
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected stale asset validation error, got %v", err)
	}
}

func TestValidateManagedOpenCodeAssetsIgnoresUnknownFiles(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	customProfilePath := filepath.Join(overlayRoot, "agents", "custom.md")
	if err := os.WriteFile(customProfilePath, []byte("custom"), 0o644); err != nil {
		t.Fatalf("write custom profile: %v", err)
	}
	customToolPath := filepath.Join(overlayRoot, "tools", "custom.ts")
	if err := os.WriteFile(customToolPath, []byte("custom"), 0o644); err != nil {
		t.Fatalf("write custom tool: %v", err)
	}

	if err := ValidateManagedOpenCodeAgentProfiles(overlayRoot); err != nil {
		t.Fatalf("validate managed profiles with unknown custom file: %v", err)
	}
	if err := ValidateManagedOpenCodePlannerAssets(overlayRoot); err != nil {
		t.Fatalf("validate planner assets with unknown custom file: %v", err)
	}
}

func TestWriteScaffoldDoesNotCreateProjectOpencodeDirectory(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	if _, err := os.Stat(filepath.Join(projectRoot, ".opencode")); !os.IsNotExist(err) {
		t.Fatalf("expected project .opencode to stay untouched, stat err=%v", err)
	}
}

func TestWriteScaffoldIncludesRoleToolCapabilityOverrides(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	assertContains := func(rel string, expected ...string) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(kingdomHome, rel))
		if err != nil {
			t.Fatalf("read scaffold file %s: %v", rel, err)
		}
		content := string(raw)
		for _, token := range expected {
			if !strings.Contains(content, token) {
				t.Fatalf("expected %s to include %q", rel, token)
			}
		}
	}

	assertContains(
		filepath.Join("opencode", "agents", "advisor.md"),
		"list: true",
		"glob: true",
		"grep: true",
		"webfetch: true",
	)
	assertContains(
		filepath.Join("opencode", "agents", "planner.md"),
		"bash: false",
		"write: false",
		"list: true",
		"glob: true",
		"grep: true",
		"webfetch: true",
		"kingdom_complete_attempt",
		"task: false",
		"todowrite: false",
		"todoread: false",
		`"*": deny`,
	)
	assertContains(
		filepath.Join("opencode", "agents", "council.md"),
		"list: true",
		"glob: true",
		"grep: true",
		"webfetch: true",
		"todowrite: true",
	)
	assertContains(
		filepath.Join("opencode", "agents", "reviewer.md"),
		"bash: false",
		"list: true",
		"glob: true",
		"grep: true",
	)
	assertContains(
		filepath.Join("opencode", "agents", "qa_scout.md"),
		"bash: true",
		"Validate implemented behavior against decree requirements",
	)
	assertContains(
		filepath.Join("opencode", "agents", "nightraven.md"),
		"bash: true",
		"watcher of trust boundaries",
	)
	assertContains(
		filepath.Join("opencode", "agents", "tester.md"),
		"bash: true",
		"list: true",
		"glob: true",
		"grep: true",
	)
}

func TestWriteScaffoldNonBuilderProfilesDenyMarkdownEdits(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	nonBuilderProfiles := []string{
		filepath.Join("opencode", "agents", "advisor.md"),
		filepath.Join("opencode", "agents", "council.md"),
		filepath.Join("opencode", "agents", "nightraven.md"),
		filepath.Join("opencode", "agents", "planner.md"),
		filepath.Join("opencode", "agents", "qa_scout.md"),
		filepath.Join("opencode", "agents", "reviewer.md"),
		filepath.Join("opencode", "agents", "tester.md"),
		filepath.Join("opencode", "agents", "king.md"),
		filepath.Join("opencode", "agents", "chancellor.md"),
	}

	for _, rel := range nonBuilderProfiles {
		raw, err := os.ReadFile(filepath.Join(kingdomHome, rel))
		if err != nil {
			t.Fatalf("read scaffold file %s: %v", rel, err)
		}
		content := string(raw)
		if !strings.Contains(content, `"*": deny`) {
			t.Fatalf("expected %s to deny all edits by default", rel)
		}
		if strings.Contains(content, `"*.md": allow`) {
			t.Fatalf("expected %s to disallow markdown edit exceptions", rel)
		}
	}
}

func TestWriteScaffoldTesterProfileStaysValidationOnly(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(kingdomHome, "opencode", "agents", "tester.md"))
	if err != nil {
		t.Fatalf("read tester profile: %v", err)
	}

	content := string(raw)
	if strings.Contains(content, "Write comprehensive test suites") {
		t.Fatalf("tester profile must not instruct a no-write role to author test suites")
	}
	for _, expected := range []string{
		"Execute resilience-focused validation commands and drills",
		"Report defects with clear reproduction steps",
		"VERDICT: PASS",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("expected tester profile to include %q", expected)
		}
	}
}
