package bootstrapassets

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/dbmigrations"
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

func TestManagedOpenCodeDatabasePath(t *testing.T) {
	t.Parallel()

	projectRoot := filepath.Join(t.TempDir(), "workspace")
	want := filepath.Join(projectRoot, kingdomDirName, opencodeDirName, "opencode.db")
	if got := ManagedOpenCodeDatabasePath(projectRoot); got != want {
		t.Fatalf("expected managed opencode db path %q, got %q", want, got)
	}

	if got := ManagedOpenCodeDatabasePath("   "); got != "" {
		t.Fatalf("expected blank project root to return empty path, got %q", got)
	}
}

func TestManagedOpenCodePathsForProjectRoot(t *testing.T) {
	t.Parallel()

	projectRoot := filepath.Join(t.TempDir(), "workspace")
	paths := ManagedOpenCodePathsForProjectRoot(projectRoot)
	if paths.ConfigDir != ManagedOpenCodeRoot(projectRoot) {
		t.Fatalf("expected managed config dir %q, got %q", ManagedOpenCodeRoot(projectRoot), paths.ConfigDir)
	}
	if paths.DatabasePath != ManagedOpenCodeDatabasePath(projectRoot) {
		t.Fatalf("expected managed database path %q, got %q", ManagedOpenCodeDatabasePath(projectRoot), paths.DatabasePath)
	}

	blank := ManagedOpenCodePathsForProjectRoot("   ")
	if blank.ConfigDir != "" || blank.DatabasePath != "" {
		t.Fatalf("expected blank managed opencode paths, got %#v", blank)
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
		filepath.Join("opencode", "agents", "coordinator.md"),
		filepath.Join("opencode", "agents", "plan-author.md"),
		filepath.Join("opencode", "agents", "governance.md"),
		filepath.Join("opencode", "agents", "security-validator.md"),
		filepath.Join("opencode", "agents", "planning-assistant.md"),
		filepath.Join("opencode", "agents", "qa-validator.md"),
		filepath.Join("opencode", "agents", "reviewer.md"),
		filepath.Join("opencode", "agents", "resilience-validator.md"),
	} {
		if _, err := os.Stat(filepath.Join(kingdomHome, rel)); err != nil {
			t.Fatalf("expected scaffold file %s: %v", rel, err)
		}
	}
}

func TestWriteScaffoldForceRemovesRetiredManagedOpenCodeToolFromProjectAndRunTargets(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	runTargetToolPath := filepath.Join(kingdomHome, "runtime", "run-targets", "run-1", "target", kingdomDirName, opencodeDirName, "tools", retiredManagedOpenCodeToolName)
	for _, toolPath := range []string{
		filepath.Join(overlayRoot, "tools", retiredManagedOpenCodeToolName),
		runTargetToolPath,
	} {
		if err := os.MkdirAll(filepath.Dir(toolPath), 0o755); err != nil {
			t.Fatalf("create retired tool directory %s: %v", toolPath, err)
		}
		if err := os.WriteFile(toolPath, []byte("retired managed tool"), 0o644); err != nil {
			t.Fatalf("write retired tool %s: %v", toolPath, err)
		}
	}
	profiles, err := ListManagedOpenCodeAgentProfiles()
	if err != nil {
		t.Fatalf("list managed profiles: %v", err)
	}
	for _, profile := range profiles {
		profilePath := filepath.Join(overlayRoot, "agents", profile)
		if err := os.MkdirAll(filepath.Dir(profilePath), 0o755); err != nil {
			t.Fatalf("create stale profile directory %s: %v", profilePath, err)
		}
		if err := os.WriteFile(profilePath, []byte("tools:\n  kingdom_complete_attempt: false\n"), 0o644); err != nil {
			t.Fatalf("write stale profile %s: %v", profilePath, err)
		}
	}

	if err := WriteScaffold(kingdomHome, projectRoot, true); err != nil {
		t.Fatalf("forced scaffold write: %v", err)
	}

	for _, toolPath := range []string{
		filepath.Join(overlayRoot, "tools", retiredManagedOpenCodeToolName),
		runTargetToolPath,
	} {
		if _, err := os.Stat(toolPath); !os.IsNotExist(err) {
			t.Fatalf("expected retired managed tool %s to be removed, stat err=%v", toolPath, err)
		}
	}
	for _, profile := range profiles {
		profilePath := filepath.Join(overlayRoot, "agents", profile)
		raw, err := os.ReadFile(profilePath)
		if err != nil {
			t.Fatalf("read refreshed profile %s: %v", profilePath, err)
		}
		if strings.Contains(string(raw), "kingdom_complete_attempt") {
			t.Fatalf("forced scaffold retained the retired tool toggle in %s", profilePath)
		}
	}
}

func TestWriteManagedOpenCodeAssetsForceRemovesRetiredToolFromTargetOverlay(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	overlayRoot := ManagedOpenCodeRoot(workspaceRoot)
	toolPath := filepath.Join(overlayRoot, "tools", retiredManagedOpenCodeToolName)
	if err := os.MkdirAll(filepath.Dir(toolPath), 0o755); err != nil {
		t.Fatalf("create retired tool directory: %v", err)
	}
	if err := os.WriteFile(toolPath, []byte("retired managed tool"), 0o644); err != nil {
		t.Fatalf("write retired managed tool: %v", err)
	}

	if err := WriteManagedOpenCodeAssets(overlayRoot, true); err != nil {
		t.Fatalf("force managed assets into target overlay: %v", err)
	}
	if _, err := os.Stat(toolPath); !os.IsNotExist(err) {
		t.Fatalf("expected retired managed tool to be removed, stat err=%v", err)
	}
}

func TestManagedOpenCodeAgentProfilesOmitRetiredToolToggle(t *testing.T) {
	t.Parallel()

	profiles, err := ListManagedOpenCodeAgentProfiles()
	if err != nil {
		t.Fatalf("list managed profiles: %v", err)
	}
	for _, profile := range profiles {
		raw, err := readAssetFile("files/opencode/agents", "", profile)
		if err != nil {
			t.Fatalf("read managed profile %s: %v", profile, err)
		}
		if strings.Contains(string(raw), "kingdom_complete_attempt") {
			t.Errorf("managed profile %s still configures the retired completion tool", profile)
		}
	}
}

func TestManagedOpenCodeAgentProfilesHaveValidFrontmatterAndPreserveCapabilities(t *testing.T) {
	t.Parallel()

	type profileMetadata struct {
		Mode       string          `yaml:"mode"`
		Tools      map[string]bool `yaml:"tools"`
		Permission struct {
			Edit map[string]string `yaml:"edit"`
		} `yaml:"permission"`
	}
	type expectedProfile struct {
		name      string
		tools     map[string]bool
		denyEdits bool
	}
	toolSet := func(bash, list, glob, grep, webfetch, todowrite, todoread bool) map[string]bool {
		return map[string]bool{
			"bash": bash, "write": false, "list": list, "glob": glob, "grep": grep,
			"webfetch": webfetch, "task": false, "todowrite": todowrite, "todoread": todoread,
		}
	}
	expected := []expectedProfile{
		{name: "advisor.md", tools: toolSet(false, true, true, true, true, true, true), denyEdits: true},
		{name: "builder.md"},
		{name: "coordinator.md", tools: toolSet(false, false, false, false, false, true, true), denyEdits: true},
		{name: "governance.md", tools: toolSet(false, false, false, false, false, true, true), denyEdits: true},
		{name: "plan-author.md", tools: toolSet(false, true, true, true, true, true, true), denyEdits: true},
		{name: "planning-assistant.md", tools: toolSet(false, true, true, true, true, false, false), denyEdits: true},
		{name: "qa-validator.md", tools: toolSet(true, true, true, true, false, true, true), denyEdits: true},
		{name: "resilience-validator.md", tools: toolSet(true, true, true, true, false, true, true), denyEdits: true},
		{name: "reviewer.md", tools: toolSet(false, true, true, true, false, true, true), denyEdits: true},
		{name: "security-validator.md", tools: toolSet(true, true, true, true, false, true, true), denyEdits: true},
	}
	profiles, err := ListManagedOpenCodeAgentProfiles()
	if err != nil {
		t.Fatalf("list managed profiles: %v", err)
	}
	if len(profiles) != len(expected) {
		t.Fatalf("managed profile count = %d, want %d", len(profiles), len(expected))
	}
	for _, want := range expected {
		t.Run(want.name, func(t *testing.T) {
			raw, err := readAssetFile("files/opencode/agents", "", want.name)
			if err != nil {
				t.Fatalf("read embedded profile: %v", err)
			}
			lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
			if len(lines) < 3 || lines[0] != "---" || lines[len(lines)-1] != "---" {
				t.Fatalf("profile must have one standalone frontmatter block and no prompt body, got:\n%s", raw)
			}
			for _, line := range lines[1 : len(lines)-1] {
				if line == "---" {
					t.Fatalf("profile has more than one frontmatter closing delimiter")
				}
			}
			var metadata profileMetadata
			if err := yaml.Unmarshal([]byte(strings.Join(lines[1:len(lines)-1], "\n")), &metadata); err != nil {
				t.Fatalf("decode profile frontmatter: %v", err)
			}
			if metadata.Mode != "primary" {
				t.Errorf("mode = %q, want primary", metadata.Mode)
			}
			if !reflect.DeepEqual(metadata.Tools, want.tools) {
				t.Errorf("tool capabilities = %#v, want baseline %#v", metadata.Tools, want.tools)
			}
			if want.denyEdits && metadata.Permission.Edit["*"] != "deny" {
				t.Errorf("edit permission = %#v, want wildcard deny", metadata.Permission.Edit)
			}
		})
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
}

func TestValidateManagedOpenCodeAssetsFailsWhenManagedFileMissing(t *testing.T) {
	t.Parallel()

	projectRoot, kingdomHome, overlayRoot := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("scaffold write: %v", err)
	}

	plannerPath := filepath.Join(overlayRoot, "agents", "planning-assistant.md")
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

	plannerPath := filepath.Join(overlayRoot, "agents", "planning-assistant.md")
	if err := os.WriteFile(plannerPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("overwrite managed planner profile: %v", err)
	}

	err := ValidateManagedOpenCodePlannerAssets(overlayRoot)
	if err == nil {
		t.Fatal("expected planner asset validation to fail when managed profile drifts")
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
	if err := os.MkdirAll(filepath.Dir(customToolPath), 0o755); err != nil {
		t.Fatalf("create custom tool directory: %v", err)
	}
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
		filepath.Join("opencode", "agents", "planning-assistant.md"),
		"bash: false",
		"write: false",
		"list: true",
		"glob: true",
		"grep: true",
		"webfetch: true",
		"task: false",
		"todowrite: false",
		"todoread: false",
		`"*": deny`,
	)
	assertContains(
		filepath.Join("opencode", "agents", "plan-author.md"),
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
		filepath.Join("opencode", "agents", "qa-validator.md"),
		"bash: true",
	)
	assertContains(
		filepath.Join("opencode", "agents", "security-validator.md"),
		"bash: true",
	)
	assertContains(
		filepath.Join("opencode", "agents", "resilience-validator.md"),
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
		filepath.Join("opencode", "agents", "plan-author.md"),
		filepath.Join("opencode", "agents", "security-validator.md"),
		filepath.Join("opencode", "agents", "planning-assistant.md"),
		filepath.Join("opencode", "agents", "qa-validator.md"),
		filepath.Join("opencode", "agents", "reviewer.md"),
		filepath.Join("opencode", "agents", "resilience-validator.md"),
		filepath.Join("opencode", "agents", "governance.md"),
		filepath.Join("opencode", "agents", "coordinator.md"),
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

func TestFreshScaffoldContainsCanonicalMigrations(t *testing.T) {
	t.Parallel()
	projectRoot, kingdomHome, _ := newScaffoldPaths(t)
	if err := WriteScaffold(kingdomHome, projectRoot, false); err != nil {
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
