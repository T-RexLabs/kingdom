package bootstrapassets

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"kingdom/internal/dbmigrations"
)

//go:embed files/policies/*.yaml files/schemas/*.json files/workflows/*.json files/opencode/agents/*.md files/opencode/tools/*
var embeddedFiles embed.FS

const (
	policyDirName    = "policies"
	schemaDirName    = "schemas"
	workflowDirName  = "workflows"
	migrationDirName = "migrations"
	kingdomDirName   = ".kingdom"
	opencodeDirName  = "opencode"
)

var canonicalAssetDirs = map[string]string{
	"files/policies": "policies",
	"files/schemas":  filepath.Join("contracts", "schemas"),
}

type WorkflowSeed struct {
	Name       string
	WorkflowID string
	Definition map[string]any
}

func WriteScaffold(root string, projectRoot string, force bool) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("scaffold root is required")
	}
	if strings.TrimSpace(projectRoot) == "" {
		return fmt.Errorf("project root is required")
	}

	targetRoot := filepath.Clean(root)
	targetProjectRoot := filepath.Clean(projectRoot)
	if err := os.MkdirAll(targetRoot, 0o755); err != nil {
		return fmt.Errorf("create scaffold root: %w", err)
	}

	if err := os.MkdirAll(targetProjectRoot, 0o755); err != nil {
		return fmt.Errorf("create project root: %w", err)
	}

	if err := writeDir("files/policies", canonicalAssetDirs["files/policies"], filepath.Join(targetRoot, policyDirName), force); err != nil {
		return err
	}
	if err := writeDir("files/schemas", canonicalAssetDirs["files/schemas"], filepath.Join(targetRoot, schemaDirName), force); err != nil {
		return err
	}
	if err := writeDir("files/workflows", "", filepath.Join(targetRoot, workflowDirName), force); err != nil {
		return err
	}
	if err := writeMigrations(filepath.Join(targetRoot, migrationDirName), force); err != nil {
		return err
	}
	if err := WriteManagedOpenCodeAssets(filepath.Join(targetRoot, opencodeDirName), force); err != nil {
		return err
	}

	return nil
}
func ReadWorkflowSeed(name string) ([]byte, error) {
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return nil, fmt.Errorf("workflow seed name is required")
	}

	raw, err := readAssetFile("files/workflows", "", cleanName)
	if err != nil {
		return nil, fmt.Errorf("read workflow seed %q: %w", cleanName, err)
	}
	return raw, nil
}

func ListWorkflowSeeds() ([]WorkflowSeed, error) {
	names, err := listAssetFiles("files/workflows", "")
	if err != nil {
		return nil, fmt.Errorf("list workflow seeds: %w", err)
	}

	seeds := make([]WorkflowSeed, 0, len(names))
	for _, name := range names {
		raw, err := readAssetFile("files/workflows", "", name)
		if err != nil {
			return nil, fmt.Errorf("read workflow seed %q: %w", name, err)
		}

		var definition map[string]any
		if err := json.Unmarshal(raw, &definition); err != nil {
			return nil, fmt.Errorf("decode workflow seed %q: %w", name, err)
		}

		workflowID, _ := definition["workflow_id"].(string)
		workflowID = strings.TrimSpace(workflowID)
		if workflowID == "" {
			return nil, fmt.Errorf("workflow seed %q is missing workflow_id", name)
		}

		seeds = append(seeds, WorkflowSeed{Name: name, WorkflowID: workflowID, Definition: definition})
	}

	return seeds, nil
}

func ListManagedOpenCodeAgentProfiles() ([]string, error) {
	names, err := listAssetFiles("files/opencode/agents", "")
	if err != nil {
		return nil, fmt.Errorf("list managed opencode agent profiles: %w", err)
	}
	return names, nil
}

func ListManagedOpenCodeTools() ([]string, error) {
	names, err := listAssetFiles("files/opencode/tools", "")
	if err != nil {
		return nil, fmt.Errorf("list managed opencode tools: %w", err)
	}
	return names, nil
}

func ManagedOpenCodeRoot(projectRoot string) string {
	trimmed := strings.TrimSpace(projectRoot)
	if trimmed == "" {
		return ""
	}
	return filepath.Join(filepath.Clean(trimmed), kingdomDirName, opencodeDirName)
}

func ManagedOpenCodeDatabasePath(projectRoot string) string {
	overlayRoot := ManagedOpenCodeRoot(projectRoot)
	if overlayRoot == "" {
		return ""
	}
	return filepath.Join(overlayRoot, "opencode.db")
}

func WriteManagedOpenCodeAssets(overlayRoot string, force bool) error {
	if err := WriteManagedOpenCodeAgentProfiles(overlayRoot, force); err != nil {
		return err
	}
	return WriteManagedOpenCodeTools(overlayRoot, force)
}

func WriteManagedOpenCodeAgentProfiles(overlayRoot string, force bool) error {
	resolvedOverlayRoot, err := cleanManagedOpenCodeRoot(overlayRoot)
	if err != nil {
		return err
	}
	return writeDir("files/opencode/agents", "", filepath.Join(resolvedOverlayRoot, "agents"), force)
}

func WriteManagedOpenCodeTools(overlayRoot string, force bool) error {
	resolvedOverlayRoot, err := cleanManagedOpenCodeRoot(overlayRoot)
	if err != nil {
		return err
	}
	return writeDir("files/opencode/tools", "", filepath.Join(resolvedOverlayRoot, "tools"), force)
}

func ValidateManagedOpenCodeAgentProfiles(overlayRoot string) error {
	return validateManagedOpenCodeAssets(overlayRoot, "files/opencode/agents", "agents", ListManagedOpenCodeAgentProfiles, "managed opencode agent profile")
}

func ValidateManagedOpenCodeTools(overlayRoot string) error {
	return validateManagedOpenCodeAssets(overlayRoot, "files/opencode/tools", "tools", ListManagedOpenCodeTools, "managed opencode tool")
}

func ValidateManagedOpenCodePlannerAssets(overlayRoot string) error {
	if err := validateManagedOpenCodeNamedAssets(overlayRoot, "files/opencode/agents", "agents", []string{"planner.md"}, "managed opencode planner profile"); err != nil {
		return err
	}
	return validateManagedOpenCodeNamedAssets(overlayRoot, "files/opencode/tools", "tools", []string{"kingdom_complete_attempt.ts"}, "managed opencode tool")
}

func writeDir(sourceDir string, canonicalDir string, targetDir string, force bool) error {
	entries, err := listAssetFiles(sourceDir, canonicalDir)
	if err != nil {
		return fmt.Errorf("read asset dir %q: %w", sourceDir, err)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("create target dir %q: %w", targetDir, err)
	}

	for _, name := range entries {
		content, readErr := readAssetFile(sourceDir, canonicalDir, name)
		if readErr != nil {
			return fmt.Errorf("read asset file %q: %w", filepath.Join(sourceDir, name), readErr)
		}

		targetPath := filepath.Join(targetDir, name)
		if !force {
			if _, statErr := os.Stat(targetPath); statErr == nil {
				continue
			} else if !os.IsNotExist(statErr) {
				return fmt.Errorf("stat target %q: %w", targetPath, statErr)
			}
		}

		if writeErr := os.WriteFile(targetPath, content, 0o644); writeErr != nil {
			return fmt.Errorf("write target %q: %w", targetPath, writeErr)
		}
	}

	return nil
}

func validateManagedOpenCodeAssets(overlayRoot string, sourceDir string, relDir string, list func() ([]string, error), label string) error {
	names, err := list()
	if err != nil {
		return err
	}
	return validateManagedOpenCodeNamedAssets(overlayRoot, sourceDir, relDir, names, label)
}

func validateManagedOpenCodeNamedAssets(overlayRoot string, sourceDir string, relDir string, names []string, label string) error {
	resolvedOverlayRoot, err := cleanManagedOpenCodeRoot(overlayRoot)
	if err != nil {
		return err
	}
	for _, name := range names {
		cleanName := strings.TrimSpace(name)
		if cleanName == "" {
			continue
		}
		wanted, err := readAssetFile(sourceDir, "", cleanName)
		if err != nil {
			return fmt.Errorf("read %s %q: %w", label, cleanName, err)
		}
		targetPath := filepath.Join(resolvedOverlayRoot, relDir, cleanName)
		actual, err := os.ReadFile(targetPath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%s %q is missing; run `kd init --force` to reinstall Kingdom-managed OpenCode assets", label, targetPath)
			}
			return fmt.Errorf("read %s %q: %w", label, targetPath, err)
		}
		if !bytesEqual(actual, wanted) {
			return fmt.Errorf("%s %q is stale; run `kd init --force` to reinstall Kingdom-managed OpenCode assets", label, targetPath)
		}
	}
	return nil
}

func cleanManagedOpenCodeRoot(overlayRoot string) (string, error) {
	trimmed := strings.TrimSpace(overlayRoot)
	if trimmed == "" {
		return "", fmt.Errorf("managed opencode root is required")
	}
	return filepath.Clean(trimmed), nil
}

func writeMigrations(targetDir string, force bool) error {
	names, err := dbmigrations.List()
	if err != nil {
		return fmt.Errorf("read migration assets: %w", err)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("create target dir %q: %w", targetDir, err)
	}

	for _, name := range names {
		content, err := dbmigrations.Read(name)
		if err != nil {
			return fmt.Errorf("read migration asset %q: %w", name, err)
		}

		targetPath := filepath.Join(targetDir, name)
		if !force {
			if _, statErr := os.Stat(targetPath); statErr == nil {
				continue
			} else if !os.IsNotExist(statErr) {
				return fmt.Errorf("stat target %q: %w", targetPath, statErr)
			}
		}

		if err := os.WriteFile(targetPath, content, 0o644); err != nil {
			return fmt.Errorf("write target %q: %w", targetPath, err)
		}
	}

	return nil
}

func readAssetFile(embeddedDir string, canonicalDir string, name string) ([]byte, error) {
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return nil, fmt.Errorf("asset name is required")
	}
	if canonicalDir != "" {
		if path, ok := resolveRepoAssetPath(filepath.Join(canonicalDir, cleanName)); ok {
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read canonical asset %q: %w", path, err)
			}
			return raw, nil
		}
	}

	raw, err := fs.ReadFile(embeddedFiles, filepath.ToSlash(filepath.Join(embeddedDir, cleanName)))
	if err != nil {
		return nil, fmt.Errorf("read embedded asset %q: %w", filepath.Join(embeddedDir, cleanName), err)
	}
	return raw, nil
}

func listAssetFiles(embeddedDir string, canonicalDir string) ([]string, error) {
	if canonicalDir != "" {
		if dir, ok := resolveRepoAssetDir(canonicalDir); ok {
			entries, err := os.ReadDir(dir)
			if err != nil {
				return nil, fmt.Errorf("read canonical asset dir %q: %w", dir, err)
			}
			return fileNames(entries), nil
		}
	}

	entries, err := fs.ReadDir(embeddedFiles, embeddedDir)
	if err != nil {
		return nil, fmt.Errorf("read embedded asset dir %q: %w", embeddedDir, err)
	}
	return fileNames(entries), nil
}

func fileNames(entries []fs.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

func resolveRepoAssetPath(relativePath string) (string, bool) {
	for _, root := range repoRootCandidates() {
		candidate := filepath.Join(root, relativePath)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, true
		}
	}
	return "", false
}

func resolveRepoAssetDir(relativePath string) (string, bool) {
	for _, root := range repoRootCandidates() {
		candidate := filepath.Join(root, relativePath)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, true
		}
	}
	return "", false
}

func repoRootCandidates() []string {
	starts := make([]string, 0, 2)
	if wd, err := os.Getwd(); err == nil {
		starts = append(starts, wd)
	}
	if _, currentFile, _, ok := runtime.Caller(0); ok {
		starts = append(starts, filepath.Dir(currentFile))
	}

	seen := make(map[string]struct{})
	candidates := make([]string, 0, len(starts))
	for _, start := range starts {
		for dir := filepath.Clean(start); ; dir = filepath.Dir(dir) {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				if _, ok := seen[dir]; !ok {
					seen[dir] = struct{}{}
					candidates = append(candidates, dir)
				}
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
		}
	}
	return candidates
}
