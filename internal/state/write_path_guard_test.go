package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateWritePathGuardNoDirectStateUpdatesOutsideApprovedStores(t *testing.T) {
	t.Parallel()

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	internalRoot := filepath.Join(repoRoot, "internal")
	allowed := map[string]struct{}{
		filepath.Clean(filepath.Join(internalRoot, "state", "transition_service.go")):     {},
		filepath.Clean(filepath.Join(internalRoot, "state", "optimistic_concurrency.go")): {},
	}

	var violations []string
	err = filepath.WalkDir(internalRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		cleanPath := filepath.Clean(path)
		if _, ok := allowed[cleanPath]; ok {
			return nil
		}

		raw, readErr := os.ReadFile(cleanPath)
		if readErr != nil {
			return readErr
		}

		content := string(raw)
		if strings.Contains(content, "UPDATE workflow_runs") || strings.Contains(content, "UPDATE node_runs") {
			rel, relErr := filepath.Rel(repoRoot, cleanPath)
			if relErr != nil {
				rel = cleanPath
			}
			violations = append(violations, rel)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}

	if len(violations) > 0 {
		t.Fatalf("direct workflow_runs/node_runs UPDATE statements found outside approved state stores: %v", violations)
	}
}
