package dbmigrations

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// List returns the canonical forward-only migration filenames in sorted order.
func List() ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" || !strings.HasSuffix(name, ".sql") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Read loads a canonical migration file by name.
func Read(name string) ([]byte, error) {
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return nil, fmt.Errorf("migration name is required")
	}

	raw, err := fs.ReadFile(files, cleanName)
	if err != nil {
		return nil, fmt.Errorf("read embedded migration %q: %w", cleanName, err)
	}
	return raw, nil
}
