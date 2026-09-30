package trackedfs

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"kingdom/internal/securefs"
)

var protectedRootSegments = map[string]struct{}{
	".git":              {},
	".hg":               {},
	".svn":              {},
	".jj":               {},
	".kingdom":          {},
	".opencode":         {},
	".playwright-cli":   {},
	".playwright-mcp":   {},
	".tmp":              {},
	"kd-sandbox":        {},
	"kingdom-artifacts": {},
	"node_modules":      {},
}

type Metadata struct {
	SizeBytes  int64
	ModeBits   int64
	ModifiedNS int64
}

type TrackedFile struct {
	RelativePath string
	Metadata     Metadata
}

// TrackedEntry is a non-directory entry discovered through a pinned workspace
// root. Its identity lets callers safely remove only the entry they observed.
type TrackedEntry struct {
	RelativePath string
	Type         securefs.EntryType
	Identity     securefs.FileIdentity
}

func NormalizeRelativePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	clean := filepath.ToSlash(filepath.Clean(trimmed))
	clean = strings.TrimPrefix(clean, "./")
	if clean == "." || clean == "" {
		return ""
	}
	return clean
}

func IsProtectedRelativePath(path string) bool {
	normalized := NormalizeRelativePath(path)
	if normalized == "" {
		return false
	}
	segments := strings.Split(normalized, "/")
	for _, segment := range segments {
		if _, ok := protectedRootSegments[segment]; ok {
			return true
		}
	}
	return false
}

func ShouldSkipDirectory(path string) bool {
	return IsProtectedRelativePath(path)
}

func MetadataFromInfo(info fs.FileInfo) Metadata {
	if info == nil {
		return Metadata{}
	}
	modeBits := int64(info.Mode().Perm())
	return Metadata{
		SizeBytes:  info.Size(),
		ModeBits:   modeBits,
		ModifiedNS: info.ModTime().UnixNano(),
	}
}

func VisitTrackedFiles(ctx context.Context, root string, visit func(TrackedFile) error) error {
	cleanRoot := strings.TrimSpace(root)
	if cleanRoot == "" {
		return fmt.Errorf("tracked root is required")
	}
	pinned, err := securefs.OpenRoot(cleanRoot)
	if err != nil {
		return err
	}
	defer pinned.Close()
	return VisitTrackedFilesRoot(ctx, pinned, visit)
}

// VisitTrackedFilesRoot streams regular entries without collecting the tree.
func VisitTrackedFilesRoot(ctx context.Context, root *securefs.Root, visit func(TrackedFile) error) error {
	return VisitTrackedEntriesRoot(ctx, root, func(entry TrackedEntry) error {
		if entry.Type != securefs.EntryTypeRegular {
			return nil
		}
		file, err := root.OpenRegular(entry.RelativePath)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		_ = file.Close()
		if err != nil {
			return err
		}
		return visit(TrackedFile{RelativePath: entry.RelativePath, Metadata: MetadataFromInfo(info)})
	})
}

// VisitTrackedEntriesRoot visits non-directory entries in filesystem order,
// retaining only the active directory stack and fixed-size enumeration pages.
func VisitTrackedEntriesRoot(ctx context.Context, root *securefs.Root, visit func(TrackedEntry) error) error {
	if root == nil {
		return fmt.Errorf("tracked root is required")
	}
	var walk func(string) error
	walk = func(directory string) error {
		return root.IterateDir(ctx, directory, func(entry securefs.Entry) error {
			relativePath := entry.Name
			if directory != "" {
				relativePath = directory + "/" + entry.Name
			}
			if IsProtectedRelativePath(relativePath) {
				return nil
			}
			if entry.Type == securefs.EntryTypeDirectory {
				return walk(relativePath)
			}
			return visit(TrackedEntry{RelativePath: relativePath, Type: entry.Type, Identity: entry.Identity})
		})
	}
	if err := walk(""); err != nil {
		return fmt.Errorf("walk tracked files: %w", err)
	}
	return nil
}

func ResolveWithinRoot(root, relativePath string) (string, error) {
	cleanRoot := strings.TrimSpace(root)
	if cleanRoot == "" {
		return "", fmt.Errorf("root path is required")
	}
	normalized := NormalizeRelativePath(relativePath)
	if normalized == "" {
		return "", fmt.Errorf("relative path is required")
	}
	absRoot, err := filepath.Abs(cleanRoot)
	if err != nil {
		return "", fmt.Errorf("resolve root path: %w", err)
	}
	resolved := filepath.Join(absRoot, filepath.FromSlash(normalized))
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve target path: %w", err)
	}
	if absResolved != absRoot && !strings.HasPrefix(absResolved, absRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %q", relativePath)
	}
	return absResolved, nil
}
