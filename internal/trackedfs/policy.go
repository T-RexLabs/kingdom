package trackedfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
	AbsolutePath string
	RelativePath string
	Metadata     Metadata
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
	if info.Mode()&0o111 != 0 {
		modeBits |= 0o111
	}
	return Metadata{
		SizeBytes:  info.Size(),
		ModeBits:   modeBits,
		ModifiedNS: info.ModTime().UnixNano(),
	}
}

func WalkTrackedFiles(root string) ([]TrackedFile, error) {
	cleanRoot := strings.TrimSpace(root)
	if cleanRoot == "" {
		return nil, fmt.Errorf("tracked root is required")
	}
	files := make([]TrackedFile, 0)
	err := filepath.WalkDir(cleanRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == cleanRoot {
			return nil
		}
		rel, err := filepath.Rel(cleanRoot, path)
		if err != nil {
			return err
		}
		normalized := NormalizeRelativePath(rel)
		if normalized == "" {
			return nil
		}
		if entry.IsDir() {
			if ShouldSkipDirectory(normalized) {
				return filepath.SkipDir
			}
			return nil
		}
		if IsProtectedRelativePath(normalized) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		files = append(files, TrackedFile{
			AbsolutePath: path,
			RelativePath: normalized,
			Metadata:     MetadataFromInfo(info),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk tracked files: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].RelativePath < files[j].RelativePath })
	return files, nil
}

// WalkTrackedFilesRoot enumerates tracked files through an already pinned,
// no-follow root. It is used when subsequent decisions mutate that same tree.
func WalkTrackedFilesRoot(root *securefs.Root) ([]TrackedFile, error) {
	if root == nil {
		return nil, fmt.Errorf("tracked root is required")
	}
	files := make([]TrackedFile, 0)
	var walk func(string) error
	walk = func(directory string) error {
		entries, err := root.ReadDir(directory)
		if err != nil {
			return err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, entry := range entries {
			relativePath := entry.Name()
			if directory != "" {
				relativePath = directory + "/" + entry.Name()
			}
			normalized := NormalizeRelativePath(relativePath)
			if normalized == "" || IsProtectedRelativePath(normalized) {
				continue
			}
			if entry.IsDir() {
				if err := walk(normalized); err != nil {
					return err
				}
				continue
			}
			if entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			file, err := root.OpenRegular(normalized)
			if err != nil {
				if info, infoErr := entry.Info(); infoErr == nil && !info.Mode().IsRegular() {
					continue
				}
				return err
			}
			info, err := file.Stat()
			_ = file.Close()
			if err != nil {
				return err
			}
			files = append(files, TrackedFile{RelativePath: normalized, Metadata: MetadataFromInfo(info)})
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, fmt.Errorf("walk tracked files: %w", err)
	}
	return files, nil
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
