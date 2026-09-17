package trackedfs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"kingdom/internal/providerlimits"
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
	var budget providerlimits.WorkspaceBudget
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
		if err := budget.CheckDepth(relativePathDepth(normalized)); err != nil {
			return err
		}
		if entry.IsDir() {
			if err := budget.AddEntry(); err != nil {
				return err
			}
			if ShouldSkipDirectory(normalized) {
				return filepath.SkipDir
			}
			return nil
		}
		if IsProtectedRelativePath(normalized) {
			return budget.AddEntry()
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return budget.AddEntry()
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return budget.AddEntry()
		}
		if err := budget.AddFile(info.Size()); err != nil {
			return err
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
	entries, err := WalkTrackedEntriesRoot(root)
	if err != nil {
		return nil, err
	}
	files := make([]TrackedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.Type != securefs.EntryTypeRegular {
			continue
		}
		file, err := root.OpenRegular(entry.RelativePath)
		if err != nil {
			return nil, err
		}
		info, err := file.Stat()
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		files = append(files, TrackedFile{RelativePath: entry.RelativePath, Metadata: MetadataFromInfo(info)})
	}
	return files, nil
}

// WalkTrackedEntriesRoot enumerates every non-directory entry through an
// already pinned, no-follow root while skipping protected runtime paths.
func WalkTrackedEntriesRoot(root *securefs.Root) ([]TrackedEntry, error) {
	if root == nil {
		return nil, fmt.Errorf("tracked root is required")
	}
	entries := make([]TrackedEntry, 0)
	var budget providerlimits.WorkspaceBudget
	var walk func(string) error
	walk = func(directory string) error {
		directoryEntries, err := root.ReadDirBounded(directory, budget.RemainingEntries())
		if err != nil {
			if errors.Is(err, securefs.ErrLimitExceeded) {
				return &providerlimits.CountExceededError{Resource: "tracked workspace", Limit: providerlimits.TrackedWorkspaceEntries}
			}
			return err
		}
		for _, entry := range directoryEntries {
			relativePath := entry.Name
			if directory != "" {
				relativePath = directory + "/" + entry.Name
			}
			normalized := NormalizeRelativePath(relativePath)
			if normalized == "" {
				continue
			}
			if err := budget.CheckDepth(relativePathDepth(normalized)); err != nil {
				return err
			}
			if IsProtectedRelativePath(normalized) {
				if err := budget.AddEntry(); err != nil {
					return err
				}
				continue
			}
			if entry.Type == securefs.EntryTypeDirectory {
				if err := budget.AddEntry(); err != nil {
					return err
				}
				if err := walk(normalized); err != nil {
					return err
				}
				continue
			}
			if entry.Type != securefs.EntryTypeRegular {
				if err := budget.AddEntry(); err != nil {
					return err
				}
				entries = append(entries, TrackedEntry{RelativePath: normalized, Type: entry.Type, Identity: entry.Identity})
				continue
			}
			file, err := root.OpenRegular(normalized)
			if err != nil {
				return err
			}
			info, err := file.Stat()
			_ = file.Close()
			if err != nil {
				return err
			}
			if err := budget.AddFile(info.Size()); err != nil {
				return err
			}
			entries = append(entries, TrackedEntry{RelativePath: normalized, Type: entry.Type, Identity: entry.Identity})
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, fmt.Errorf("walk tracked files: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].RelativePath < entries[j].RelativePath })
	return entries, nil
}

func relativePathDepth(path string) int {
	normalized := NormalizeRelativePath(path)
	if normalized == "" {
		return 0
	}
	return strings.Count(normalized, "/") + 1
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
