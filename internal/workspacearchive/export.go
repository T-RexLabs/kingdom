package workspacearchive

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
)

// Export describes the exact ZIP bytes and their canonical import manifest.
// It does not establish that workload writers were stopped. The caller must
// prove writer closure before exporting and discard all output on any error.
type Export struct {
	ArchiveBytes int64
	Manifest     Manifest
	Digest       string
}

type exportEntry struct {
	entry securefs.Entry
	name  string
}

// ExportZIP writes a deterministic bounded ZIP through descriptor-relative,
// no-follow reads. Protected metadata and generated directories are excluded.
// Symlinks and special files elsewhere are rejected, never followed or omitted.
func ExportZIP(ctx context.Context, root *securefs.Root, destination io.Writer, limits Limits) (Export, error) {
	if root == nil || destination == nil || limits.ArchiveBytes <= 0 || limits.ExpandedBytes <= 0 || limits.FileBytes <= 0 || limits.Entries <= 0 || limits.Depth <= 0 || limits.PathBytes <= 0 {
		return Export{}, errors.New("invalid export limits")
	}
	entries, err := collectExport(ctx, root, limits)
	if err != nil {
		return Export{}, err
	}
	if len(entries) >= 0xffff {
		return Export{}, errZIPProfile
	}
	hash := sha256.New()
	maximum := min(limits.ArchiveBytes, int64(0xfffffffe))
	bounded := &archiveWriter{writer: io.MultiWriter(destination, hash), remaining: maximum}
	zw := zip.NewWriter(bounded)
	manifest := Manifest{Version: 1, Entries: make([]Entry, 0, len(entries))}
	for _, item := range entries {
		if err := ctx.Err(); err != nil {
			return Export{}, err
		}
		out := Entry{Path: item.name, Kind: "directory"}
		header := &zip.FileHeader{Name: item.name + "/", Method: zip.Store}
		header.SetMode(fs.ModeDir | 0755)
		if item.entry.Type == securefs.EntryTypeRegular {
			out.Kind = "file"
			out.Size = item.entry.SizeBytes
			info, err := root.OpenRegular(item.name)
			if err != nil {
				return Export{}, err
			}
			stat, statErr := info.Stat()
			closeErr := info.Close()
			if statErr != nil || closeErr != nil {
				return Export{}, errors.Join(statErr, closeErr)
			}
			out.Executable = stat.Mode().Perm()&0111 != 0
			mode := fs.FileMode(0644)
			if out.Executable {
				mode = 0755
			}
			header.Name = item.name
			header.Method = zip.Deflate
			header.SetMode(mode)
		}
		dst, err := zw.CreateHeader(header)
		if err != nil {
			return Export{}, err
		}
		if out.Kind == "file" {
			fileLimit := &archiveWriter{writer: dst, remaining: item.entry.SizeBytes}
			identity, info, err := root.CopyRegular(ctx, item.name, fileLimit)
			if err != nil {
				return Export{}, err
			}
			comparison := identity
			comparison.ContentSHA256 = ""
			if comparison != item.entry.Identity || info.Size() != out.Size || (info.Mode().Perm()&0111 != 0) != out.Executable {
				return Export{}, securefs.ErrIdentityMismatch
			}
			out.SHA256 = identity.ContentSHA256
			manifest.ExpandedBytes += out.Size
		}
		manifest.Entries = append(manifest.Entries, out)
	}
	if err := zw.Close(); err != nil {
		return Export{}, err
	}
	// Detect replacement, deletion or additions across the full observed tree.
	after, err := collectExport(ctx, root, limits)
	if err != nil {
		return Export{}, err
	}
	if len(after) != len(entries) {
		return Export{}, securefs.ErrIdentityMismatch
	}
	for i := range entries {
		if after[i] != entries[i] {
			return Export{}, securefs.ErrIdentityMismatch
		}
	}
	manifest.ArchiveSHA256 = hex.EncodeToString(hash.Sum(nil))
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return Export{}, err
	}
	digest := sha256.Sum256(encoded)
	return Export{ArchiveBytes: maximum - bounded.remaining, Manifest: manifest, Digest: hex.EncodeToString(digest[:])}, nil
}

func collectExport(ctx context.Context, root *securefs.Root, limits Limits) ([]exportEntry, error) {
	entries := make([]exportEntry, 0)
	aliases := make(map[string]string)
	fold := cases.Fold()
	var total int64
	visited := 0
	var walk func(string) error
	walk = func(directory string) error {
		children, err := root.ReadDirBounded(directory, limits.Entries)
		if err != nil {
			return err
		}
		for _, entry := range children {
			if err := ctx.Err(); err != nil {
				return err
			}
			visited++
			if visited > limits.Entries {
				return securefs.ErrLimitExceeded
			}
			name := entry.Name
			if directory != "" {
				name = directory + "/" + name
			}
			if trackedfs.IsProtectedRelativePath(strings.ToLower(name)) || entry.Type == securefs.EntryTypeDirectory && trackedfs.ShouldSkipDirectory(name) {
				continue
			}
			if err := validatePath(name, limits); err != nil {
				return err
			}
			key := fold.String(name)
			if prior, ok := aliases[key]; ok && prior != name {
				return errors.New("workspace contains ambiguous path aliases")
			}
			aliases[key] = name
			switch entry.Type {
			case securefs.EntryTypeDirectory:
			case securefs.EntryTypeRegular:
				if entry.SizeBytes >= 0xffffffff {
					return errZIPProfile
				}
				if entry.SizeBytes < 0 || entry.SizeBytes > limits.FileBytes || entry.SizeBytes > limits.ExpandedBytes-total {
					return securefs.ErrLimitExceeded
				}
				total += entry.SizeBytes
			default:
				return errors.New("workspace contains an unsupported entry type")
			}
			entries = append(entries, exportEntry{entry: entry, name: name})
			if entry.Type == securefs.EntryTypeDirectory {
				if err := walk(name); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries, nil
}

type archiveWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *archiveWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, securefs.ErrLimitExceeded
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}
