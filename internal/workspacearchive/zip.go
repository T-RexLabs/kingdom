// Package workspacearchive stages bounded, untrusted project archives. A staged
// import is input only; it is never evidence that workload execution completed.
package workspacearchive

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
)

// Limits are explicit operator limits. Entries includes implicit directories.
type Limits struct {
	ArchiveBytes  int64
	ExpandedBytes int64
	FileBytes     int64
	Entries       int
	Depth         int
	PathBytes     int
}

type Entry struct {
	Path       string `json:"path"`
	Kind       string `json:"kind"`
	Size       int64  `json:"size,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Executable bool   `json:"executable,omitempty"`
}

type Manifest struct {
	Version       int     `json:"version"`
	ArchiveSHA256 string  `json:"archive_sha256"`
	ExpandedBytes int64   `json:"expanded_bytes"`
	Entries       []Entry `json:"entries"`
}

// Staged owns a private directory until the caller finishes importing its input.
// Close removes both retained archive and extracted content. The caller must not
// give a workload access to this directory or mutate it before import finishes.
type Staged struct {
	Path     string
	Manifest Manifest
	Digest   string
	private  string
}

func (s *Staged) Close() error { return os.RemoveAll(s.private) }

type plannedEntry struct {
	file     *zip.File
	entry    Entry
	explicit bool
}

// ImportZIP retains an exact bounded archive, preflights all paths and sizes,
// then extracts it into a generated private directory under trusted parent.
// Nothing is published into a project by this operation.
func ImportZIP(ctx context.Context, parent string, source io.ReaderAt, size int64, limits Limits) (_ *Staged, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || limits.ArchiveBytes <= 0 || limits.ExpandedBytes <= 0 || limits.FileBytes <= 0 || limits.Entries <= 0 || limits.Depth <= 0 || limits.PathBytes <= 0 || size <= 0 || size > limits.ArchiveBytes {
		return nil, errors.New("invalid archive size or import limits")
	}
	private, err := os.MkdirTemp(parent, "kingdom-import-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, os.RemoveAll(private))
		}
	}()
	archive, err := os.OpenFile(filepath.Join(private, "input.zip"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer archive.Close()
	hash := sha256.New()
	n, err := securefs.Copy(ctx, io.MultiWriter(archive, hash), io.NewSectionReader(source, 0, size))
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, io.ErrUnexpectedEOF
	}
	if err := boundDirectory(ctx, archive, size, limits); err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(archive, size)
	if err != nil {
		return nil, fmt.Errorf("read project archive: %w", err)
	}
	entries, total, err := preflight(ctx, zr, limits)
	if err != nil {
		return nil, err
	}
	destination := filepath.Join(private, "workspace")
	if err := os.Mkdir(destination, 0700); err != nil {
		return nil, err
	}
	root, err := securefs.OpenRoot(destination)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	manifest := Manifest{Version: 1, ArchiveSHA256: hex.EncodeToString(hash.Sum(nil)), ExpandedBytes: total, Entries: make([]Entry, 0, len(entries))}
	for _, item := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item.entry.Kind == "directory" {
			if err := root.EnsureDir(item.entry.Path, 0755); err != nil {
				return nil, err
			}
		} else {
			contentHash := sha256.New()
			mode := fs.FileMode(0644)
			if item.entry.Executable {
				mode = 0755
			}
			err := root.MaterializeRegularFrom(ctx, item.entry.Path, func(dst io.Writer) error {
				reader, err := item.file.Open()
				if err != nil {
					return err
				}
				defer reader.Close()
				count, err := securefs.Copy(ctx, io.MultiWriter(dst, contentHash), io.LimitReader(reader, item.entry.Size+1))
				if err != nil {
					return err
				}
				if count != item.entry.Size {
					return errors.New("archive entry size mismatch")
				}
				return nil
			}, mode, 0755, time.Time{})
			if err != nil {
				return nil, fmt.Errorf("extract project entry: %w", err)
			}
			item.entry.SHA256 = hex.EncodeToString(contentHash.Sum(nil))
		}
		manifest.Entries = append(manifest.Entries, item.entry)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	return &Staged{Path: destination, Manifest: manifest, Digest: hex.EncodeToString(digest[:]), private: private}, nil
}

func preflight(ctx context.Context, archive *zip.Reader, limits Limits) ([]plannedEntry, int64, error) {
	if len(archive.File) > limits.Entries {
		return nil, 0, errors.New("archive entry limit exceeded")
	}
	entries := make(map[string]plannedEntry)
	aliases := make(map[string]string)
	fold := cases.Fold()
	var total int64
	for _, file := range archive.File {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		mode := file.Mode()
		directory := strings.HasSuffix(file.Name, "/")
		if mode&(fs.ModeSymlink|fs.ModeDevice|fs.ModeNamedPipe|fs.ModeSocket|fs.ModeIrregular|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 || mode.IsDir() != directory || (!directory && !mode.IsRegular()) {
			return nil, 0, errors.New("archive contains an unsupported entry type")
		}
		name := strings.TrimSuffix(file.Name, "/")
		if err := validatePath(name, limits); err != nil {
			return nil, 0, err
		}
		if directory && file.UncompressedSize64 != 0 {
			return nil, 0, errors.New("archive directory contains data")
		}
		if !directory {
			if file.UncompressedSize64 > uint64(limits.FileBytes) || file.UncompressedSize64 > uint64(limits.ExpandedBytes-total) || file.UncompressedSize64 >= uint64(1<<63-1) {
				return nil, 0, errors.New("archive expansion limit exceeded")
			}
			total += int64(file.UncompressedSize64)
		}
		parts := strings.Split(name, "/")
		for i := range parts {
			prefix := strings.Join(parts[:i+1], "/")
			key := fold.String(prefix)
			if previous, ok := aliases[key]; ok && previous != prefix {
				return nil, 0, errors.New("archive contains ambiguous path aliases")
			}
			aliases[key] = prefix
			leaf := i == len(parts)-1
			kind := "directory"
			if leaf && !directory {
				kind = "file"
			}
			existing, ok := entries[prefix]
			if ok && (existing.entry.Kind != kind || (leaf && existing.explicit)) {
				return nil, 0, errors.New("archive contains conflicting paths")
			}
			if !ok {
				if len(entries) >= limits.Entries {
					return nil, 0, errors.New("archive entry limit exceeded")
				}
				existing.entry = Entry{Path: prefix, Kind: kind}
			}
			if leaf {
				existing.explicit = true
				existing.file = file
				if !directory {
					existing.entry.Size = int64(file.UncompressedSize64)
					existing.entry.Executable = mode.Perm()&0111 != 0
				}
			}
			entries[prefix] = existing
		}
	}
	result := make([]plannedEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].entry.Path < result[j].entry.Path })
	return result, total, nil
}

func validatePath(name string, limits Limits) error {
	if name == "" || len(name) > limits.PathBytes || !utf8.ValidString(name) || !norm.NFC.IsNormalString(name) || path.IsAbs(name) || path.Clean(name) != name || name == "." || strings.ContainsAny(name, "\\:") {
		return errors.New("archive contains an invalid path")
	}
	parts := strings.Split(name, "/")
	if len(parts) > limits.Depth {
		return errors.New("archive path depth exceeded")
	}
	for _, part := range parts {
		if part == ".." || strings.TrimRight(part, ". ") != part || strings.IndexFunc(part, unicode.IsControl) >= 0 {
			return errors.New("archive contains an invalid path component")
		}
		lower := strings.ToLower(part)
		if trackedfs.IsProtectedRelativePath(lower) {
			return errors.New("archive contains protected project metadata")
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || (len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '0' && base[3] <= '9') {
			return errors.New("archive contains a reserved path component")
		}
	}
	return nil
}
