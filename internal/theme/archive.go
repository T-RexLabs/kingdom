package theme

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// ImportZIP validates the entire archive before any filesystem mutation.
func (r *Resolver) ImportZIP(raw []byte) (Package, error) {
	if len(raw) > MaxPackageBytes+MaxManifestBytes {
		return Package{}, invalid("/", "size", "Theme archive exceeds its resource budget.")
	}
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return Package{}, invalid("/", "archive", "Invalid theme ZIP archive.")
	}
	if len(archive.File) > MaxAssets+1 {
		return Package{}, invalid("/", "size", "Theme archive contains too many entries.")
	}
	files := map[string][]byte{}
	var manifest []byte
	total := 0
	seen := map[string]bool{}
	for _, entry := range archive.File {
		name := entry.Name
		if !utf8.ValidString(name) || strings.Contains(name, "\\") || seen[name] || !entry.Mode().IsRegular() || (name != "theme.json" && !assetPathPattern.MatchString(name)) {
			return Package{}, invalid("/", "archive_path", "Archive entries must be unique regular theme.json or declared assets files.")
		}
		seen[name] = true
		limit := MaxAssetBytes
		if name == "theme.json" {
			limit = MaxManifestBytes
		}
		if entry.UncompressedSize64 > uint64(limit) {
			return Package{}, invalid("/", "size", "Expanded archive entry exceeds its resource budget.")
		}
		reader, err := entry.Open()
		if err != nil {
			return Package{}, err
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, int64(limit+1)))
		closeErr := reader.Close()
		if readErr != nil {
			return Package{}, readErr
		}
		if closeErr != nil {
			return Package{}, closeErr
		}
		total += len(data)
		if len(data) > limit || total > MaxPackageBytes+MaxManifestBytes {
			return Package{}, invalid("/", "size", "Expanded theme archive exceeds its resource budget.")
		}
		if name == "theme.json" {
			manifest = data
		} else {
			files[name] = data
		}
	}
	if manifest == nil {
		return Package{}, invalid("/", "archive", "Archive must contain theme.json at its root.")
	}
	return r.Resolve(manifest, files)
}

// ExportZIP uses resolved presentation plus stable asset IDs and byte digests.
// Export/reimport preserves ResolvedDigest and never needs an installed base.
func (r *Resolver) ExportZIP(p Package) ([]byte, error) {
	// Revalidate caller-supplied data: Package is an API value, not a trust token.
	checked, err := r.Resolve(mustJSON(p.Manifest), p.Assets)
	if err != nil {
		return nil, err
	}
	m, files := addressAssets(checked.Manifest, checked.Assets)
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	files["theme.json"] = append(raw, '\n')
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(0644)
		header.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
		dst, err := writer.CreateHeader(header)
		if err != nil {
			return nil, err
		}
		if _, err = dst.Write(files[name]); err != nil {
			return nil, err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func addressAssets(m Manifest, files map[string][]byte) (Manifest, map[string][]byte) {
	m = cloneManifest(m)
	if m.Assets == nil {
		m.Assets = map[string]Asset{}
	}
	if m.Artwork == nil {
		m.Artwork = map[string]string{}
	}
	out := map[string][]byte{}
	for id, a := range m.Assets {
		data := files[a.Path]
		a.Path = fmt.Sprintf("assets/%s-%s%s", id, hashBytes(data), path.Ext(a.Path))
		m.Assets[id] = a
		out[a.Path] = bytes.Clone(data)
	}
	return m, out
}
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}
