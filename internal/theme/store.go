package theme

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"kingdom/internal/securefs"
)

var packageIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

const storePrefix = "themes/"
const catalogPath = storePrefix + ".catalog.json"
const maxCatalogBytes = 4 << 20

type Store struct {
	root        *securefs.Root
	resolver    *Resolver
	workspaceID string
}
type revisionIndex map[string]catalogPackage

type catalogPackage struct {
	Versions  map[string]string `json:"versions"`
	Published bool              `json:"published"`
}

type Summary struct {
	ID             string `json:"id"`
	Version        string `json:"version"`
	Name           string `json:"name"`
	Description    string `json:"description,omitempty"`
	ResolvedDigest string `json:"resolvedDigest"`
	Builtin        bool   `json:"builtin"`
}
type Catalog struct {
	WorkspaceID string       `json:"workspaceID"`
	Items       []Summary    `json:"items"`
	Sources     []string     `json:"sources"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
}
type Source struct {
	Content      string            `json:"content"`
	Manifest     json.RawMessage   `json:"-"`
	SourceDigest string            `json:"sourceDigest"`
	Assets       map[string][]byte `json:"-"`
	Diagnostics  []Diagnostic      `json:"diagnostics,omitempty"`
}

// OpenStore expects an initialized Kingdom home, not the project root. The
// pinned secure root rejects symlink traversal on every descendant access.
func OpenStore(kingdomHome string) (*Store, error) {
	resolver, err := NewResolver()
	if err != nil {
		return nil, err
	}
	root, err := securefs.OpenRoot(kingdomHome)
	if err != nil {
		return nil, err
	}
	s := &Store{root: root, resolver: resolver}
	if err = root.EnsureDir("themes", 0700); err != nil {
		root.Close()
		return nil, err
	}
	err = s.withLock(func() error {
		raw, err := root.ReadRegular(storePrefix+".workspace-id", 64)
		if errors.Is(err, fs.ErrNotExist) {
			id := make([]byte, 16)
			if _, err = rand.Read(id); err != nil {
				return err
			}
			raw = []byte(hex.EncodeToString(id))
			if _, err = root.CreateRegularExact(storePrefix+".workspace-id", raw, 0600, 0700); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		if len(raw) != 32 {
			return fmt.Errorf("invalid workspace presentation identity")
		}
		if _, err = hex.DecodeString(string(raw)); err != nil {
			return fmt.Errorf("invalid workspace presentation identity")
		}
		s.workspaceID = string(raw)
		return nil
	})
	if err != nil {
		root.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error        { return s.root.Close() }
func (s *Store) WorkspaceID() string { return s.workspaceID }
func (s *Store) Resolver() *Resolver { return s.resolver }
func (s *Store) withLock(fn func() error) error {
	const name = storePrefix + ".lock"
	if _, err := s.root.CreateRegularExact(name, []byte{}, 0600, 0700); err != nil {
		return err
	}
	file, err := s.root.OpenRegular(name)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = lockFile(file); err != nil {
		return err
	}
	defer unlockFile(file)
	return fn()
}
func (s *Store) readIndex() (revisionIndex, error) {
	raw, err := s.root.ReadRegular(catalogPath, maxCatalogBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return revisionIndex{}, nil
	}
	if err != nil {
		return nil, err
	}
	var index revisionIndex
	if err = json.Unmarshal(raw, &index); err != nil {
		return nil, fmt.Errorf("read theme catalog: %w", err)
	}
	if index == nil {
		return nil, fmt.Errorf("invalid theme catalog")
	}
	for id, record := range index {
		if !packageIDPattern.MatchString(id) || s.resolver.IsBuiltin(id) || len(record.Versions) == 0 {
			return nil, fmt.Errorf("invalid theme catalog identity")
		}
		for version, digest := range record.Versions {
			if !versionPattern.MatchString(version) || !digestPattern.MatchString(digest) {
				return nil, fmt.Errorf("invalid theme catalog revision")
			}
		}
	}
	return index, nil
}
func (s *Store) writeIndex(index revisionIndex) error {
	raw := mustJSON(index)
	if len(raw) > maxCatalogBytes {
		return fmt.Errorf("theme catalog exceeds its resource budget")
	}
	return s.root.ReplaceRegular(catalogPath, raw, 0600, 0700, time.Time{})
}
func revisionPath(id, digest string) string {
	return storePrefix + ".revisions/" + id + "/" + digest + "/"
}

// List keeps built-ins and other healthy revisions available if one custom
// revision is corrupt. Diagnostics identify failures instead of hiding them.
func (s *Store) List() (Catalog, error) {
	result := Catalog{WorkspaceID: s.workspaceID, Items: []Summary{}, Sources: []string{}}
	if sources, err := s.sourceIDs(); err == nil {
		result.Sources = sources
	} else {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{"/sources", "unavailable", "Editable theme sources could not be listed safely."})
	}
	for _, p := range s.resolver.Builtins() {
		result.Items = append(result.Items, summarize(p, true))
	}
	index, err := s.readIndex()
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, Diagnostic{"/catalog", "unavailable", "Custom theme catalog could not be read."})
		return result, nil
	}
	for id, record := range index {
		if !record.Published {
			continue
		}
		for _, digest := range record.Versions {
			p, err := s.readRevision(id, digest)
			if err != nil {
				result.Diagnostics = append(result.Diagnostics, Diagnostic{"/themes/" + id + "/" + digest, "invalid_revision", "Saved revision could not be validated; choose a known-good theme."})
				continue
			}
			result.Items = append(result.Items, summarize(p, false))
		}
	}
	sort.Slice(result.Items, func(i, j int) bool {
		a, b := result.Items[i], result.Items[j]
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Version < b.Version
	})
	return result, nil
}

// sourceIDs discovers authored files only when requested. It does not resolve
// assets or publish files; ReadSource provides field diagnostics on selection.
func (s *Store) sourceIDs() ([]string, error) {
	entries, err := s.root.ReadDirBounded("themes", 1024)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for _, entry := range entries {
		id := entry.Name
		if entry.Type != securefs.EntryTypeDirectory || !packageIDPattern.MatchString(id) || !portableName(id) || s.resolver.IsBuiltin(id) {
			continue
		}
		if _, err := s.root.ReadEntry(storePrefix + id + "/theme.json"); errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func summarize(p Package, builtin bool) Summary {
	return Summary{p.Manifest.ID, p.Manifest.Version, p.Manifest.Name, p.Manifest.Description, p.ResolvedDigest, builtin}
}
func (s *Store) ReadRevision(id, digest string) (Package, error) {
	if !packageIDPattern.MatchString(id) || !digestPattern.MatchString(digest) {
		return Package{}, ErrNotFound
	}
	if p, ok := s.resolver.builtins[id]; ok {
		if p.ResolvedDigest != digest {
			return Package{}, ErrNotFound
		}
		return clonePackage(p), nil
	}
	index, err := s.readIndex()
	if err != nil {
		return Package{}, err
	}
	found := false
	if !index[id].Published {
		return Package{}, ErrNotFound
	}
	for _, d := range index[id].Versions {
		if d == digest {
			found = true
			break
		}
	}
	if !found {
		return Package{}, ErrNotFound
	}
	return s.readRevision(id, digest)
}
func (s *Store) readRevision(id, digest string) (Package, error) {
	prefix := revisionPath(id, digest)
	raw, err := s.root.ReadRegular(prefix+"theme.json", MaxManifestBytes)
	if err != nil {
		return Package{}, err
	}
	m, err := s.resolver.parse(raw)
	if err != nil {
		return Package{}, err
	}
	files, err := s.readAssets(prefix, m)
	if err != nil {
		return Package{}, err
	}
	p, err := s.resolver.Resolve(raw, files)
	if err != nil {
		return Package{}, err
	}
	if p.Manifest.ID != id || p.ResolvedDigest != digest {
		return Package{}, fmt.Errorf("theme revision integrity mismatch")
	}
	source, err := s.root.ReadRegular(prefix+"source.json", MaxManifestBytes)
	if err != nil {
		return Package{}, err
	}
	expectedSourceDigest, err := s.root.ReadRegular(prefix+"source.sha256", 64)
	if err != nil || !digestPattern.Match(expectedSourceDigest) {
		return Package{}, fmt.Errorf("theme source snapshot identity is unavailable")
	}
	p.Source = source
	authored, err := s.revisionSource(p)
	if err != nil {
		return Package{}, err
	}
	if authored.SourceDigest != string(expectedSourceDigest) {
		return Package{}, fmt.Errorf("theme source snapshot integrity mismatch")
	}
	p.SourceDigest = authored.SourceDigest
	return p, nil
}

// ReadRevisionSource restores exact authored intent, including inheritance and
// asset paths. Package.Assets otherwise remains keyed by resolved serving paths.
func (s *Store) ReadRevisionSource(id, digest string) (Source, error) {
	p, err := s.ReadRevision(id, digest)
	if err != nil {
		return Source{}, err
	}
	return s.revisionSource(p)
}
func (s *Store) revisionSource(p Package) (Source, error) {
	m, err := s.resolver.parse(p.Source)
	if err != nil {
		return Source{}, err
	}
	if m.ID != p.Manifest.ID || m.Version != p.Manifest.Version {
		return Source{}, fmt.Errorf("theme source snapshot package mismatch")
	}
	files := map[string][]byte{}
	for id, original := range m.Assets {
		resolved, ok := p.Manifest.Assets[id]
		if !ok || resolved.MediaType != original.MediaType || resolved.License != original.License {
			return Source{}, fmt.Errorf("theme source snapshot asset mismatch")
		}
		data, ok := p.Assets[resolved.Path]
		if !ok {
			return Source{}, fmt.Errorf("theme source snapshot asset is unavailable")
		}
		files[original.Path] = bytes.Clone(data)
	}
	return Source{Content: string(p.Source), Manifest: bytes.Clone(p.Source), Assets: files, SourceDigest: sourceDigest(p.Source, files)}, nil
}
func (s *Store) readAssets(prefix string, m Manifest) (map[string][]byte, error) {
	files := map[string][]byte{}
	total := 0
	for _, asset := range m.Assets {
		raw, err := s.root.ReadRegular(prefix+asset.Path, MaxAssetBytes)
		if err != nil {
			return nil, err
		}
		total += len(raw)
		if total > MaxPackageBytes {
			return nil, invalid("/assets", "size", "Package exceeds its resource budget.")
		}
		files[asset.Path] = raw
	}
	return files, nil
}

// Publish validates before locking or mutating. The final catalog pointer is
// published only after every immutable asset and manifest is durable. Orphaned
// partial revisions are never listed and can be completed by an exact replay.
func (s *Store) Publish(raw []byte, files map[string][]byte) (Package, error) {
	p, err := s.resolver.Resolve(raw, files)
	if err != nil {
		return Package{}, err
	}
	if s.resolver.IsBuiltin(p.Manifest.ID) {
		return Package{}, ErrBuiltin
	}
	var saved Package
	err = s.withLock(func() error {
		index, err := s.readIndex()
		if err != nil {
			return err
		}
		saved, err = s.publish(p, index)
		return err
	})
	return saved, err
}
func (s *Store) publish(p Package, index revisionIndex) (Package, error) {
	id, version, digest := p.Manifest.ID, p.Manifest.Version, p.ResolvedDigest
	record := index[id]
	if prior := record.Versions[version]; prior != "" {
		if prior != digest {
			return Package{}, fmt.Errorf("%w: ID/version already identifies another presentation", ErrConflict)
		}
		saved, err := s.readRevision(id, prior)
		if err != nil {
			return Package{}, err
		}
		if !record.Published {
			record.Published = true
			index[id] = record
			if err := s.writeIndex(index); err != nil {
				return Package{}, err
			}
		}
		return saved, nil
	}
	m, files := addressAssets(p.Manifest, p.Assets)
	prefix := revisionPath(id, digest)
	if err := s.root.EnsureDir(strings.TrimSuffix(prefix, "/"), 0700); err != nil {
		return Package{}, err
	}
	for name, data := range files {
		if err := s.root.EnsureDir(path.Dir(prefix+name), 0700); err != nil {
			return Package{}, err
		}
		if _, err := s.root.CreateRegularExact(prefix+name, data, 0600, 0700); err != nil {
			return Package{}, err
		}
	}
	if _, err := s.root.CreateRegularExact(prefix+"theme.json", mustJSON(m), 0600, 0700); err != nil {
		return Package{}, err
	}
	if _, err := s.root.CreateRegularExact(prefix+"source.json", p.Source, 0600, 0700); err != nil {
		return Package{}, err
	}
	if _, err := s.root.CreateRegularExact(prefix+"source.sha256", []byte(p.SourceDigest), 0600, 0700); err != nil {
		return Package{}, err
	}
	// Check the complete saved bytes before publishing the discoverable pointer.
	saved, err := s.readRevision(id, digest)
	if err != nil {
		return Package{}, err
	}
	if record.Versions == nil {
		record.Versions = map[string]string{}
	}
	record.Versions[version] = digest
	record.Published = true
	index[id] = record
	if err = s.writeIndex(index); err != nil {
		return Package{}, err
	}
	return saved, nil
}
func (s *Store) ImportZIP(raw []byte) (Package, error) {
	p, err := s.resolver.ImportZIP(raw)
	if err != nil {
		return Package{}, err
	}
	source, err := s.revisionSource(p)
	if err != nil {
		return Package{}, err
	}
	return s.Publish(source.Manifest, source.Assets)
}
func (s *Store) ExportZIP(id, digest string) ([]byte, error) {
	p, err := s.ReadRevision(id, digest)
	if err != nil {
		return nil, err
	}
	return s.resolver.ExportZIP(p)
}

func (s *Store) ReadSource(id string) (Source, error) {
	if !packageIDPattern.MatchString(id) {
		return Source{}, ErrNotFound
	}
	if s.resolver.IsBuiltin(id) {
		return Source{}, ErrBuiltin
	}
	raw, err := s.root.ReadRegular(storePrefix+id+"/theme.json", MaxManifestBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return Source{}, ErrNotFound
	}
	if err != nil {
		return Source{}, err
	}
	result := Source{Content: string(raw), Manifest: bytes.Clone(raw), Assets: map[string][]byte{}}
	m, err := s.resolver.parse(raw)
	if err == nil && m.ID != id {
		err = invalid("/id", "identity", "Source folder and manifest ID must match.")
	}
	if err == nil {
		result.Assets, err = s.readAssets(storePrefix+id+"/", m)
	}
	result.SourceDigest = sourceDigest(raw, result.Assets)
	if err != nil {
		var validation *ValidationError
		if errors.As(err, &validation) {
			result.Diagnostics = validation.Diagnostics
		} else {
			result.Diagnostics = []Diagnostic{{"/assets", "unavailable", "Source assets could not be read safely."}}
		}
		return result, nil
	}
	if _, err = s.resolver.Resolve(raw, result.Assets); err != nil {
		var validation *ValidationError
		if errors.As(err, &validation) {
			result.Diagnostics = validation.Diagnostics
		} else {
			return result, err
		}
	}
	return result, nil
}

// SaveSource writes a valid editable source using optimistic source identity.
// Its asset paths become content-addressed, so readers of the previous manifest
// retain the previous bytes until its atomic replacement. Publishing remains a
// separate operation; callers can report a saved draft if publication fails.
func (s *Store) SaveSource(raw []byte, files map[string][]byte, expectedDigest string) (Source, error) {
	p, err := s.resolver.Resolve(raw, files)
	if err != nil {
		return Source{}, err
	}
	id := p.Manifest.ID
	if s.resolver.IsBuiltin(id) {
		return Source{}, ErrBuiltin
	}
	authored, err := s.resolver.parse(raw)
	if err != nil {
		return Source{}, err
	}
	authored, safeFiles := addressAssets(authored, files)
	sourceRaw := mustJSON(authored)
	targetDigest := sourceDigest(sourceRaw, safeFiles)
	var result Source
	err = s.withLock(func() error {
		current, err := s.ReadSource(id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if current.SourceDigest == targetDigest {
			result = current
			return nil
		}
		if current.SourceDigest != expectedDigest {
			return fmt.Errorf("%w: source changed; preserve your draft and reload the current file", ErrConflict)
		}
		if err = s.root.EnsureDir(storePrefix+id, 0700); err != nil {
			return err
		}
		for name, data := range safeFiles {
			if err = s.root.EnsureDir(path.Dir(storePrefix+id+"/"+name), 0700); err != nil {
				return err
			}
			if _, err = s.root.CreateRegularExact(storePrefix+id+"/"+name, data, 0600, 0700); err != nil {
				return err
			}
		}
		// Recheck after asset staging, including direct file edits made by an agent.
		current, err = s.ReadSource(id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if current.SourceDigest != expectedDigest {
			return fmt.Errorf("%w: source changed during save", ErrConflict)
		}
		if err = s.root.ReplaceRegular(storePrefix+id+"/theme.json", sourceRaw, 0600, 0700, time.Time{}); err != nil {
			return err
		}
		result, err = s.ReadSource(id)
		return err
	})
	return result, err
}

// Refresh publishes an explicitly inspected source without changing any
// browser's selected revision. Invalid files leave all saved revisions intact.
func (s *Store) Refresh(id string) (Package, error) {
	source, err := s.ReadSource(id)
	if err != nil {
		return Package{}, err
	}
	if len(source.Diagnostics) > 0 {
		return Package{}, &ValidationError{source.Diagnostics}
	}
	return s.Publish(source.Manifest, source.Assets)
}

// Remove unpublishes a custom package. Revisions remain as unlisted immutable
// bytes and immutable ID/version ownership; no automated pruning runs. Source deletion is an explicit separate
// choice so removing a package cannot discard an agent's unpublished draft.
func (s *Store) Remove(id string) error {
	if !packageIDPattern.MatchString(id) {
		return ErrNotFound
	}
	if s.resolver.IsBuiltin(id) {
		return ErrBuiltin
	}
	return s.withLock(func() error {
		index, err := s.readIndex()
		if err != nil {
			return err
		}
		record := index[id]
		if !record.Published {
			return ErrNotFound
		}
		record.Published = false
		index[id] = record
		return s.writeIndex(index)
	})
}
func (s *Store) RemoveSource(id, expectedDigest string) error {
	if !packageIDPattern.MatchString(id) {
		return ErrNotFound
	}
	if s.resolver.IsBuiltin(id) {
		return ErrBuiltin
	}
	return s.withLock(func() error {
		source, err := s.ReadSource(id)
		if err != nil {
			return err
		}
		if source.SourceDigest != expectedDigest {
			return ErrConflict
		}
		return s.root.RemoveTree(context.Background(), storePrefix+id, 256)
	})
}

// No absolute path from a theme is used by this package. Names are validated
// before joining them to the descriptor-relative store root.
