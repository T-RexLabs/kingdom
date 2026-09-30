package theme

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func storeForTest(t *testing.T) (*Store, string) {
	t.Helper()
	home := t.TempDir()
	s, err := OpenStore(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, home
}
func TestPublicationReplayConflictAndFlatExport(t *testing.T) {
	s, _ := storeForTest(t)
	m := draftForTest()
	p, err := s.Publish(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Publish(rawForTest(t, m), nil)
	if err != nil || replay.ResolvedDigest != p.ResolvedDigest {
		t.Fatalf("publication replay: %v", err)
	}
	archive, err := s.ExportZIP(p.Manifest.ID, p.ResolvedDigest)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := s.ImportZIP(archive)
	if err != nil || imported.ResolvedDigest != p.ResolvedDigest {
		t.Fatalf("self-import must not conflict: %v", err)
	}
	second, _ := storeForTest(t)
	other, err := second.ImportZIP(archive)
	if err != nil || other.ResolvedDigest != p.ResolvedDigest {
		t.Fatalf("independent import: %v", err)
	}
	m["name"] = "Conflicting identity"
	if _, err = s.Publish(rawForTest(t, m), nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
	m["version"] = "1.0.1"
	next, err := s.Publish(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadRevision(p.Manifest.ID, p.ResolvedDigest); err != nil {
		t.Fatal("older pinned revision lost", err)
	}
	if next.ResolvedDigest == p.ResolvedDigest {
		t.Fatal("revision did not change")
	}
	if err = s.Remove(DefaultID); !errors.Is(err, ErrBuiltin) {
		t.Fatal("built-in removal accepted")
	}
	if err = s.Remove("custom"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadRevision("custom", p.ResolvedDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("removed package still published")
	}
}
func TestWorkspaceIdentityAndConcurrentVersionOwnership(t *testing.T) {
	s, home := storeForTest(t)
	again, err := OpenStore(home)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	separate, _ := storeForTest(t)
	if s.WorkspaceID() != again.WorkspaceID() || s.WorkspaceID() == separate.WorkspaceID() {
		t.Fatal("workspace identity is not stable and isolated")
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i, store := range []*Store{s, again} {
		wg.Add(1)
		go func(i int, store *Store) {
			defer wg.Done()
			m := draftForTest()
			if i == 1 {
				m["name"] = "Another"
			}
			<-start
			_, e := store.Publish(rawForTest(t, m), nil)
			results <- e
		}(i, store)
	}
	close(start)
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("wanted one owner, got %d successful / %d conflicts", success, conflict)
	}
}
func TestSourceCASAssetStagingAndInvalidRefreshKeepRevision(t *testing.T) {
	s, home := storeForTest(t)
	raw := rawForTest(t, assetDraft("assets/tool.svg"))
	source, err := s.SaveSource(raw, map[string][]byte{"assets/tool.svg": []byte(safeSVG)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(source.Diagnostics) > 0 {
		t.Fatal(source.Diagnostics)
	}
	p, err := s.Refresh("custom")
	if err != nil {
		t.Fatal(err)
	}
	var authored Manifest
	if err = json.Unmarshal(source.Manifest, &authored); err != nil {
		t.Fatal(err)
	}
	oldPath := authored.Assets["tool"].Path
	if oldPath == "assets/tool.svg" {
		t.Fatal("UI-saved source assets must be content addressed")
	}
	var edited map[string]any
	if err = json.Unmarshal(source.Manifest, &edited); err != nil {
		t.Fatal(err)
	}
	edited["name"] = "Agent's Unsaved Work"
	sourcePath := filepath.Join(home, "themes", "custom", "theme.json")
	if err = os.WriteFile(sourcePath, rawForTest(t, edited), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SaveSource(raw, map[string][]byte{"assets/tool.svg": []byte(safeSVG)}, source.SourceDigest); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale save did not conflict: %v", err)
	}
	if got, err := os.ReadFile(sourcePath); err != nil || !bytes.Contains(got, []byte("Agent's")) {
		t.Fatal("agent edit overwritten")
	}
	if err = os.WriteFile(sourcePath, []byte(`{"broken":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Refresh("custom"); err == nil {
		t.Fatal("invalid source published")
	}
	if _, err = s.ReadRevision("custom", p.ResolvedDigest); err != nil {
		t.Fatal("last valid revision lost", err)
	}
	if _, err = os.ReadFile(filepath.Join(home, "themes", "custom", oldPath)); err != nil {
		t.Fatal("prior source asset lost", err)
	}
	invalidSource, err := s.ReadSource("custom")
	if err != nil || len(invalidSource.Diagnostics) == 0 || invalidSource.SourceDigest == "" {
		t.Fatal("invalid authored data must remain inspectable", err)
	}
}
func TestStoreRejectsSymlinkReadsAndDoesNotPublishPartialRevision(t *testing.T) {
	s, home := storeForTest(t)
	m := assetDraft("assets/tool.svg")
	p, err := s.Publish(rawForTest(t, m), map[string][]byte{"assets/tool.svg": []byte(safeSVG)})
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(home, "themes", ".revisions", "custom", p.ResolvedDigest)
	asset := filepath.Join(prefix, p.Manifest.Assets["tool"].Path)
	target := filepath.Join(t.TempDir(), "outside.svg")
	if err = os.WriteFile(target, []byte(safeSVG), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(asset); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, asset); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err = s.ReadRevision("custom", p.ResolvedDigest); err == nil {
		t.Fatal("symlink followed")
	}
	catalog, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Items) != len(s.resolver.Builtins()) || len(catalog.Diagnostics) != 1 {
		t.Fatal("bad custom revision prevented built-in fallback")
	}
	// A partial directory without a catalog commit is never discoverable.
	other := draftForTest()
	other["id"] = "partial"
	candidate, err := s.resolver.Resolve(rawForTest(t, other), nil)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := filepath.Join(home, "themes", ".revisions", "partial", candidate.ResolvedDigest)
	if err = os.MkdirAll(incomplete, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(incomplete, "theme.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadRevision("partial", candidate.ResolvedDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("unpublished revision exposed")
	}
	if _, err = s.Publish(rawForTest(t, other), nil); err == nil {
		t.Fatal("corrupt staged bytes overwritten")
	}
	catalog, _ = s.List()
	if len(catalog.Items) != len(s.resolver.Builtins()) {
		t.Fatal("failed publication changed catalog")
	}
}
func archiveForTest(t *testing.T, names []string, contents [][]byte, symlink bool) []byte {
	t.Helper()
	var out bytes.Buffer
	w := zip.NewWriter(&out)
	for i, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(0644)
		if symlink {
			header.SetMode(os.ModeSymlink | 0777)
		}
		entry, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, "/") {
			continue
		}
		if _, err = entry.Write(contents[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestArchiveRejectsTraversalDuplicatesSymlinksAndExpansion(t *testing.T) {
	r := resolverForTest(t)
	manifest := rawForTest(t, draftForTest())
	for _, name := range []string{"../theme.json", "/theme.json", "a\\theme.json", "assets/../../secret", "theme.json/", "unknown.txt"} {
		if _, err := r.ImportZIP(archiveForTest(t, []string{name}, [][]byte{manifest}, false)); err == nil {
			t.Fatalf("unsafe path accepted: %s", name)
		}
	}
	if _, err := r.ImportZIP(archiveForTest(t, []string{"theme.json", "theme.json"}, [][]byte{manifest, manifest}, false)); err == nil {
		t.Fatal("duplicate archive entry accepted")
	}
	if _, err := r.ImportZIP(archiveForTest(t, []string{"theme.json"}, [][]byte{manifest}, true)); err == nil {
		t.Fatal("archive symlink accepted")
	}
	if _, err := r.ImportZIP(archiveForTest(t, []string{"theme.json"}, [][]byte{bytes.Repeat([]byte(" "), MaxManifestBytes+1)}, false)); err == nil {
		t.Fatal("expanded budget exceeded")
	}
}

func TestInvalidSourceRemainsJSONSerializable(t *testing.T) {
	s, home := storeForTest(t)
	if err := os.MkdirAll(filepath.Join(home, "themes", "broken"), 0700); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"id":"broken",`)
	if err := os.WriteFile(filepath.Join(home, "themes", "broken", "theme.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := s.ReadSource("broken")
	if err != nil || len(source.Diagnostics) == 0 {
		t.Fatalf("invalid source diagnostics: %v, %v", source.Diagnostics, err)
	}
	catalog, err := s.List()
	if err != nil || len(catalog.Sources) != 1 || catalog.Sources[0] != "broken" || len(catalog.Items) != len(s.resolver.Builtins()) {
		t.Fatalf("invalid authored source must be discoverable alongside healthy revisions: %+v, %v", catalog, err)
	}
	if source.Content != string(raw) {
		t.Fatal("source editor did not retain authored bytes")
	}
	if _, err := json.Marshal(source); err != nil {
		t.Fatalf("source editor cannot receive invalid JSON: %v", err)
	}
}

func TestRevisionRetainsAuthoredSourceAndAssetPaths(t *testing.T) {
	s, _ := storeForTest(t)
	draft := assetDraft("assets/authored-icon.svg")
	draft["palette"] = map[string]string{"accent": "#65bac5"}
	raw := append([]byte("\n  "), rawForTest(t, draft)...)
	files := map[string][]byte{"assets/authored-icon.svg": []byte(safeSVG)}
	preview, err := s.Resolver().Resolve(raw, files)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := s.Publish(raw, files)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved.Source, raw) || saved.SourceDigest != preview.SourceDigest {
		t.Fatal("publication lost authored source or source identity")
	}
	restored, err := s.ReadRevisionSource("custom", saved.ResolvedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Content != string(raw) || restored.SourceDigest != preview.SourceDigest || !bytes.Equal(restored.Assets["assets/authored-icon.svg"], []byte(safeSVG)) {
		t.Fatal("revision editing did not restore original intent and matching asset paths")
	}
	var original Manifest
	if err := json.Unmarshal(restored.Manifest, &original); err != nil {
		t.Fatal(err)
	}
	if original.Base == nil || len(original.Tokens) != 0 {
		t.Fatal("source was flattened, so palette edits would be masked")
	}
	// Equivalent publication must replay the first authored snapshot.
	replay, err := s.Publish(rawForTest(t, saved.Manifest), saved.Assets)
	if err != nil || replay.SourceDigest != saved.SourceDigest {
		t.Fatalf("replay replaced source snapshot: %v", err)
	}
	archive, err := s.ExportZIP("custom", saved.ResolvedDigest)
	if err != nil {
		t.Fatal(err)
	}
	flat, err := s.Resolver().ImportZIP(archive)
	if err != nil || flat.ResolvedDigest != saved.ResolvedDigest {
		t.Fatalf("authored snapshot changed flattened export identity: %v", err)
	}
}

func TestUnpublishRetainsImmutableVersionOwnership(t *testing.T) {
	s, _ := storeForTest(t)
	draft := draftForTest()
	original, err := s.Publish(rawForTest(t, draft), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Remove("custom"); err != nil {
		t.Fatal(err)
	}
	draft["name"] = "Different Content"
	if _, err := s.Publish(rawForTest(t, draft), nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("unpublish allowed immutable version reassignment: %v", err)
	}
	if _, err := s.ReadRevision("custom", original.ResolvedDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("unpublished package became visible after conflict")
	}
	draft["version"] = "1.0.1"
	if _, err := s.Publish(rawForTest(t, draft), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadRevision("custom", original.ResolvedDigest); err != nil {
		t.Fatal("republished package lost previous revision", err)
	}
	if err := s.Remove("custom"); err != nil {
		t.Fatal(err)
	}
	if replay, err := s.Publish(rawForTest(t, draftForTest()), nil); err != nil || replay.ResolvedDigest != original.ResolvedDigest {
		t.Fatalf("equivalent revision could not be republished: %v", err)
	}
}
