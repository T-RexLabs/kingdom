package theme

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	themecontract "kingdom/contracts/theme"
)

func resolverForTest(t *testing.T) *Resolver {
	t.Helper()
	r, err := NewResolver()
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func draftForTest() map[string]any {
	return map[string]any{"schemaVersion": 1, "resolverVersion": 1, "id": "custom", "version": "1.0.0", "name": "Custom", "base": map[string]any{"id": DefaultID, "version": "1.1.0"}}
}
func rawForTest(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestBuiltinsAndInheritedThemeAreComplete(t *testing.T) {
	r := resolverForTest(t)
	if len(r.Builtins()) != len(builtinRevisions) {
		t.Fatalf("expected all %d themes", len(builtinRevisions))
	}
	for _, p := range r.Builtins() {
		t.Run(p.Manifest.ID, func(t *testing.T) {
			if len(p.Manifest.Identities) != 14 {
				t.Fatal("role catalog incomplete")
			}
			if p.Manifest.Base != nil {
				t.Fatal("built-in must be independent")
			}
			if _, err := r.Resolve(rawForTest(t, p.Manifest), p.Assets); err != nil {
				t.Fatal(err)
			}
		})
	}
	p, err := r.Resolve(rawForTest(t, draftForTest()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Manifest.Icons["role.builder"] == "" || p.Manifest.Typography.Body == "" {
		t.Fatal("inheritance incomplete")
	}
	raw, err := themecontract.Files.ReadFile("fixtures/harbor-workshop.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Resolve(raw, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAgentVoiceIsOptionalNormalizedAndNeverInheritedFromVisualBase(t *testing.T) {
	r := resolverForTest(t)
	withVoice := draftForTest()
	withVoice["agentVoice"] = "  Keep your language calm and concise.  "
	pkg, err := r.Resolve(rawForTest(t, withVoice), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pkg.Manifest.AgentVoice != "Keep your language calm and concise." {
		t.Fatalf("expected normalized authored voice, got %q", pkg.Manifest.AgentVoice)
	}
	withoutVoice, err := r.Resolve(rawForTest(t, draftForTest()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pkg.ResolvedDigest == withoutVoice.ResolvedDigest {
		t.Fatal("agent voice must be part of theme revision identity")
	}

	base := cloneManifest(r.builtins[DefaultID].Manifest)
	base.AgentVoice = "base voice must not be inherited"
	resolved := mergeManifest(base, Manifest{ID: "custom", Version: "1.0.0", Name: "Custom", AgentVoice: ""})
	if resolved.AgentVoice != "" {
		t.Fatalf("custom visual theme inherited base voice %q", resolved.AgentVoice)
	}

	whitespace := draftForTest()
	whitespace["agentVoice"] = " \n\t "
	if _, err := r.Resolve(rawForTest(t, whitespace), nil); err == nil {
		t.Fatal("whitespace-only agent voice should be rejected")
	}
	tooLong := draftForTest()
	tooLong["agentVoice"] = strings.Repeat("x", 2001)
	if _, err := r.Resolve(rawForTest(t, tooLong), nil); err == nil {
		t.Fatal("agent voice above the supported character limit should be rejected")
	}
}

func TestThemeInputBoundaries(t *testing.T) {
	r := resolverForTest(t)
	tests := map[string]func(map[string]any){
		"voice":         func(m map[string]any) { m["voice"] = "pirate" },
		"runtime":       func(m map[string]any) { m["runtime"] = map[string]any{"model": "anything"} },
		"css":           func(m map[string]any) { m["css"] = "body {}" },
		"unknown token": func(m map[string]any) { m["tokens"] = map[string]string{"gold": "#112233"} },
		"unknown role":  func(m map[string]any) { m["identities"] = map[string]any{"king": map[string]string{"name": "King"}} },
		"second icon source": func(m map[string]any) {
			m["identities"] = map[string]any{"builder": map[string]string{"icon": "lucide:hammer"}}
		},
		"unknown icon":    func(m map[string]any) { m["icons"] = map[string]string{"role.builder": "lucide:not-a-real-symbol"} },
		"unknown font":    func(m map[string]any) { m["typography"] = map[string]string{"body": "bundled:remote-font"} },
		"missing base":    func(m map[string]any) { m["base"] = map[string]string{"id": DefaultID, "version": "99.0.0"} },
		"incomplete root": func(m map[string]any) { delete(m, "base") },
		"empty name":      func(m map[string]any) { m["name"] = "   " },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := draftForTest()
			mutate(m)
			_, err := r.Resolve(rawForTest(t, m), nil)
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("expected validation error, got %v", err)
			}
		})
	}
	for _, raw := range [][]byte{[]byte(`{"schemaVersion":1,"schemaVersion":2}`), []byte(`{} {}`), []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}} {
		if _, err := r.Resolve(raw, nil); err == nil {
			t.Fatal("ambiguous/invalid JSON accepted")
		}
	}
}
func TestPaletteKeepsExplicitOverridesAndResolvedIdentity(t *testing.T) {
	r := resolverForTest(t)
	m := draftForTest()
	m["palette"] = map[string]string{"accent": "#65bac5"}
	m["tokens"] = map[string]string{"chart.grid": "#182f32"}
	p, err := r.Resolve(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Manifest.Tokens["accent.base"] != "#65bac5" || p.Manifest.Tokens["chart.grid"] != "#182f32" {
		t.Fatal("wrong override order")
	}
	flattened, err := r.Resolve(rawForTest(t, p.Manifest), p.Assets)
	if err != nil {
		t.Fatal(err)
	}
	if flattened.ResolvedDigest != p.ResolvedDigest {
		t.Fatal("flattening changed revision identity")
	}
	spaced := append([]byte("\n  "), rawForTest(t, m)...)
	other, err := r.Resolve(spaced, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.SourceDigest == other.SourceDigest || p.ResolvedDigest != other.ResolvedDigest {
		t.Fatal("source and resolved identity conflated")
	}
	// Returned data is not a mutable reference into the built-in registry.
	p.Manifest.Icons["role.builder"] = "lucide:circle"
	next, err := r.Resolve(rawForTest(t, draftForTest()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.Manifest.Icons["role.builder"] == "lucide:circle" {
		t.Fatal("mutable built-in leaked")
	}
}

const safeSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M2 3 L20 21" fill="none" stroke="currentColor" stroke-width="2"/></svg>`

func assetDraft(path string) map[string]any {
	m := draftForTest()
	m["assets"] = map[string]any{"tool": Asset{Path: path, MediaType: "image/svg+xml"}}
	m["icons"] = map[string]string{"role.builder": "asset:tool"}
	return m
}
func TestAssetReferencesAndDigestIgnoreIncidentalPaths(t *testing.T) {
	r := resolverForTest(t)
	m := assetDraft("assets/tool.svg")
	p, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/tool.svg": []byte(safeSVG)})
	if err != nil {
		t.Fatal(err)
	}
	moved, err := r.Resolve(rawForTest(t, assetDraft("assets/another-name.svg")), map[string][]byte{"assets/another-name.svg": []byte(safeSVG)})
	if err != nil {
		t.Fatal(err)
	}
	if p.ResolvedDigest != moved.ResolvedDigest || p.SourceDigest == moved.SourceDigest {
		t.Fatal("digest depends on archive path")
	}
	for _, files := range []map[string][]byte{nil, {"assets/tool.svg": []byte(safeSVG), "secret": []byte("no")}, {"assets/tool.svg": bytes.Repeat([]byte("x"), MaxAssetBytes+1)}} {
		if _, err := r.Resolve(rawForTest(t, m), files); err == nil {
			t.Fatal("invalid asset set accepted")
		}
	}
	for _, svg := range []string{`<svg viewBox="0 0 24 24" onload="alert(1)"/>`, `<svg viewBox="0 0 24 24"><script>alert(1)</script></svg>`, `<svg viewBox="0 0 24 24"><use href="https://example.com/x"/></svg>`, `<!DOCTYPE svg [<!ENTITY x SYSTEM "file:///etc/passwd">]><svg viewBox="0 0 24 24">&x;</svg>`, `<svg viewBox="0 0 24 24"><path d="M0 0" fill="url(https://example.com)"/></svg>`, `<svg viewBox="0 0 24 24"><foreignObject/></svg>`, `<svg viewBox="0 0 24 24"><path style="fill:red"/></svg>`} {
		if _, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/tool.svg": []byte(svg)}); err == nil {
			t.Fatalf("unsafe SVG accepted: %s", svg)
		}
	}
}

func TestBuiltinTokensMatchCanonicalPaletteResolver(t *testing.T) {
	r := resolverForTest(t)
	for _, p := range r.Builtins() {
		for key, expected := range derivePalette(p.Manifest.Palette) {
			// Classic desktop fields use cream paper within sepia window chrome.
			// Every other token, including all four existing themes, stays derived.
			if p.Manifest.ID == "workstation-98" && key == "surface.inset" {
				expected = "#fffaf0"
			}
			if p.Manifest.Tokens[key] != expected {
				t.Errorf("%s token %s = %s, canonical palette resolves %s", p.Manifest.ID, key, p.Manifest.Tokens[key], expected)
			}
		}
	}
}

func TestThemeContrastIsAdvisoryThroughoutPublication(t *testing.T) {
	s, _ := storeForTest(t)
	draft := draftForTest()
	draft["palette"] = map[string]string{"accent": "#0d1110"}
	draft["tokens"] = map[string]string{"text.primary": "#0d1110"}
	source := rawForTest(t, draft)
	preview, err := s.resolver.Resolve(source, nil)
	if err != nil {
		t.Fatalf("contrast must not prevent preview: %v", err)
	}
	assertWarnings := func(p Package) {
		t.Helper()
		var response struct {
			Warnings []Diagnostic `json:"warnings"`
		}
		if err := json.Unmarshal(rawForTest(t, p), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Warnings) == 0 {
			t.Fatal("low contrast should produce advisory warnings")
		}
		for _, warning := range response.Warnings {
			if warning.Code != "contrast" || warning.Path == "" || warning.Message == "" {
				t.Fatalf("unexpected warning: %#v", warning)
			}
		}
		if p.Manifest.Tokens["text.primary"] != "#0d1110" || p.Manifest.Tokens["accent.base"] != "#0d1110" {
			t.Fatal("contrast warning changed authored colors")
		}
	}
	assertWarnings(preview)
	saved, err := s.SaveSource(source, nil, "")
	if err != nil {
		t.Fatalf("contrast must not prevent saving: %v", err)
	}
	published, err := s.Publish(saved.Manifest, saved.Assets)
	if err != nil {
		t.Fatalf("contrast must not prevent publication: %v", err)
	}
	assertWarnings(published)
	restored, err := s.ReadRevision("custom", published.ResolvedDigest)
	if err != nil {
		t.Fatal(err)
	}
	assertWarnings(restored)
	if restored.ResolvedDigest != preview.ResolvedDigest {
		t.Fatal("advisory warnings changed revision identity")
	}
}

func TestNautilusPreservesApprovedLightPalette(t *testing.T) {
	r := resolverForTest(t)
	for _, p := range r.Builtins() {
		if p.Manifest.ID != "nautilus" {
			continue
		}
		if p.Manifest.ColorScheme != "light" {
			t.Fatalf("Nautilus color scheme=%q, want light", p.Manifest.ColorScheme)
		}
		for key, want := range map[string]string{"canvas": "#f3f0e7", "surface": "#faf8f1", "text": "#213f43", "accent": "#23616a"} {
			if got := p.Manifest.Palette[key]; got != want {
				t.Errorf("Nautilus %s=%s, want %s", key, got, want)
			}
		}
		return
	}
	t.Fatal("Nautilus builtin missing")
}
