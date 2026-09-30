package theme

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestWorkstationThemePresentation(t *testing.T) {
	r := resolverForTest(t)
	for _, p := range r.Builtins() {
		if p.Manifest.ID != "workstation-98" {
			continue
		}
		if p.Manifest.Name != "Workstation 98" || p.Manifest.ColorScheme != "light" {
			t.Fatal("classic workstation identity is incomplete")
		}
		if p.Manifest.Typography.Body != "bundled:classic-sans" || p.Manifest.Typography.Display != "bundled:classic-sans" {
			t.Fatal("classic workstation typography is missing")
		}
		if p.Manifest.Surfaces.Chrome != "classic" || p.Manifest.Surfaces.IconRendering != "original" || p.Manifest.Surfaces.Radius == nil || *p.Manifest.Surfaces.Radius != 0 {
			t.Fatal("classic structural presentation is missing")
		}
		if p.Manifest.Tokens["surface.inset"] != "#fffaf0" {
			t.Fatal("classic cream fields are missing")
		}
		for key, color := range map[string]string{"canvas": "#e9e1d2", "surface": "#d8cbb5", "text": "#332c24", "accent": "#5a3e2b"} {
			if p.Manifest.Palette[key] != color {
				t.Fatalf("full sepia %s=%s, want %s", key, p.Manifest.Palette[key], color)
			}
		}
		for role, name := range map[string]string{
			"operator": "Workstation User", "governance": "Managing Director", "coordinator": "Project Manager", "plan-author": "Business Analyst",
			"architecture-advisor": "Systems Architect", "risk-advisor": "Risk Analyst", "systems-advisor": "Systems Engineer", "usability-advisor": "Usability Specialist",
			"builder": "Software Engineer", "reviewer": "Code Reviewer", "qa-validator": "Quality Assurance", "security-validator": "Security Analyst",
			"resilience-validator": "Reliability Engineer", "planning-assistant": "Planning Consultant",
		} {
			if p.Manifest.Identities[role].Name != name || p.Manifest.Identities[role].Description != r.builtins["software-lab"].Manifest.Identities[role].Description {
				t.Fatalf("corporate identity or canonical responsibility changed for %s", role)
			}
		}
		for slot, reference := range p.Manifest.Icons {
			if !strings.HasPrefix(reference, "asset:") {
				t.Fatalf("%s does not use an original pixel asset", slot)
			}
			asset := p.Manifest.Assets[reference[6:]]
			if !bytes.Contains(p.Assets[asset.Path], []byte("<rect")) {
				t.Fatalf("%s has no embedded pixel geometry", slot)
			}
		}
		return
	}
	t.Fatal("Workstation 98 built-in is missing")
}

func TestExistingThemeDigests(t *testing.T) {
	want := map[string]string{
		"astral-council": "2ce82240b1b709e3affe56d9406a85d65c2411f8875a8a9c440c7c43939f0b40",
		"nautilus":       "c58afb750336e78f055d5d4ec65516a76a0fd389546481f67f98689b628bc680",
		"obsidian-court": "3dc6cea4927d577f6bb95d5a153e0a62c4f357a5b524709fcb4706c142de09f0",
		"software-lab":   "a822d1eaba88deeddb429ace83ac7d1f5b88941a408356722dc1fd0ec18aec11",
	}
	for _, p := range resolverForTest(t).Builtins() {
		if digest, ok := want[p.Manifest.ID]; ok && p.ResolvedDigest != digest {
			t.Errorf("%s revision changed: %s", p.Manifest.ID, p.ResolvedDigest)
		}
	}
}

func workstationDraft() map[string]any {
	m := draftForTest()
	m["base"] = map[string]string{"id": "workstation-98", "version": "1.0.0"}
	return m
}

func TestClassicSurfacesRemainFiniteAndInheritable(t *testing.T) {
	r := resolverForTest(t)
	m := workstationDraft()
	m["surfaces"] = map[string]string{"chrome": "flat", "iconRendering": "mask"}
	p, err := r.Resolve(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Manifest.Surfaces.Chrome != "flat" || p.Manifest.Surfaces.IconRendering != "mask" {
		t.Fatal("explicit presentation override was not inherited")
	}
	for _, field := range []string{"chrome", "iconRendering"} {
		m["surfaces"] = map[string]string{field: "url(https://example.invalid/style)"}
		if _, err := r.Resolve(rawForTest(t, m), nil); err == nil {
			t.Fatalf("arbitrary %s accepted", field)
		}
	}
}

func TestInheritedBuiltInAssetsPreserveAuthoredAndResolvedIdentity(t *testing.T) {
	for _, overridePath := range []string{"", "assets/custom-workstation.svg", "assets/workstation-98/workstation.svg"} {
		t.Run(overridePath, func(t *testing.T) {
			s, _ := storeForTest(t)
			m := workstationDraft()
			files := map[string][]byte{}
			if overridePath != "" {
				m["assets"] = map[string]Asset{"workstation": {Path: overridePath, MediaType: "image/svg+xml"}}
				files[overridePath] = []byte(safeSVG)
			}
			raw := rawForTest(t, m)
			p, err := s.resolver.Resolve(raw, files)
			if err != nil {
				t.Fatal(err)
			}
			if p.SourceDigest != sourceDigest(raw, files) {
				t.Fatal("source identity includes un-authored base bytes")
			}
			if len(p.Assets) != len(s.resolver.builtins["workstation-98"].Assets) {
				t.Fatal("inherited asset set is incomplete")
			}
			if overridePath != "" && !bytes.Equal(p.Assets[overridePath], []byte(safeSVG)) {
				t.Fatal("authored asset override was not preserved")
			}
			source, err := s.SaveSource(raw, files, "")
			if err != nil {
				t.Fatal(err)
			}
			published, err := s.Publish(source.Manifest, source.Assets)
			if err != nil || published.ResolvedDigest != p.ResolvedDigest {
				t.Fatalf("source publication changed resolved identity: %v", err)
			}
			stored, err := s.ReadRevision("custom", p.ResolvedDigest)
			if err != nil || stored.SourceDigest != source.SourceDigest {
				t.Fatalf("immutable source identity was not retained: %v", err)
			}
			restored, err := s.ReadRevisionSource("custom", p.ResolvedDigest)
			if err != nil || restored.Content != source.Content || !reflect.DeepEqual(restored.Assets, source.Assets) {
				t.Fatalf("authored source could not be restored exactly: %v", err)
			}
			archive, err := s.ExportZIP("custom", p.ResolvedDigest)
			if err != nil {
				t.Fatal(err)
			}
			other, _ := storeForTest(t)
			imported, err := other.ImportZIP(archive)
			if err != nil || imported.ResolvedDigest != p.ResolvedDigest {
				t.Fatalf("flattened archive changed resolved identity: %v", err)
			}
			// An authored ZIP can contain only the source's own assets. Import
			// must not mistake the resolver's inherited bytes for extra input.
			names, content := []string{"theme.json"}, [][]byte{raw}
			for path, data := range files {
				names, content = append(names, path), append(content, data)
			}
			fresh, _ := storeForTest(t)
			inherited, err := fresh.ImportZIP(archiveForTest(t, names, content, false))
			if err != nil || inherited.ResolvedDigest != p.ResolvedDigest || inherited.SourceDigest != p.SourceDigest {
				t.Fatalf("authored ZIP import changed identity: %v", err)
			}
		})
	}
}

func TestInheritedAssetsRejectShadowBytesAndPathCollisions(t *testing.T) {
	r := resolverForTest(t)
	m := workstationDraft()
	files := map[string][]byte{"assets/workstation-98/workstation.svg": []byte(safeSVG)}
	if _, err := r.Resolve(rawForTest(t, m), files); err == nil {
		t.Fatal("undeclared inherited asset replacement accepted")
	}
	m["assets"] = map[string]Asset{"unrelated": {Path: "assets/workstation-98/workstation.svg", MediaType: "image/svg+xml"}}
	if _, err := r.Resolve(rawForTest(t, m), files); err == nil {
		t.Fatal("different asset identity at an inherited path accepted")
	}
}
