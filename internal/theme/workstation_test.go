package theme

import (
	"bytes"
	"encoding/json"
	"fmt"
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
			if slot == "brand" {
				continue // The shared Technology Rex brand is validated separately.
			}
			if !bytes.Contains(p.Assets[asset.Path], []byte("<rect")) {
				t.Fatalf("%s has no embedded pixel geometry", slot)
			}
		}
		return
	}
	t.Fatal("Workstation 98 built-in is missing")
}

// builtinRevisions pins each shipped theme's exact version and resolved
// identity, so any content change must come with a new version.
var builtinRevisions = map[string]struct{ version, digest string }{
	"astral-council": {"1.1.0", "847a058fa380856124a1f903a8eacc30feb26a971dab78d173f2cd2b871251d1"},
	"nautilus":       {"1.1.0", "be98e68c4213f234735e796afed39ab114131931ecc73d25422b8d3756eba5fe"},
	"obsidian-court": {"1.1.0", "e6cbf420833435bb56451a9c7aec44b896a5c0662d522b5f6bb55a352ca2fa19"},
	"software-lab":   {"1.1.0", "7058d319649896731b28f56c5a6c548ec2481041018ebc97fd7f755ca79f9ccc"},
	"t-rex-labs":     {"1.0.0", "624e9084d7e60472ab253bb017c08af96f1f0d2b905d818948032d5732c77b5d"},
	"workstation-98": {"1.1.0", "1df4437cdcfbe2fd77ada6cb2d2a21b0519f002efea2ffb1644630f61277d9c2"},
}

func TestBuiltinThemeRevisionDigests(t *testing.T) {
	builtins := resolverForTest(t).Builtins()
	if len(builtins) != len(builtinRevisions) {
		t.Fatalf("got %d built-in themes, want %d", len(builtins), len(builtinRevisions))
	}
	for _, p := range builtins {
		if want, ok := builtinRevisions[p.Manifest.ID]; !ok || p.Manifest.Version != want.version || p.ResolvedDigest != want.digest {
			t.Errorf("%s revision changed: %s %s", p.Manifest.ID, p.Manifest.Version, p.ResolvedDigest)
		}
	}
}

func workstationDraft() map[string]any {
	m := draftForTest()
	m["base"] = map[string]string{"id": "workstation-98", "version": "1.1.0"}
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

func TestThemeNumericStylesFollowCSSRanges(t *testing.T) {
	r := resolverForTest(t)
	for _, field := range []string{"radius", "borderWidth"} {
		for _, value := range []float64{0, 0.5, 1, 9, 21, 1024.75} {
			t.Run(fmt.Sprintf("%s/%g", field, value), func(t *testing.T) {
				m := workstationDraft()
				m["surfaces"] = map[string]any{field: value}
				p, err := r.Resolve(rawForTest(t, m), nil)
				if err != nil {
					t.Fatal(err)
				}
				var resolved map[string]any
				if err := json.Unmarshal(rawForTest(t, p.Manifest.Surfaces), &resolved); err != nil {
					t.Fatal(err)
				}
				if resolved[field] != value {
					t.Fatalf("authored %s=%g was not retained: %v", field, value, resolved[field])
				}
				flattened, err := r.Resolve(rawForTest(t, p.Manifest), p.Assets)
				if err != nil || flattened.ResolvedDigest != p.ResolvedDigest {
					t.Fatalf("numeric style round trip changed: %v", err)
				}
			})
		}
		for _, value := range []any{-0.1, "4px", nil} {
			m := workstationDraft()
			m["surfaces"] = map[string]any{field: value}
			if _, err := r.Resolve(rawForTest(t, m), nil); err == nil {
				t.Fatalf("invalid %s=%v accepted", field, value)
			}
		}
	}
	for _, field := range []string{"bodyWeight", "headingWeight"} {
		for _, value := range []float64{1, 100, 250.5, 500, 950.25, 1000} {
			t.Run(fmt.Sprintf("%s/%g", field, value), func(t *testing.T) {
				m := draftForTest()
				m["typography"] = map[string]any{field: value}
				p, err := r.Resolve(rawForTest(t, m), nil)
				if err != nil {
					t.Fatal(err)
				}
				var resolved map[string]any
				if err := json.Unmarshal(rawForTest(t, p.Manifest.Typography), &resolved); err != nil {
					t.Fatal(err)
				}
				if resolved[field] != value {
					t.Fatalf("authored %s=%g was not retained: %v", field, value, resolved[field])
				}
			})
		}
		for _, value := range []any{0, 1000.1, "bold", nil} {
			m := draftForTest()
			m["typography"] = map[string]any{field: value}
			if _, err := r.Resolve(rawForTest(t, m), nil); err == nil {
				t.Fatalf("invalid %s=%v accepted", field, value)
			}
		}
	}
	for _, raw := range []string{
		`{"radius":1e400}`, `{"borderWidth":1e400}`, `{"radius":NaN}`, `{"borderWidth":Infinity}`,
	} {
		m := draftForTest()
		source := strings.TrimSuffix(string(rawForTest(t, m)), "}") + `,"surfaces":` + raw + "}"
		if _, err := r.Resolve([]byte(source), nil); err == nil {
			t.Fatalf("nonfinite surface accepted: %s", raw)
		}
	}
}
