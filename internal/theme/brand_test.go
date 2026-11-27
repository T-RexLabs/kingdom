package theme

import (
	"bytes"
	"testing"

	themecontract "kingdom/contracts/theme"
)

func TestBuiltinTechnologyRexBrandIsValidatedAndPortable(t *testing.T) {
	r := resolverForTest(t)
	for _, p := range r.Builtins() {
		t.Run(p.Manifest.ID, func(t *testing.T) {
			if p.Manifest.Icons["brand"] != "asset:brand" {
				t.Fatalf("builtin brand = %q, want the shared Technology Rex asset", p.Manifest.Icons["brand"])
			}
			if p.Manifest.Version != builtinRevisions[p.Manifest.ID].version {
				t.Fatalf("new branding reused an existing package version: %s", p.Manifest.Version)
			}
			color := "black"
			if p.Manifest.ColorScheme == "dark" {
				color = "white"
			}
			asset := p.Manifest.Assets["brand"]
			wantPath := "assets/brand/technology-rex-" + color + ".svg"
			if asset.Path != wantPath || asset.MediaType != "image/svg+xml" {
				t.Fatalf("brand does not use the contrast-appropriate shared SVG: %+v", asset)
			}
			raw, err := themecontract.Files.ReadFile("builtin/" + wantPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(p.Assets[asset.Path], raw) {
				t.Fatal("resolved brand differs from bundled artwork")
			}
			if err := validateSVG(raw); err != nil {
				t.Fatalf("brand violates the static SVG contract: %v", err)
			}
			archive, err := r.ExportZIP(p)
			if err != nil {
				t.Fatal(err)
			}
			imported, err := r.ImportZIP(archive)
			if err != nil {
				t.Fatal(err)
			}
			if imported.ResolvedDigest != p.ResolvedDigest || !bytes.Equal(imported.Assets[imported.Manifest.Assets["brand"].Path], raw) {
				t.Fatal("standalone export lost the brand or changed revision identity")
			}
			t.Logf("validated %s@%s: %s", p.Manifest.ID, p.Manifest.Version, p.ResolvedDigest)
		})
	}
}

func TestInheritedTechnologyRexBrandCanBeReplaced(t *testing.T) {
	r := resolverForTest(t)
	for _, baseID := range []string{DefaultID, "workstation-98"} {
		t.Run(baseID, func(t *testing.T) {
			base := r.builtins[baseID]
			baseBrand := bytes.Clone(base.Assets[base.Manifest.Assets["brand"].Path])
			m := draftForTest()
			m["base"] = map[string]string{"id": baseID, "version": base.Manifest.Version}
			m["assets"] = map[string]Asset{"brand": {Path: "assets/custom-brand.svg", MediaType: "image/svg+xml"}}
			p, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/custom-brand.svg": []byte(safeSVG)})
			if err != nil {
				t.Fatal(err)
			}
			if p.Manifest.Icons["brand"] != "asset:brand" || !bytes.Equal(p.Assets[p.Manifest.Assets["brand"].Path], []byte(safeSVG)) {
				t.Fatal("authored brand artwork did not replace the inherited brand")
			}
			if _, retained := p.Assets[base.Manifest.Assets["brand"].Path]; retained {
				t.Fatal("replaced built-in brand bytes remain in the resolved package")
			}
			if !bytes.Equal(baseBrand, r.builtins[baseID].Assets[base.Manifest.Assets["brand"].Path]) {
				t.Fatal("authored override mutated the bundled brand")
			}
		})
	}
}
