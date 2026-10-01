package theme

import (
	"errors"
	"reflect"
	"testing"
)

func TestOrnamentationLevelsResolveWithInheritedSurfaces(t *testing.T) {
	r := resolverForTest(t)
	for _, baseID := range []string{DefaultID, "workstation-98"} {
		t.Run(baseID, func(t *testing.T) {
			base := r.builtins[baseID]
			m := draftForTest()
			m["base"] = map[string]string{"id": baseID, "version": base.Manifest.Version}
			inherited, err := r.Resolve(rawForTest(t, m), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(inherited.Manifest.Surfaces, base.Manifest.Surfaces) {
				t.Fatal("omitted ornamentation changed inherited surfaces")
			}
			for _, level := range []string{"minimal", "balanced", "high", "ludicrous"} {
				t.Run(level, func(t *testing.T) {
					m["surfaces"] = map[string]string{"ornamentation": level}
					p, err := r.Resolve(rawForTest(t, m), nil)
					if err != nil {
						t.Fatal(err)
					}
					want := base.Manifest.Surfaces
					want.Ornamentation = level
					if !reflect.DeepEqual(p.Manifest.Surfaces, want) {
						t.Fatalf("ornamentation override changed other inherited surfaces: got %+v, want %+v", p.Manifest.Surfaces, want)
					}
					flattened, err := r.Resolve(rawForTest(t, p.Manifest), p.Assets)
					if err != nil {
						t.Fatal(err)
					}
					if flattened.Manifest.Surfaces.Ornamentation != level || flattened.ResolvedDigest != p.ResolvedDigest {
						t.Fatal("standalone round trip changed ornamentation or revision identity")
					}
				})
			}
		})
	}
}

func TestOrnamentationRejectsUnsupportedValues(t *testing.T) {
	r := resolverForTest(t)
	for name, value := range map[string]any{
		"unknown": "extreme", "unsupported name": "crazy", "case mismatch": "High", "empty": "",
		"css": "linear-gradient(red, blue)", "number": 3, "null": nil,
	} {
		t.Run(name, func(t *testing.T) {
			m := draftForTest()
			m["surfaces"] = map[string]any{"ornamentation": value}
			_, err := r.Resolve(rawForTest(t, m), nil)
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("expected validation error for ornamentation=%v, got %v", value, err)
			}
			for _, diagnostic := range validation.Diagnostics {
				if diagnostic.Path == "/surfaces/ornamentation" && diagnostic.Code == "schema" {
					return
				}
			}
			t.Fatalf("missing ornamentation diagnostic: %+v", validation.Diagnostics)
		})
	}
}
