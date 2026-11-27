package theme

import (
	"strings"
	"testing"
)

func TestActionTokensAreOptionalAndChecked(t *testing.T) {
	r := resolverForTest(t)
	for _, p := range r.Builtins() {
		if p.Manifest.ID == "t-rex-labs" {
			continue
		}
		for key := range p.Manifest.Tokens {
			if strings.HasPrefix(key, "action.") {
				t.Fatalf("%s gained %s; action tokens must stay optional so existing revisions keep their identity", p.Manifest.ID, key)
			}
		}
	}

	m := draftForTest()
	m["tokens"] = map[string]string{"action.base": "#e3ac42", "action.hover": "#eab955", "action.foreground": "#0b1c20", "action.border": "#0b1c20"}
	p, err := r.Resolve(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Manifest.Tokens["action.base"] != "#e3ac42" {
		t.Fatal("authored action token was not kept")
	}
	for _, warning := range contrastWarnings(p.Manifest) {
		if strings.HasPrefix(warning.Path, "/tokens/action.") {
			t.Fatalf("readable action colors were flagged: %+v", warning)
		}
	}

	m["tokens"] = map[string]string{"action.base": "#e3ac42", "action.foreground": "#ffffff"}
	p, err = r.Resolve(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, warning := range contrastWarnings(p.Manifest) {
		if warning.Path == "/tokens/action.foreground" {
			return
		}
	}
	t.Fatal("white on gold was not flagged")
}

func TestStorybookChromeResolves(t *testing.T) {
	r := resolverForTest(t)
	m := draftForTest()
	m["surfaces"] = map[string]string{"chrome": "storybook"}
	p, err := r.Resolve(rawForTest(t, m), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Manifest.Surfaces.Chrome != "storybook" {
		t.Fatalf("chrome = %q, want storybook", p.Manifest.Surfaces.Chrome)
	}
}
