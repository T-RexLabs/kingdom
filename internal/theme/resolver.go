package theme

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
	themecontract "kingdom/contracts/theme"
)

type Resolver struct {
	schema   *jsonschema.Schema
	icons    map[string]bool
	builtins map[string]Package
}

func NewResolver() (*Resolver, error) {
	raw, err := themecontract.Files.ReadFile("theme.schema.json")
	if err != nil {
		return nil, err
	}
	var document any
	if err = json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	const uri = "https://kingdom.local/contracts/theme.schema.json"
	if err = compiler.AddResource(uri, document); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile(uri)
	if err != nil {
		return nil, err
	}
	r := &Resolver{schema: schema, icons: map[string]bool{}, builtins: map[string]Package{}}
	raw, err = themecontract.Files.ReadFile("lucide-icons.json")
	if err != nil {
		return nil, err
	}
	var names []string
	if err = json.Unmarshal(raw, &names); err != nil {
		return nil, err
	}
	for _, n := range names {
		r.icons[n] = true
	}
	entries, err := themecontract.Files.ReadDir("builtin")
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		raw, err = themecontract.Files.ReadFile("builtin/" + entry.Name())
		if err != nil {
			return nil, err
		}
		p, err := r.Resolve(raw, nil)
		if err != nil {
			return nil, fmt.Errorf("built-in %s: %w", entry.Name(), err)
		}
		r.builtins[p.Manifest.ID] = p
	}
	return r, nil
}
func SchemaJSON() []byte { raw, _ := themecontract.Files.ReadFile("theme.schema.json"); return raw }
func IconCatalogJSON() []byte {
	raw, _ := themecontract.Files.ReadFile("lucide-icons.json")
	return raw
}
func (r *Resolver) Builtins() []Package {
	result := make([]Package, 0, len(r.builtins))
	for _, p := range r.builtins {
		result = append(result, clonePackage(p))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Manifest.ID < result[j].Manifest.ID })
	return result
}
func (r *Resolver) IsBuiltin(id string) bool { _, ok := r.builtins[id]; return ok }

func (r *Resolver) parse(raw []byte) (Manifest, error) {
	if len(raw) > MaxManifestBytes {
		return Manifest{}, invalid("/", "size", "Theme manifest exceeds 256 KiB.")
	}
	if !utf8.Valid(raw) {
		return Manifest{}, invalid("/", "json", "Theme JSON must be valid UTF-8.")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := checkJSONValue(decoder, 0); err != nil {
		return Manifest{}, invalid("/", "json", err.Error())
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Manifest{}, invalid("/", "json", "Expected one JSON document.")
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Manifest{}, invalid("/", "json", err.Error())
	}
	if err := r.schema.Validate(doc); err != nil {
		var se *jsonschema.ValidationError
		if errors.As(err, &se) {
			var issues []Diagnostic
			var visit func(*jsonschema.ValidationError)
			visit = func(e *jsonschema.ValidationError) {
				if len(e.Causes) > 0 {
					for _, cause := range e.Causes {
						visit(cause)
					}
					return
				}
				output := e.BasicOutput()
				location := output.InstanceLocation
				if location == "" {
					location = "/"
				}
				message := e.Error()
				if output.Error != nil {
					message = output.Error.String()
				}
				issues = append(issues, Diagnostic{location, "schema", message})
			}
			visit(se)
			return Manifest{}, &ValidationError{issues}
		}
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
func checkJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return fmt.Errorf("JSON nesting exceeds 32 levels")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delimiter == '{' {
		keys := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			if !ok {
				return fmt.Errorf("expected object key")
			}
			if keys[s] {
				return fmt.Errorf("duplicate JSON key %q", s)
			}
			keys[s] = true
			if err = checkJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	} else if delimiter == '[' {
		for d.More() {
			if err = checkJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

// Resolve validates authored data and assets before returning a complete,
// independent package. Its resolved digest is unchanged by flattening a base,
// JSON formatting, editor hints, or relocating the same asset bytes.
func (r *Resolver) Resolve(raw []byte, files map[string][]byte) (Package, error) {
	authored, err := r.parse(raw)
	if err != nil {
		return Package{}, err
	}
	if !portableName(authored.ID) {
		return Package{}, invalid("/id", "path", "Choose a portable package ID that is not a reserved device name.")
	}
	m := authored
	if authored.Base != nil {
		base, ok := r.builtins[authored.Base.ID]
		if !ok || base.Manifest.Version != authored.Base.Version {
			return Package{}, invalid("/base", "missing_base", "The exact built-in base version is unavailable.")
		}
		m = mergeManifest(cloneManifest(base.Manifest), authored)
	}
	m.Schema = ""
	m.Base = nil
	if m.Assets == nil {
		m.Assets = map[string]Asset{}
	}
	if m.Artwork == nil {
		m.Artwork = map[string]string{}
	}
	for key, c := range m.Palette {
		m.Palette[key] = strings.ToLower(c)
	}
	for key, c := range m.Tokens {
		m.Tokens[key] = strings.ToLower(c)
	}
	resolvedRaw, err := json.Marshal(m)
	if err != nil {
		return Package{}, err
	}
	if _, err = r.parse(resolvedRaw); err != nil {
		return Package{}, err
	}
	if strings.TrimSpace(m.Name) == "" {
		return Package{}, invalid("/name", "empty", "Provide a theme name.")
	}
	for role, id := range m.Identities {
		if strings.TrimSpace(id.Name) == "" || strings.TrimSpace(id.Description) == "" {
			return Package{}, invalid("/identities/"+role, "empty", "Name and description must contain text.")
		}
	}
	for key, value := range m.Copy {
		if strings.TrimSpace(value) == "" {
			return Package{}, invalid("/copy/"+key, "empty", "Copy must contain text.")
		}
	}
	if err = validateAssets(m, files); err != nil {
		return Package{}, err
	}
	ref := func(value, path, kind string) error {
		if strings.HasPrefix(value, "asset:") {
			id := strings.TrimPrefix(value, "asset:")
			a, ok := m.Assets[id]
			if !ok {
				return invalid(path, "missing_asset", "Referenced asset is not declared.")
			}
			allowed := kind == "image" && strings.HasPrefix(a.MediaType, "image/") || kind == "icon" && a.MediaType == "image/svg+xml" || kind == "font" && a.MediaType == "font/woff2"
			if !allowed {
				return invalid(path, "asset_type", "Asset type does not match this slot.")
			}
			if kind == "font" && strings.TrimSpace(a.License) == "" {
				return invalid("/assets/"+id+"/license", "license", "Provide font license or ownership information.")
			}
			return nil
		}
		if kind == "icon" && strings.HasPrefix(value, "lucide:") && r.icons[strings.TrimPrefix(value, "lucide:")] {
			return nil
		}
		if kind == "font" && strings.HasPrefix(value, "bundled:") {
			return nil
		}
		return invalid(path, "unknown_reference", "Use a catalog symbol or a declared asset of the correct type.")
	}
	for key, value := range m.Icons {
		if err = ref(value, "/icons/"+key, "icon"); err != nil {
			return Package{}, err
		}
	}
	for key, value := range map[string]string{"body": m.Typography.Body, "display": m.Typography.Display, "mono": m.Typography.Mono} {
		if err = ref(value, "/typography/"+key, "font"); err != nil {
			return Package{}, err
		}
	}
	for key, value := range m.Artwork {
		if err = ref(value, "/artwork/"+key, "image"); err != nil {
			return Package{}, err
		}
	}
	for key, id := range m.Identities {
		if id.Portrait != "" {
			if err = ref(id.Portrait, "/identities/"+key+"/portrait", "image"); err != nil {
				return Package{}, err
			}
		}
	}
	if err = validateContrast(m); err != nil {
		return Package{}, err
	}
	p := Package{Manifest: m, Source: bytes.Clone(raw), Assets: cloneFiles(files)}
	p.SourceDigest = sourceDigest(raw, files)
	p.ResolvedDigest = resolvedDigest(m, files)
	return p, nil
}
func mergeManifest(base, overlay Manifest) Manifest {
	base.ID, base.Version, base.Name, base.Description = overlay.ID, overlay.Version, overlay.Name, overlay.Description
	if overlay.ColorScheme != "" {
		base.ColorScheme = overlay.ColorScheme
	}
	maps.Copy(base.Palette, overlay.Palette)
	if len(overlay.Palette) > 0 {
		base.Tokens = derivePalette(base.Palette)
	}
	maps.Copy(base.Tokens, overlay.Tokens)
	maps.Copy(base.Icons, overlay.Icons)
	maps.Copy(base.Copy, overlay.Copy)
	maps.Copy(base.Artwork, overlay.Artwork)
	maps.Copy(base.Assets, overlay.Assets)
	for key, v := range overlay.Identities {
		old := base.Identities[key]
		if v.Name != "" {
			old.Name = v.Name
		}
		if v.Description != "" {
			old.Description = v.Description
		}
		if v.Lore != "" {
			old.Lore = v.Lore
		}
		if v.Portrait != "" {
			old.Portrait = v.Portrait
		}
		base.Identities[key] = old
	}
	a, b := &base.Typography, overlay.Typography
	if b.Body != "" {
		a.Body = b.Body
	}
	if b.Display != "" {
		a.Display = b.Display
	}
	if b.Mono != "" {
		a.Mono = b.Mono
	}
	if b.BodyWeight != 0 {
		a.BodyWeight = b.BodyWeight
	}
	if b.HeadingWeight != 0 {
		a.HeadingWeight = b.HeadingWeight
	}
	x, y := &base.Surfaces, overlay.Surfaces
	if y.Radius != nil {
		x.Radius = y.Radius
	}
	if y.BorderWidth != 0 {
		x.BorderWidth = y.BorderWidth
	}
	if y.Elevation != "" {
		x.Elevation = y.Elevation
	}
	if y.Ornamentation != "" {
		x.Ornamentation = y.Ornamentation
	}
	return base
}
func hashBytes(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func sourceDigest(raw []byte, files map[string][]byte) string {
	parts := map[string]string{"theme.json": hashBytes(raw)}
	for path, data := range files {
		parts[path] = hashBytes(data)
	}
	data, _ := json.Marshal(parts)
	return hashBytes(data)
}
func resolvedDigest(m Manifest, files map[string][]byte) string {
	m = cloneManifest(m)
	m.Schema = ""
	m.Base = nil
	digests := map[string]string{}
	for id, a := range m.Assets {
		digests[id] = hashBytes(files[a.Path])
		a.Path = ""
		m.Assets[id] = a
	}
	raw, _ := json.Marshal(struct {
		Manifest Manifest          `json:"manifest"`
		Assets   map[string]string `json:"assetDigests"`
	}{m, digests})
	return hashBytes(raw)
}
func cloneManifest(m Manifest) Manifest {
	raw, _ := json.Marshal(m)
	var c Manifest
	_ = json.Unmarshal(raw, &c)
	return c
}
func cloneFiles(files map[string][]byte) map[string][]byte {
	out := map[string][]byte{}
	for key, value := range files {
		out[key] = bytes.Clone(value)
	}
	return out
}
func clonePackage(p Package) Package {
	p.Manifest = cloneManifest(p.Manifest)
	p.Source = bytes.Clone(p.Source)
	p.Assets = cloneFiles(p.Assets)
	return p
}
