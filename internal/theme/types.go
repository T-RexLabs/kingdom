// Package theme resolves data-only presentation packages. It has no dependency
// on orchestration, provider configuration, authority policies, or prompt assembly.
package theme

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	SchemaVersion    = 1
	ResolverVersion  = 1
	DefaultID        = "obsidian-court"
	MaxManifestBytes = 256 << 10
	MaxAssetBytes    = 8 << 20
	MaxPackageBytes  = 32 << 20
	MaxArchiveBytes  = MaxPackageBytes + MaxManifestBytes
	MaxAssets        = 64
	MaxImagePixels   = 16_000_000
)

var (
	ErrConflict = errors.New("theme revision or source conflict")
	ErrNotFound = errors.New("theme not found")
	ErrBuiltin  = errors.New("built-in themes cannot be modified or removed")
)

type Reference struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}
type Typography struct {
	Body          string `json:"body,omitempty"`
	Display       string `json:"display,omitempty"`
	Mono          string `json:"mono,omitempty"`
	BodyWeight    int    `json:"bodyWeight,omitempty"`
	HeadingWeight int    `json:"headingWeight,omitempty"`
}
type Surfaces struct {
	Radius        *int   `json:"radius,omitempty"`
	BorderWidth   int    `json:"borderWidth,omitempty"`
	Elevation     string `json:"elevation,omitempty"`
	Ornamentation string `json:"ornamentation,omitempty"`
	Chrome        string `json:"chrome,omitempty"`
	IconRendering string `json:"iconRendering,omitempty"`
}
type Identity struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Lore        string `json:"lore,omitempty"`
	Portrait    string `json:"portrait,omitempty"`
}
type Asset struct {
	Path      string `json:"path"`
	MediaType string `json:"mediaType"`
	License   string `json:"license,omitempty"`
}
type Manifest struct {
	Schema          string              `json:"$schema,omitempty"`
	SchemaVersion   int                 `json:"schemaVersion"`
	ResolverVersion int                 `json:"resolverVersion"`
	ID              string              `json:"id"`
	Version         string              `json:"version"`
	Name            string              `json:"name"`
	Description     string              `json:"description,omitempty"`
	Base            *Reference          `json:"base,omitempty"`
	ColorScheme     string              `json:"colorScheme,omitempty"`
	Palette         map[string]string   `json:"palette,omitempty"`
	Tokens          map[string]string   `json:"tokens,omitempty"`
	Typography      Typography          `json:"typography"`
	Surfaces        Surfaces            `json:"surfaces"`
	Icons           map[string]string   `json:"icons,omitempty"`
	Identities      map[string]Identity `json:"identities,omitempty"`
	Copy            map[string]string   `json:"copy,omitempty"`
	Artwork         map[string]string   `json:"artwork"`
	Assets          map[string]Asset    `json:"assets"`
}

// Package owns immutable copies of its input bytes. Asset bytes are keyed by
// their resolved manifest path; API code must serve only those paths. A stored
// revision preserves authored Source independently; ReadRevisionSource restores
// its matching authored asset paths for editing.
type Package struct {
	Manifest       Manifest          `json:"manifest"`
	SourceDigest   string            `json:"sourceDigest"`
	ResolvedDigest string            `json:"resolvedDigest"`
	Source         json.RawMessage   `json:"-"`
	Assets         map[string][]byte `json:"-"`
}
type Diagnostic struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ValidationError struct {
	Diagnostics []Diagnostic `json:"diagnostics"`
}

func (e *ValidationError) Error() string {
	if len(e.Diagnostics) == 0 {
		return "theme validation failed"
	}
	return fmt.Sprintf("theme validation failed at %s: %s", e.Diagnostics[0].Path, e.Diagnostics[0].Message)
}
func invalid(path, code, message string) error {
	return &ValidationError{[]Diagnostic{{path, code, message}}}
}
