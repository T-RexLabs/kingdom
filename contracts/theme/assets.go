// Package themecontract embeds the canonical data-only presentation contract.
// Browser types and validators must be generated from theme.schema.json.
package themecontract

import "embed"

// Files contains the schema, icon catalog, built-in packages and cross-client fixtures.
//
//go:embed theme.schema.json lucide-icons.json builtin/*.json fixtures/*.json
var Files embed.FS
