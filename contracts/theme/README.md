# Theme Packages

`theme.schema.json` is the canonical version 1 presentation contract. `internal/theme` validates it and performs the additional reference, asset, digest and contrast checks. The complete packages in `builtin/` are both shipped themes and resolved fixtures. `fixtures/harbor-workshop.json` is a minimal inherited authoring example. `lucide-icons.json` is the allowed symbol catalog from the installed Lucide React dependency; keep that catalog synchronized when upgrading Lucide.

The canonical schema uses JSON Schema Draft 07. Generate browser types and any OpenAPI 3.0 projection from it; do not hand-maintain another manifest definition. A 3.0 projection must translate `const` to `enum`, rewrite local definition references, and retain unsupported validation details in the canonical schema/runtime validator. Editor `$schema` values are hints and are never fetched by the runtime.

## Presentation Boundary

A package controls semantic colors, typography, surfaces, fixed plain-text copy slots, role display names/descriptions/lore, icons and optional local artwork. Role icons have exactly one source: `icons["role.<canonical-role-id>"]`. Canonical responsibilities, authorization, execution state, error meaning and commands remain application-owned. Render all authored text as text, never HTML or Markdown.

No executable CSS, JavaScript, remote assets, provider settings, prompt fragments or generated-reply voice settings are accepted. A future generated-voice feature needs a separately versioned execution configuration and run/session snapshot; changing a theme must never implicitly activate it.

`surfaces.chrome` selects one application-owned structural preset: `flat` or `classic`. `surfaces.iconRendering` selects `mask` (foreground-colored SVG icons) or `original` (the validated artwork's original colors). Omission means `flat` and `mask` without inserting those fields into the resolved manifest or changing existing revision identities. These finite choices never accept stylesheets. Workstation 98 uses classic chrome in a sepia palette, matching original pixel artwork, `bundled:classic-sans` system typography, and corporate role names while preserving canonical responsibilities. Its cream inset token is an explicit authored override; operational status colors remain distinct semantic tones.

A manifest either references one exact built-in version or supplies every required resolved slot. Custom inheritance chains are not supported. Resolution merges the base, derives semantic tokens if an authored palette override exists, and then applies explicit semantic token overrides. The browser uses the resolver response for its preview; there is no independent browser palette algorithm. Colors are normalized to lowercase. The resolver validates the supported foreground/background pairs and gives field diagnostics with suggested contrast corrections.

Built-in assets are embedded and validated from their declared paths. An inherited package supplies only its authored asset files; the resolver clones other asset bytes from the exact built-in revision. An authored declaration can override an inherited asset ID, while undeclared shadow files and duplicate paths are rejected. The resolved package owns the complete asset set. Its `sourceDigest` still covers only authored JSON and authored assets, so source restoration does not flatten inheritance or silently turn built-in artwork into source overrides.

## Storage and Identity

`theme.OpenStore(kingdomHome)` receives the initialized `.kingdom` directory. It creates a stable nonsecret workspace presentation ID. Use this ID to scope a browser's personal selected `(package ID, resolvedDigest)`; no shared selection is stored here.

- `themes/<id>/theme.json` and `assets/` are editable, shared package sources.
- `themes/.revisions/<id>/<resolvedDigest>/` contains immutable complete presentations plus exact authored `source.json` and its digest. `ReadRevisionSource` restores that source with assets mapped to its original paths; editing must use this method so inherited palette intent is not replaced by explicit generated tokens.
- `themes/.catalog.json` publishes validated revisions only after their bytes are durable.
- `themes/.workspace-id` identifies this workspace for personal preferences.

`List` returns published revisions, editable source IDs and availability diagnostics. Source discovery happens during an explicit call, does not publish anything, and is bounded to 1,024 entries in the themes directory. `ReadSource` returns `content` as a string so even malformed JSON remains editable, along with `sourceDigest` and field diagnostics. `SaveSource` requires the last source digest (empty for a new source). It stores validated drafts with content-addressed asset filenames and preserves old asset bytes. `Refresh` validates and publishes the current source without changing personal selection. If a caller has already inspected a specific source, publish its captured `Manifest` and `Assets` rather than rereading a potentially changed file.

Store mutations use a cross-process lock. File authors should edit through the API for strict coordination; direct local edits are detected by digest checks before and after staging, but an external editor that ignores the lock cannot participate in a filesystem transaction. Save conflicts preserve the existing source and return `ErrConflict`. Invalid source updates leave previously published revisions usable.

`sourceDigest` identifies exact authored JSON and all declared asset bytes/paths. `resolvedDigest` identifies complete normalized presentation and declared asset content by stable asset ID. The latter excludes `$schema`, inheritance syntax, JSON formatting and incidental asset paths. Metadata, package version and resolver version remain part of revision identity. One ID/version cannot be republished with different resolved content. Use a new version or duplicate under a new ID.

Import validates an entire ZIP before publication and does not overwrite editable source. Export produces a complete, baseless package with deterministic entry order and content-addressed assets. Export/reimport preserves `resolvedDigest`. `Remove` unpublishes a custom package; immutable bytes, version ownership and editable source remain. Removal cannot make an existing ID/version available for different content; equivalent reimport can republish it. `RemoveSource` is an explicit digest-checked deletion of the editable draft. Built-ins cannot be changed or removed. There is no automatic pruning or watcher.

## Asset Contract

Assets must have unique declared paths under `assets/`, portable filenames, matching extensions/media types, and no links, traversal or remote references. Limits are 256 KiB for JSON, 64 assets, 8 MiB per asset, 32 MiB total asset bytes, 16 megapixels and an 8,192-pixel image edge. ZIP expansion is bounded before and during reads; all non-manifest entries must be declared assets.

SVG is a strict static geometry subset with one numeric `viewBox`; scripts, event handlers, CSS, links, embedded documents, external references, animation, filters and XML directives are rejected. PNG/JPEG dimensions and decoding are validated. WebP/AVIF use bounded container and dimension inspection; pixel decoding remains the browser's responsibility. Animated WebP/AVIF are rejected. WOFF2 headers, lengths and declared expansion budgets are checked; browser font sanitization remains required. A font reference also requires declared license/ownership information. These checks are not a general-purpose image transcoder or font sanitizer.

Serve only bytes in a validated revision's asset map, by declared asset identity, with the declared MIME type, `X-Content-Type-Options: nosniff` and the application's restrictive content policy. Do not expose an arbitrary local path endpoint. Browser image/font loading must use these validated local assets, with safe fallback when decoding fails. Built-in typography uses self-hosted `bundled:dm-sans`, `bundled:instrument-serif` and `bundled:jetbrains-mono`; Software Lab uses DM Sans for display too. System-family options remain available. Font files, OFL licenses, pinned official source URLs and checksums live under `web/src/theme/fonts/`. No runtime Google Fonts request is needed.

## Verification

`go test -race ./internal/theme ./contracts/theme` covers schema and execution-boundary rejection, palette determinism, contrast, asset safety, source conflicts, invalid-source editing, immutable publication/replay, concurrent version ownership, failed-publication fallback and export/import identity. `go vet ./internal/theme ./contracts/theme` checks the focused packages. HTTP authentication, CSRF, idempotency, asset headers and personal browser selection are integration responsibilities outside this package.
