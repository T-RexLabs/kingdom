# Theme Package Contract

This guide is for contributors changing theme packages, validation, or storage. For the operator workflow, see [Themes and Appearance](../../docs/themes.md).

## Start With the Source

* [`theme.schema.json`](theme.schema.json) defines the version 1 package shape and allowed values.
* [`assets.go`](assets.go) embeds the schema, icon catalog, built in themes, and fixtures.
* [`internal/theme/resolver.go`](../../internal/theme/resolver.go) resolves packages and calculates digests.
* [`internal/theme/colors.go`](../../internal/theme/colors.go) derives semantic colors and readability warnings.
* [`internal/theme/assets.go`](../../internal/theme/assets.go) validates package assets.
* [`internal/theme/store.go`](../../internal/theme/store.go) manages editable sources and published revisions.
* [`internal/theme/archive.go`](../../internal/theme/archive.go) imports and exports ZIP packages.
* [`builtin/`](builtin/) contains the shipped themes. [`fixtures/harbor-workshop.json`](fixtures/harbor-workshop.json) is a small example that extends one bundled theme.
* [`lucide-icons.json`](lucide-icons.json) lists the allowed Lucide symbols. Update it from the installed `lucide-react` catalog when that dependency changes.

The JSON Schema Draft 07 file is canonical. [`scripts/generate_theme_contract.mjs`](../../scripts/generate_theme_contract.mjs) projects it into the OpenAPI contract, and generated Go and TypeScript types come from that contract. Keep one manifest definition. The projection maps `const` to `enum` and rewrites local references. OpenAPI 3.0 does not express every rule in the canonical schema, so the resolver remains responsible for completeness and allowed map keys. The runtime treats `$schema` as an editor hint and never fetches its value. After changing the schema, update generated contracts with `make contracts-generate` and check them with `make contracts-check`.

## Keep Themes in the Presentation Boundary

A package may set semantic colors, typography, surfaces, fixed plain text slots, role display names and descriptions, lore, icons, local artwork, and an optional bounded communication style preference (`agentVoice` in the manifest). A role icon has one source: `icons["role.<canonical-role-id>"]`. Render authored values as text. Never interpret them as HTML or Markdown.

Themes do not control canonical role responsibilities, authorization, execution state, error meaning, or commands. They cannot supply CSS, JavaScript, remote assets, provider settings, or role instructions. `agentVoice` is style-only guidance: an operator must explicitly choose the exact theme revision as a Communication Style in Agents settings before it affects new work. Visual theme selection does not update agent settings. Workspace and per-agent Communication Styles and Agent Guidance are stored separately from theme selection, and runs and planning sessions capture immutable configuration snapshots when created.

`surfaces.chrome` selects the app's `flat` or `classic` control style. `surfaces.iconRendering` selects `mask`, which uses the foreground color, or `original`, which keeps the validated artwork colors. A missing value means `flat` or `mask` at presentation time. The resolver does not add these defaults to the resolved manifest, preserving existing revision identities. An inherited value from the selected base stays in effect unless the author overrides it. These are fixed app styles, not stylesheets supplied by a theme.

Workstation 98 uses `classic` chrome, original pixel artwork, `bundled:classic-sans`, and different role names. Its explicit cream inset token and distinct semantic status colors are part of that theme. Names may change; canonical responsibilities and status meanings do not.

## Resolve a Package

A manifest either names one exact built in ID and version or supplies a complete standalone package. Chained inheritance is not supported. Resolution applies the base, merges authored fields, derives semantic tokens when a palette is supplied, then applies explicit token values. The browser previews the resolved response; it has no second palette algorithm. Resolved colors are lowercase.

The resolver returns contrast warnings with suggested values. Warnings are advisory: they do not change authored colors or block preview, saving, or publication. Structural, reference, and asset validation is still required. Surface radius and border width accept finite nonnegative numbers, including decimals and zero. Font weights accept CSS numeric values from 1 through 1000.

For an inherited package, the author supplies only its own assets. The resolver copies every other asset from the exact built in revision. An authored asset may replace an inherited asset with the same ID, but asset paths must remain unique. Undeclared files and authored paths that shadow inherited assets are rejected.

## Revision Identity and Storage

`theme.OpenStore(kingdomHome)` expects the initialized `.kingdom` directory. It creates a stable workspace ID that contains no secret. The browser uses that ID to scope each person's selected `(id, resolvedDigest)` in browser storage. Kingdom does not store a shared selection.

The store keeps its data under `.kingdom/themes/`:

* `<id>/theme.json` and `<id>/assets/` hold editable shared sources.
* `.catalog.json` lists published revisions after their bytes are safely written.
* `.workspace-id` identifies the workspace for browser preferences.
* `.revisions/<id>/<resolvedDigest>/` holds immutable resolved bytes, authored `source.json`, and its source digest.

`ReadRevisionSource` restores the exact authored manifest and original asset paths. Use it when editing a published revision so inherited palette intent is not replaced by generated token values.

`sourceDigest` hashes the exact authored JSON and each declared asset path and byte sequence. `resolvedDigest` hashes the complete normalized manifest and each asset's bytes, keyed by stable asset ID. The resolved digest does not depend on `$schema`, inheritance syntax, JSON formatting, or asset paths. The resolved manifest includes the package metadata, ID, version, schema version, and resolver version, so those affect its digest. A package ID and version cannot be reused for different resolved content. Make a new version or use a new ID.

## Edit, Publish, and Share

Source discovery happens only when requested. It scans at most 1,024 entries under `themes/` and does not publish anything. `List` returns published revisions, source IDs, and availability diagnostics. `ReadSource` returns source text even when it is malformed, with its digest and validation messages, so the editor can preserve and repair it.

`SaveSource` requires the digest from the last read. Use an empty digest for a new source. It validates the draft, stores assets by content address, and preserves old asset bytes while replacing the manifest. `Refresh` validates a source and publishes it without changing anyone's selected theme. If a caller has already inspected the source, publish the captured `Manifest` and `Assets`; do not reread files that may have changed.

Store mutations use a file lock shared across processes. Prefer the API for coordinated editing. A direct file editor does not participate in that lock. Digest checks detect many concurrent edits before and after staging, but cannot make an external edit part of a filesystem transaction. On `ErrConflict`, keep the draft and read the current source again. Invalid source changes do not replace published revisions.

Import validates the entire ZIP before changing the store and never overwrites editable source. Export creates a complete standalone package with deterministic entry order and asset paths derived from their content. Importing an export preserves `resolvedDigest`.

`Remove` unpublishes a custom package but keeps its immutable revisions, version ownership, and editable source. Republishing the same ID and version is allowed only for the same resolved content. `RemoveSource` separately deletes an editable source when its expected digest matches. Built in themes cannot be changed or removed. There is no automatic source watcher or revision pruning.

## Asset Safety

Assets must be declared under `assets/`, use portable paths, have unique names, and match their declared media type. Remote references, links, traversal, and undeclared files are rejected. Limits are:

* 256 KiB for the manifest
* 64 assets
* 8 MiB for one asset
* 32 MiB total asset bytes
* 16 megapixels and an 8,192 pixel edge for raster images

The ZIP archive is limited to 32 MiB plus the 256 KiB manifest allowance. Expansion is also bounded before and while reading. It may contain only root `theme.json` and declared asset files.

SVG is restricted to static geometry with one numeric `viewBox`. Scripts, event handlers, CSS, links, embedded documents, external references, animation, filters, and XML directives are rejected. PNG and JPEG dimensions and pixels are decoded during validation. WebP and AVIF containers and dimensions are checked within bounds; the browser performs pixel decoding. Animated WebP and AVIF are rejected. WOFF2 headers, lengths, and declared expansion are checked, but this is not a general font sanitizer. A custom font must include license or ownership information.

Serve assets only from the validated revision map, by declared asset ID and with its declared media type. Send `X-Content-Type-Options: nosniff` and the application's restrictive content policy. Never expose an endpoint that accepts an arbitrary local path. Browser image and font loading must use these validated assets and keep a safe fallback when decoding fails.

Built in fonts are local files with their licenses, pinned source URLs, and checksums in [`web/src/theme/fonts/`](../../web/src/theme/fonts/). No runtime Google Fonts request is needed. See the [font update guide](../../web/src/theme/fonts/README.md) before changing a bundled family.

## Verify Contract Changes

`go test -race ./internal/theme ./contracts/theme` checks schema and execution boundary rejection, palette resolution, contrast warnings, asset safety, source conflicts, invalid source editing, immutable publication, version ownership, fallback, and export identity. `go vet ./internal/theme ./contracts/theme` checks these packages. Authentication, CSRF, request idempotency, response headers, and browser selection are covered by integration code outside this package.
