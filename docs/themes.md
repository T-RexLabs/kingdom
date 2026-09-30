# Themes and Appearance

Appearance changes presentation across Kingdom. It does not change workflow authority, execution configuration, approvals, or generated replies. The four bundled themes are Obsidian Court, Nautilus, Astral Council, and Software Lab. Operational status meanings and canonical role responsibilities remain consistent in every theme.

## Choose and Preview

Open **Appearance** to choose an exact package revision. The choice is stored in this browser for the workspace's stable presentation ID; publishing a theme does not change anyone else's selection. If a pinned revision becomes unavailable, the app uses the bundled default and reports the problem without erasing the saved preference.

**Create Theme** starts from a bundled base. Colors and typography, identities and lore, and the advanced theme file edit the same draft. Validation uses the server's canonical resolver. **Preview in App** applies the validated draft privately while you navigate; the preview banner can restore the previous appearance. Preview neither saves shared files nor changes the pinned selection. The editor retains the draft across navigation and browser-tab reloads when session storage is available; download a draft backup before clearing browser data or closing the tab.

**Save Source** writes shared editable files. **Publish Revision** makes that exact saved source available in Appearance. **Use This Revision** is a separate personal choice. If a source changed since it was opened, keep or download the local draft before reloading the shared source. A published ID/version is immutable: increment the version or create a copy with a new ID for different resolved content.

An uncertain shared mutation keeps its original request and idempotency key. In the original browser session, recovery can retry that exact request. After session renewal, recovery only reads the retained result for the same local operator and original key; it never replays the mutation. A matching current theme is not proof of completion. Unknown or pending results remain unresolved, so keep the draft backup until the original result is confirmed.

## Author With Files or an Agent

The canonical contract is [`contracts/theme/theme.schema.json`](../contracts/theme/theme.schema.json); the authenticated API also serves it at `GET /api/v1/themes/schema`. Start with [`harbor-workshop.json`](../contracts/theme/fixtures/harbor-workshop.json) or a bundled manifest from [`contracts/theme/builtin/`](../contracts/theme/builtin/).

Put editable sources under `.kingdom/themes/<id>/theme.json`, with declared assets beneath that directory's `assets/`. The folder and manifest ID must match. Do not edit the dot-prefixed catalog, workspace identity, lock, or immutable revision directories. Appearance discovers file-authored sources on **Refresh**; discovery does not publish them. Open the source in Create Theme, inspect validation diagnostics, preview it, and publish explicitly. Invalid local source text remains editable, and a failed validation does not replace a previously published revision.

For coordinated automated editing, use the authenticated source API and its `expected_source_digest` precondition. Direct filesystem authors do not participate in the API's cross-process lock, so they should avoid writing while an editor is saving or publishing. The digest check detects changed source; it is not a filesystem transaction with unrelated external editors.

Version 1 supports one exact bundled base, or a complete standalone manifest. It does not support arbitrary inheritance chains. Palette overrides derive semantic tokens deterministically; explicit token overrides win afterward. Exported packages are complete standalone manifests, while editing a published revision retrieves the preserved authored source and inheritance intent.

A theme can change:

- Semantic colors, typography, surfaces, artwork, and ornamentation.
- Icons from the supported Lucide catalog or declared static SVG assets. A role icon has one source: `icons["role.<canonical-role-id>"]`.
- Role display names, descriptions, lore, and optional portraits. Keep the canonical role keys unchanged.
- Fixed plain-text UI copy slots. These do not accept HTML, Markdown, template expressions, operational status overrides, or prompt fragments.

Generated-reply voice is neutral and application-owned. A future voice feature requires a separately versioned execution configuration and an explicit session/run snapshot; adding presentation fields must not silently alter prompts or work already underway.

## Share and Maintain Packages

**Export** downloads a ZIP containing `theme.json` and its declared assets. **Import Package** validates the archive, then publishes its immutable revision; it does not overwrite editable source or choose the theme for you. Export/reimport preserves the resolved digest. Unpublishing removes a package from the catalog while retaining its immutable version ownership and editable source. Deleting editable source is a separate action and leaves published revisions intact. Bundled themes cannot be edited or unpublished; create a copy instead.

Assets are package-relative regular files, never remote URLs or filesystem links. Limits are 256 KiB for JSON, 64 assets, 8 MiB per asset, and 32 MiB total asset bytes. Raster images have a 16 megapixel and 8,192-pixel edge limit. ZIP expansion is bounded; undeclared files, duplicate paths, traversal, and special files are rejected. SVG accepts only static geometry. Custom fonts use WOFF2 with declared license information. Validation is not a general-purpose font sanitizer; the browser still validates decoding and has fallback fonts.

Instrument Serif, DM Sans, and JetBrains Mono are bundled locally with their licenses. No runtime Google Fonts request is required. See the [package engineering contract](../contracts/theme/README.md) for detailed resolver, digest, and asset rules, and the [shared presentation layer](../web/src/theme/README.md) when adding UI components.

The ZIP archive transport budget is 32 MiB plus 256 KiB, including archive overhead. Both archive validation and import accept up to 45,088,768 base64 characters; expanded assets and the manifest retain their separate limits.
