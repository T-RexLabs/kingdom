# Themes and Appearance

Appearance changes presentation across Kingdom. It does not change workflow authority, execution configuration, approvals, or generated replies. The five bundled themes are Obsidian Court, Nautilus, Astral Council, Software Lab, and Workstation 98. Operational status meanings and canonical role responsibilities remain consistent in every theme.

The bundled themes share the crowned Technology Rex logo and matching browser icon in version `1.1.0`. File-authored themes should reference that exact bundled base version. The brand remains an editable theme slot. In every ornamentation mode, the Kingdom logo and title remain still until the pointer has hovered over them for three uninterrupted seconds; there is no immediate greeting animation. Then Rexie approaches the wordmark; the impact scatters its letters and produces a brief, jagged comic-book “RAAWR!” burst in every level before Rexie takes a walk. The burst keeps a broad, outlined silhouette and legible lettering rather than a rounded chat bubble. It sits beside Rexie; the original brand header position stays fixed. On mobile it sits beyond the wordmark. The resting and animated brand Rexie share one 52 px size, including during recoil: the animation must never enlarge or shrink him. The letters remain opaque and tumble out of view through movement, never fading away. The collapsed sidebar keeps Rexie stepping in place. Moving away or clicking restores the usual brand. Reduced-motion preferences show a static Rexie and keep the wordmark readable, and touch interaction continues to navigate normally.

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

**Workstation 98** brings classic desktop chrome in a warm sepia palette: parchment surfaces, beige beveled controls, cream inset fields, espresso window title bars, square menus, pixel-art navigation, the shared Technology Rex brand, and 1990s corporate role titles. The underlying actions, status meanings, and canonical responsibilities stay the same. Its original icons ship locally; it does not depend on Windows assets or remote fonts.

A theme can change:

- Semantic colors, typography, surfaces, artwork, and ornamentation. `surfaces.chrome` selects application-owned `flat` or `classic` control styling. Missing values use `flat`; copies inherit their base styling.
- Icons from the supported Lucide catalog or declared static SVG assets. `surfaces.iconRendering` selects `mask` (the default, matching text color) or `original` (the artwork’s full color). A role icon has one source: `icons["role.<canonical-role-id>"]`.
- Role display names, descriptions, lore, and optional portraits. Keep the canonical role keys unchanged.
- Fixed plain-text UI copy slots. These do not accept HTML, Markdown, template expressions, operational status overrides, or prompt fragments.

Generated-reply voice is neutral and application-owned. A future voice feature requires a separately versioned execution configuration and an explicit session/run snapshot; adding presentation fields must not silently alter prompts or work already underway.

### Ornamentation

`surfaces.ornamentation` controls application-owned decorative details:

| Level | Details |
| --- | --- |
| `minimal` | Plain surfaces without decorative accents. |
| `balanced` | Small accent lines on page headers and previews. |
| `high` | Stronger header accents and decorative panel corners. |
| `ludicrous` (Ludicrous Mode) | A bolder 72 px Rexie in a continuous landscape of layered mountains, low ground, and ferns, extravagant double-and-dashed frames, embossed headings, claw-mark buttons with dust, and stomping navigation icons. The landscape uses the dinosaur motif, without a mode-name plaque or star decorations. |

All levels keep header and window-title backgrounds solid in Modern and Classic Desktop styles; ornamentation never introduces background gradients. Minimal, Balanced, and High add static decoration. Ludicrous Mode adds brief flourishes on entry and interaction, never continuous ambient movement. Hover or focus a primary action for claw marks and dust, hover or focus a navigation item for a stomp, or hover the mountain scene to make its Rexie stomp. Decorations must not move control hit targets, obscure labels, or alter operational statuses. Reduced motion retains static decoration; forced colors removes it. Rexie's size, three-second hover trigger, and comic roar remain independent of the ornamentation level.

Corner radius, border width, elevation, typography, icons, and artwork remain separate settings. Draft previews apply their own ornamentation without changing the active App until you choose **Preview in App**. The Ludicrous preview includes the same mountain scene and primary-action flourishes as the App.

## Share and Maintain Packages

**Export** downloads a ZIP containing `theme.json` and its declared assets. **Import Package** validates the archive, then publishes its immutable revision; it does not overwrite editable source or choose the theme for you. Export/reimport preserves the resolved digest. Unpublishing removes a package from the catalog while retaining its immutable version ownership and editable source. Deleting editable source is a separate action and leaves published revisions intact. Bundled themes cannot be edited or unpublished; create a copy instead.

Assets are package-relative regular files, never remote URLs or filesystem links. Limits are 256 KiB for JSON, 64 assets, 8 MiB per asset, and 32 MiB total asset bytes. Raster images have a 16 megapixel and 8,192-pixel edge limit. ZIP expansion is bounded; undeclared files, duplicate paths, traversal, and special files are rejected. SVG accepts only static geometry. Custom fonts use WOFF2 with declared license information. Validation is not a general-purpose font sanitizer; the browser still validates decoding and has fallback fonts.

Instrument Serif, DM Sans, and JetBrains Mono are bundled locally with their licenses. The `bundled:classic-sans` stack uses locally available Tahoma, Microsoft Sans Serif, or Arial. No runtime Google Fonts request is required. See the [package engineering contract](../contracts/theme/README.md) for detailed resolver, digest, and asset rules, and the [shared presentation layer](../web/src/theme/README.md) when adding UI components.

The ZIP archive transport budget is 32 MiB plus 256 KiB, including archive overhead. Both archive validation and import accept up to 45,088,768 base64 characters; expanded assets and the manifest retain their separate limits.
