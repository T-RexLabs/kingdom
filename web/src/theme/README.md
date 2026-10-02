# Shared Presentation Layer

This guide is for contributors building UI that responds to the active theme. The manifest contract and asset rules live in [`contracts/theme/README.md`](../../../contracts/theme/README.md). The shared presentation code is under this directory.

## Mounting and Startup

Call `initializeDefaultPresentation()` once before mounting authentication and loading surfaces. Then mount [`ThemeProvider`](theme-provider.tsx) inside the authenticated local session boundary. The default theme styles the page immediately. `ThemeProvider` keeps application content behind a loading surface until it has loaded the workspace catalog and checked the person's saved theme. See [`presentation.ts`](presentation.ts) for root styling and [`preferences.ts`](preferences.ts) for browser preference storage.

The selected preference is only `{id, resolvedDigest}`. It is stored in browser `localStorage` under a key that includes the server's nonsecret workspace ID. If the saved revision is missing or cannot be read, Kingdom falls back to Obsidian Court and preserves the preference. `error` exposes the recovery message.

## Read and Change Theme State

Use `useTheme()` from [`context.ts`](context.ts) inside `ThemeProvider`. It provides:

* `theme` and `activePackage` for the resolved presentation
* `selected`, `catalog`, `error`, and `isPreviewing` for current state
* `refreshCatalog()`, `selectTheme()`, `previewTheme()`, and `cancelPreview()` for changes

Selection belongs to the **Appearance** page. There is no global theme selector. `selectTheme(reference)` loads and validates the exact requested revision before saving its reference. It returns `false` when a newer selection or preview has already taken over. Publishing a revision on the server never changes a person's selection by itself.

## Keep Preview Data Together

`previewTheme(package, assets)` accepts the canonical `/themes/validate` result and the exact base64 asset map submitted with that draft. Pass them from the same validation request. Never pair a response with assets from a newer edit.

Preview is private and reversible. It does not publish or save the selected preference. `cancelPreview()` restores the pinned revision. Preview asset URLs are revoked when replaced, cancelled, or unmounted. The editor or preview owner decides when navigation out of its flow should cancel the preview.

The Appearance editor stores its draft and pending operation in `sessionStorage`. Keep the source and validation result from the same edit together until the user saves, publishes, or discards the draft.

## Use Shared Theme Components

[`primitives.tsx`](primitives.tsx) provides `ThemeIcon`, `RoleIdentity`, `RolePortrait`, `ThemeArtwork`, and `ThemeCopy` for the package's supported slots. Use `useThemeToken()`, `useRoleIdentity()`, and `useThemeCopy()` for existing components.

Role identities show the canonical responsibility alongside the themed name. Unknown future roles get a neutral label and icon. Generated replies and operational labels remain application owned.

## Style Through Semantic Tokens

[`index.css`](../index.css) maps Tailwind's `background`, `surface`, `foreground`, `accent`, and status vocabulary to resolved theme values. It also defines `--color-*` variables for SVG and D3 consumers. Roles use the accent color, and statuses use their semantic tones.

Use shared UI primitives for controls. `surfaces.chrome` chooses `flat` or `classic` control styling, and `surfaces.iconRendering` chooses `mask` or `original` artwork colors. `applyPresentation()` always sets a `data-chrome` boundary, including for the default `flat` style. The bounded scope in [`classic-theme.css`](classic-theme.css) keeps classic controls inside their own boundary. A flat sample inside a classic page must remain flat.

## Preserve Status and Motion Meaning

For DAG components, use `.dag-node`, `.dag-edge`, and `.dag-agent-icon`. Set `data-provider-running="true"` only while that provider is running now. Historical attempts and general run activity are not evidence that a provider is running. Completed edges may use `data-completed="true"`.

Reduced motion must retain a clear static state. `.theme-status` with `data-tone` supplies a foreground, background, and border color; render a status word or icon as well. Theme colors never replace the meaning or label of an operational status.

## Keep Assets Inside The Boundary

Resolved manifests and bundled fixtures use the canonical schema and generated API types. The browser does not derive palettes. Do not accept CSS strings, HTML, or token maps from storage.

[`assets.ts`](assets.ts) builds image and font URLs from validated asset IDs. Revision assets use the authenticated revision route. Uploaded preview assets use the exact submitted bytes as isolated blob resources. Authored SVG stays an image and is never inserted into the document. Fonts load from local files, use fixed fallback stacks, and still pass through browser decoding and sanitization.

Bundled SVG URLs may fill missing preview assets inherited from a built in theme. Draft upload bytes take priority. Revoke only blob URLs allocated by the preview; bundled URLs and revision URLs are owned elsewhere. Browser branding follows the active theme asset and is restored when a private preview ends.

For bundled font provenance and update steps, see [`fonts/README.md`](fonts/README.md).
