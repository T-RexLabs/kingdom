# Bundled Fonts

Kingdom ships its fonts locally, so building and using the app does not make a Google Fonts request. The files are original font binaries from the official Google Fonts repository. [`sources.json`](sources.json) pins their repository commit and records each source URL, SHA256 checksum, and byte length. Each family includes its SIL Open Font License.

## Families and Use

* Instrument Serif supplies display text for Obsidian Court, Nautilus, and Astral Council. Regular and italic faces are static at weight 400.
* DM Sans supplies body text. Software Lab also uses it for display. Regular and italic files are variable across weights 100 through 1000.
* JetBrains Mono supplies code, identifiers, and compact numeric data. Regular and italic files are variable across weights 100 through 800.

[`fonts.css`](../fonts.css) references these local files so Vite includes fingerprinted build assets. [`assets.ts`](../assets.ts) maps the supported `bundled:` font references to their family names and readable system fallbacks. If a font fails to load, the fallback stack remains available.

## Update A Font

Keep each binary and its license together. When updating a family:

1. Choose a specific commit in the official Google Fonts repository.
2. Replace the font files and the matching `OFL.txt` with originals from that commit. Do not modify or convert the binaries.
3. Update [`sources.json`](sources.json) with the chosen commit, exact source URLs, SHA256 checksums, and byte lengths.
4. Check [`fonts.css`](../fonts.css) so each face has the correct file, style, and supported weight range.
5. Run `make test-web` and `make build`.

When adding or renaming a bundled family, also update the typography enum in [`theme.schema.json`](../../../../contracts/theme/theme.schema.json), the family mapping and fallback in [`assets.ts`](../assets.ts), and any bundled themes that use it. Regenerate and check the API types with `make contracts-generate` and `make contracts-check`.

The full package rules for custom WOFF2 fonts, including the required license field and validation limits, are in the [theme package contract](../../../../contracts/theme/README.md).
