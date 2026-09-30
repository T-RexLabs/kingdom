# Offline Study Typography

These are unmodified font binaries from the official [Google Fonts repository](https://github.com/google/fonts), pinned to the commit and source URLs recorded in `sources.json`. That manifest also records every file's SHA-256 and byte length. Each family includes its original SIL Open Font License.

- Instrument Serif provides the display face for Obsidian Court, Nautilus and Astral Council.
- DM Sans provides body text and the Software Lab display face.
- JetBrains Mono provides code, identifiers and compact numeric data.

Normal and italic faces are included. DM Sans and JetBrains Mono retain their variable weight axes. `../fonts.css` lets Vite include only local, fingerprinted build assets; neither building nor using Kingdom requires a Google Fonts request. The system stacks in `../assets.ts` remain readable when a browser cannot load a font.

To update these files, choose an official repository commit, replace the corresponding originals and license files, update `sources.json` checksums/URLs, and run the theme tests and frontend build. A font family rename or addition must also update the canonical bundled-font enum in `contracts/theme/theme.schema.json` and its frontend family mapping.
