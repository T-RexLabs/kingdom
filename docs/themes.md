# Themes and Appearance

Appearance lets you choose how Kingdom looks. A theme can change colors, fonts, icons, artwork, and some descriptive text. It cannot change what a role is allowed to do, what a status means, or what Kingdom sends to an LLM.

Kingdom includes five themes: Obsidian Court, Nautilus, Astral Council, Software Lab, and Workstation 98. Obsidian Court is the default.

## Choose a Theme

Open **Appearance** and choose a theme. Your choice is saved in this browser for this Kingdom workspace. Publishing a theme makes it available to people using the workspace; it does not select it for them.

If a selected revision is no longer available, Kingdom uses Obsidian Court and shows a message. Your saved choice is kept so you can restore it if the revision becomes available again.

## Create and Preview

Choose **Create Theme** to start from a bundled theme. The visual controls and the theme file editor change the same draft.

Choose **Preview in App** to try the draft while you move through Kingdom. Preview is private and temporary. It does not save shared files or change your selected theme. You can restore your previous appearance from the preview banner.

The editor keeps its draft while you navigate and after reloading the same tab, when session storage is available. Download a copy if you need to keep your work before clearing browser data or closing the tab.

## Save and Publish

**Save Source** writes editable theme files to the workspace. **Publish Revision** makes the saved version available in Appearance. **Use This Revision** selects it for your browser. These are separate steps, so publishing does not change anyone's selection.

A published theme ID and version belong to that exact presentation. To publish different content under the same ID, use a new version. You can also make a copy with a new ID.

If saving or publishing reports an uncertain result, keep your draft and use the recovery option in Kingdom. A theme looking current does not prove that a previous save or publish finished.

## Edit Theme Files

The [theme schema](../contracts/theme/theme.schema.json) defines the supported fields. Start with the [Harbor Workshop example](../contracts/theme/fixtures/harbor-workshop.json) or a file under [`contracts/theme/builtin/`](../contracts/theme/builtin/).

Editable sources live at `.kingdom/themes/<id>/theme.json`. Put declared assets under that theme's `assets/` folder. The folder name and the manifest ID must match. You may build on one exact bundled version or provide a complete theme. Longer chains of themes are not supported.

In **Appearance**, choose **Refresh** to discover themes you added in files. Discovery does not publish them. Open the source in **Create Theme**, check any validation messages, preview it, then save and publish it when ready. Invalid edits stay available in the editor, and they do not replace a revision that is already published.

For an automated editor, use Kingdom's authenticated source API and pass the `expected_source_digest` returned when reading the source. Direct file edits do not take part in the API's shared write lock. Avoid editing a file while Kingdom is saving or publishing it; a digest conflict means the source changed and should be read again before continuing.

## What a Theme Can Change

Themes can set semantic colors, typography, surfaces, icons, artwork, role names and descriptions, lore, and a small set of fixed interface text. Role names can change, but canonical role IDs and responsibilities remain the same.

Theme text is plain text. Themes cannot provide HTML, Markdown, CSS, scripts, prompts, provider settings, or generated reply voices. Assets must be declared in the package and served by Kingdom. Remote assets, links, and undeclared files are rejected. See the [theme package guide](../contracts/theme/README.md) for supported asset formats, safety checks, and size limits.

The `surfaces.ornamentation` setting accepts `minimal`, `balanced`, `high`, or `ludicrous`. **Ludicrous Mode** adds the Rexie mountain scene and interaction flourishes. Reduced motion disables animation while keeping the decoration visible.

## Export and Remove

**Export** downloads a package with `theme.json` and its declared assets. **Import Package** validates and publishes the package. It does not replace editable source or select the imported theme for you.

**Unpublish** removes a custom theme from the available catalog. Its editable files and published history remain. **Delete Source** removes the editable files as a separate action; published revisions remain saved. Bundled themes cannot be edited or unpublished, but you can create a copy.

For implementation details about validation, asset limits, revision identity, and storage, see the [theme package guide](../contracts/theme/README.md). For shared interface components, see the [presentation layer guide](../web/src/theme/README.md).
