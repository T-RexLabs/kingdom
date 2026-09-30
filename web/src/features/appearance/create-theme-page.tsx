import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import {
  ArrowLeft,
  Copy,
  Download,
  Eye,
  FileCode2,
  Save,
  Upload,
} from "lucide-react";
import { ApiError } from "@/api/client";
import { WorkspacePage } from "@/components/workspace-page";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/ui/modal";
import { SelectField } from "@/components/ui/select-field";
import {
  builtInThemes,
  defaultTheme,
  useTheme,
  type RoleID,
  type ThemeManifest,
  type ThemePackage,
} from "@/theme";
import { newDraft, parseDraft, useThemeDraft } from "./draft-store";
import { EditorColors, EditorIdentity, Field } from "./editor-fields";
import { objectValue, setDraftField, stringAt } from "./editor-values";
import { EditorAssets } from "./editor-assets";
import {
  themeAPI,
  themeDiagnostics,
  themeError,
  downloadJSON,
  type ThemeDraft,
  type ThemeSource,
} from "./theme-api";
import { useThemeOperation } from "./use-theme-operation";
import { ThemePreviewCard } from "./theme-preview-card";
import "./appearance.css";

type EditorTab = "colors" | "identity" | "file";
function sourceTarget(source?: string, revision?: string, base?: string) {
  return source
    ? `source:${source}:${revision ?? "editable"}`
    : base
      ? `base:${base}`
      : "";
}
function uniqueID(stem: string, used: string[]) {
  const base =
    stem
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, "-")
      .replace(/^-+/, "")
      .slice(0, 54) || "my-theme";
  const safe = /^[a-z]/.test(base) ? base : `theme-${base}`;
  let id = safe;
  let index = 2;
  while (used.includes(id)) id = `${safe}-${index++}`;
  return id;
}
function readText(file: File): Promise<string> {
  if (file.size > 48 << 20)
    return Promise.reject(new Error("The theme draft file is too large."));
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () =>
      reject(new Error("The theme file could not be read."));
    reader.readAsText(file);
  });
}
export function CreateThemePage() {
  const search = useSearch({ from: "/appearance/create" });
  const navigate = useNavigate();
  const appearance = useTheme();
  const workspace = appearance.catalog?.workspaceID ?? "unavailable";
  const [draft, store] = useThemeDraft(workspace);
  const operation = useThemeOperation(workspace);
  const [tab, setTab] = useState<EditorTab>(
    search.source && !search.revision ? "file" : "colors",
  );
  const [role, setRole] = useState<RoleID>("builder");
  const [approvedTarget, setApprovedTarget] = useState<{
    target: string;
    generation: number;
  } | null>(null);
  const [confirmReload, setConfirmReload] = useState(false);
  const [confirmNew, setConfirmNew] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [published, setPublished] = useState<ThemePackage | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const [incomingFile, setIncomingFile] = useState<ThemeDraft | null>(null);
  const [debounced, setDebounced] = useState(draft);
  useEffect(() => {
    const timeout = setTimeout(() => setDebounced(draft), 500);
    return () => clearTimeout(timeout);
  }, [draft]);
  const target = sourceTarget(search.source, search.revision, search.base);
  const mismatch = !!target && target !== draft.origin;
  const askReplace =
    mismatch &&
    draft.dirty &&
    (approvedTarget?.target !== target ||
      approvedTarget.generation !== draft.generation);
  const locked =
    !appearance.catalog ||
    operation.busy ||
    !!operation.pending ||
    operation.blockedRecovery;
  const allowLoad = mismatch && !askReplace && !locked;
  const sourceQuery = useQuery({
    queryKey: ["theme-editor-source", workspace, target],
    queryFn: ({ signal }) =>
      themeAPI.source(search.source!, search.revision, signal),
    enabled: allowLoad && !!search.source,
    retry: false,
    staleTime: 0,
    gcTime: 0,
  });
  const occupied = useMemo(
    () => [
      ...(appearance.catalog?.sources ?? []),
      ...(appearance.catalog?.items.map((item) => item.id) ?? []),
    ],
    [appearance.catalog],
  );
  useEffect(() => {
    if (!allowLoad) return;
    if (search.source && sourceQuery.data) {
      const source = sourceQuery.data;
      if (search.revision) {
        const manifest = parseDraft(source.source);
        if (!manifest) return;
        const copy = structuredClone(manifest);
        copy.id = uniqueID(`${copy.id}-copy`, occupied);
        copy.name = `${copy.name.slice(0, 75)} Copy`;
        copy.version = "1.0.0";
        store.replace(
          { source: JSON.stringify(copy, null, 2), assets: source.assets },
          target,
          null,
          "",
          true,
        );
      } else
        store.replace(
          { source: source.source, assets: source.assets },
          target,
          search.source,
          source.sourceDigest,
        );
    } else if (search.base)
      store.replace(
        newDraft(search.base, uniqueID(`${search.base}-custom`, occupied)),
        target,
      );
  }, [
    allowLoad,
    search.source,
    search.revision,
    search.base,
    sourceQuery.data,
    store,
    target,
    occupied,
  ]);
  const manifest = parseDraft(draft.source);
  const previewQuery = useQuery({
    queryKey: ["theme-editor-preview", workspace, debounced.generation],
    queryFn: async ({ signal }) => ({
      pkg: await themeAPI.validate(
        { source: debounced.source, assets: debounced.assets },
        signal,
      ),
      assets: debounced.assets,
    }),
    enabled: !!parseDraft(debounced.source) && !mismatch,
    retry: false,
    staleTime: Infinity,
    gcTime: 0,
    placeholderData: (prior) => prior,
  });
  const currentValidation =
    draft.generation === debounced.generation &&
    previewQuery.isSuccess &&
    !previewQuery.isPlaceholderData;
  const validated = currentValidation ? previewQuery.data.pkg : undefined;
  const base =
    builtInThemes.find(
      (theme) => theme.id === stringAt(manifest, "base", "id"),
    ) ?? defaultTheme;
  const visual = previewQuery.data?.pkg.manifest ?? base;
  const diagnostics = themeDiagnostics(previewQuery.error);
  const id = stringAt(manifest, "id");
  const identityMismatch = !!draft.sourceID && draft.sourceID !== id;
  const conflict =
    operation.error instanceof ApiError && operation.error.status === 409;
  const effectiveTab = manifest ? tab : "file";
  function edit(next: ThemeDraft) {
    if (appearance.isPreviewing) appearance.cancelPreview();
    store.edit(next);
    setPublished(null);
    setFailure(null);
    setNotice(null);
  }
  function change(next: ThemeManifest) {
    edit({ source: JSON.stringify(next, null, 2), assets: draft.assets });
  }
  function fresh() {
    if (appearance.isPreviewing) appearance.cancelPreview();
    store.replace(
      newDraft(defaultTheme.id, uniqueID("my-theme", occupied)),
      "new",
    );
    operation.clearError();
    setPublished(null);
    setNotice(null);
    setFailure(null);
    setConfirmNew(false);
    void navigate({ to: "/appearance/create", search: {} });
  }
  function duplicate() {
    if (!manifest) return;
    const copy = structuredClone(manifest);
    copy.id = uniqueID(`${id}-copy`, occupied);
    copy.name = `${stringAt(manifest, "name").slice(0, 75)} Copy`;
    copy.version = "1.0.0";
    if (appearance.isPreviewing) appearance.cancelPreview();
    store.replace(
      { source: JSON.stringify(copy, null, 2), assets: draft.assets },
      "new",
      null,
      "",
      true,
    );
    operation.clearError();
    setPublished(null);
    setNotice(
      "A private copy is ready. Save its source when you want to share the files.",
    );
    void navigate({ to: "/appearance/create", search: {} });
  }
  async function save() {
    if (!validated || identityMismatch) return;
    setFailure(null);
    try {
      const result = (await operation.run({
        method: "PUT",
        path: `/themes/${encodeURIComponent(id)}/source`,
        body: {
          source: draft.source,
          assets: draft.assets,
          expected_source_digest: draft.expectedDigest,
        },
      })) as ThemeSource;
      store.saved(result, draft.generation);
      setNotice(
        "Source files saved. Publish when this revision is ready to share.",
      );
      await appearance.refreshCatalog();
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function publish() {
    if (!validated || draft.dirty || !draft.expectedDigest || identityMismatch)
      return;
    setFailure(null);
    try {
      const result = (await operation.run({
        method: "POST",
        path: `/themes/${encodeURIComponent(id)}/publish`,
        body: { expected_source_digest: draft.expectedDigest },
        verify: { id, resolvedDigest: validated.resolvedDigest },
      })) as ThemePackage;
      setPublished(result);
      setNotice(
        "Revision published. Everyone can choose it; your personal appearance stays the same.",
      );
      await appearance.refreshCatalog();
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function recover() {
    try {
      const { request, result } = await operation.retry();
      if (request.method === "PUT" && request.path.endsWith("/source")) {
        const attempted = objectValue(request.body);
        const current = store.getSnapshot();
        if (
          current.source === attempted.source &&
          JSON.stringify(current.assets) === JSON.stringify(attempted.assets)
        )
          store.saved(result as ThemeSource, current.generation);
      }
      setFailure(null);
      setNotice(
        "The previous change has been recovered. Review the current shared state before continuing.",
      );
      await appearance.refreshCatalog();
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function reloadSource() {
    if (!draft.sourceID) return;
    try {
      const source = await themeAPI.source(draft.sourceID);
      if (store.getSnapshot().generation !== draft.generation)
        throw new Error(
          "The draft changed while loading shared source. Your edits are preserved; reload again if needed.",
        );
      if (appearance.isPreviewing) appearance.cancelPreview();
      operation.clearError();
      store.replace(
        { source: source.source, assets: source.assets },
        draft.origin,
        draft.sourceID,
        source.sourceDigest,
      );
      setFailure(null);
      setNotice("Current shared source loaded.");
      setConfirmReload(false);
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function importJSON(file?: File) {
    if (!file) return;
    try {
      const content = await readText(file);
      const parsed: unknown = JSON.parse(content);
      const backup = objectValue(parsed);
      const imported =
        typeof backup.source === "string" &&
        backup.assets &&
        typeof backup.assets === "object"
          ? {
              source: backup.source,
              assets: backup.assets as Record<string, string>,
            }
          : { source: content, assets: {} };
      if (
        store.getSnapshot().dirty ||
        store.getSnapshot().generation !== draft.generation
      )
        setIncomingFile(imported);
      else acceptFile(imported);
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  function acceptFile(imported: ThemeDraft) {
    if (appearance.isPreviewing) appearance.cancelPreview();
    store.replace(imported, "imported-file", null, "", true);
    setTab("file");
    setIncomingFile(null);
    setPublished(null);
    void navigate({ to: "/appearance/create", search: {} });
  }
  const busyLoading = mismatch && !askReplace;
  return (
    <WorkspacePage
      section="Appearance / Create Theme"
      title={draft.sourceID ? "Edit Theme" : "Create a Theme"}
      description="Start with a world you love, then make it your own. The visual controls and theme.json edit one draft. Preview the real app before sharing a revision."
      actions={
        <>
          <Link
            to="/appearance"
            className="appearance-link inline-flex items-center gap-2"
          >
            <ArrowLeft size={14} />
            Appearance
          </Link>
          <Button
            variant="outline"
            disabled={locked}
            onClick={() => (draft.dirty ? setConfirmNew(true) : fresh())}
          >
            New Theme
          </Button>
        </>
      }
    >
      {!appearance.catalog && (
        <div className="appearance-notice" role="alert">
          <p>Load the workspace catalog before editing or sharing themes.</p>
          <Button
            variant="outline"
            onClick={() => {
              void appearance.refreshCatalog();
            }}
          >
            Retry Catalog
          </Button>
        </div>
      )}
      {(failure || operation.message) && (
        <div className="appearance-notice error" role="alert">
          {failure || operation.message}
        </div>
      )}
      {notice && (
        <p className="appearance-notice" role="status">
          {notice}
        </p>
      )}
      {(draft.storageWarning || operation.storageWarning) && (
        <p className="appearance-notice">
          Browser storage is unavailable or full. Keep this tab open until any
          pending change is recovered, and download a draft backup before
          closing it.
        </p>
      )}
      {operation.pending && !operation.busy && (
        <div className="appearance-notice">
          <p>
            The previous shared change needs recovery. Your draft is preserved.
          </p>
          <Button
            variant="outline"
            onClick={() => {
              void recover();
            }}
          >
            Recover Change
          </Button>
        </div>
      )}
      {conflict && (
        <div className="appearance-notice">
          <p>
            Another edit or published version conflicts with this draft.
            Download your draft before reloading, or create a copy with a new
            ID. Published versions are immutable; increment Version for a new
            revision.
          </p>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              onClick={() =>
                downloadJSON(`${id || "theme"}.theme-draft.json`, {
                  source: draft.source,
                  assets: draft.assets,
                })
              }
            >
              Download Draft
            </Button>
            {draft.sourceID && (
              <Button variant="outline" onClick={() => setConfirmReload(true)}>
                Reload Shared Source
              </Button>
            )}
            <Button variant="outline" onClick={duplicate}>
              Create a Copy
            </Button>
          </div>
        </div>
      )}
      {busyLoading ? (
        <div className="appearance-empty">
          {sourceQuery.error ? (
            <>
              <p role="alert">{themeError(sourceQuery.error)}</p>
              <Button
                variant="outline"
                className="mt-4"
                onClick={() => {
                  void sourceQuery.refetch();
                }}
              >
                Try Again
              </Button>
              <Button variant="ghost" className="mt-4" onClick={fresh}>
                Start a New Theme
              </Button>
            </>
          ) : (
            <p role="status">Loading theme source…</p>
          )}
        </div>
      ) : (
        <div className="appearance-editor-grid">
          <div className="min-w-0">
            {!draft.sourceID && (
              <div className="mb-6">
                <SelectField
                  id="theme-starting-theme"
                  label="Start With"
                    disabled={locked}
                    value={stringAt(manifest, "base", "id")}
                    onChange={(next) => {
                      if (next)
                        void navigate({
                          to: "/appearance/create",
                          search: { base: next },
                        });
                    }}
                  placeholder="Choose a starting theme"
                  options={builtInThemes.map((theme) => ({ value: theme.id, label: theme.name }))}
                />
                <p className="appearance-help mt-2">Choosing a starting theme creates a fresh draft. Unsaved edits are kept until you confirm replacing them.</p>
              </div>
            )}
            <fieldset disabled={locked} className="grid gap-4 sm:grid-cols-2">
              <Field label="Theme Name">
                <input
                  className="appearance-input"
                  value={stringAt(manifest, "name")}
                  disabled={!manifest}
                  onChange={(event) =>
                    manifest &&
                    change(
                      setDraftField(manifest, ["name"], event.target.value),
                    )
                  }
                />
              </Field>
              <Field
                label="Theme ID"
                help={
                  draft.sourceID
                    ? "To change a shared ID, create a copy."
                    : "Lowercase letters, digits and hyphens."
                }
              >
                <input
                  className="appearance-input font-mono text-xs"
                  value={id}
                  disabled={!manifest || !!draft.sourceID}
                  onChange={(event) =>
                    manifest &&
                    change(setDraftField(manifest, ["id"], event.target.value))
                  }
                />
              </Field>
              <Field
                label="Version"
                help="Use a new version when publishing changed content."
              >
                <input
                  className="appearance-input font-mono text-xs"
                  value={stringAt(manifest, "version")}
                  placeholder="1.0.0"
                  disabled={!manifest}
                  onChange={(event) =>
                    manifest &&
                    change(
                      setDraftField(manifest, ["version"], event.target.value),
                    )
                  }
                />
              </Field>
              <Field label="Description">
                <input
                  className="appearance-input"
                  value={stringAt(manifest, "description")}
                  disabled={!manifest}
                  onChange={(event) =>
                    manifest &&
                    change(
                      setDraftField(
                        manifest,
                        ["description"],
                        event.target.value || undefined,
                      ),
                    )
                  }
                />
              </Field>
            </fieldset>
            <div
              className="ui-tabs appearance-editor-tabs mt-7"
              role="tablist"
              aria-label="Theme editor sections"
            >
              {(
                [
                  ["colors", "Colors & Type"],
                  ["identity", "Identity & Lore"],
                  ["file", "Theme File"],
                ] as const
              ).map(([key, label]) => (
                <button
                  key={key}
                  type="button"
                  className="ui-tab"
                  role="tab"
                  id={`theme-tab-${key}`}
                  aria-selected={effectiveTab === key}
                  aria-controls="theme-editor-panel"
                  tabIndex={effectiveTab === key ? 0 : -1}
                  onKeyDown={(event) => {
                    const tabs = Array.from(
                      event.currentTarget.parentElement!.querySelectorAll<HTMLButtonElement>(
                        '[role="tab"]:not(:disabled)',
                      ),
                    );
                    const index = tabs.indexOf(event.currentTarget);
                    const next =
                      event.key === "ArrowRight"
                        ? (index + 1) % tabs.length
                        : event.key === "ArrowLeft"
                          ? (index + tabs.length - 1) % tabs.length
                          : event.key === "Home"
                            ? 0
                            : event.key === "End"
                              ? tabs.length - 1
                              : -1;
                    if (next >= 0) {
                      event.preventDefault();
                      tabs[next].click();
                      tabs[next].focus();
                    }
                  }}
                  disabled={!manifest && key !== "file"}
                  onClick={() => setTab(key)}
                >
                  {label}
                </button>
              ))}
            </div>
            <div
              id="theme-editor-panel"
              role="tabpanel"
              aria-labelledby={`theme-tab-${effectiveTab}`}
            >
              {effectiveTab === "colors" && manifest && (
                <EditorColors
                  manifest={manifest}
                  resolved={validated?.manifest}
                  disabled={locked}
                  change={change}
                />
              )}
              {effectiveTab === "identity" && manifest && (
                <EditorIdentity
                  manifest={manifest}
                  resolved={validated?.manifest}
                  disabled={locked}
                  role={role}
                  setRole={setRole}
                  change={change}
                />
              )}
              {effectiveTab === "file" && (
                <>
                  <p className="appearance-help mb-4">
                    The same canonical draft, in JSON. Files stay local until
                    Save Source. Agent-authored files can also be edited in{" "}
                    <code>.kingdom/themes/&lt;id&gt;/</code>.
                  </p>
                  <textarea
                    className="appearance-input appearance-file-editor"
                    aria-label="Theme JSON"
                    spellCheck={false}
                    disabled={locked}
                    value={draft.source}
                    onChange={(event) => {
                      setTab("file");
                      edit({
                        source: event.target.value,
                        assets: draft.assets,
                      });
                    }}
                  />
                  <div className="appearance-actions">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={locked}
                      onClick={() => fileInput.current?.click()}
                    >
                      <Upload size={14} />
                      Load JSON or Draft
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        downloadJSON(`${id || "theme"}.theme-draft.json`, {
                          source: draft.source,
                          assets: draft.assets,
                        })
                      }
                    >
                      <Download size={14} />
                      Download Draft
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => downloadJSON("theme.json", draft.source)}
                    >
                      <FileCode2 size={14} />
                      Download Theme File
                    </Button>
                  </div>
                  <input
                    ref={fileInput}
                    type="file"
                    hidden
                    accept=".json,application/json"
                    aria-label="Load theme JSON or draft backup"
                    onChange={(event) => {
                      void importJSON(event.target.files?.[0]);
                      event.target.value = "";
                    }}
                  />
                </>
              )}
            </div>
            {manifest && (
              <EditorAssets
                manifest={manifest}
                assets={draft.assets}
                disabled={locked}
                generation={draft.generation}
                currentGeneration={() => store.getSnapshot().generation}
                change={edit}
              />
            )}
          </div>
          <aside
            className="appearance-preview-column"
            aria-label="Theme preview and publication"
          >
            <ThemePreviewCard
              manifest={visual}
              pkg={previewQuery.data?.pkg}
              files={previewQuery.data?.assets}
            />
            <div className="appearance-validation" aria-live="polite">
              {!manifest
                ? "JSON needs correction before previewing or saving."
                : currentValidation
                  ? "Validated · Contrast, references and package structure checked."
                  : previewQuery.isError &&
                      draft.generation === debounced.generation
                    ? "Validation found issues. Your last published theme stays usable."
                    : "Checking the current draft…"}
            </div>
            {!manifest && (
              <p className="text-sm text-status-failed" role="alert">
                Enter one valid JSON object in Theme File.
              </p>
            )}
            {diagnostics.length > 0 ? (
              <ul
                className="appearance-diagnostics"
                aria-label="Theme validation issues"
              >
                {diagnostics.map((diagnostic, index) => (
                  <li key={index}>
                    <code>{diagnostic.path}</code>
                    {diagnostic.message}
                  </li>
                ))}
              </ul>
            ) : (
              previewQuery.error && (
                <p className="text-sm text-status-failed" role="alert">
                  {themeError(previewQuery.error)}
                </p>
              )
            )}
            {identityMismatch && (
              <p className="appearance-notice error">
                The JSON ID differs from its source folder. Use Create a Copy to
                save a new identity.
              </p>
            )}
            <div className="appearance-actions">
              <Button
                disabled={!validated || locked}
                onClick={() => {
                  if (validated) {
                    appearance.previewTheme(validated, draft.assets);
                    setNotice(
                      "Private preview is active. Navigate through the app, then use Back to Editor in the preview banner.",
                    );
                  }
                }}
              >
                <Eye size={15} />
                Preview in App
              </Button>
              {appearance.isPreviewing && (
                <Button variant="outline" onClick={appearance.cancelPreview}>
                  Exit Preview
                </Button>
              )}
            </div>
            <section className="appearance-form-section">
              <h3>Shared Source</h3>
              <p className="appearance-help">
                {draft.sourceID
                  ? `.kingdom/themes/${draft.sourceID}/theme.json`
                  : "Saving creates shared, editable files. Publishing makes an immutable revision available in Appearance."}
              </p>
              <div className="appearance-actions">
                <Button
                  variant="outline"
                  disabled={
                    !validated ||
                    identityMismatch ||
                    locked ||
                    (!!draft.expectedDigest && !draft.dirty)
                  }
                  onClick={() => {
                    void save();
                  }}
                >
                  <Save size={14} />
                  {operation.busy ? "Working…" : "Save Source"}
                </Button>
                <Button
                  variant="ghost"
                  disabled={!manifest || locked}
                  onClick={duplicate}
                >
                  <Copy size={14} />
                  Create a Copy
                </Button>
                {draft.sourceID && (
                  <Button
                    variant="ghost"
                    disabled={locked}
                    onClick={() => setConfirmReload(true)}
                  >
                    Reload Source
                  </Button>
                )}
              </div>
            </section>
            <section className="appearance-form-section">
              <h3>Publish a Revision</h3>
              <p className="appearance-help">
                {draft.dirty || !draft.expectedDigest
                  ? "Save this draft before publishing."
                  : "The source is saved. Publish only when this exact revision is ready to share."}{" "}
                Publishing never changes anyone's appearance automatically.
              </p>
              <div className="appearance-actions">
                <Button
                  disabled={
                    !validated ||
                    draft.dirty ||
                    !draft.expectedDigest ||
                    identityMismatch ||
                    locked
                  }
                  onClick={() => {
                    void publish();
                  }}
                >
                  Publish Revision
                </Button>
                {published && (
                  <Button
                    variant="outline"
                    onClick={() => {
                      void appearance
                        .selectTheme({
                          id: published.manifest.id,
                          resolvedDigest: published.resolvedDigest,
                        })
                        .catch((error) => setFailure(themeError(error)));
                    }}
                  >
                    Use This Revision
                  </Button>
                )}
              </div>
            </section>
          </aside>
        </div>
      )}
      <ConfirmDialog
        open={askReplace}
        onClose={() => {
          void navigate({ to: "/appearance/create", search: {} });
        }}
        onConfirm={() =>
          setApprovedTarget({ target, generation: draft.generation })
        }
        title="Replace the Current Draft"
        description="Your private draft has unsaved edits. Download it before opening a different source or base."
        confirmLabel="Replace Draft"
        tone="default"
      >
        <Button
          variant="outline"
          onClick={() =>
            downloadJSON(`${id || "theme"}.theme-draft.json`, {
              source: draft.source,
              assets: draft.assets,
            })
          }
        >
          Download Draft
        </Button>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirmReload}
        onClose={() => setConfirmReload(false)}
        onConfirm={() => {
          void reloadSource();
        }}
        title="Reload Shared Source"
        description="Reloading replaces the private draft with the current shared file. Download your draft first if you want to keep its edits."
        confirmLabel="Reload Source"
        tone="default"
      >
        <Button
          variant="outline"
          onClick={() =>
            downloadJSON(`${id || "theme"}.theme-draft.json`, {
              source: draft.source,
              assets: draft.assets,
            })
          }
        >
          Download Draft
        </Button>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirmNew}
        onClose={() => setConfirmNew(false)}
        onConfirm={fresh}
        title="Start a New Theme"
        description="The current private draft has unsaved edits. Shared source and published revisions remain available."
        confirmLabel="New Theme"
        tone="default"
      >
        <Button
          variant="outline"
          onClick={() =>
            downloadJSON(`${id || "theme"}.theme-draft.json`, {
              source: draft.source,
              assets: draft.assets,
            })
          }
        >
          Download Draft
        </Button>
      </ConfirmDialog>
      <ConfirmDialog
        open={!!incomingFile}
        onClose={() => setIncomingFile(null)}
        onConfirm={() => incomingFile && acceptFile(incomingFile)}
        title="Load a Theme File"
        description="Loading this file replaces the current private draft. Shared sources are not changed."
        confirmLabel="Load File"
        tone="default"
      />
    </WorkspacePage>
  );
}
