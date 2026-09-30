import { useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import {
  Check,
  Download,
  FileCode2,
  Plus,
  RefreshCw,
  Trash2,
  Upload,
} from "lucide-react";
import { WorkspacePage } from "@/components/workspace-page";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/ui/modal";
import {
  builtInThemes,
  useTheme,
  type ThemeCatalog,
  type ThemeReference,
} from "@/theme";
import { themeAPI, themeError, fileBase64, exportTheme } from "./theme-api";
import { useThemeOperation } from "./use-theme-operation";
import { ThemePreviewCard } from "./theme-preview-card";
import "./appearance.css";

type Summary = ThemeCatalog["items"][number];
export function AppearancePage() {
  const appearance = useTheme();
  const operation = useThemeOperation(
    appearance.catalog?.workspaceID ?? "unavailable",
  );
  const input = useRef<HTMLInputElement>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const [selecting, setSelecting] = useState<string | null>(null);
  const [confirm, setConfirm] = useState<{
    id: string;
    kind: "package" | "source";
    digest?: string;
  } | null>(null);
  const groups = new Map<string, Summary[]>();
  for (const item of appearance.catalog?.items ?? [])
    groups.set(item.id, [...(groups.get(item.id) ?? []), item]);
  const grouped = [...groups.values()]
    .map((items) =>
      items.sort((a, b) =>
        b.version.localeCompare(a.version, undefined, { numeric: true }),
      ),
    )
    .sort(
      (a, b) =>
        Number(b[0].builtin) - Number(a[0].builtin) ||
        a[0].name.localeCompare(b[0].name),
    );
  const locked =
    !appearance.catalog ||
    operation.busy ||
    !!operation.pending ||
    operation.blockedRecovery;
  async function choose(ref: ThemeReference) {
    setSelecting(ref.id);
    setFailure(null);
    try {
      if (await appearance.selectTheme(ref))
        setNotice("Your appearance has been updated for this workspace.");
    } catch (error) {
      setFailure(themeError(error));
    } finally {
      setSelecting(null);
    }
  }
  async function importFile(file?: File) {
    if (!file) return;
    setFailure(null);
    setNotice(null);
    try {
      const archive = await fileBase64(file, (32 << 20) + (256 << 10));
      const validated = await themeAPI.validateArchive(archive);
      await operation.run({
        method: "POST",
        path: "/themes/import",
        body: { archive },
        verify: {
          id: validated.manifest.id,
          resolvedDigest: validated.resolvedDigest,
        },
      });
      await appearance.refreshCatalog();
      setNotice(
        "Package imported. Choose it below to change your personal appearance.",
      );
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function removeSource(id: string) {
    try {
      const source = await themeAPI.source(id);
      setConfirm({ id, kind: "source", digest: source.sourceDigest });
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  async function remove() {
    if (!confirm) return;
    try {
      await operation.run({
        method: "DELETE",
        path: `/themes/${encodeURIComponent(confirm.id)}${confirm.kind === "source" ? "/source" : ""}`,
        body:
          confirm.kind === "source"
            ? { expected_source_digest: confirm.digest }
            : undefined,
      });
      setConfirm(null);
      await appearance.refreshCatalog();
      setNotice(
        confirm.kind === "source"
          ? "Editable source removed. Published revisions remain available."
          : "Package unpublished. Editable source files remain available.",
      );
    } catch (error) {
      setFailure(themeError(error));
    }
  }
  return (
    <WorkspacePage
      section="Appearance"
      title="Make It Yours"
      description={appearance.theme.copy["appearance.description"]}
      actions={
        <>
          <Button
            variant="outline"
            onClick={() => {
              setFailure(null);
              void appearance.refreshCatalog();
            }}
          >
            <RefreshCw size={15} />
            Refresh
          </Button>
          <Link to="/appearance/create" className="appearance-action">
            <Plus size={15} />
            Create Theme
          </Link>
        </>
      }
    >
      {(failure || operation.message || appearance.error) && (
        <div className="appearance-notice error" role="alert">
          {failure || operation.message || appearance.error}
        </div>
      )}
      {operation.storageWarning && (
        <p className="appearance-notice">
          Browser storage is unavailable or full. Keep this tab open until any
          pending change has been recovered.
        </p>
      )}
      {notice && (
        <p className="appearance-notice" role="status">
          {notice}
        </p>
      )}
      {operation.pending && !operation.busy && (
        <div className="appearance-notice">
          <p>
            The previous shared change needs recovery. Recovery retries only in
            the original browser session. A renewed session checks shared state
            without replaying the change.
          </p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              void operation
                .retry()
                .then(() => {
                  setFailure(null);
                  return appearance.refreshCatalog();
                })
                .catch((error) => setFailure(themeError(error)));
            }}
          >
            Recover Change
          </Button>
        </div>
      )}
      <section aria-labelledby="installed-themes-heading">
        <div className="appearance-section-heading">
          <div>
            <h2 id="installed-themes-heading">Your Themes</h2>
            <p>Your selection is personal. Published packages are shared.</p>
          </div>
          <Button
            variant="outline"
            disabled={locked}
            onClick={() => input.current?.click()}
          >
            <Upload size={15} />
            Import Package
          </Button>
          <input
            ref={input}
            type="file"
            accept=".zip,application/zip"
            hidden
            aria-label="Import theme package"
            onChange={(event) => {
              void importFile(event.target.files?.[0]);
              event.target.value = "";
            }}
          />
        </div>
        {grouped.length ? (
          <div className="appearance-theme-grid">
            {grouped.map((items) => (
              <InstalledTheme
                key={items[0].id}
                items={items}
                active={appearance.selected}
                busy={selecting === items[0].id || locked}
                onChoose={choose}
                onRemove={() =>
                  setConfirm({ id: items[0].id, kind: "package" })
                }
                onError={(error) => setFailure(themeError(error))}
              />
            ))}
          </div>
        ) : (
          <div className="appearance-empty">
            <h3>No Theme Packages Available</h3>
            <p>
              The bundled fallback is still active. Refresh to load the
              workspace catalog.
            </p>
          </div>
        )}
      </section>
      <section className="mt-10" aria-labelledby="source-files-heading">
        <div className="appearance-section-heading">
          <div>
            <h2 id="source-files-heading">Editable Theme Files</h2>
            <p>
              Agent-friendly sources live in{" "}
              <code>.kingdom/themes/&lt;id&gt;/theme.json</code>. Refresh after
              file edits to inspect and publish them.
            </p>
          </div>
        </div>
        {(appearance.catalog?.sources ?? []).length ? (
          <div className="appearance-source-list">
            {appearance.catalog!.sources.map((id) => (
              <div key={id}>
                <FileCode2 size={18} className="text-accent" />
                <div className="min-w-0 flex-1">
                  <p className="truncate font-medium">{id}</p>
                  <code className="text-xs text-foreground-muted">
                    theme.json
                  </code>
                </div>
                <Link
                  className="appearance-link"
                  to="/appearance/create"
                  search={{ source: id }}
                >
                  Edit Source
                </Link>
                <button
                  className="appearance-icon-button"
                  aria-label={`Delete editable source ${id}`}
                  disabled={locked}
                  onClick={() => {
                    void removeSource(id);
                  }}
                >
                  <Trash2 size={16} />
                </button>
              </div>
            ))}
          </div>
        ) : (
          <div className="appearance-empty">
            <FileCode2 className="mx-auto mb-3 text-accent" />
            <h3>Start with a File or an Idea</h3>
            <p>
              Create a theme here, or add an agent-authored package to the
              shared theme directory.
            </p>
            <Link
              to="/appearance/create"
              className="appearance-link mt-4 inline-flex"
            >
              Create Theme
            </Link>
          </div>
        )}
      </section>
      {appearance.catalog?.diagnostics?.map((diagnostic, index) => (
        <p key={index} className="appearance-notice error mt-4" role="alert">
          {diagnostic.message}
        </p>
      ))}
      <ConfirmDialog
        open={!!confirm}
        onClose={() => setConfirm(null)}
        onConfirm={() => {
          void remove();
        }}
        title={
          confirm?.kind === "source"
            ? "Delete Editable Source"
            : "Unpublish Theme"
        }
        description={
          confirm?.kind === "source"
            ? `Delete the editable files for ${confirm.id}? Published revisions remain available. Unsaved local drafts are kept in their editors.`
            : `Unpublish ${confirm?.id ?? "this package"} for this workspace? People using it will fall back when they refresh. Its editable files and version history remain preserved.`
        }
        confirmLabel={
          operation.busy
            ? "Removing…"
            : confirm?.kind === "source"
              ? "Delete Source"
              : "Unpublish"
        }
        confirmDisabled={locked}
        dismissDisabled={operation.busy}
      />
    </WorkspacePage>
  );
}
function InstalledTheme({
  items,
  active,
  busy,
  onChoose,
  onRemove,
  onError,
}: {
  items: Summary[];
  active: ThemeReference | null;
  busy: boolean;
  onChoose: (ref: ThemeReference) => Promise<void>;
  onRemove: () => void;
  onError: (error: unknown) => void;
}) {
  const [digest, setDigest] = useState("");
  const item =
    items.find((entry) => entry.resolvedDigest === digest) ??
    items.find((entry) => entry.resolvedDigest === active?.resolvedDigest) ??
    items[0];
  const query = useQuery({
    queryKey: ["theme-thumbnail", item.id, item.resolvedDigest],
    queryFn: () => themeAPI.revision(item),
    staleTime: Infinity,
    retry: false,
  });
  const manifest =
    query.data?.manifest ?? builtInThemes.find((theme) => theme.id === item.id);
  const chosen =
    active?.id === item.id && active.resolvedDigest === item.resolvedDigest;
  return (
    <article className={`appearance-theme-card ${chosen ? "selected" : ""}`}>
      {manifest ? (
        <ThemePreviewCard manifest={manifest} pkg={query.data} compact />
      ) : (
        <div className="appearance-thumbnail-placeholder">
          {query.isPending ? "Loading preview…" : "Preview unavailable"}
        </div>
      )}
      <div className="p-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h3 className="font-display text-2xl">{item.name}</h3>
            <p className="mt-1 text-xs text-foreground-muted">
              {item.builtin ? "Built-In" : "Shared Package"} · {item.version}
            </p>
          </div>
          {chosen && (
            <span className="inline-flex items-center gap-1 text-xs text-accent">
              <Check size={13} />
              Selected
            </span>
          )}
        </div>
        <p className="mt-3 min-h-10 text-sm text-foreground-muted">
          {item.description}
        </p>
        {items.length > 1 && (
          <label className="mt-3 block text-xs text-foreground-muted">
            Revision
            <select
              className="appearance-input mt-1"
              value={item.resolvedDigest}
              onChange={(event) => setDigest(event.target.value)}
            >
              {items.map((revision) => (
                <option
                  key={revision.resolvedDigest}
                  value={revision.resolvedDigest}
                >
                  {revision.version} · {revision.resolvedDigest.slice(0, 8)}
                </option>
              ))}
            </select>
          </label>
        )}
        <div className="mt-5 flex flex-wrap items-center gap-2">
          <Button
            size="sm"
            disabled={busy || chosen}
            onClick={() => {
              void onChoose(item);
            }}
          >
            {chosen ? "Selected" : "Choose Theme"}
          </Button>
          <Link
            className="appearance-link text-xs"
            to="/appearance/create"
            search={
              item.builtin
                ? { base: item.id }
                : { source: item.id, revision: item.resolvedDigest }
            }
          >
            Create a Copy
          </Link>
          <div className="ml-auto flex">
            <button
              className="appearance-icon-button"
              aria-label={`Export ${item.name}`}
              onClick={() => {
                void exportTheme(item).catch(onError);
              }}
            >
              <Download size={15} />
            </button>
            {!item.builtin && (
              <button
                className="appearance-icon-button"
                disabled={busy}
                aria-label={`Unpublish ${item.name}`}
                onClick={onRemove}
              >
                <Trash2 size={15} />
              </button>
            )}
          </div>
        </div>
      </div>
    </article>
  );
}
