import { Download, File, FileX, RefreshCw } from "lucide-react";
import type { components } from "@/api/schema";
import { API_BASE } from "@/api/client";
import {
  useRunArtifacts,
  useRunPublicationEntries,
  outputSnapshotChanged,
} from "@/api/hooks-run-output";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RelativeTime } from "@/components/ui/relative-time";
import { RoleIdentity } from "@/theme";
import { Virtuoso } from "react-virtuoso";
import { statusLabel } from "@/lib/status-system";

type Publication = components["schemas"]["RunExecutionPublication"];

export function RunOutput({
  runId,
  attemptId,
  publication,
  onRefreshPublication,
}: {
  runId: string;
  attemptId?: string;
  publication?: Publication;
  onRefreshPublication: () => Promise<unknown>;
}) {
  const artifacts = useRunArtifacts(runId);
  const pages = artifacts.data?.pages ?? [];
  const snapshot = pages[0]?.snapshot_id;
  const consistent = pages.every((page) => page.snapshot_id === snapshot);
  const items = consistent ? pages.flatMap((page) => page.items) : [];
  const changed = !consistent || outputSnapshotChanged(artifacts.error);
  return (
    <div className="space-y-5">
      {publication && attemptId && (
        <PublicationResults
          key={`${attemptId}:${publication.batch_id}:${publication.entries_revision}`}
          runId={runId}
          attemptId={attemptId}
          publication={publication}
          onRefreshSummary={onRefreshPublication}
        />
      )}
      <section
        className="overflow-hidden rounded-xl border border-border bg-surface/40"
        aria-label="Run artifacts"
      >
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border p-4">
          <div>
            <h3 className="text-sm font-medium">Run Artifacts</h3>
            <p className="mt-1 text-xs text-foreground-muted">
              {items.length} loaded · Canonical deliverables and retained audit
              evidence.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => void artifacts.refresh()}
              disabled={artifacts.isFetching}
            >
              <RefreshCw size={14} />
              Refresh
            </Button>
            <a
              className="inline-flex min-h-9 items-center gap-2 rounded-lg border border-border px-3 text-xs hover:bg-surface-raised"
              href={`${API_BASE}/runs/${encodeURIComponent(runId)}/artifacts/archive`}
              download
            >
              <Download size={14} />
              Download All
            </a>
          </div>
        </div>
        {changed && (
          <SnapshotNotice onRefresh={() => void artifacts.refresh()} />
        )}
        {artifacts.isError && !changed && (
          <ReadError
            error={artifacts.error}
            onRetry={() =>
              void (artifacts.isFetchNextPageError
                ? artifacts.fetchNextPage()
                : artifacts.refetch())
            }
          />
        )}
        {artifacts.isPending ? (
          <p className="p-5 text-sm text-foreground-muted">
            Loading artifacts…
          </p>
        ) : !items.length && !artifacts.isError && consistent ? (
          <p className="p-5 text-sm text-foreground-muted">
            No artifacts have been recorded.
          </p>
        ) : items.length > 0 ? (
          <Virtuoso
            style={{ height: Math.min(520, Math.max(130, items.length * 120)) }}
            data={items}
            increaseViewportBy={200}
            itemContent={(_index, artifact) => (
              <article className="border-b border-border/70 p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex min-w-0 items-start gap-3">
                    {artifact.artifact_action === "delete" ? (
                      <FileX
                        size={17}
                        className="mt-0.5 shrink-0 text-status-blocked"
                      />
                    ) : (
                      <File size={17} className="mt-0.5 shrink-0 text-accent" />
                    )}
                    <div className="min-w-0">
                      <p className="break-all font-mono text-xs">
                        {artifact.path || artifact.name}
                      </p>
                      <div className="mt-2 flex flex-wrap items-center gap-2 text-[10px] text-foreground-muted">
                        <span
                          className={
                            artifact.lineage === "canonical_deliverable"
                              ? "text-accent"
                              : ""
                          }
                        >
                          {artifact.lineage === "canonical_deliverable"
                            ? "Canonical Deliverable"
                            : "Audit Evidence"}
                        </span>
                        <span>
                          ·{" "}
                          {artifact.artifact_action === "delete"
                            ? "Delete Action"
                            : `${artifact.size_bytes.toLocaleString()} Bytes`}
                        </span>
                        <span>· Attempt {artifact.attempt}</span>
                        <RelativeTime iso={artifact.created_at} />
                      </div>
                    </div>
                  </div>
                  {artifact.artifact_action === "write" &&
                    artifact.download_url && (
                      <a
                        href={artifact.download_url}
                        download={artifact.name}
                        className="inline-flex min-h-9 shrink-0 items-center gap-1.5 text-xs text-accent"
                      >
                        <Download size={13} />
                        Download File
                      </a>
                    )}
                </div>
                <details className="mt-2 text-xs text-foreground-muted">
                  <summary className="min-h-8 cursor-pointer py-1">
                    Provenance
                  </summary>
                  <div className="space-y-2 pb-2">
                    <RoleIdentity role={artifact.producer_role} />
                    <p>
                      Attempt ID:{" "}
                      <code className="break-all">{artifact.attempt_id}</code>
                    </p>
                    <p>
                      Artifact ID:{" "}
                      <code className="break-all">{artifact.artifact_id}</code>
                    </p>
                    {artifact.sha256 && (
                      <p>
                        SHA-256:{" "}
                        <code className="break-all">{artifact.sha256}</code>
                      </p>
                    )}
                  </div>
                </details>
              </article>
            )}
          />
        ) : null}
        {artifacts.hasNextPage && !changed && (
          <PageButton
            loading={artifacts.isFetchingNextPage}
            label="Load More Artifacts"
            onClick={() => void artifacts.fetchNextPage()}
          />
        )}
      </section>
    </div>
  );
}

function PublicationResults({
  runId,
  attemptId,
  publication,
  onRefreshSummary,
}: {
  runId: string;
  attemptId: string;
  publication: Publication;
  onRefreshSummary: () => Promise<unknown>;
}) {
  const query = useRunPublicationEntries(runId, attemptId, publication);
  const pages = query.data?.pages ?? [];
  const revision = pages[0]?.entries_revision;
  const consistent = pages.every(
    (page) =>
      page.entries_revision === publication.entries_revision &&
      page.entries_revision === revision,
  );
  const changed = !consistent || outputSnapshotChanged(query.error);
  const items = consistent ? pages.flatMap((page) => page.items) : [];
  return (
    <section
      className="overflow-hidden rounded-xl border border-border bg-surface/40"
      aria-label="Publication results"
    >
      <div className="border-b border-border p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-sm font-medium">Publication</h3>
          <Badge
            variant="status"
            status={
              publication.status === "succeeded"
                ? "succeeded"
                : publication.status === "failed"
                  ? "failed"
                  : "blocked"
            }
          >
            {statusLabel(publication.status)}
          </Badge>
        </div>
        <p className="mt-2 text-xs text-foreground-muted">
          {publication.published_count} Published ·{" "}
          {publication.conflicted_count} Conflicted · {publication.failed_count}{" "}
          Failed ·{" "}
          {publication.target_mode === "git_worktree" ? "Worktree" : "Direct"}
        </p>
        <p className="mt-1 text-xs text-foreground-muted">
          Publication records file outcomes; run approval remains separate.
        </p>
      </div>
      {changed && (
        <SnapshotNotice
          onRefresh={() => void onRefreshSummary().then(() => query.refresh())}
        />
      )}
      {query.isError && !changed && (
        <ReadError
          error={query.error}
          onRetry={() =>
            void (query.isFetchNextPageError
              ? query.fetchNextPage()
              : query.refetch())
          }
        />
      )}
      {query.isPending ? (
        <p className="p-5 text-sm text-foreground-muted">
          Loading publication results…
        </p>
      ) : !items.length && !query.isError ? (
        <p className="p-5 text-sm text-foreground-muted">
          No file results have been recorded.
        </p>
      ) : items.length > 0 ? (
        <Virtuoso
          style={{ height: Math.min(400, Math.max(80, items.length * 76)) }}
          data={items}
          increaseViewportBy={160}
          itemContent={(_index, entry) => (
            <div className="border-b border-border/70 px-4 py-3">
              <div className="flex flex-wrap justify-between gap-2">
                <span className="break-all font-mono text-xs">
                  {entry.path}
                </span>
                <span className="text-xs text-foreground-muted">
                  {statusLabel(entry.action)} · {statusLabel(entry.status)}
                </span>
              </div>
              {entry.reason_text && (
                <p className="mt-1 text-xs text-foreground-muted">
                  {entry.reason_text}
                </p>
              )}
              {entry.reason_code && (
                <code className="mt-1 block text-[10px] text-foreground-subtle">
                  {entry.reason_code}
                </code>
              )}
            </div>
          )}
        />
      ) : null}
      {query.hasNextPage && !changed && (
        <PageButton
          loading={query.isFetchingNextPage}
          label="Load More File Results"
          onClick={() => void query.fetchNextPage()}
        />
      )}
    </section>
  );
}

function SnapshotNotice({ onRefresh }: { onRefresh: () => void }) {
  return (
    <div
      role="status"
      className="flex flex-wrap items-center justify-between gap-2 bg-status-blocked/5 p-4 text-xs text-status-blocked"
    >
      <p>These results changed. Refresh to load the current snapshot.</p>
      <Button size="sm" variant="outline" onClick={onRefresh}>
        Refresh Results
      </Button>
    </div>
  );
}
function ReadError({
  error,
  onRetry,
}: {
  error: Error | null;
  onRetry: () => void;
}) {
  return (
    <div
      role="alert"
      className="flex items-center justify-between gap-3 p-4 text-sm text-status-failed"
    >
      <p>{error?.message ?? "Results could not be loaded."}</p>
      <Button size="sm" variant="outline" onClick={onRetry}>
        Retry
      </Button>
    </div>
  );
}
function PageButton({
  loading,
  label,
  onClick,
}: {
  loading: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <div className="flex justify-center border-t border-border p-3">
      <Button variant="ghost" size="sm" disabled={loading} onClick={onClick}>
        {loading ? "Loading results…" : label}
      </Button>
    </div>
  );
}
