import { useState, type ReactNode } from "react";
import { Download, File, FileX, RefreshCw } from "lucide-react";
import type { components } from "@/api/schema";
import { API_BASE } from "@/api/client";
import {
  useRunArtifacts,
  useRunArtifactAgents,
  useRunPublicationEntries,
  outputSnapshotChanged,
} from "@/api/hooks-run-output";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { SelectField } from "@/components/ui/select-field";
import { roleIdentity, useTheme } from "@/theme";
import { Virtuoso } from "react-virtuoso";
import { statusLabel } from "@/lib/status-system";

type Publication = components["schemas"]["RunExecutionPublication"];

export function RunOutput({ runId }: { runId: string }) {
  const [agentId, setAgentId] = useState("");
  const { theme } = useTheme();
  const agents = useRunArtifactAgents(runId);
  const artifacts = useRunArtifacts(runId, agentId);
  const participants = agents.data?.items ?? [];
  const agentOptions = participants.map((agent) => {
    const name = roleIdentity(theme, agent.role_id).name;
    const sameRole =
      participants.filter((other) => other.role_id === agent.role_id).length >
      1;
    return {
      value: agent.agent_id,
      label: sameRole
        ? `${name} · ${agent.display_name !== name ? agent.display_name : agent.agent_id}`
        : name,
    };
  });
  const pages = artifacts.data?.pages ?? [];
  const snapshot = pages[0]?.snapshot_id;
  const consistent = pages.every((page) => page.snapshot_id === snapshot);
  const items = consistent ? pages.flatMap((page) => page.items) : [];
  const changed = !consistent || outputSnapshotChanged(artifacts.error);
  return (
    <section className="space-y-5" aria-label="Run files">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <SelectField
          id="run-files-agent"
          label="Agent"
          ariaLabel="Filter files by agent"
          value={agentId}
          options={[{ label: "All", value: "" }, ...agentOptions]}
          onChange={setAgentId}
          className="w-64 max-w-full"
        />
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              void artifacts.refresh();
              void agents.refetch();
            }}
            disabled={artifacts.isFetching}
          >
            <RefreshCw size={14} /> Refresh
          </Button>
          <a
            className="inline-flex min-h-9 items-center gap-2 rounded-lg px-2 text-sm text-accent hover:bg-surface-raised"
            href={`${API_BASE}/runs/${encodeURIComponent(runId)}/artifacts/archive`}
            download
            title="Download all files from this run"
          >
            <Download size={14} />{" "}
            {agentId ? "Download All Run Files" : "Download All"}
          </a>
        </div>
      </div>
      {agents.isError && (
        <ReadError error={agents.error} onRetry={() => void agents.refetch()} />
      )}
      {changed && <SnapshotNotice onRefresh={() => void artifacts.refresh()} />}
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
        <p className="py-5 text-sm text-foreground-muted">Loading files…</p>
      ) : !items.length && !artifacts.isError && consistent ? (
        <p className="py-5 text-sm text-foreground-muted">
          {agentId
            ? "No files have been recorded by this agent."
            : "No files have been recorded."}
        </p>
      ) : items.length > 0 ? (
        <Virtuoso
          key={`${runId}:${agentId}`}
          style={{ height: Math.min(520, Math.max(88, items.length * 88)) }}
          data={items}
          computeItemKey={(_index, artifact) => artifact.artifact_id}
          increaseViewportBy={200}
          itemContent={(_index, artifact) => (
            <article className="flex items-center justify-between gap-3 border-b border-border/50 py-4 pr-2">
              <div className="flex min-w-0 items-center gap-3">
                {artifact.artifact_action === "delete" ? (
                  <FileX size={18} className="shrink-0 text-status-blocked" />
                ) : (
                  <File size={18} className="shrink-0 text-foreground-subtle" />
                )}
                <div className="min-w-0">
                  <p
                    className="truncate text-sm font-medium"
                    title={artifact.path || artifact.name}
                  >
                    {artifact.path || artifact.name}
                  </p>
                  <div className="mt-1 flex flex-wrap items-center gap-x-2 text-xs text-foreground-muted">
                    {artifact.artifact_action === "delete" ? (
                      <span className="text-status-blocked">Delete Action</span>
                    ) : (
                      <span>{artifact.size_bytes.toLocaleString()} Bytes</span>
                    )}
                    {!agentId && (
                      <span>
                        · {roleIdentity(theme, artifact.producer_role).name}
                      </span>
                    )}
                    <span>
                      ·{" "}
                      <span>
                        {artifact.lineage === "canonical_deliverable"
                          ? "Canonical Deliverable"
                          : "Audit Evidence"}
                      </span>
                    </span>
                  </div>
                </div>
              </div>
              {artifact.artifact_action === "write" &&
                artifact.download_url && (
                  <a
                    href={artifact.download_url}
                    download={artifact.name}
                    className="inline-flex min-h-9 shrink-0 items-center gap-1.5 rounded-md px-2 text-xs text-accent hover:bg-surface-raised"
                    aria-label={`Download ${artifact.path || artifact.name}`}
                  >
                    <Download size={14} />
                    <span className="hidden sm:inline">Download</span>
                  </a>
                )}
            </article>
          )}
        />
      ) : null}
      {artifacts.hasNextPage && !changed && (
        <PageButton
          loading={artifacts.isFetchingNextPage}
          label="Load More Files"
          onClick={() => void artifacts.fetchNextPage()}
        />
      )}
    </section>
  );
}

export function RunPublicationPanel({
  runId,
  attemptId,
  publication,
  picker,
  isLoading,
  error,
  onRefresh,
}: {
  runId: string;
  attemptId?: string;
  publication?: Publication;
  picker: ReactNode;
  isLoading: boolean;
  error: Error | null;
  onRefresh: () => Promise<unknown>;
}) {
  const [open, setOpen] = useState(false);
  return (
    <section className="mt-6 border-t border-border/60 pt-3">
      <button
        type="button"
        aria-expanded={open}
        aria-controls="run-publication-results"
        onClick={() => setOpen(!open)}
        className="min-h-10 text-sm text-foreground-muted hover:text-foreground"
      >
        {open ? "Hide Publication Results" : "Publication Results"}
      </button>
      <div id="run-publication-results" hidden={!open}>
        {open && (
          <div className="space-y-4 pt-3">
            <p className="text-sm text-foreground-muted">
              Inspect file publication for a specific execution attempt.
            </p>
            {picker}
            {error ? (
              <ReadError error={error} onRetry={() => void onRefresh()} />
            ) : isLoading ? (
              <p className="text-sm text-foreground-muted">
                Loading publication…
              </p>
            ) : publication && attemptId ? (
              <PublicationResults
                key={`${attemptId}:${publication.batch_id}:${publication.entries_revision}`}
                runId={runId}
                attemptId={attemptId}
                publication={publication}
                onRefreshSummary={onRefresh}
              />
            ) : (
              <p className="text-sm text-foreground-muted">
                No publication results are recorded for this attempt.
              </p>
            )}
          </div>
        )}
      </div>
    </section>
  );
}

export function PublicationResults({
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
    <section className="space-y-3" aria-label="Publication results">
      <div className="py-3">
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
            <div className="border-b border-border/50 py-3 pr-2">
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
