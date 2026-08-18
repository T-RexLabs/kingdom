import { apiFetch } from "@/api/client";
import { newIdempotencyKey, type PlanningSessionDetail } from "@/api/hooks";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { externalSourceFailureGuidance, externalSourceStatusLabel } from "@/features/connections/external-source-status";
import { safeGitHubIssueURL } from "@/features/connections/github-url";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { cn } from "@/lib/utils";
import { ChevronRight, ExternalLink, Github, Loader2, RefreshCw, Trash2 } from "lucide-react";
import { useState } from "react";
import { isAmbiguousMutationFailure } from "./mutation-recovery";

type PlanningExternalSource = PlanningSessionDetail["external_sources"][number];

// Beyond a handful of sources the list competes with the transcript, so it
// starts collapsed and summarises itself instead.
const collapseThreshold = 4;
type ExternalRefreshResult = components["schemas"]["ExternalRefreshResult"];

/**
 * Linked GitHub issues for a planning session.
 *
 * One row per source, because a session can carry ten of them and the
 * transcript below is the point of the page. Detail — the refresh outcome and
 * the snapshot diff — expands in place for one row at a time.
 */
export function PlanningSourcesPanel({
  sources,
  onAttach,
  onChanged,
}: {
  sources: PlanningExternalSource[];
  onAttach: (value: IssueSelection) => Promise<void>;
  onChanged: () => Promise<unknown>;
}) {
  const [expanded, setExpanded] = useState(sources.length <= collapseThreshold);
  const [openRow, setOpenRow] = useState("");
  const updateCount = sources.filter(hasUpdate).length;

  return (
    <aside
      aria-label="Planning sources"
      className="mx-4 mt-3 shrink-0 rounded-lg border border-gold/20 bg-gold/[0.03] px-2.5 py-2"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <button
          type="button"
          aria-expanded={expanded}
          onClick={() => setExpanded((open) => !open)}
          className="flex min-w-0 items-center gap-2 rounded px-1 py-0.5 text-left"
        >
          <ChevronRight
            className={cn("h-3.5 w-3.5 shrink-0 text-foreground-subtle transition-transform", expanded && "rotate-90")}
          />
          <span className="text-[10px] font-medium uppercase tracking-[0.18em] text-foreground-subtle">
            Planning sources
          </span>
          <span className="rounded-full border border-gold/20 bg-gold/5 px-2 py-0.5 text-[10px] text-gold">
            {sources.length}
          </span>
          {!expanded && updateCount > 0 ? (
            <span className="truncate text-[10px] text-gold">
              {updateCount} update{updateCount === 1 ? "" : "s"} available
            </span>
          ) : null}
        </button>
        <IssuePicker
          onSelect={onAttach}
          actionLabel="Add source"
          triggerLabel={sources.length > 0 ? "Add source" : "Add GitHub issue"}
          compact
        />
      </div>
      {expanded ? (
        sources.length > 0 ? (
          <ul className="mt-2 divide-y divide-gold/10 border-t border-gold/10">
            {sources.map((source) => (
              <li key={source.link_id}>
                <PlanningSourceRow
                  source={source}
                  open={openRow === source.link_id}
                  onToggle={() => setOpenRow((current) => (current === source.link_id ? "" : source.link_id))}
                  onChanged={onChanged}
                />
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 border-t border-gold/10 px-1 pt-2 text-xs text-foreground-muted">
            Add a GitHub issue as immutable context for future planner operations.
          </p>
        )
      ) : null}
    </aside>
  );
}

function hasUpdate(source: PlanningExternalSource) {
  const active = source.active_snapshot_id ?? source.used_snapshot_id;
  return source.latest_snapshot_id !== active;
}

function PlanningSourceRow({
  source,
  open,
  onToggle,
  onChanged,
}: {
  source: PlanningExternalSource;
  open: boolean;
  onToggle: () => void;
  onChanged: () => Promise<unknown>;
}) {
  const [busy, setBusy] = useState<"refresh" | "adopt" | "detach" | "">("");
  const [error, setError] = useState("");
  const [confirmingRemoval, setConfirmingRemoval] = useState(false);
  const [refreshResult, setRefreshResult] = useState<ExternalRefreshResult>();
  const [keys] = useState(() => new Map<string, string>());
  const activeSnapshotID = source.active_snapshot_id ?? source.used_snapshot_id;
  const latestSnapshotID = refreshResult?.link?.latest_snapshot_id ?? source.latest_snapshot_id;
  const sourceStatus = refreshResult?.outcome ?? source.status;
  const updateAvailable = latestSnapshotID !== activeSnapshotID;
  const failureGuidance = externalSourceFailureGuidance(sourceStatus);
  const sourceURL = safeGitHubIssueURL(source.canonical_url, source.repository, source.issue_number);
  const label = `${source.repository}#${source.issue_number}`;

  // One key per logical mutation so an ambiguous failure retries as the same
  // request rather than as a second one.
  const stableKey = (scope: string) => {
    const existing = keys.get(scope);
    if (existing) return existing;
    const key = newIdempotencyKey(scope);
    keys.set(scope, key);
    return key;
  };

  const run = async (
    action: "refresh" | "adopt" | "detach",
    scope: string,
    path: string,
    failure: string,
    onResult?: (value: ExternalRefreshResult) => void,
  ) => {
    setBusy(action);
    setError("");
    const key = stableKey(scope);
    try {
      const result = await apiFetch<ExternalRefreshResult>(path, {
        method: "POST",
        headers: { "Idempotency-Key": key },
      });
      keys.delete(scope);
      onResult?.(result);
      await onChanged();
    } catch (cause) {
      if (!isAmbiguousMutationFailure(cause)) keys.delete(scope);
      setError(cause instanceof Error ? cause.message : failure);
    } finally {
      setBusy("");
    }
  };

  const statusText = refreshResult?.message
    ?? `${externalSourceStatusLabel(sourceStatus, updateAvailable)}${updateAvailable ? ". Adopt it to use it in future planner operations." : "."}`;

  return (
    <section aria-label={`Linked GitHub source ${label}`} className="py-1.5">
      <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1.5">
        <button
          type="button"
          aria-expanded={open}
          aria-label={`Details for ${label}`}
          onClick={onToggle}
          className="flex min-w-0 flex-1 items-center gap-2 rounded px-1 py-0.5 text-left hover:bg-gold/5"
        >
          <ChevronRight
            className={cn("h-3 w-3 shrink-0 text-foreground-subtle transition-transform", open && "rotate-90")}
          />
          <Github className="h-3.5 w-3.5 shrink-0 text-gold" />
          <span className="truncate text-xs font-medium text-foreground">{label}</span>
          <span
            className={cn(
              "truncate text-xs",
              failureGuidance ? "text-status-failed" : updateAvailable ? "text-gold" : "text-foreground-muted",
            )}
          >
            {statusText}
          </span>
        </button>
        <div className="flex shrink-0 items-center gap-1">
          {sourceURL ? (
            <a
              href={sourceURL}
              target="_blank"
              rel="noopener noreferrer"
              aria-label={`Open ${label} on GitHub`}
              className="rounded p-1.5 text-foreground-subtle hover:text-gold"
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          ) : null}
          <button
            type="button"
            aria-label={`Refresh ${label}`}
            disabled={busy !== ""}
            className="rounded p-1.5 text-foreground-subtle hover:text-gold disabled:opacity-50"
            onClick={() =>
              void run("refresh", "external-refresh", `/external-source-links/${source.link_id}/refreshes`, "Could not refresh GitHub source", setRefreshResult)
            }
          >
            {busy === "refresh" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          </button>
          {updateAvailable ? (
            <Button
              type="button"
              size="sm"
              aria-label={`Adopt latest ${label}`}
              disabled={busy !== ""}
              onClick={() =>
                void run("adopt", "external-adopt", `/external-source-links/${source.link_id}/adoptions`, "Could not adopt the latest GitHub source", () => setRefreshResult(undefined))
              }
            >
              {busy === "adopt" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
              Adopt
            </Button>
          ) : null}
          <button
            type="button"
            aria-label={`Remove ${label}`}
            disabled={busy !== "" || confirmingRemoval}
            className="rounded p-1.5 text-foreground-subtle hover:text-status-failed disabled:opacity-50"
            onClick={() => setConfirmingRemoval(true)}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      {confirmingRemoval ? (
        <div className="mt-1.5 flex flex-wrap items-center gap-2 rounded-md border border-border bg-background/60 px-2.5 py-2 text-xs">
          <span className="text-foreground-muted">
            Remove {label} from future planner operations? Earlier operations keep it, and you can add it back.
          </span>
          <div className="ml-auto flex gap-2">
            <Button type="button" size="sm" variant="ghost" disabled={busy !== ""} onClick={() => setConfirmingRemoval(false)}>
              Cancel
            </Button>
            <Button
              type="button"
              size="sm"
              variant="destructive"
              disabled={busy !== ""}
              onClick={() =>
                void run("detach", "external-detach", `/external-source-links/${source.link_id}/detachments`, "Could not remove the GitHub source")
              }
            >
              {busy === "detach" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
              Remove
            </Button>
          </div>
        </div>
      ) : null}
      {error ? (
        <p role="alert" className="mt-1.5 px-1 text-xs text-status-failed">
          {error}
        </p>
      ) : null}
      {/* A source Kingdom can no longer read is not a detail: it stays visible
          whether or not the row is expanded. */}
      {failureGuidance && !refreshResult ? (
        <p role="status" className="mt-1.5 px-1 text-xs text-status-failed">
          {failureGuidance}
        </p>
      ) : null}
      {open ? (
        <div className="mt-1.5 space-y-2 border-l border-gold/20 pl-3 text-xs">
          {refreshResult?.retry_at ? (
            <p className="text-foreground-muted">Retry after {new Date(refreshResult.retry_at).toLocaleString()}.</p>
          ) : null}
          {updateAvailable ? (
            <ExternalSourceComparison baselineId={activeSnapshotID} candidateId={latestSnapshotID} />
          ) : (
            <p className="text-foreground-muted">
              Future planner operations use this issue exactly as it reads now.
            </p>
          )}
        </div>
      ) : null}
    </section>
  );
}
