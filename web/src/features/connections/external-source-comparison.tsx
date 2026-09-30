import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { Loader2, RotateCcw } from "lucide-react";
import { useEffect, useState } from "react";

type Snapshot = components["schemas"]["ExternalSnapshotResponse"];
type Comment = components["schemas"]["ExternalComment"];
type CommentPage = components["schemas"]["ExternalCommentPage"];
type ComparisonSource = { snapshot: Snapshot; comments: Comment[] };

async function loadComments(snapshot: Snapshot, signal: AbortSignal): Promise<Comment[]> {
  if (snapshot.retained_comment_count === 0) return [];

  const comments: Comment[] = [];
  let cursor: string | undefined;
  do {
    const page = await apiFetch<CommentPage>(`/external-source-snapshots/${snapshot.snapshot_id}/comments`, {
      signal,
      params: { cursor },
    });
    comments.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor);
  return comments;
}

async function loadComparisonSource(snapshotId: string, signal: AbortSignal): Promise<ComparisonSource> {
  const snapshot = await apiFetch<Snapshot>(`/external-source-snapshots/${snapshotId}`, { signal });
  return { snapshot, comments: await loadComments(snapshot, signal) };
}

export function ExternalSourceComparison({ baselineId, candidateId }: { baselineId: string; candidateId: string }) {
  const [attempt, setAttempt] = useState(0);
  const loadKey = `${baselineId}:${candidateId}:${attempt}`;
  const [result, setResult] = useState<{
    key: string;
    baseline?: ComparisonSource;
    candidate?: ComparisonSource;
    error?: string;
  }>({ key: "" });

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([
      loadComparisonSource(baselineId, controller.signal),
      loadComparisonSource(candidateId, controller.signal),
    ])
      .then(([left, right]) => {
        setResult({ key: loadKey, baseline: left, candidate: right });
      })
      .catch((reason) => {
        if (!controller.signal.aborted) {
          setResult({ key: loadKey, error: reason instanceof Error ? reason.message : "Comparison unavailable." });
        }
      });
    return () => controller.abort();
  }, [baselineId, candidateId, loadKey]);

  const baseline = result.key === loadKey ? result.baseline : undefined;
  const candidate = result.key === loadKey ? result.candidate : undefined;
  const error = result.key === loadKey ? result.error : undefined;

  if (error) {
    return (
      <div role="alert" className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed">
        <span>Comparison unavailable: {error}</span>
        <Button type="button" size="sm" variant="outline" onClick={() => setAttempt((value) => value + 1)}>
          <RotateCcw className="h-3 w-3" /> Retry comparison
        </Button>
      </div>
    );
  }
  if (!baseline || !candidate) {
    return <p role="status" aria-live="polite" className="mt-3 flex items-center gap-2 text-xs text-foreground-muted"><Loader2 className="h-3.5 w-3.5 animate-spin" />Loading source comparison...</p>;
  }

  const left = baseline.snapshot;
  const right = candidate.snapshot;
  const titleChanged = left.issue.title !== right.issue.title;
  const bodyChanged = left.issue.body !== right.issue.body;
  const stateChanged = left.issue.state !== right.issue.state;
  const commentCountChanged = left.comment_count !== right.comment_count;
  const commentsChanged = JSON.stringify(baseline.comments) !== JSON.stringify(candidate.comments);
  const commentDelta = summarizeCommentChanges(baseline.comments, candidate.comments);
  const changeCount = [titleChanged, bodyChanged, stateChanged, commentCountChanged || commentsChanged].filter(Boolean).length;

  return (
    <section aria-label="GitHub source comparison" className="mt-3 space-y-3 rounded-lg border border-border/70 bg-background/55 p-3 text-xs">
      <div>
        <p className="font-medium text-foreground">{changeCount ? `${changeCount} source areas changed` : "No imported content changed"}</p>
        <p className="mt-0.5 text-foreground-subtle">Current selection compared with the latest captured version.</p>
      </div>
      {titleChanged ? <ComparisonRow label="Title" before={left.issue.title} after={right.issue.title} /> : null}
      {bodyChanged ? <ComparisonRow label="Body" before={summarizeBody(left.issue.body)} after={summarizeBody(right.issue.body)} /> : null}
      {stateChanged ? <ComparisonRow label="State" before={capitalize(left.issue.state)} after={capitalize(right.issue.state)} /> : null}
      {commentCountChanged || commentsChanged ? (
        <ComparisonRow
          label="Comments"
          before={commentSummary(left, baseline.comments)}
          after={`${commentSummary(right, candidate.comments)}${commentDelta ? `; ${commentDelta}` : ""}`}
        />
      ) : null}
    </section>
  );
}

function ComparisonRow({ label, before, after }: { label: string; before: string; after: string }) {
  return (
    <div className="grid min-w-0 gap-2 border-t border-border/60 pt-3 sm:grid-cols-[5rem_minmax(0,1fr)_minmax(0,1fr)]">
      <p className="font-medium uppercase tracking-wide text-foreground-subtle">{label}</p>
      <div className="min-w-0 rounded-md border border-border/60 bg-surface/70 p-2 text-foreground-muted"><span className="mb-1 block text-[10px] uppercase tracking-wide text-foreground-subtle">Selected</span><p className="break-words">{before}</p></div>
      <div className="min-w-0 rounded-md border border-gold/25 bg-gold/5 p-2 text-foreground"><span className="mb-1 block text-[10px] uppercase tracking-wide text-gold/80">Latest</span><p className="break-words">{after}</p></div>
    </div>
  );
}

function summarizeBody(body: string): string {
  const compact = body.replace(/\s+/g, " ").trim();
  if (!compact) return "No body";
  return compact.length > 180 ? `${compact.slice(0, 177)}... (${compact.length} characters)` : `${compact} (${compact.length} characters)`;
}

function commentSummary(snapshot: Snapshot, comments: Comment[]): string {
  const retained = comments.length;
  const truncation = snapshot.comments_truncated || snapshot.omitted_comment_count > 0 ? ", bounded import" : "";
  return `${snapshot.comment_count} reported, ${retained} retained${truncation}`;
}

function summarizeCommentChanges(before: Comment[], after: Comment[]): string {
  const beforeById = new Map(before.map((comment) => [comment.provider_comment_id, comment]));
  const afterById = new Map(after.map((comment) => [comment.provider_comment_id, comment]));
  let added = 0;
  let removed = 0;
  let changed = 0;

  for (const [id, comment] of afterById) {
    const previous = beforeById.get(id);
    if (!previous) {
      added += 1;
    } else if (JSON.stringify(previous) !== JSON.stringify(comment)) {
      changed += 1;
    }
  }
  for (const id of beforeById.keys()) {
    if (!afterById.has(id)) removed += 1;
  }

  return [
    added ? `${added} added` : "",
    changed ? `${changed} changed` : "",
    removed ? `${removed} removed` : "",
  ].filter(Boolean).join(", ");
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}
