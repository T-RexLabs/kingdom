import { useState } from "react";
import { apiFetch } from "@/api/client";
import { usePlanningContextEntries, type PlanningSessionDetail } from "@/api/hooks";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";

export function PlanningContextPanel({ session, onChanged }: { session: PlanningSessionDetail; onChanged: () => Promise<unknown> }) {
  const [kind, setKind] = useState<"included" | "excluded">("included");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const entries = usePlanningContextEntries(session.planning_session_id, session.context_snapshot_id ?? "", kind);
  const refresh = usePlanningMutationJournal("planning-context-refresh", (_body: Record<string, never>, key) => apiFetch<components["schemas"]["RefreshPlanningSessionContextResult"]>(`/planning-sessions/${session.planning_session_id}/context/refresh`, { method: "POST", headers: { "Idempotency-Key": key } }), session.planning_session_id, "planning-context-refresh");
  const refreshContext = async () => {
    setBusy(true); setError("");
    try { await refresh.mutateAsync({}); await onChanged(); } catch (failure) { setError(failure instanceof Error ? failure.message : "Context could not be refreshed."); } finally { setBusy(false); }
  };
  return <section className="space-y-5">
    <div className="flex flex-wrap items-start justify-between gap-4"><div><h2 className="font-display text-2xl">Project Context</h2><p className="mt-2 text-sm text-foreground-muted">The saved project snapshot used to ground this conversation.</p></div><Button variant="outline" size="sm" disabled={busy || session.conversation_state !== "ready"} onClick={() => void refreshContext()}>{busy ? "Refreshing..." : refresh.hasPendingRecovery ? "Resolve Context Refresh" : "Refresh Context"}</Button></div>
    <p className="break-all rounded-lg border border-border bg-surface px-4 py-3 font-mono text-xs text-foreground-muted">{session.project_root}</p>
    {error && <p role="alert" className="text-sm text-status-failed">{error}</p>}
    {session.context_snapshot_id ? <>
      <div className="flex gap-2" aria-label="Context file visibility">{(["included", "excluded"] as const).map((value) => <Button key={value} variant={kind === value ? "outline" : "ghost"} size="sm" aria-pressed={kind === value} onClick={() => setKind(value)}>{value === "included" ? "Included Files" : "Excluded Files"}</Button>)}</div>
      {entries.isLoading ? <p className="text-sm text-foreground-muted">Loading context files...</p> : null}
      {entries.error ? <div role="alert" className="flex items-center gap-3 text-sm text-status-failed">Context files could not be loaded.<Button size="sm" variant="outline" onClick={() => void entries.refetch()}>Retry</Button></div> : null}
      <ul className="divide-y divide-border rounded-xl border border-border">{entries.data?.pages.flatMap((page) => page.items).map((entry) => <li key={entry.path} className="px-4 py-3"><p className="break-all font-mono text-xs">{entry.path}</p>{entry.reason && <p className="mt-1 text-xs text-foreground-muted">{entry.reason}</p>}</li>)}</ul>
      {!entries.isLoading && !entries.error && entries.data?.pages.every((page) => page.items.length === 0) && <p className="text-sm text-foreground-muted">No {kind} files in this snapshot.</p>}
      {entries.hasNextPage && <Button variant="outline" size="sm" disabled={entries.isFetchingNextPage} onClick={() => void entries.fetchNextPage()}>{entries.isFetchingNextPage ? "Loading files..." : "Load More Files"}</Button>}
      <details className="text-xs text-foreground-muted"><summary className="cursor-pointer py-2">Snapshot Details</summary><p className="break-all font-mono">{session.context_snapshot_id}</p></details>
    </> : <p className="text-sm text-foreground-muted">No project snapshot is available yet. Refresh context to capture one.</p>}
  </section>;
}
