import { useDeletePlanningSession, usePlanningSessions, useWorkspaceCapabilities, type PlanningSessionSummary } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";

import { ConfirmDialog, MODAL_BODY_TEXT_CLASS } from "@/components/ui/modal-shell";
import { Link, useNavigate, useRouterState } from "@tanstack/react-router";
import { Plus, Search, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Virtuoso } from "react-virtuoso";
import { getPlanningSessionDisplayTitle, hasPlaceholderTitle } from "./planning-session-title";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";
import { useQueryClient } from "@tanstack/react-query";
import { removeDeletedPlanningSession } from "./planner-session-cache";

function displaySessionTitle(session: PlanningSessionSummary): string {
  return getPlanningSessionDisplayTitle(session, "New session");
}

export function PlanningSessionList({ onSelect }: { onSelect?: () => void } = {}) {
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  useEffect(() => {
    const timer = setTimeout(() => setQuery(search.trim()), 200);
    return () => clearTimeout(timer);
  }, [search]);
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const navigate = useNavigate();
  const { data, isLoading, error, refetch, hasNextPage, fetchNextPage, isFetchingNextPage } = usePlanningSessions(100, query);
  const workspace = useWorkspaceCapabilities();
  const deletePlanningSession = useDeletePlanningSession();
  const recoverableDelete = usePlanningMutationJournal(
    "planning-delete",
    (sessionId: string, key) => deletePlanningSession.mutateAsync({ value: sessionId, key }),
    workspace.data?.project_root ?? "__pending__",
    "planning-session-delete",
    (_result, sessionId) => removeDeletedPlanningSession(queryClient, sessionId),
  );
  const sessions = data?.items ?? [];
  const [recoveryError, setRecoveryError] = useState("");
  const [pendingDelete, setPendingDelete] = useState<PlanningSessionSummary | null>(null);

  const confirmDelete = async () => {
    if (!pendingDelete) {
      return;
    }
    const deletingSession = pendingDelete;
    const retainedSessionId = recoverableDelete.getPendingRequest();
    if (retainedSessionId && retainedSessionId !== deletingSession.planning_session_id) {
      setRecoveryError("Resolve the previous session deletion before choosing another session.");
      return;
    }
    const isActiveSession = pathname === `/work-orders/planning/${deletingSession.planning_session_id}`;
    try {
      await recoverableDelete.mutateAsync(deletingSession.planning_session_id);
    } catch (error) {
      setRecoveryError(error instanceof Error ? error.message : "The deletion outcome could not be confirmed.");
      return;
    }
    setRecoveryError("");
    setPendingDelete(null);
    if (isActiveSession) {
      await navigate({ to: "/work-orders/planning" });
    }
  };

  return (
    <>
      <nav aria-label="Planning sessions" className="flex h-full min-h-0 flex-col">
        <div className="shrink-0 space-y-4 px-3 pb-5 pt-2">
          <Link to="/work-orders/planning" onClick={onSelect} className={buttonVariants({ variant: "outline", className: "w-full justify-start" })}>
            <Plus className="h-4 w-4" /> New Session
          </Link>
          <div className="relative">
            <Search aria-hidden="true" className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-foreground-subtle" />
            <input aria-label="Search planning sessions" type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search sessions..." className="w-full rounded-lg border border-border bg-background py-2 pl-9 pr-3 text-sm outline-none focus:border-accent focus:ring-1 focus:ring-accent" />
          </div>
          <p className="px-1 text-xs font-medium text-foreground-subtle">{query ? "Search Results" : "Recent Sessions"}</p>
        </div>
        <div className="flex min-h-0 flex-1 flex-col gap-1 overflow-hidden px-3 pb-4" aria-busy={isLoading}>
        {recoverableDelete.hasPendingRecovery && !pendingDelete ? <div role="status" className="space-y-2 rounded-lg border border-border p-3 text-xs text-foreground-muted"><p>A previous session deletion is unconfirmed.</p><p className="break-all font-mono">{recoverableDelete.pendingRequest}</p><Button size="sm" variant="outline" onClick={() => { const id = recoverableDelete.pendingRequest; void recoverableDelete.retry().then(() => { setRecoveryError(""); if (pathname === `/work-orders/planning/${id}`) void navigate({ to: "/work-orders/planning" }); }).catch((error: unknown) => setRecoveryError(error instanceof Error ? error.message : "Deletion could not be confirmed.")); }}>Resolve Deletion</Button>{recoveryError && <p role="alert">{recoveryError}</p>}</div> : null}
        {sessions.length > 0 && <Virtuoso
          key={query}
          className="min-h-0 flex-1"
          style={{ height: "100%" }}
          role="list"
          aria-label="Planning session results"
          data={sessions}
          increaseViewportBy={150}
          computeItemKey={(_index, session) => session.planning_session_id}
          itemContent={(_index, session) => {
          const isActive = pathname === `/work-orders/planning/${session.planning_session_id}`;
          return (
            <div role="listitem" className={`group flex items-center gap-1 rounded-xl transition-colors ${isActive ? "bg-accent/10" : "hover:bg-surface-raised"}`}>
              <Link to="/work-orders/planning/$sessionId" params={{ sessionId: session.planning_session_id }} onClick={onSelect} aria-current={isActive ? "page" : undefined} className="min-w-0 flex-1 rounded-xl px-3 py-3 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent">
                <p className={`truncate text-sm font-medium ${hasPlaceholderTitle(session) ? "text-foreground-subtle italic" : "text-foreground"}`}>{displaySessionTitle(session)}</p>
                <p className="mt-1 text-xs text-foreground-subtle">{session.conversation_state === "processing" ? "In Progress" : session.current_draft_id ? "Plan Available" : "Discussion"}</p>
              </Link>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="mr-1 shrink-0 opacity-70 transition-opacity sm:opacity-0 sm:group-hover:opacity-100 focus:opacity-100"
                aria-label={`Delete ${displaySessionTitle(session)}`}
                disabled={!workspace.data || recoverableDelete.hasPendingRecovery}
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  if (!recoverableDelete.getPendingRequest()) setPendingDelete(session);
                }}
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          );
        }} />}

        {hasNextPage ? (
          <Button className="w-full shrink-0" variant="ghost" size="sm" disabled={isFetchingNextPage} onClick={() => void fetchNextPage()}>
            {isFetchingNextPage ? "Loading sessions..." : "Load More Sessions"}
          </Button>
        ) : null}

        {isLoading && sessions.length === 0 ? (
          <div className="rounded-md border border-dashed border-border px-3 py-4 text-xs text-foreground-subtle">
            Loading planning sessions...
          </div>
        ) : null}

        {!isLoading && !error && sessions.length === 0 ? (
          <div className="rounded-md border border-dashed border-border px-3 py-4 text-xs text-foreground-subtle">
            {query ? "No sessions match this search." : "Your planning sessions will appear here."}
          </div>
        ) : null}

        {workspace.error ? <p role="alert" className="px-3 py-2 text-xs text-status-failed">Project settings are unavailable. <button className="underline" onClick={() => void workspace.refetch()}>Retry</button></p> : null}
        {error ? (
          <div className="rounded-md border border-status-failed/30 bg-status-failed/10 px-3 py-4 text-xs text-status-failed">
            Failed to load planning sessions.
            <div className="mt-2">
              <Button type="button" size="sm" variant="ghost" onClick={() => void refetch()}>
                Retry
              </Button>
            </div>
          </div>
        ) : null}

          {deletePlanningSession.error ? (
            <div className="rounded-md border border-status-failed/30 bg-status-failed/10 px-3 py-3 text-xs text-status-failed">
              {deletePlanningSession.error.message}
            </div>
          ) : null}
        </div>
      </nav>

      {pendingDelete ? (
        <ConfirmDialog
          open
          onClose={() => {
            if (!recoverableDelete.hasPendingRecovery) setPendingDelete(null);
          }}
          onConfirm={() => void confirmDelete()}
          title={displaySessionTitle(pendingDelete)}
          caption="Delete Session"
          description="This removes the planning session and its drafts, messages, and saved context snapshots."
          confirmLabel={deletePlanningSession.isPending ? "Deleting..." : recoverableDelete.hasPendingRecovery ? "Retry Delete Session" : "Delete Session"}
          confirmDisabled={deletePlanningSession.isPending}
          dismissDisabled={recoverableDelete.hasPendingRecovery}
        >
          <p className={MODAL_BODY_TEXT_CLASS}>This action cannot be undone.</p>
          {recoveryError && <p role="alert" className="mt-3 text-sm text-status-failed">{recoveryError}</p>}
        </ConfirmDialog>
      ) : null}
    </>
  );
}
