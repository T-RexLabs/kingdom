import { useDeletePlanningSession, usePlanningSessions, type PlanningSessionSummary } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ConfirmDialog, MODAL_BODY_TEXT_CLASS } from "@/components/ui/modal-shell";
import { Link, useNavigate, useRouterState } from "@tanstack/react-router";
import { Plus, Sparkles, X } from "lucide-react";
import { useState } from "react";
import { getPlanningSessionDisplayTitle, hasPlaceholderTitle } from "./planning-session-title";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

function displaySessionTitle(session: PlanningSessionSummary): string {
  return getPlanningSessionDisplayTitle(session, "New session");
}

export function PlanningSessionList() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const navigate = useNavigate();
  const { data, isLoading, error, refetch, hasNextPage, fetchNextPage, isFetchingNextPage } = usePlanningSessions();
  const deletePlanningSession = useDeletePlanningSession();
  const recoverableDelete = useRecoverableMutation(
    "planning-delete",
    (sessionId: string, key) => deletePlanningSession.mutateAsync({ value: sessionId, key }),
  );
  const sessions = data?.items ?? [];
  const [pendingDelete, setPendingDelete] = useState<PlanningSessionSummary | null>(null);

  const confirmDelete = async () => {
    if (!pendingDelete) {
      return;
    }
    const deletingSession = pendingDelete;
    const isActiveSession = pathname === `/decrees/planning/${deletingSession.planning_session_id}`;
    try {
      await recoverableDelete.mutateAsync(deletingSession.planning_session_id);
    } catch {
      return;
    }
    setPendingDelete(null);
    if (isActiveSession) {
      await navigate({ to: "/decrees/planning" });
    }
  };

  return (
    <>
      <Card className="flex h-full min-h-0 flex-col">
        <CardHeader className="shrink-0 space-y-3">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2 text-xs uppercase tracking-wider">
            <Sparkles className="h-3.5 w-3.5" />
            Sessions
          </CardTitle>
          <Link to="/decrees/planning" className={buttonVariants({ variant: "ghost", size: "sm" })}>
            <Plus className="h-3.5 w-3.5" />
            New
          </Link>
        </div>
        </CardHeader>
        <CardContent className="flex-1 min-h-0 overflow-y-auto space-y-2">
        {sessions.map((session) => {
          const isActive = pathname === `/decrees/planning/${session.planning_session_id}`;
          return (
            <Link
              key={session.planning_session_id}
              to="/decrees/planning/$sessionId"
              params={{ sessionId: session.planning_session_id }}
              className={`group flex items-center gap-2 rounded-xl border px-3 py-2 transition-colors ${
                isActive ? "border-gold/60 bg-gold/10" : "border-border bg-surface-raised/20 hover:bg-surface-raised/35"
              }`}
            >
              <p className={`min-w-0 flex-1 truncate text-sm font-medium ${hasPlaceholderTitle(session) ? "text-foreground-subtle italic" : "text-foreground"}`}>
                {displaySessionTitle(session)}
              </p>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="shrink-0 opacity-0 transition-opacity group-hover:opacity-100 focus:opacity-100"
                aria-label={`Delete ${displaySessionTitle(session)}`}
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  setPendingDelete(session);
                }}
              >
                <X className="h-4 w-4" />
              </Button>
            </Link>
          );
        })}

        {hasNextPage ? (
          <Button className="w-full" variant="ghost" size="sm" disabled={isFetchingNextPage} onClick={() => void fetchNextPage()}>
            {isFetchingNextPage ? "Loading sessions..." : "Load More Sessions"}
          </Button>
        ) : null}

        {isLoading && sessions.length === 0 ? (
          <div className="rounded-md border border-dashed border-border px-3 py-4 text-xs text-foreground-subtle">
            Loading planning sessions...
          </div>
        ) : null}

        {!isLoading && sessions.length === 0 ? (
          <div className="rounded-md border border-dashed border-border px-3 py-4 text-xs text-foreground-subtle">
            No planning sessions yet. Start one from the planning workspace.
          </div>
        ) : null}

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
        </CardContent>
      </Card>

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
        </ConfirmDialog>
      ) : null}
    </>
  );
}
