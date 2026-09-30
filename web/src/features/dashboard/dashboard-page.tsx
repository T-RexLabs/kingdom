import { useDeferredValue, useMemo, type ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import {
  useDashboardSnapshot,
  useInfiniteRuns,
  usePlanningSessions,
  type RunsStateFilter,
  type RunSummary,
} from "@/api/hooks";
import { WorkspacePage } from "@/components/workspace-page";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RelativeTime } from "@/components/ui/relative-time";
import { Sparkline } from "@/components/ui/sparkline";
import { Skeleton } from "@/components/ui/skeleton";
import { useThemeCopy } from "@/theme";
import {
  runDisplayBadgeStatus,
  runDisplayStateLabel,
} from "@/features/runs/run-display-state";

/** Overview queues use server filters across all history. Recent-created
 * snapshots are useful for trends, never as a source of pending decisions. */
export function DashboardPage() {
  const query = useDashboardSnapshot();
  const snapshot = query.data;
  const title = useThemeCopy("overview.welcome");
  const workOrderName = useThemeCopy("workOrder.singular");
  const trend = useMemo(
    () => snapshot?.run_trend.map((point) => point.run_count) ?? [],
    [snapshot?.run_trend],
  );
  const deferredTrend = useDeferredValue(trend);
  return (
    <WorkspacePage
      section="Overview"
      title={title}
      description="Keep work moving. Pick up a plan, review a decision, or follow your agents."
      actions={
        <>
          <Link
            to="/work-orders/planning"
            className="rounded-lg border border-border px-4 py-2.5 text-sm hover:bg-surface"
          >
            Plan Work
          </Link>
          <Link
            to="/work-orders/new"
            className="rounded-lg bg-accent px-4 py-2.5 text-sm font-medium text-accent-foreground hover:bg-accent-bright"
          >
            New {workOrderName}
          </Link>
        </>
      }
    >
      <div className="space-y-10">
        <OverviewQueue
          title="Needs Your Decision"
          state="awaiting_human_decision"
          empty="No decisions waiting for you."
        />
        <div className="grid items-start gap-10 lg:grid-cols-2">
          <OverviewQueue
            title="In Progress"
            state="running"
            empty="No work is running right now."
          />
          <PlanningQueue />
        </div>
        <div className="grid items-start gap-10 lg:grid-cols-2">
          <OverviewQueue
            title="Failed Runs"
            state="failed"
            hideWhenEmpty
            empty="No failed runs."
          />
          <OverviewQueue
            title="Ready to Start"
            state="pending"
            hideWhenEmpty
            empty="No queued runs."
          />
        </div>
        <section
          className="border-t border-border pt-8"
          aria-labelledby="overview-summary-heading"
        >
          <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 id="overview-summary-heading" className="text-lg font-medium">
                Last 24 Hours
              </h2>
              <p className="mt-1 text-xs text-foreground-muted">
                Run outcomes are scoped to runs created in this window.
              </p>
            </div>
            <Link
              to="/analytics"
              className="text-sm text-accent hover:underline"
            >
              View Analytics
            </Link>
          </div>
          {query.error && (
            <QueryNotice
              retained={Boolean(snapshot)}
              onRetry={() => {
                void query.refetch();
              }}
            >
              The overview snapshot could not be refreshed.
            </QueryNotice>
          )}
          {!snapshot && query.isLoading ? (
            <div className="grid grid-cols-3 gap-6">
              <Skeleton className="h-20" />
              <Skeleton className="h-20" />
              <Skeleton className="h-20" />
            </div>
          ) : (
            snapshot && (
              <>
                <div className="grid gap-6 sm:grid-cols-3">
                  <SummaryMetric
                    label="Succeeded"
                    value={snapshot.overview.succeeded_runs}
                  />
                  <SummaryMetric
                    label="Failed"
                    value={snapshot.overview.failed_runs}
                  />
                  <SummaryMetric
                    label="Reported Tokens"
                    value={snapshot.tokens.derived_total_tokens.toLocaleString()}
                  />
                </div>
                <div className="mt-6 flex flex-wrap items-center justify-between gap-4 text-xs text-foreground-muted">
                  <span>
                    Runtime{" "}
                    {snapshot.runtime.status === "ok" ? "Online" : "Degraded"} ·{" "}
                    {snapshot.runtime.running_runs} Running ·{" "}
                    {snapshot.runtime.stale_running_nodes} Stale Nodes
                  </span>
                  <Sparkline
                    ariaLabel="Run trend over the last 24h"
                    data={deferredTrend}
                    width={180}
                    height={32}
                  />
                  <span>
                    Updated <RelativeTime iso={snapshot.snapshot_at} />
                  </span>
                </div>
              </>
            )
          )}
        </section>
      </div>
    </WorkspacePage>
  );
}

function OverviewQueue({
  title,
  state,
  empty,
  hideWhenEmpty = false,
}: {
  title: string;
  state: RunsStateFilter;
  empty: string;
  hideWhenEmpty?: boolean;
}) {
  const query = useInfiniteRuns({ stateFilter: state, limit: 5 });
  const items = query.data?.pages.flatMap((page) => page.items) ?? [];
  const seen = new Set<string>();
  const runs = items.filter((run) => {
    if (seen.has(run.run_id)) return false;
    seen.add(run.run_id);
    return true;
  });
  if (hideWhenEmpty && query.data && !query.error && runs.length === 0)
    return null;
  return (
    <section aria-label={title}>
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-lg font-medium">{title}</h2>
        <Link
          to="/runs"
          search={{ state_filter: state }}
          className="text-xs text-accent hover:underline"
        >
          View All
        </Link>
      </div>
      {query.error && (
        <QueryNotice
          retained={Boolean(query.data)}
          onRetry={() => {
            void query.refetch();
          }}
        >
          {title} could not be refreshed.
        </QueryNotice>
      )}
      {query.isLoading && !query.data ? (
        <Skeleton className="h-28" />
      ) : runs.length > 0 ? (
        <div className="ui-card divide-y divide-border rounded-xl border border-border bg-surface">
          {runs.map((run) => (
            <RunRow
              key={run.run_id}
              run={run}
              decision={state === "awaiting_human_decision"}
            />
          ))}
        </div>
      ) : (
        !query.error && (
          <p className="py-4 text-sm text-foreground-muted">{empty}</p>
        )
      )}
      {query.hasNextPage && (
        <Button
          variant="ghost"
          size="sm"
          className="mt-3"
          disabled={query.isFetchingNextPage}
          onClick={() => {
            void query.fetchNextPage();
          }}
        >
          {query.isFetchingNextPage ? "Loading…" : `Load More ${title}`}
        </Button>
      )}
    </section>
  );
}

function RunRow({ run, decision }: { run: RunSummary; decision: boolean }) {
  return (
    <Link
      to="/runs/$runId"
      params={{ runId: run.run_id }}
      className="flex flex-wrap items-center justify-between gap-4 p-5 transition-colors hover:bg-surface-raised"
    >
      <div className="min-w-0 flex-1">
        <h3 className="break-words text-sm font-medium">
          {run.work_order_title || run.run_id}
        </h3>
        <p className="mt-2 text-xs text-foreground-muted">
          Updated <RelativeTime iso={run.last_activity_at} />
        </p>
      </div>
      <div className="flex flex-col items-end gap-2">
        <Badge variant="status" status={runDisplayBadgeStatus(run)}>
          {runDisplayStateLabel(run)}
        </Badge>
        {decision && (
          <span className="text-xs text-accent">Review Decision →</span>
        )}
      </div>
    </Link>
  );
}

function PlanningQueue() {
  const query = usePlanningSessions(5);
  const sessions = query.data?.items ?? [];
  return (
    <section aria-label="Continue Planning">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-lg font-medium">Continue Planning</h2>
        <Link
          to="/work-orders/planning"
          className="text-xs text-accent hover:underline"
        >
          View All
        </Link>
      </div>
      {query.error && (
        <QueryNotice
          retained={Boolean(query.data)}
          onRetry={() => {
            void query.refetch();
          }}
        >
          Planning sessions could not be refreshed.
        </QueryNotice>
      )}
      {query.isLoading && !query.data ? (
        <Skeleton className="h-28" />
      ) : sessions.length > 0 ? (
        <div className="ui-card divide-y divide-border rounded-xl border border-border bg-surface">
          {sessions.map((session) => (
            <Link
              key={session.planning_session_id}
              to="/work-orders/planning/$sessionId"
              params={{ sessionId: session.planning_session_id }}
              className="block p-5 hover:bg-surface-raised"
            >
              <h3 className="break-words text-sm font-medium">
                {session.title}
              </h3>
              <p className="mt-2 text-xs text-foreground-muted">
                {session.conversation_state === "processing"
                  ? "Working · "
                  : session.lifecycle_state === "archived"
                    ? "Archived · "
                    : ""}
                <RelativeTime iso={session.updated_at} />
              </p>
            </Link>
          ))}
        </div>
      ) : (
        !query.error && (
          <p className="py-4 text-sm text-foreground-muted">
            Start a planning session to shape your next piece of work.
          </p>
        )
      )}
      {query.hasNextPage && (
        <Button
          variant="ghost"
          size="sm"
          className="mt-3"
          disabled={query.isFetchingNextPage}
          onClick={() => {
            void query.fetchNextPage();
          }}
        >
          {query.isFetchingNextPage ? "Loading…" : "Load More Sessions"}
        </Button>
      )}
    </section>
  );
}
function SummaryMetric({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <div>
      <p className="text-sm text-foreground-muted">{label}</p>
      <p className="mt-3 font-display text-4xl tabular-nums">{value}</p>
    </div>
  );
}
function QueryNotice({
  retained,
  onRetry,
  children,
}: {
  retained: boolean;
  onRetry: () => void;
  children: ReactNode;
}) {
  return (
    <div
      role="alert"
      className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-status-blocked/40 p-4 text-sm"
    >
      <p>
        {children}{" "}
        {retained
          ? "Showing the last successful result."
          : "This information is unavailable."}
      </p>
      <Button variant="outline" size="sm" onClick={onRetry}>
        Retry
      </Button>
    </div>
  );
}
