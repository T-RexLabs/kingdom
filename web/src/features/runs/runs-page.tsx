import { useInfiniteRuns } from "@/api/hooks";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { formatTimestamp } from "@/lib/utils";
import { activeRunsFilterConfig, RUN_QUICK_FILTERS } from "@/lib/run-filters";
import { useRetainedData } from "@/lib/use-retained-data";
import { Play, Filter, RefreshCw, Scroll, Shield } from "lucide-react";
import { useCallback } from "react";
import { PageHeader, PageContent } from "@/components/page-layout";
import { describeManualGovernanceState } from "./manual-governance-state";
import { runDisplayBadgeStatus, runDisplayStateLabel } from "./run-display-state";

export function RunsPage() {
  const navigate = useNavigate({ from: "/runs" });
  const activeStateFilter = useSearch({
    from: "/runs",
    select: (search) => search.state_filter,
  });
  const activeFilter = activeStateFilter ?? "all";
  const activeFilterConfig = activeRunsFilterConfig(activeFilter);
  const setActiveFilter = useCallback((nextFilter: typeof activeFilter) => {
    navigate({
      search: (previous) => {
        if (nextFilter === "all") {
          const { state_filter: _ignored, ...rest } = previous;
          return rest;
        }
        return {
          ...previous,
          state_filter: nextFilter,
        };
      },
      replace: true,
    });
  }, [navigate]);

  const runsQuery = useInfiniteRuns({
    stateFilter: activeStateFilter,
    limit: 50,
  });

  const retainedRuns = useRetainedData({
    identity: activeFilter,
    data: runsQuery.data,
    isPending: runsQuery.isPending,
    isFetching: runsQuery.isFetching && !runsQuery.isFetchingNextPage,
  });
  const visibleRuns = retainedRuns.displayData;
  const items = visibleRuns?.pages.flatMap((page) => page.items ?? []) ?? [];
  const blockingError = runsQuery.isError && !visibleRuns ? runsQuery.error : null;
  const loadMoreError = visibleRuns && runsQuery.isFetchNextPageError ? runsQuery.error : null;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <div className="flex w-full items-center justify-between">
        <div>
          <h1 className="font-display text-2xl font-bold text-gold">
            Decree Runs
          </h1>
          <p className="text-sm text-foreground-muted">
            Monitor and manage workflow executions
          </p>
        </div>
        <Link to="/decrees">
          <Button size="sm">
            <Play className="h-3.5 w-3.5" />
            New Decree
          </Button>
        </Link>
        </div>
      </PageHeader>

      <PageContent className="space-y-6">

      {/* Filters */}
      <div className="flex items-center gap-2">
        <Filter className="h-4 w-4 text-foreground-subtle" />
        {RUN_QUICK_FILTERS.map((filter) => (
          <button
            key={filter.value}
            type="button"
            onClick={() => setActiveFilter(filter.value)}
            className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
              activeFilter === filter.value
                ? "bg-gold/15 text-gold"
                : "text-foreground-muted hover:text-foreground"
            }`}
          >
            {filter.label}
          </button>
        ))}
      </div>

      {/* Runs list */}
      <Card aria-busy={retainedRuns.isRetainingPreviousData}>
        <CardHeader>
          <CardTitle className="flex flex-wrap items-center justify-between gap-2">
            <span>{items.length} run{items.length !== 1 ? "s" : ""}</span>
            {retainedRuns.isRetainingPreviousData ? (
              <span className="inline-flex items-center gap-2 text-xs font-normal text-foreground-subtle">
                <Spinner className="h-3.5 w-3.5 border-[1.5px]" />
                Updating runs...
              </span>
            ) : null}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {retainedRuns.isInitialLoading ? (
            <LoadingSkeleton />
          ) : blockingError ? (
            <div className="flex flex-col items-center gap-3 py-12 text-center">
              <p className="text-sm font-medium text-foreground">Unable to load runs</p>
              <p className="max-w-md text-sm text-foreground-subtle">
                {blockingError instanceof Error ? blockingError.message : "The runs list could not be loaded right now."}
              </p>
              <Button type="button" variant="outline" onClick={() => { void runsQuery.refetch(); }}>
                Retry
              </Button>
            </div>
          ) : items.length === 0 ? (
            <div className="flex flex-col items-center py-12 text-center">
              <Scroll className="h-8 w-8 text-foreground-subtle/40 mb-3" />
              <p className="text-sm text-foreground-subtle">
                {activeFilter === "all"
                  ? "No runs yet. Create a decree to begin."
                  : `No ${activeFilterConfig.label.toLowerCase()} runs found.`}
              </p>
            </div>
          ) : (
            <div className="divide-y divide-border">
              {items.map((run) => {
                const governanceState = describeManualGovernanceState(run);
                const isActionRequired = governanceState.isAwaitingPlanApproval || governanceState.isAwaitingFinalApproval;
                return (
                  <Link
                    key={run.run_id}
                    to="/runs/$runId"
                    params={{ runId: run.run_id }}
                    className="flex items-center justify-between px-2 py-3 transition-colors hover:bg-surface-raised"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="truncate text-sm font-medium text-foreground">
                          {run.decree_title || run.run_id}
                        </span>
                        <Badge variant="status" status={runDisplayBadgeStatus(run)}>
                          {runDisplayStateLabel(run)}
                        </Badge>
					  </div>
					  <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-foreground-subtle">
						<span>Created: {formatTimestamp(run.created_at)}</span>
						<span>Last Activity: {formatTimestamp(run.last_activity_at)}</span>
					</div>
                    </div>
                    <div className="ml-3 flex items-center gap-2">
                      {isActionRequired && (
                        <span className="flex items-center gap-1 rounded-md bg-gold/10 px-1.5 py-0.5 text-[10px] font-medium text-gold">
                          <Shield className="h-2.5 w-2.5" />
                          Action Required
                        </span>
                      )}
                    </div>
                  </Link>
                );
              })}
            </div>
          )}

          {items.length > 0 && runsQuery.hasNextPage ? (
            <div className="mt-4 space-y-3 border-t border-border pt-4">
              {loadMoreError ? (
                <div className="flex flex-col items-center gap-3 rounded-md border border-status-failed/30 bg-status-failed/5 px-4 py-3 text-center sm:flex-row sm:justify-between sm:text-left">
                  <p className="text-sm text-foreground-subtle">
                    {loadMoreError instanceof Error ? loadMoreError.message : "Unable to load more runs right now."}
                  </p>
                  <Button type="button" variant="outline" onClick={() => { void runsQuery.fetchNextPage(); }}>
                    <RefreshCw className="h-3.5 w-3.5" />
                    Retry
                  </Button>
                </div>
              ) : null}
              <div className="flex justify-center">
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => {
                    void runsQuery.fetchNextPage();
                  }}
                  disabled={runsQuery.isFetchingNextPage}
                >
                  {runsQuery.isFetchingNextPage ? "Loading More Runs..." : "Load More Runs"}
                </Button>
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
      </PageContent>
    </div>
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-3 py-4">
      {Array.from({ length: 5 }).map((_, i) => (
        <div key={i} className="flex items-center gap-4">
          <Skeleton className="h-4 w-48" />
          <div className="flex-1" />
          <Skeleton className="h-5 w-20" />
          <Skeleton className="h-4 w-16" />
        </div>
      ))}
    </div>
  );
}
