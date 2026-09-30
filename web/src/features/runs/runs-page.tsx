import { useInfiniteRuns } from "@/api/hooks";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RelativeTime } from "@/components/ui/relative-time";
import { WorkspacePage } from "@/components/workspace-page";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { formatExactTime, timestampDescription } from "@/lib/date-time";
import { RUN_QUICK_FILTERS } from "@/lib/run-filters";
import { useRetainedData } from "@/lib/use-retained-data";
import { ArrowUpRight, Search, Plus, Shield } from "lucide-react";
import { Virtuoso } from "react-virtuoso";
import { ThemeIcon, useThemeCopy } from "@/theme";
import { describeManualGovernanceState } from "./manual-governance-state";
import {
  runDisplayBadgeStatus,
  runDisplayStateLabel,
} from "./run-display-state";

export function RunsPage() {
  const workName = useThemeCopy("workOrder.singular");
  const workNames = useThemeCopy("workOrder.plural");
  const navigate = useNavigate({ from: "/runs" });
  const search = useSearch({ from: "/runs" });
  const activeFilter = search.state_filter ?? "all";
  const runs = useInfiniteRuns({
    stateFilter: search.state_filter,
    search: search.q,
    limit: 50,
  });
  const retained = useRetainedData({
    identity: JSON.stringify([activeFilter, search.q]),
    data: runs.data,
    isPending: runs.isPending,
    isFetching: runs.isFetching && !runs.isFetchingNextPage,
  });
  const items =
    retained.displayData?.pages.flatMap((page) => page.items ?? []) ?? [];
  const error = runs.isError
    ? runs.error instanceof Error
      ? runs.error.message
      : "Runs could not be loaded."
    : null;

  return (
    <WorkspacePage
      section="Runs"
      title="Runs"
      description={`Follow ${workNames} from their first observation to their final result.`}
      actions={
        <Link to="/work-orders/new">
          <Button size="sm">
            <Plus size={15} />
            New {workName}
          </Button>
        </Link>
      }
    >
      <div className="space-y-5">
        <form
          key={search.q ?? ""}
          className="flex flex-wrap gap-3"
          role="search"
          onSubmit={(event) => {
            event.preventDefault();
            const query = String(
              new FormData(event.currentTarget).get("q") ?? "",
            );
            void navigate({
              search: { ...search, q: query.trim() || undefined },
              replace: true,
            });
          }}
        >
          <label className="relative min-w-56 flex-1">
            <span className="sr-only">Search Runs</span>
            <Search
              size={16}
              className="pointer-events-none absolute left-3 top-3.5 text-foreground-muted"
            />
            <input data-ui-field=""
              name="q"
              defaultValue={search.q ?? ""}
              placeholder="Search run titles or content…"
              className="min-h-11 w-full rounded-xl border border-border bg-surface/60 pl-10 pr-4 text-sm outline-none focus:border-accent"
            />
          </label>
          <Button type="submit" variant="outline">
            Search
          </Button>
          {search.q && (
            <Button
              type="button"
              variant="ghost"
              onClick={() => {
                void navigate({
                  search: { ...search, q: undefined },
                  replace: true,
                });
              }}
            >
              Clear
            </Button>
          )}
        </form>
        <div
          className="flex flex-wrap items-center gap-1.5"
          aria-label="Run status filters"
        >
          {RUN_QUICK_FILTERS.map((filter) => (
            <button
              key={filter.value}
              type="button"
              aria-pressed={activeFilter === filter.value}
              onClick={() =>
                void navigate({
                  search: {
                    ...search,
                    state_filter:
                      filter.value === "all" ? undefined : filter.value,
                  },
                  replace: true,
                })
              }
              className={`min-h-9 rounded-lg px-3 text-xs font-medium transition-colors ${activeFilter === filter.value ? "bg-accent/10 text-accent" : "text-foreground-muted hover:bg-surface-raised hover:text-foreground"}`}
            >
              {filter.label}
            </button>
          ))}
        </div>
        <section
          aria-label="Runs"
          aria-busy={retained.isRetainingPreviousData}
          className="ui-inset-panel overflow-hidden rounded-2xl border border-border bg-surface/40"
        >
          <div className="flex items-center justify-between border-b border-border px-5 py-3 text-xs text-foreground-muted">
            <span>{items.length} Loaded</span>
            <span>
              {retained.isRetainingPreviousData
                ? "Updating runs…"
                : search.q
                  ? `Results for “${search.q}”`
                  : "Most Recent First"}
            </span>
          </div>
          {error && (
            <div
              role="alert"
              className="flex items-center justify-between gap-3 border-b border-border p-4 text-sm text-status-failed"
            >
              <span>{error}</span>
              <Button
                size="sm"
                variant="outline"
                onClick={() =>
                  void (runs.isFetchNextPageError
                    ? runs.fetchNextPage()
                    : runs.refetch())
                }
              >
                Retry
              </Button>
            </div>
          )}
          {retained.isInitialLoading ? (
            <p className="p-10 text-center text-sm text-foreground-muted">
              Loading runs…
            </p>
          ) : items.length === 0 && !error ? (
            <div className="flex flex-col items-center gap-3 px-5 py-14 text-center">
              <ThemeIcon slot="nav.runs" size={28} className="text-accent/60" />
              <p className="text-sm text-foreground-muted">
                {search.q || activeFilter !== "all"
                  ? "No runs match these filters."
                  : "Create a work order to start your first run."}
              </p>
            </div>
          ) : items.length > 0 ? (
            <Virtuoso
              style={{
                height: Math.min(640, Math.max(112, items.length * 104)),
              }}
              data={items}
              increaseViewportBy={200}
              itemContent={(_index, run) => {
                const governance = describeManualGovernanceState(run);
                const needsDecision =
                  governance.isAwaitingPlanApproval ||
                  governance.isAwaitingFinalApproval;
                return (
                  <Link
                    to="/runs/$runId"
                    params={{ runId: run.run_id }}
                    className="group flex min-h-24 items-center gap-4 border-b border-border/70 px-5 py-4 transition-colors hover:bg-surface-raised/60"
                  >
                    <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-border bg-background text-accent">
                      <ThemeIcon slot="nav.runs" size={19} />
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="truncate text-sm font-medium">
                          {run.work_order_title || run.run_id}
                        </span>
                        <Badge
                          variant="status"
                          status={runDisplayBadgeStatus(run)}
                        >
                          {runDisplayStateLabel(run)}
                        </Badge>
                      </div>
                      <div className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-xs text-foreground-muted">
                        <span>
                          Started{" "}
                          <time
                            title={
                              run.started_at
                                ? timestampDescription(run.started_at)
                                : undefined
                            }
                            dateTime={run.started_at}
                          >
                            {run.started_at
                              ? formatExactTime(run.started_at)
                              : run.start_state === "unrecorded"
                                ? "Observation Not Recorded"
                                : "Not Started"}
                          </time>
                        </span>
                        <span>
                          Updated <RelativeTime iso={run.last_activity_at} />
                        </span>
                      </div>
                      {needsDecision && (
                        <span className="mt-2 inline-flex items-center gap-1.5 text-xs text-accent">
                          <Shield size={12} />
                          {governance.isAwaitingPlanApproval
                            ? "Plan Decision Required"
                            : "Final Decision Required"}
                        </span>
                      )}
                    </div>
                    <ArrowUpRight
                      size={16}
                      className="shrink-0 text-foreground-subtle transition-transform group-hover:-translate-y-0.5 group-hover:translate-x-0.5"
                    />
                  </Link>
                );
              }}
            />
          ) : null}
          {runs.hasNextPage && (
            <div className="flex justify-center border-t border-border p-4">
              <Button
                variant="outline"
                disabled={
                  runs.isFetchingNextPage || retained.isRetainingPreviousData
                }
                onClick={() => void runs.fetchNextPage()}
              >
                {runs.isFetchingNextPage ? "Loading runs…" : "Load More Runs"}
              </Button>
            </div>
          )}
        </section>
      </div>
    </WorkspacePage>
  );
}
