import {
  startTransition,
  useState,
  useMemo,
  useDeferredValue,
  useEffect,
} from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Badge } from "@/components/ui/badge";
import { InfoTooltip } from "@/components/ui/info-tooltip";
import { TimeRangePicker } from "@/components/ui/time-range-picker";
import { selectTriggerClass } from "@/components/ui/field-styles";
import { SelectTriggerContent } from "@/components/ui/select-trigger";
import { HorizontalBarChart } from "@/components/ui/horizontal-bar-chart";
import {
  PopoverListbox,
  type PopoverListboxItem,
} from "@/components/ui/popover-listbox";
import {
  StackedBarChart,
  type StackedBarDatum,
} from "@/components/ui/stacked-bar-chart";
import { WorkspacePage } from "@/components/workspace-page";
import { roleIdentity, useTheme } from "@/theme";
import {
  useAnalyticsExecutionDetailSnapshot,
  useInfiniteRuns,
  useRuntimeMetrics,
  useAnalyticsSnapshot,
  type AnalyticsExecutionDetail,
  type AnalyticsOverview,
  type AnalyticsRunAnalytics,
  type TokenUsageByRole as TokensByRole,
  type TokenUsageByRoleModel as TokensByRoleModel,
} from "@/api/hooks";
import { ApiError } from "@/api/client";
import {
  analyticsRangeParams,
  analyticsTimeRangeFromSearch,
  analyticsTimeRangeIdentity,
  analyticsTimeWindowDescription,
  type AnalyticsTimeRange,
} from "@/lib/analytics-time-range";
import { useRetainedData } from "@/lib/use-retained-data";
import { cn, formatTimestamp } from "@/lib/utils";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { describeManualGovernanceState } from "@/features/runs/manual-governance-state";
import {
  runDisplayBadgeStatus,
  runDisplayStateLabel,
} from "@/features/runs/run-display-state";
import { Zap, RefreshCw, Shield, ChevronDown } from "lucide-react";

/* ── Formatting helpers ── */

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function formatCompactNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function formatBytes(bytes: number): string {
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  if (bytes >= 1_024) return `${(bytes / 1_024).toFixed(1)} KB`;
  return `${bytes} B`;
}

function pluralize(
  count: number,
  singular: string,
  plural = `${singular}s`,
): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

function buildRoleUsageSummary(role: TokensByRole): string {
  const parts: string[] = [];
  const modelCount = role.by_model?.length ?? 0;
  if (modelCount > 0) {
    parts.push(pluralize(modelCount, "model"));
  } else if (role.mixed_model) {
    parts.push("Mixed models");
  } else if (role.model?.trim()) {
    parts.push("1 model");
  }

  if (role.record_count > 0) {
    parts.push(pluralize(role.record_count, "record"));
  }

  return parts.join(" • ");
}

function buildModelUsageSummary(model: TokensByRoleModel): string {
  const parts: string[] = [];
  if (model.provider_name?.trim()) {
    parts.push(model.provider_name.trim());
  }
  if (model.model_name?.trim()) {
    parts.push(model.model_name.trim());
  } else if (model.model?.trim()) {
    parts.push(model.model.trim());
  }
  if (model.thinking_effort?.trim()) {
    parts.push(`Thinking: ${model.thinking_effort.trim()}`);
  }
  return parts.join(" • ");
}

function buildRoleBreakdownButtonLabel(
  role: TokensByRole,
  name: string,
): string {
  const summary = buildRoleUsageSummary(role).replace(/ • /g, " ");
  return `${name} ${summary}`.trim();
}

/* ── Page ── */

export function AnalyticsPage() {
  const navigate = useNavigate({ from: "/analytics" });
  const view = useSearch({
    from: "/analytics",
    select: (search) => search.view ?? "runs",
  });
  const search = useSearch({ from: "/analytics" });
  const timeRange = useMemo(() => analyticsTimeRangeFromSearch(search), [search]);
  const rangeIdentity = analyticsTimeRangeIdentity(timeRange);
  const setTimeRange = (range: AnalyticsTimeRange) => {
    startTransition(() => {
      navigate({
        search: (previous) => {
          const next = { ...previous };
          delete next.window_minutes;
          delete next.start_at;
          delete next.end_at;
          return { ...next, ...analyticsRangeParams(range) };
        },
        replace: true,
      });
    });
  };

  const snapshotQuery = useAnalyticsSnapshot(timeRange);
  const snapshot = snapshotQuery.data;
  const snapshotBlockingError =
    snapshotQuery.error && !snapshot ? snapshotQuery.error : null;
  const snapshotWarningMessage =
    snapshot && snapshotQuery.isRefetchError
      ? describeRefreshFailure(
          snapshotQuery.error,
          "A refresh failed. Showing the last successful analytics snapshot.",
        )
      : null;
  return (
    <WorkspacePage
      section="Analytics"
      title="Analytics"
      description="Understand outcomes, resource use, and where work gets stuck."
      actions={
        <TimeRangePicker
          value={timeRange}
          onChange={setTimeRange}
        />
      }
    >
      <div
        className="mb-8 flex gap-6 border-b border-border"
        role="group"
        aria-label="Analytics view"
      >
        {(["runs", "usage", "reliability"] as const).map((key) => (
          <button
            key={key}
            type="button"
            aria-pressed={view === key}
            className={`border-b-2 px-1 pb-3 text-sm transition-colors ${view === key ? "border-accent text-accent" : "border-transparent text-foreground-muted hover:text-foreground"}`}
            onClick={() =>
              navigate({
                search: (previous) => ({ ...previous, view: key }),
                replace: true,
              })
            }
          >
            {key[0].toUpperCase() + key.slice(1)}
          </button>
        ))}
      </div>
      {snapshotQuery.isPending && (
        <p className="mb-4 text-sm text-foreground-muted" role="status">
          Loading analytics for the selected time window…
        </p>
      )}
      <div hidden={view !== "runs"} className="space-y-8">
        {snapshotWarningMessage && (
          <InlineRefreshWarning
            message={snapshotWarningMessage}
            onRetry={() => {
              void snapshotQuery.refetch();
            }}
          />
        )}
        {snapshotBlockingError ? (
          <SnapshotErrorState
            title="Analytics snapshot unavailable"
            description="The analytics snapshot could not be loaded. Retry to restore outcomes and duration metrics."
            onRetry={() => {
              void snapshotQuery.refetch();
            }}
          />
        ) : (
          <RunAnalyticsSection
            key={rangeIdentity}
            overview={snapshot?.overview}
            runAnalytics={snapshot?.run_analytics}
            allTimeRunCount={snapshot?.all_time_run_count ?? 0}
            windowLabel={analyticsTimeWindowDescription(timeRange)}
            isLoading={snapshotQuery.isPending}
          />
        )}
      </div>
      <div hidden={view !== "usage"} className="space-y-6">
        <Section title="Execution Detail">
          <ExecutionDetailSection timeRange={timeRange} />
        </Section>
      </div>
      <div hidden={view !== "reliability"} className="space-y-8">
        <Section title="Runtime Health">
          <p className="mb-5 text-sm text-foreground-muted">
            Live runtime state, independent of the selected time window.
          </p>
          <RuntimeHealthSection />
        </Section>
        <Section title="Review and Validation">
          <p className="mb-5 text-sm text-foreground-muted">
            Share of runs created in this window with at least one failed review
            or validation attempt. These are failure indicators, not
            verdict-based quality scores.
          </p>
          {snapshotBlockingError ? (
            <SnapshotErrorState
              title="Rework metrics unavailable"
              description="The analytics snapshot could not be loaded."
              onRetry={() => {
                void snapshotQuery.refetch();
              }}
            />
          ) : (
            <>
              {snapshotWarningMessage && (
                <InlineRefreshWarning
                  message={snapshotWarningMessage}
                  onRetry={() => {
                    void snapshotQuery.refetch();
                  }}
                />
              )}
              <div className="grid gap-6 sm:grid-cols-2">
                <MetricCard
                  title="Runs with Review Failures"
                  value={
                    snapshot && snapshot.overview.total_runs > 0
                      ? `${snapshot.overview.review_rework_rate.toFixed(1)}%`
                      : "—"
                  }
                />
                <MetricCard
                  title="Runs with Validation Failures"
                  value={
                    snapshot && snapshot.overview.total_runs > 0
                      ? `${snapshot.overview.validation_rework_rate.toFixed(1)}%`
                      : "—"
                  }
                />
              </div>
            </>
          )}
        </Section>
      </div>
    </WorkspacePage>
  );
}

function RuntimeHealthSection() {
  const runtimeQuery = useRuntimeMetrics();
  const runtime = runtimeQuery.data;
  const blockingError =
    runtimeQuery.error && !runtime ? runtimeQuery.error : null;
  const warningMessage =
    runtime && runtimeQuery.isRefetchError
      ? describeRefreshFailure(
          runtimeQuery.error,
          "A refresh failed. Showing the last successful runtime health snapshot.",
        )
      : null;
  const showRuntimeSkeletons = runtimeQuery.isLoading && !runtime;

  if (blockingError) {
    return (
      <SnapshotErrorState
        title="Runtime health unavailable"
        description="The runtime health metrics could not be loaded right now. Retry to restore live runtime visibility."
        onRetry={() => {
          void runtimeQuery.refetch();
        }}
      />
    );
  }

  return (
    <div className="space-y-4">
      {warningMessage ? (
        <InlineRefreshWarning
          message={warningMessage}
          onRetry={() => {
            void runtimeQuery.refetch();
          }}
        />
      ) : null}
      {showRuntimeSkeletons ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
        </div>
      ) : runtime ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <MetricCard
            title="Status"
            value={runtime.status === "ok" ? "Online" : "Degraded"}
          />
          <MetricCard title="Active Runs" value={runtime.running_runs} />
          <MetricCard title="Active Nodes" value={runtime.running_nodes} />
          <MetricCard title="Stale Nodes" value={runtime.stale_running_nodes} />
        </div>
      ) : null}
      {runtime?.planner && (
        <details className="border-t border-border pt-6">
          <summary className="cursor-pointer text-sm font-medium">
            Planning Reliability
          </summary>
          <p className="mt-3 text-xs text-foreground-muted">
            Current operation counts and cumulative recovery counters reported
            by this runtime. These are independent of the selected date window.
          </p>
          <dl className="mt-5 grid gap-x-8 gap-y-4 sm:grid-cols-2">
            {(
              [
                ["Queued Operations", runtime.planner.queued_operations],
                ["Running Operations", runtime.planner.running_operations],
                ["Failed Operations", runtime.planner.failed_operations],
                ["Active Sessions", runtime.planner.active_sessions],
                ["Processing Sessions", runtime.planner.processing_sessions],
                [
                  "Stream Replays",
                  runtime.planner.planning_stream_replay_count,
                ],
                [
                  "Stream Resumes",
                  runtime.planner.planning_stream_resume_count,
                ],
                [
                  "Operation Recoveries",
                  runtime.planner.planning_operation_recovery_count,
                ],
                [
                  "Operation Failures",
                  runtime.planner.planning_operation_failure_count,
                ],
              ] as const
            ).map(([label, value]) => (
              <div key={label} className="flex justify-between gap-3 text-sm">
                <dt className="text-foreground-muted">{label}</dt>
                <dd className="tabular-nums">{value}</dd>
              </div>
            ))}
          </dl>
        </details>
      )}
    </div>
  );
}

/* ── Section wrapper ── */

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <h2 className="mb-4 font-display text-lg font-semibold text-foreground">
        {title}
      </h2>
      {children}
    </div>
  );
}

/* ── Section 2: Run Analytics ── */

function RunAnalyticsSection({
  overview,
  runAnalytics,
  allTimeRunCount,
  windowLabel,
  isLoading,
}: {
  overview?: AnalyticsOverview;
  runAnalytics?: AnalyticsRunAnalytics;
  allTimeRunCount: number;
  windowLabel: string;
  isLoading: boolean;
}) {
  const { theme } = useTheme();
  const terminalRuns = overview
    ? overview.succeeded_runs + overview.failed_runs + overview.cancelled_runs
    : 0;
  const outcomeBars = useMemo<StackedBarDatum[]>(
    () =>
      runAnalytics?.outcome_series.map((b) => {
        // Shorten bucket labels for display
        const label =
          runAnalytics.bucket_type === "hour"
            ? b.bucket.slice(11, 16) // "HH:MM"
            : b.bucket.slice(5, 10); // "MM-DD"
        return {
          label,
          values: [
            {
              key: "succeeded",
              value: b.succeeded,
              color: "var(--color-status-succeeded)",
            },
            {
              key: "failed",
              value: b.failed,
              color: "var(--color-status-failed)",
            },
            {
              key: "cancelled",
              value: b.cancelled,
              color: "var(--color-status-cancelled)",
            },
          ],
        };
      }) ?? [],
    [runAnalytics],
  );

  const durationByRoleBars = useMemo(
    () =>
      runAnalytics?.duration_by_role.map((r) => ({
        label: roleIdentity(theme, r.role).name,
        value: r.avg_duration_seconds,
        color: "var(--color-accent)",
      })) ?? [],
    [runAnalytics, theme],
  );

  const deferredOutcomeBars = useDeferredValue(outcomeBars);
  const deferredDurationByRoleBars = useDeferredValue(durationByRoleBars);

  const recentRunItems = runAnalytics?.recent_runs ?? [];

  return (
    <div className="space-y-4">
      {/* Run overview stat cards */}
      <div className="grid grid-cols-3 gap-4 sm:gap-6">
        <MetricCard
          title="Total Runs"
          value={overview ? overview.total_runs : "--"}
        />
        <MetricCard
          title="Average Duration"
          titleTooltip="Mean time from creation to terminal update for succeeded, failed and cancelled runs created in this window."
          value={
            overview && terminalRuns > 0
              ? formatDuration(overview.avg_duration_seconds)
              : "—"
          }
        />
        <MetricCard
          title="Success Rate"
          titleTooltip="Succeeded runs divided by succeeded, failed and cancelled runs created in this window. Pending and running work is excluded."
          value={
            overview && terminalRuns > 0
              ? `${overview.success_rate.toFixed(1)}%`
              : "—"
          }
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="min-h-[320px] min-w-0">
          <CardHeader>
            <CardTitle>Run Outcomes Over Time</CardTitle>
            <p className="mt-2 text-xs text-foreground-muted">
              Terminal runs grouped by when they were created.
            </p>
          </CardHeader>
          <CardContent className="h-[320px]">
            <StackedBarChart
              ariaLabel="Run outcomes over time"
              data={deferredOutcomeBars}
              integerTicks
            />
          </CardContent>
        </Card>

        <Card className="min-h-[320px] min-w-0">
          <CardHeader>
            <CardTitle>Execution Duration by Role</CardTitle>
            <p className="mt-2 text-xs text-foreground-muted">
              Completed execution attempts started in this window; average
              duration.
            </p>
          </CardHeader>
          <CardContent className="h-[320px] overflow-y-auto [scrollbar-gutter:stable]">
            <HorizontalBarChart
              ariaLabel="Average execution duration by role"
              data={deferredDurationByRoleBars}
              emptyHeight={320}
              formatValue={formatDuration}
            />
          </CardContent>
        </Card>
      </div>

      {/* Recent runs table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Recent Runs</span>
            {allTimeRunCount > 0 && (
              <Link
                to="/runs"
                className="text-xs font-normal text-accent hover:text-accent-bright"
              >
                View All
              </Link>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading && recentRunItems.length === 0 ? (
            <div className="space-y-3 py-4">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex items-center gap-4">
                  <Skeleton className="h-4 w-4" />
                  <Skeleton className="h-4 w-48" />
                  <div className="flex-1" />
                  <Skeleton className="h-5 w-20" />
                  <Skeleton className="h-4 w-16" />
                </div>
              ))}
            </div>
          ) : recentRunItems.length > 0 ? (
            <div className="max-h-[320px] space-y-1 overflow-auto pr-1">
              {recentRunItems.map((run) => {
                const governanceState = describeManualGovernanceState(run);
                const isActionRequired =
                  governanceState.isAwaitingPlanApproval ||
                  governanceState.isAwaitingFinalApproval;
                return (
                  <Link
                    key={run.run_id}
                    to="/runs/$runId"
                    params={{ runId: run.run_id }}
                    className="flex items-center justify-between rounded-md px-3 py-2 transition-colors hover:bg-surface-raised"
                  >
                    <div className="flex min-w-0 flex-1 items-start gap-3">
                      <Zap className="mt-0.5 h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
                      <div className="min-w-0 flex-1">
                        <p className="text-wrap-anywhere text-sm font-medium">
                          {run.work_order_title || run.run_id}
                        </p>
                        <p className="mt-1 text-xs text-foreground-subtle">
                          {run.workflow_id} · Created{" "}
                          {formatTimestamp(run.created_at)} · Last Activity{" "}
                          {formatTimestamp(run.last_activity_at)}
                        </p>
                      </div>
                    </div>
                    <div className="ml-3 flex shrink-0 flex-col items-end gap-2 sm:flex-row sm:items-center">
                      {isActionRequired ? (
                        <span className="inline-flex items-center gap-1 rounded-md bg-accent/10 px-1.5 py-0.5 text-[10px] font-medium text-accent">
                          <Shield className="h-2.5 w-2.5" />
                          Action Required
                        </span>
                      ) : null}
                      <Badge
                        variant="status"
                        status={runDisplayBadgeStatus(run)}
                      >
                        {runDisplayStateLabel(run)}
                      </Badge>
                    </div>
                  </Link>
                );
              })}
            </div>
          ) : allTimeRunCount === 0 ? (
            <p className="text-sm text-foreground-subtle">No runs yet.</p>
          ) : (
            <p className="text-sm text-foreground-subtle">
              {`No completed runs in ${windowLabel}.`}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

/* ── Section 4: Execution Detail ── */

function ExecutionDetailSection({
  timeRange,
}: {
  timeRange: AnalyticsTimeRange;
}) {
  const { theme } = useTheme();
  const selectedRunId = useSearch({
    from: "/analytics",
    select: (search) => search.run_id ?? null,
  });
  const navigate = useNavigate({ from: "/analytics" });
  const setSelectedRunId = (runID: string | null) => {
    startTransition(() => {
      navigate({
        search: (previous) => {
          if (!runID) {
            const next = { ...previous };
            Reflect.deleteProperty(next, "run_id");
            return next;
          }
          return {
            ...previous,
            run_id: runID,
          };
        },
        replace: true,
      });
    });
  };

  const executionDetailQuery = useAnalyticsExecutionDetailSnapshot(
    timeRange,
    selectedRunId,
  );
  const retainedExecutionDetail = useRetainedData({
    identity: selectedRunId
      ? `run:${selectedRunId}`
      : `window:${analyticsTimeRangeIdentity(timeRange)}`,
    retentionScope: analyticsTimeRangeIdentity(timeRange),
    data: executionDetailQuery.data?.execution_detail,
    isPending: executionDetailQuery.isPending,
    isFetching: executionDetailQuery.isFetching,
  });
  const executionDetail = retainedExecutionDetail.displayData;
  const executionDetailError = executionDetailQuery.error;
  const selectedRunNotFound =
    executionDetailError instanceof ApiError &&
    executionDetailError.status === 404;
  const blockingError =
    selectedRunNotFound || executionDetailQuery.isLoadingError
      ? executionDetailError
      : null;
  const warningMessage =
    executionDetail &&
    executionDetailQuery.isRefetchError &&
    !selectedRunNotFound
      ? describeRefreshFailure(
          executionDetailError,
          "A refresh failed. Showing the last successful execution detail.",
        )
      : null;
  const [showAdvancedTokenDetails, setShowAdvancedTokenDetails] =
    useState(false);
  const showSelectedRunSkeletons =
    retainedExecutionDetail.isInitialLoading &&
    !executionDetail &&
    !blockingError;

  const scopedTokenStats = useMemo(
    () => ({
      total: executionDetail?.tokens.derived_total_tokens ?? 0,
      providerTotal: executionDetail?.tokens.provider_total_tokens ?? 0,
      input: executionDetail?.tokens.input ?? 0,
      output: executionDetail?.tokens.output ?? 0,
      reasoning: executionDetail?.tokens.reasoning ?? 0,
      cacheRead: executionDetail?.tokens.cache_read ?? 0,
      cacheWrite: executionDetail?.tokens.cache_write ?? 0,
      byRole: executionDetail?.tokens.by_role ?? [],
    }),
    [executionDetail],
  );

  const byRoleBars = useMemo(
    () =>
      scopedTokenStats.byRole.map((r) => ({
        label: roleIdentity(theme, r.required_role).name,
        value: r.derived_total_tokens,
        color: "var(--color-accent)",
      })),
    [scopedTokenStats.byRole, theme],
  );
  const deferredByRoleBars = useDeferredValue(byRoleBars);

  const hasUsageRecords = scopedTokenStats.byRole.some(
    (entry) => entry.record_count > 0,
  );
  const formatReportedTokens = (value: number) =>
    hasUsageRecords ? formatCompactNumber(value) : "—";
  const tokenTotal = scopedTokenStats.total;
  const tokenProviderTotal = scopedTokenStats.providerTotal;
  const tokenInput = scopedTokenStats.input;
  const tokenOutput = scopedTokenStats.output;
  const tokenReasoning = scopedTokenStats.reasoning;
  const tokenCacheRead = scopedTokenStats.cacheRead;
  const tokenCacheWrite = scopedTokenStats.cacheWrite;

  const artifactCount = executionDetail?.artifacts.total_count ?? 0;
  const artifactSize = executionDetail?.artifacts.total_size_bytes ?? 0;
  const nodesExecuted = executionDetail?.nodes.total_executed ?? 0;

  return (
    <div className="space-y-4">
      <p className="text-sm text-foreground-muted">
        {selectedRunId
          ? "All execution usage for the selected run, regardless of the time window."
          : "Execution usage recorded in the selected time window."}
      </p>
      <div className="flex flex-wrap items-center justify-start gap-3">
        <RunScopePicker
          selectedRunId={selectedRunId}
          selectedRun={
            selectedRunNotFound ? undefined : executionDetail?.selected_run
          }
          onSelect={setSelectedRunId}
        />
        {retainedExecutionDetail.isRetainingPreviousData ? (
          <span className="inline-flex items-center gap-2 text-xs text-foreground-subtle">
            <Spinner className="h-3.5 w-3.5 border-[1.5px]" />
            Updating execution detail...
          </span>
        ) : null}
      </div>

      {blockingError ? (
        <ExecutionDetailErrorState
          selectedRunId={selectedRunId}
          selectedRunNotFound={selectedRunNotFound}
          onClearSelection={() => setSelectedRunId(null)}
          onRetry={() => {
            void executionDetailQuery.refetch();
          }}
        />
      ) : (
        <>
          {warningMessage && (
            <div className="flex flex-col gap-3 rounded-md border border-status-blocked/30 bg-status-blocked/10 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-start gap-2 text-sm text-foreground">
                <RefreshCw className="mt-0.5 h-4 w-4 shrink-0 text-status-blocked" />
                <p>{warningMessage}</p>
              </div>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  void executionDetailQuery.refetch();
                }}
              >
                Retry
              </Button>
            </div>
          )}
          {!showSelectedRunSkeletons && !hasUsageRecords && (
            <p className="text-sm text-foreground-muted">
              No token usage has been reported for this scope.
            </p>
          )}
          <div className="grid gap-4 lg:grid-cols-2">
            <Card className="min-h-[312px]">
              <CardHeader>
                <CardTitle>Token Usage</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {showSelectedRunSkeletons ? (
                  <div className="space-y-4">
                    <Skeleton className="h-10 w-full" />
                    <Skeleton className="h-10 w-full" />
                  </div>
                ) : (
                  <>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono text-accent">
                          {formatReportedTokens(tokenTotal)}
                        </p>
                        <div className="flex items-center gap-1">
                          <p className="text-xs text-foreground-muted">
                            Total Tokens
                          </p>
                          <InfoTooltip
                            label="Total Tokens formula"
                            message="Total tokens calculated as Input + Output + Reasoning."
                          />
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono text-status-running">
                          {formatReportedTokens(tokenInput)}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Input Tokens
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono text-status-cancelled">
                          {formatReportedTokens(tokenReasoning)}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Reasoning Tokens
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono text-status-succeeded">
                          {formatReportedTokens(tokenOutput)}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Output Tokens
                        </p>
                      </div>
                    </div>
                    <div className="border-t border-border pt-3">
                      <button
                        type="button"
                        className="flex items-center gap-2 text-xs font-medium text-foreground-muted transition-colors hover:text-foreground"
                        onClick={() =>
                          setShowAdvancedTokenDetails((previous) => !previous)
                        }
                      >
                        <ChevronDown
                          className={`h-3.5 w-3.5 transition-transform ${showAdvancedTokenDetails ? "rotate-180" : ""}`}
                        />
                        <span>Advanced Token Details</span>
                      </button>
                      {showAdvancedTokenDetails && (
                        <div className="mt-3 space-y-3 rounded-md border border-border bg-surface-raised p-3">
                          <div className="flex items-center justify-between gap-3">
                            <div className="flex items-center gap-1.5">
                              <span className="text-xs text-foreground-muted">
                                Provider Total Tokens
                              </span>
                              <InfoTooltip
                                label="Provider Total Tokens definition"
                                message="Provider Total Tokens are model/provider-reported totals that may include cached tokens or provider-specific accounting, so they can differ from Input + Output + Reasoning."
                              />
                            </div>
                            <span className="font-mono text-xs text-foreground">
                              {formatReportedTokens(tokenProviderTotal)}
                            </span>
                          </div>
                          <div className="flex items-center justify-between gap-3">
                            <span className="text-xs text-foreground-muted">
                              Cache Read
                            </span>
                            <span className="font-mono text-xs text-foreground">
                              {formatReportedTokens(tokenCacheRead)}
                            </span>
                          </div>
                          <div className="flex items-center justify-between gap-3">
                            <span className="text-xs text-foreground-muted">
                              Cache Write
                            </span>
                            <span className="font-mono text-xs text-foreground">
                              {formatReportedTokens(tokenCacheWrite)}
                            </span>
                          </div>
                        </div>
                      )}
                    </div>
                  </>
                )}
              </CardContent>
            </Card>

            <Card className="min-h-[312px]">
              <CardHeader>
                <CardTitle>Artifacts & Nodes</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {showSelectedRunSkeletons ? (
                  <div className="space-y-4">
                    <Skeleton className="h-10 w-full" />
                    <Skeleton className="h-10 w-full" />
                    <Skeleton className="h-10 w-full" />
                  </div>
                ) : (
                  <>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono">
                          {artifactCount}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Artifacts Produced
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono">
                          {formatBytes(artifactSize)}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Total Artifact Size
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="text-lg font-bold font-mono">
                          {nodesExecuted}
                        </p>
                        <p className="text-xs text-foreground-muted">
                          Execution Node Attempts
                        </p>
                      </div>
                    </div>
                  </>
                )}
              </CardContent>
            </Card>
          </div>

          <Card className="min-h-[312px] lg:col-span-2">
            <CardHeader>
              <CardTitle className="flex items-center gap-1">
                <span>Tokens by Role</span>
                <InfoTooltip
                  label="Tokens by Role formula"
                  message="Total tokens calculated as Input + Output + Reasoning."
                />
              </CardTitle>
            </CardHeader>
            <CardContent>
              {showSelectedRunSkeletons ? (
                <div className="space-y-3">
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                </div>
              ) : (
                <div className="grid items-start gap-4 lg:grid-cols-[minmax(0,1.1fr)_1px_minmax(0,0.9fr)]">
                  <div className="max-h-[360px] min-w-0 overflow-y-auto [scrollbar-gutter:stable] lg:pr-4">
                    <HorizontalBarChart
                      ariaLabel="Tokens by role"
                      data={deferredByRoleBars}
                      emptyHeight={250}
                      formatValue={formatCompactNumber}
                    />
                  </div>
                  <div
                    className="hidden self-stretch bg-border lg:block"
                    aria-hidden="true"
                  />
                  <div className="max-h-[360px] min-w-0 overflow-auto [scrollbar-gutter:stable] lg:pl-1">
                    {scopedTokenStats.byRole.length > 0 && (
                      <TokenRoleBreakdownList roles={scopedTokenStats.byRole} />
                    )}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}

function ExecutionDetailErrorState({
  selectedRunId,
  selectedRunNotFound,
  onClearSelection,
  onRetry,
}: {
  selectedRunId: string | null;
  selectedRunNotFound: boolean;
  onClearSelection: () => void;
  onRetry: () => void;
}) {
  return (
    <Card className="border-status-failed/40 bg-status-failed/5">
      <CardContent className="flex flex-col gap-3 py-5 sm:flex-row sm:items-center sm:justify-between">
        <div className="space-y-1">
          <p className="text-sm font-semibold text-foreground">
            {selectedRunNotFound
              ? "Selected run not found"
              : "Execution detail unavailable"}
          </p>
          <p className="text-sm text-foreground-subtle">
            {selectedRunNotFound
              ? `Run ${selectedRunId ?? ""} is unavailable. Clear the scope to return to aggregate execution metrics.`
              : "The scoped execution detail could not be loaded right now. Retry or clear the scope to keep working."}
          </p>
        </div>
        <div className="flex gap-2">
          <Button type="button" variant="outline" onClick={onClearSelection}>
            Show All Runs
          </Button>
          {!selectedRunNotFound ? (
            <Button type="button" onClick={onRetry}>
              Retry
            </Button>
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}

function SnapshotErrorState({
  title,
  description,
  onRetry,
}: {
  title: string;
  description: string;
  onRetry: () => void;
}) {
  return (
    <Card className="border-status-failed/40 bg-status-failed/5">
      <CardContent className="flex flex-col gap-3 py-5 sm:flex-row sm:items-center sm:justify-between">
        <div className="space-y-1">
          <p className="text-sm font-semibold text-foreground">{title}</p>
          <p className="text-sm text-foreground-subtle">{description}</p>
        </div>
        <Button type="button" onClick={onRetry}>
          Retry
        </Button>
      </CardContent>
    </Card>
  );
}

function InlineRefreshWarning({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="flex flex-col gap-3 rounded-md border border-status-blocked/30 bg-status-blocked/10 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-start gap-2 text-sm text-foreground">
        <RefreshCw className="mt-0.5 h-4 w-4 shrink-0 text-status-blocked" />
        <p>{message}</p>
      </div>
      <Button type="button" variant="outline" onClick={onRetry}>
        Retry
      </Button>
    </div>
  );
}

function describeRefreshFailure(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return `${fallback} ${error.message.trim()}`;
  }
  return fallback;
}

const ALL_RUNS_SCOPE_ID = "__all_runs__";
const RUN_SCOPE_SEARCH_DEBOUNCE_MS = 250;

function RunScopePicker({
  selectedRunId,
  selectedRun,
  onSelect,
}: {
  selectedRunId: string | null;
  selectedRun?: AnalyticsExecutionDetail["selected_run"];
  onSelect: (runID: string | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [committedSearch, setCommittedSearch] = useState("");
  const [stableSelectedLabel, setStableSelectedLabel] = useState<string | null>(
    null,
  );

  useEffect(() => {
    if (!open) return;
    const timer = window.setTimeout(() => {
      setCommittedSearch(search.trim());
    }, RUN_SCOPE_SEARCH_DEBOUNCE_MS);
    return () => {
      window.clearTimeout(timer);
    };
  }, [open, search]);

  const runsQuery = useInfiniteRuns({
    enabled: open,
    search: committedSearch,
    limit: 25,
  });

  const items = runsQuery.data?.pages.flatMap((page) => page.items ?? []) ?? [];
  const options: PopoverListboxItem[] = items.map((run) => ({
    id: run.run_id,
    label: `${run.work_order_title || run.run_id} — ${formatTimestamp(run.created_at)}`,
  }));

  const selectedOptionLabel = options.find(
    (option) => option.id === selectedRunId,
  )?.label;
  const selectedRunLabel = selectedRun
    ? `${selectedRun.work_order_title || selectedRun.run_id} — ${formatTimestamp(selectedRun.created_at)}`
    : undefined;
  const latestSelectedLabel = selectedOptionLabel ?? selectedRunLabel;
  if (!selectedRunId && stableSelectedLabel !== null) {
    setStableSelectedLabel(null);
  } else if (
    latestSelectedLabel &&
    latestSelectedLabel !== stableSelectedLabel
  ) {
    setStableSelectedLabel(latestSelectedLabel);
  }

  const selectedLabel = selectedRunId
    ? (selectedOptionLabel ?? stableSelectedLabel ?? selectedRunId)
    : "All Runs";

  const pickerItems: PopoverListboxItem[] = [
    ...(search.trim() === ""
      ? [{ id: ALL_RUNS_SCOPE_ID, label: "All Runs" }]
      : []),
    ...options,
  ];

  const footer = runsQuery.isError ? (
    <div className="flex items-center justify-between gap-3">
      <p className="text-xs text-status-failed">
        Unable to load more runs right now.
      </p>
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() => {
          void runsQuery.refetch();
        }}
      >
        Retry
      </Button>
    </div>
  ) : runsQuery.hasNextPage ? (
    <div className="flex justify-center">
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() => {
          void runsQuery.fetchNextPage();
        }}
        disabled={runsQuery.isFetchingNextPage}
      >
        {runsQuery.isFetchingNextPage
          ? "Loading More Runs..."
          : "Load More Runs"}
      </Button>
    </div>
  ) : null;

  return (
    <PopoverListbox
      triggerId="analytics-run-scope"
      triggerRole="combobox"
      triggerAriaLabel="Run scope"
      triggerClassName={cn(selectTriggerClass, "max-w-[360px]")}
      triggerContent={
        <SelectTriggerContent>{selectedLabel}</SelectTriggerContent>
      }
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen);
        if (!nextOpen) {
          setSearch("");
          setCommittedSearch("");
        }
      }}
      items={pickerItems}
      selectedId={selectedRunId ?? ALL_RUNS_SCOPE_ID}
      onSelect={(nextRunId) => {
        setSearch("");
        setCommittedSearch("");
        if (nextRunId === ALL_RUNS_SCOPE_ID) {
          setStableSelectedLabel(null);
          onSelect(null);
          return;
        }
        const nextSelectionLabel =
          pickerItems.find((item) => item.id === nextRunId)?.label ?? nextRunId;
        setStableSelectedLabel(nextSelectionLabel);
        onSelect(nextRunId);
      }}
      search={{
        value: search,
        onChange: setSearch,
        placeholder: "Search runs",
      }}
      minWidth={420}
      maxHeight={320}
      emptyState={
        <p className="px-3 py-2 text-xs text-foreground-subtle">
          {runsQuery.isLoading ? "Loading runs..." : "No matching runs found."}
        </p>
      }
      footer={footer}
    />
  );
}

function TokenRoleBreakdownList({ roles }: { roles: TokensByRole[] }) {
  const { theme } = useTheme();
  const [expandedRoleIDs, setExpandedRoleIDs] = useState<
    Record<string, boolean>
  >({});

  const toggleRole = (roleID: string) => {
    setExpandedRoleIDs((current) => ({
      ...current,
      [roleID]: !current[roleID],
    }));
  };

  return (
    <div className="space-y-2">
      {roles.map((role) => {
        const summary =
          buildRoleUsageSummary(role) || "Execution metadata unavailable";
        const isExpanded = expandedRoleIDs[role.required_role] ?? false;
        const modelBreakdown = role.by_model ?? [];
        return (
          <div
            key={role.required_role}
            className="rounded-lg border border-border/70 bg-surface-raised/25"
          >
            <button
              type="button"
              aria-expanded={isExpanded}
              aria-label={buildRoleBreakdownButtonLabel(
                role,
                roleIdentity(theme, role.required_role).name,
              )}
              onClick={() => toggleRole(role.required_role)}
              className="flex w-full items-start justify-between gap-3 px-3 py-3 text-left transition-colors hover:bg-surface-raised/50"
            >
              <div className="min-w-0 space-y-1">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold text-foreground">
                    {roleIdentity(theme, role.required_role).name}
                  </span>
                  <Badge className="text-[10px] uppercase tracking-wide">
                    {formatCompactNumber(role.derived_total_tokens)}
                  </Badge>
                </div>
                <p className="text-xs text-foreground-subtle">{summary}</p>
              </div>
              <div className="flex items-center gap-2">
                <ChevronDown
                  className={`h-4 w-4 shrink-0 text-foreground-subtle transition-transform ${isExpanded ? "rotate-180" : ""}`}
                />
              </div>
            </button>

            {isExpanded && modelBreakdown.length > 0 && (
              <div className="border-t border-border/70 px-3 py-3">
                <div className="space-y-2">
                  {modelBreakdown.map((model, index) => {
                    const summaryLabel =
                      buildModelUsageSummary(model) ||
                      "Execution metadata unavailable";
                    return (
                      <div
                        key={`${role.required_role}-${model.model || "unknown"}-${model.thinking_effort || index}`}
                        className="rounded-md border border-border/60 bg-background/70 px-3 py-2"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0 space-y-1">
                            <div className="flex flex-wrap items-center gap-2">
                              {model.provider_name?.trim() && (
                                <span className="text-xs font-medium text-foreground">
                                  {model.provider_name.trim()}
                                </span>
                              )}
                              {(model.model_name?.trim() ||
                                model.model?.trim()) && (
                                <span className="text-xs text-foreground-subtle">
                                  {model.model_name?.trim() ||
                                    model.model?.trim()}
                                </span>
                              )}
                              {model.thinking_effort?.trim() && (
                                <Badge className="text-[10px] uppercase tracking-wide">
                                  {`Thinking: ${model.thinking_effort.trim()}`}
                                </Badge>
                              )}
                            </div>
                            {!model.provider_name?.trim() &&
                              !model.model_name?.trim() &&
                              !model.model?.trim() && (
                                <p className="text-xs font-medium text-foreground">
                                  {summaryLabel}
                                </p>
                              )}
                            <p className="text-[11px] text-foreground-subtle">
                              {pluralize(model.run_count, "run")} •{" "}
                              {pluralize(model.record_count, "record")}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="font-mono text-xs font-semibold text-foreground">
                              {formatCompactNumber(model.derived_total_tokens)}
                            </p>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ── Shared sub-components ── */

function MetricCard({
  title,
  value,
  titleTooltip,
}: {
  title: string;
  value: number | string;
  titleTooltip?: string;
}) {
  return (
    <dl className="border-b border-border pb-6">
      <dt className="mb-3 flex min-h-10 items-start gap-2 text-xs text-foreground-muted sm:text-sm">
        <span>{title}</span>
        {titleTooltip && (
          <InfoTooltip label={`${title} definition`} message={titleTooltip} />
        )}
      </dt>
      <dd className="font-display text-2xl tabular-nums text-foreground sm:text-4xl">
        {value}
      </dd>
    </dl>
  );
}

function SkeletonCard() {
  return (
    <div className="space-y-5 border-b border-border pb-6">
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-10 w-16" />
    </div>
  );
}
