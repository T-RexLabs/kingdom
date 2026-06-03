import { startTransition, useState, useMemo, useCallback, useDeferredValue, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Badge } from "@/components/ui/badge";
import { InfoTooltip } from "@/components/ui/info-tooltip";
import { TimeRangePicker } from "@/components/ui/time-range-picker";
import { HorizontalBarChart } from "@/components/ui/horizontal-bar-chart";
import { PopoverListbox, type PopoverListboxItem } from "@/components/ui/popover-listbox";
import { StackedBarChart, type StackedBarDatum } from "@/components/ui/stacked-bar-chart";
import { PageHeader, PageContent } from "@/components/page-layout";
import {
  useRealmAnalyticsExecutionDetailSnapshot,
  useInfiniteRuns,
  useRuntimeMetrics,
  useRealmAnalyticsSnapshot,
  type RealmAnalyticsExecutionDetail,
  type RealmAnalyticsOverview,
  type RealmAnalyticsRunAnalytics,
  type TokenUsageByRole as TokensByRole,
  type TokenUsageByRoleModel as TokensByRoleModel,
} from "@/api/hooks";
import { ApiError } from "@/api/client";
import { useRetainedData } from "@/lib/use-retained-data";
import { roleTitle, formatTimestamp } from "@/lib/utils";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { describeManualGovernanceState } from "@/features/runs/manual-governance-state";
import { runDisplayBadgeStatus, runDisplayStateLabel } from "@/features/runs/run-display-state";
import {
  Activity,
  Cpu,
  AlertTriangle,
  Zap,
  TrendingUp,
  Timer,
  RefreshCw,
  Coins,
  BarChart3,
  Hash,
  FileText,
  HardDrive,
  Shield,
  ShieldCheck,
  ChevronDown,
} from "lucide-react";

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

function pluralize(count: number, singular: string, plural = `${singular}s`): string {
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

function buildRoleBreakdownButtonLabel(role: TokensByRole): string {
  const summary = buildRoleUsageSummary(role).replace(/ • /g, " ");
  return `${roleTitle(role.required_role)} ${summary}`.trim();
}

/** Map role IDs to their CSS variable colors for chart bars. */
const ROLE_CHART_COLORS: Record<string, string> = {
  king: "var(--color-role-king)",
  chancellor: "var(--color-role-chancellor)",
  "council-member": "var(--color-role-council)",
  "noble-delegate": "var(--color-role-noble)",
  "clergy-delegate": "var(--color-role-clergy)",
  "burgher-delegate": "var(--color-role-burgher)",
  "peasant-delegate": "var(--color-role-peasant)",
  "builder-berserker": "var(--color-role-berserker)",
  "reviewer-shieldmaiden": "var(--color-role-shieldmaiden)",
  "intelligence-qa-scout": "var(--color-role-scout)",
  "intelligence-nightraven": "var(--color-role-nightraven)",
  "intelligence-loki-chaos": "var(--color-role-loki)",
};

function roleColor(roleId: string): string {
  return ROLE_CHART_COLORS[roleId] ?? "var(--color-gold)";
}

/* ── Page ── */

export function RealmAnalyticsPage() {
  const navigate = useNavigate({ from: "/realm-analytics" });
  const windowTotalMinutes = useSearch({
    from: "/realm-analytics",
    select: (search) => search.window_minutes ?? 1440,
  });

  const setWindowTotalMinutes = useCallback((totalMinutes: number) => {
    startTransition(() => {
      navigate({
        search: (previous) => ({
          ...previous,
          window_minutes: totalMinutes,
        }),
        replace: true,
      });
    });
  }, [navigate]);

  const snapshotQuery = useRealmAnalyticsSnapshot(windowTotalMinutes);
  const retainedSnapshot = useRetainedData({
    identity: windowTotalMinutes,
    data: snapshotQuery.data,
    isPending: snapshotQuery.isPending,
    isFetching: snapshotQuery.isFetching,
  });
  const snapshot = retainedSnapshot.displayData;
  const snapshotBlockingError = snapshotQuery.error && !snapshot ? snapshotQuery.error : null;
  const snapshotWarningMessage = snapshot && snapshotQuery.isRefetchError
    ? describeRefreshFailure(snapshotQuery.error, "A refresh failed. Showing the last successful analytics snapshot.")
    : null;
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <div className="flex w-full flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h1 className="font-display text-2xl font-bold text-gold">
                Realm Analytics
              </h1>
              <p className="text-sm text-foreground-muted">
                System-wide runtime and execution metrics
              </p>
            </div>
            <div className="flex w-full items-center justify-end gap-3 sm:w-auto">
              {retainedSnapshot.isRetainingPreviousData ? (
                <span className="inline-flex items-center gap-2 text-xs text-foreground-subtle">
                  <Spinner className="h-3.5 w-3.5 border-[1.5px]" />
                  Refreshing analytics...
                </span>
              ) : null}
              <TimeRangePicker value={windowTotalMinutes} onChange={setWindowTotalMinutes} />
            </div>
        </div>
      </PageHeader>

      <PageContent className="space-y-8">
	      <Section title="Runtime Health">
	        <RuntimeHealthSection />
	      </Section>

	      {snapshotWarningMessage ? (
	        <InlineRefreshWarning message={snapshotWarningMessage} onRetry={() => { void snapshotQuery.refetch(); }} />
	      ) : null}

	      {snapshotBlockingError ? (
	        <SnapshotErrorState
	          title="Realm analytics snapshot unavailable"
	          description="The aggregate analytics snapshot could not be loaded right now. Retry to restore runtime health and run analytics."
	          onRetry={() => {
	            void snapshotQuery.refetch();
	          }}
	        />
	      ) : (
	        <>
	        {/* Section 2: Run Analytics */}
        <Section title="Run Analytics">
            <RunAnalyticsSection
	            overview={snapshot?.overview}
	            runAnalytics={snapshot?.run_analytics}
	            allTimeRunCount={snapshot?.all_time_run_count ?? 0}
	            windowLabel={formatWindowLabel(windowTotalMinutes)}
	            isLoading={retainedSnapshot.isInitialLoading && !snapshot}
          />
        </Section>
	        </>
	      )}

        {/* Section 3: Execution Detail */}
        <Section title="Execution Detail">
          <ExecutionDetailSection windowTotalMinutes={windowTotalMinutes} />
        </Section>
      </PageContent>
    </div>
  );
}

function RuntimeHealthSection() {
  const runtimeQuery = useRuntimeMetrics();
  const runtime = runtimeQuery.data;
  const blockingError = runtimeQuery.error && !runtime ? runtimeQuery.error : null;
  const warningMessage = runtime && runtimeQuery.isRefetchError
    ? describeRefreshFailure(runtimeQuery.error, "A refresh failed. Showing the last successful runtime health snapshot.")
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
        <InlineRefreshWarning message={warningMessage} onRetry={() => { void runtimeQuery.refetch(); }} />
      ) : null}
      {showRuntimeSkeletons ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <SkeletonCard /><SkeletonCard /><SkeletonCard /><SkeletonCard />
        </div>
      ) : runtime ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <MetricCard
            title="Status"
            value={runtime.status === "ok" ? "Online" : "Degraded"}
            icon={<Activity className="h-4 w-4" />}
            accentClass={runtime.status === "ok" ? "text-status-succeeded" : "text-status-failed"}
          />
          <MetricCard
            title="Active Runs"
            value={runtime.running_runs}
            icon={<Zap className="h-4 w-4 text-status-running" />}
            accentClass="text-status-running"
          />
          <MetricCard
            title="Active Nodes"
            value={runtime.running_nodes}
            icon={<Cpu className="h-4 w-4 text-gold" />}
            accentClass="text-gold"
          />
          <MetricCard
            title="Stale Nodes"
            value={runtime.stale_running_nodes}
            icon={<AlertTriangle className="h-4 w-4 text-status-blocked" />}
            accentClass={runtime.stale_running_nodes > 0 ? "text-status-failed" : "text-foreground"}
          />
        </div>
      ) : null}
    </div>
  );
}

/* ── Section wrapper ── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
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
	overview?: RealmAnalyticsOverview;
	runAnalytics?: RealmAnalyticsRunAnalytics;
	allTimeRunCount: number;
	windowLabel: string;
	isLoading: boolean;
}) {
  const outcomeBars = useMemo<StackedBarDatum[]>(() => {
	    if (!runAnalytics?.outcome_series) return [];
	    return runAnalytics.outcome_series.map((b) => {
      // Shorten bucket labels for display
      const label =
	        runAnalytics.bucket_type === "hour"
          ? b.bucket.slice(11, 16) // "HH:MM"
          : b.bucket.slice(5, 10); // "MM-DD"
      return {
        label,
        values: [
          { key: "succeeded", value: b.succeeded, color: "var(--color-status-succeeded)" },
          { key: "failed", value: b.failed, color: "var(--color-status-failed)" },
          { key: "cancelled", value: b.cancelled, color: "var(--color-status-cancelled)" },
        ],
      };
    });
	  }, [runAnalytics?.outcome_series, runAnalytics?.bucket_type]);

  const durationByRoleBars = useMemo(() => {
	    if (!runAnalytics?.duration_by_role) return [];
	    return runAnalytics.duration_by_role.map((r) => ({
      label: roleTitle(r.role),
      value: r.avg_duration_seconds,
      color: roleColor(r.role),
    }));
	  }, [runAnalytics?.duration_by_role]);

  const formatDurationValue = useCallback((v: number) => formatDuration(v), []);
  const deferredOutcomeBars = useDeferredValue(outcomeBars);
  const deferredDurationByRoleBars = useDeferredValue(durationByRoleBars);

	  const recentRunItems = runAnalytics?.recent_runs ?? [];

  return (
    <div className="space-y-4">
      {/* Run overview stat cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
        <MetricCard
          title="Total Runs"
	          value={overview ? overview.total_runs : "--"}
          icon={<BarChart3 className="h-4 w-4 text-gold" />}
          accentClass="text-gold"
        />
        <MetricCard
          title="Avg Duration"
	          value={overview ? formatDuration(overview.avg_duration_seconds) : "--"}
          icon={<Timer className="h-4 w-4 text-status-running" />}
          accentClass="text-status-running"
        />
        <MetricCard
          title="Success Rate"
	          value={overview ? `${overview.success_rate.toFixed(1)}%` : "--"}
          icon={<TrendingUp className="h-4 w-4 text-status-succeeded" />}
          accentClass="text-status-succeeded"
        />
        <MetricCard
          title="Review Rework Rate"
	          value={overview ? `${overview.review_rework_rate.toFixed(1)}%` : "--"}
          icon={<RefreshCw className="h-4 w-4 text-status-blocked" />}
          accentClass="text-status-blocked"
          titleTooltip="Percentage of runs in the selected window that required at least one reviewer rework loop."
        />
        <MetricCard
          title="Validation Rework Rate"
	          value={overview ? `${overview.validation_rework_rate.toFixed(1)}%` : "--"}
          icon={<ShieldCheck className="h-4 w-4 text-status-cancelled" />}
          accentClass="text-status-cancelled"
          titleTooltip="Percentage of runs in the selected window that required at least one validation rework loop."
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="min-h-[320px]">
          <CardHeader>
            <CardTitle>Run Outcomes Over Time</CardTitle>
          </CardHeader>
          <CardContent className="h-[250px]">
            <StackedBarChart ariaLabel="Run outcomes over time" data={deferredOutcomeBars} integerTicks />
          </CardContent>
        </Card>

        <Card className="min-h-[320px]">
          <CardHeader>
            <CardTitle>Avg Execution Duration by Role</CardTitle>
          </CardHeader>
          <CardContent className="h-[250px] overflow-y-auto [scrollbar-gutter:stable]">
            <HorizontalBarChart
	              ariaLabel="Average execution duration by role"
              data={deferredDurationByRoleBars}
              formatValue={formatDurationValue}
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
                className="text-xs font-normal text-gold hover:text-gold-bright"
              >
                View all
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
	              {recentRunItems.slice(0, 10).map((run) => {
	                const governanceState = describeManualGovernanceState(run);
	                const isActionRequired = governanceState.isAwaitingPlanApproval || governanceState.isAwaitingFinalApproval;
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
	                        {run.decree_title || run.run_id}
	                      </p>
	                      <p className="mt-1 text-xs text-foreground-subtle">
	                        {run.workflow_id} · Created {formatTimestamp(run.created_at)} · Last Activity {formatTimestamp(run.last_activity_at)}
	                      </p>
	                    </div>
	                  </div>
	                  <div className="ml-3 flex shrink-0 flex-col items-end gap-2 sm:flex-row sm:items-center">
	                    {isActionRequired ? (
	                      <span className="inline-flex items-center gap-1 rounded-md bg-gold/10 px-1.5 py-0.5 text-[10px] font-medium text-gold">
	                        <Shield className="h-2.5 w-2.5" />
	                        Action Required
	                      </span>
	                    ) : null}
	                    <Badge variant="status" status={runDisplayBadgeStatus(run)}>
	                      {runDisplayStateLabel(run)}
	                    </Badge>
	                  </div>
                </Link>
	                );
	              })}
            </div>
	          ) : allTimeRunCount === 0 ? (
            <p className="text-sm text-foreground-subtle">
              No runs yet.
            </p>
          ) : (
            <p className="text-sm text-foreground-subtle">
	              {`No completed runs in the last ${windowLabel}.`}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function formatWindowLabel(windowMinutes: number): string {
	if (windowMinutes % 1440 === 0) {
		const days = windowMinutes / 1440;
		return days === 1 ? "24h" : `${days}d`;
	}
	if (windowMinutes % 60 === 0) {
		return `${windowMinutes / 60}h`;
	}
	return `${windowMinutes}m`;
}

/* ── Section 4: Execution Detail ── */

function ExecutionDetailSection({ windowTotalMinutes }: { windowTotalMinutes: number }) {
  const selectedRunId = useSearch({
    from: "/realm-analytics",
    select: (search) => search.run_id ?? null,
  });
  const navigate = useNavigate({ from: "/realm-analytics" });
  const setSelectedRunId = useCallback((runID: string | null) => {
    startTransition(() => {
      navigate({
        search: (previous) => {
          if (!runID) {
            const { run_id: _ignored, ...rest } = previous;
            return rest;
          }
          return {
            ...previous,
            run_id: runID,
          };
        },
        replace: true,
      });
    });
  }, [navigate]);

  const executionDetailQuery = useRealmAnalyticsExecutionDetailSnapshot(windowTotalMinutes, selectedRunId);
  const retainedExecutionDetail = useRetainedData({
    identity: selectedRunId ? `run:${selectedRunId}` : `window:${windowTotalMinutes}`,
    data: executionDetailQuery.data?.execution_detail,
    isPending: executionDetailQuery.isPending,
    isFetching: executionDetailQuery.isFetching,
  });
  const executionDetail = retainedExecutionDetail.displayData;
  const executionDetailError = executionDetailQuery.error;
  const selectedRunNotFound = executionDetailError instanceof ApiError && executionDetailError.status === 404;
  const blockingError = selectedRunNotFound || executionDetailQuery.isLoadingError
    ? executionDetailError
    : null;
  const warningMessage = executionDetail && executionDetailQuery.isRefetchError && !selectedRunNotFound
    ? describeRefreshFailure(executionDetailError, "A refresh failed. Showing the last successful execution detail.")
    : null;
  const [showAdvancedTokenDetails, setShowAdvancedTokenDetails] = useState(false);
  const showSelectedRunSkeletons = retainedExecutionDetail.isInitialLoading && !executionDetail && !blockingError;

  const scopedTokenStats = useMemo(() => {
    return {
      total: executionDetail?.tokens.derived_total_tokens ?? 0,
      providerTotal: executionDetail?.tokens.provider_total_tokens ?? 0,
      input: executionDetail?.tokens.input ?? 0,
      output: executionDetail?.tokens.output ?? 0,
      reasoning: executionDetail?.tokens.reasoning ?? 0,
      cacheRead: executionDetail?.tokens.cache_read ?? 0,
      cacheWrite: executionDetail?.tokens.cache_write ?? 0,
      byRole: executionDetail?.tokens.by_role ?? [],
    };
  }, [executionDetail]);

  const byRoleBars = useMemo(() => {
    return scopedTokenStats.byRole.map((r) => ({
      label: roleTitle(r.required_role),
      value: r.derived_total_tokens,
      color: roleColor(r.required_role),
    }));
  }, [scopedTokenStats.byRole]);
  const deferredByRoleBars = useDeferredValue(byRoleBars);

  const formatTokenValue = useCallback((v: number) => formatCompactNumber(v), []);

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
      <div className="flex flex-wrap items-center justify-start gap-3">
        <RunScopePicker
          selectedRunId={selectedRunId}
          selectedRun={selectedRunNotFound ? undefined : executionDetail?.selected_run}
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
              <Button type="button" variant="outline" onClick={() => { void executionDetailQuery.refetch(); }}>
                Retry
              </Button>
            </div>
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
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <Coins className="h-4 w-4 text-gold" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono text-gold">{formatCompactNumber(tokenTotal)}</p>
                    <div className="flex items-center gap-1">
                      <p className="text-xs text-foreground-muted">Total Tokens</p>
                      <InfoTooltip
                        label="Total Tokens formula"
                        message="Total tokens calculated as Input + Output + Reasoning."
                      />
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <Hash className="h-4 w-4 text-status-running" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono text-status-running">{formatCompactNumber(tokenInput)}</p>
                    <p className="text-xs text-foreground-muted">Input Tokens</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <Hash className="h-4 w-4 text-status-cancelled" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono text-status-cancelled">{formatCompactNumber(tokenReasoning)}</p>
                    <p className="text-xs text-foreground-muted">Reasoning Tokens</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <Hash className="h-4 w-4 text-status-succeeded" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono text-status-succeeded">{formatCompactNumber(tokenOutput)}</p>
                    <p className="text-xs text-foreground-muted">Output Tokens</p>
                  </div>
                </div>
                <div className="border-t border-border pt-3">
                  <button
                    type="button"
                    className="flex items-center gap-2 text-xs font-medium text-foreground-muted transition-colors hover:text-foreground"
                    onClick={() => setShowAdvancedTokenDetails((previous) => !previous)}
                  >
                    <ChevronDown className={`h-3.5 w-3.5 transition-transform ${showAdvancedTokenDetails ? "rotate-180" : ""}`} />
                    <span>Advanced Token Details</span>
                  </button>
                  {showAdvancedTokenDetails && (
                    <div className="mt-3 space-y-3 rounded-md border border-border bg-surface-raised p-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex items-center gap-1.5">
                          <span className="text-xs text-foreground-muted">Provider Total Tokens</span>
                          <InfoTooltip
                            label="Provider Total Tokens definition"
                            message="Provider Total Tokens are model/provider-reported totals that may include cached tokens or provider-specific accounting, so they can differ from Input + Output + Reasoning."
                          />
                        </div>
                        <span className="font-mono text-xs text-foreground">{formatCompactNumber(tokenProviderTotal)}</span>
                      </div>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-xs text-foreground-muted">Cache Read</span>
                        <span className="font-mono text-xs text-foreground">{formatCompactNumber(tokenCacheRead)}</span>
                      </div>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-xs text-foreground-muted">Cache Write</span>
                        <span className="font-mono text-xs text-foreground">{formatCompactNumber(tokenCacheWrite)}</span>
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
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <FileText className="h-4 w-4 text-gold" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono">{artifactCount}</p>
                    <p className="text-xs text-foreground-muted">Artifacts produced</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <HardDrive className="h-4 w-4 text-foreground-subtle" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono">{formatBytes(artifactSize)}</p>
                    <p className="text-xs text-foreground-muted">Total artifact size</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
                    <Cpu className="h-4 w-4 text-status-running" />
                  </div>
                  <div>
                    <p className="text-lg font-bold font-mono">{nodesExecuted}</p>
                    <p className="text-xs text-foreground-muted">Execution node attempts</p>
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
                <div className="grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_1px_minmax(320px,0.9fr)] lg:items-stretch">
                  <div className="min-h-[250px] lg:pr-4">
                    <HorizontalBarChart
                      ariaLabel="Tokens by role"
                      data={deferredByRoleBars}
                      emptyHeight={250}
                      formatValue={formatTokenValue}
                    />
                  </div>
                  <div className="hidden self-stretch bg-border lg:block" aria-hidden="true" />
                  <div className="max-h-[360px] overflow-auto lg:pl-1">
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
            {selectedRunNotFound ? "Selected run not found" : "Execution detail unavailable"}
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
  selectedRun?: RealmAnalyticsExecutionDetail["selected_run"];
  onSelect: (runID: string | null) => void;
}) {
	  const [open, setOpen] = useState(false);
	  const [search, setSearch] = useState("");
	  const [committedSearch, setCommittedSearch] = useState("");
	  const [stableSelectedLabel, setStableSelectedLabel] = useState<string | null>(null);

	  useEffect(() => {
	    if (!open) {
	      setCommittedSearch("");
	      return;
	    }
	    const timer = window.setTimeout(() => {
	      setCommittedSearch(search.trim());
	    }, RUN_SCOPE_SEARCH_DEBOUNCE_MS);
	    return () => {
	      window.clearTimeout(timer);
	    };
	  }, [open, search]);

	  const runsQuery = useInfiniteRuns({ enabled: open, search: committedSearch, limit: 25 });

	  const options = useMemo<PopoverListboxItem[]>(() => {
	    const items = runsQuery.data?.pages.flatMap((page) => page.items ?? []) ?? [];
	    return items.map((run) => ({
	      id: run.run_id,
	      label: `${run.decree_title || run.run_id} — ${formatTimestamp(run.created_at)}`,
	    }));
	  }, [runsQuery.data?.pages]);

	  useEffect(() => {
	    if (!selectedRunId) {
	      setStableSelectedLabel(null);
	      return;
	    }
	    const selectedOptionLabel = options.find((opt) => opt.id === selectedRunId)?.label;
	    if (selectedOptionLabel) {
	      setStableSelectedLabel(selectedOptionLabel);
	      return;
	    }
	    if (selectedRun) {
	      setStableSelectedLabel(`${selectedRun.decree_title || selectedRun.run_id} — ${formatTimestamp(selectedRun.created_at)}`);
	    }
	  }, [options, selectedRun, selectedRunId]);

	  const selectedLabel = selectedRunId
	    ? (options.find((opt) => opt.id === selectedRunId)?.label
	      ?? stableSelectedLabel
	      ?? (selectedRun ? `${selectedRun.decree_title || selectedRun.run_id} — ${formatTimestamp(selectedRun.created_at)}` : selectedRunId))
	    : "All Runs";

	  const pickerItems = useMemo<PopoverListboxItem[]>(() => [
	    ...(search.trim() === "" ? [{ id: ALL_RUNS_SCOPE_ID, label: "All Runs" }] : []),
	    ...options,
	  ], [options, search]);

	  const footer = runsQuery.isError ? (
	    <div className="flex items-center justify-between gap-3">
	      <p className="text-xs text-status-failed">Unable to load more runs right now.</p>
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
	        {runsQuery.isFetchingNextPage ? "Loading More Runs..." : "Load More Runs"}
	      </Button>
	    </div>
	  ) : null;

	  return (
	    <PopoverListbox
	      triggerId="realm-analytics-run-scope"
	      triggerRole="combobox"
	      triggerAriaLabel="Run scope"
	      triggerClassName="flex max-w-[360px] items-center gap-2 rounded-md border border-border bg-background px-2.5 py-1.5 text-left text-xs text-foreground transition-colors hover:border-gold/40 focus:border-gold/50 focus:outline-none focus:ring-1 focus:ring-gold/30"
	      triggerContent={(
	        <>
	          <span className="truncate font-mono">{selectedLabel}</span>
	          <ChevronDown className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
	        </>
	      )}
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
	        const nextSelectionLabel = pickerItems.find((item) => item.id === nextRunId)?.label ?? nextRunId;
	        setStableSelectedLabel(nextSelectionLabel);
	        onSelect(nextRunId);
	      }}
	      search={{ value: search, onChange: setSearch, placeholder: "Search runs" }}
	      minWidth={420}
	      maxHeight={320}
	      panelClassName="flex flex-col overflow-hidden rounded-lg border border-border bg-surface shadow-xl shadow-black/40"
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
  const [expandedRoleIDs, setExpandedRoleIDs] = useState<Record<string, boolean>>({});

  const toggleRole = useCallback((roleID: string) => {
    setExpandedRoleIDs((current) => ({
      ...current,
      [roleID]: !current[roleID],
    }));
  }, []);

  return (
    <div className="space-y-2">
      {roles.map((role) => {
        const summary = buildRoleUsageSummary(role) || "Execution metadata unavailable";
        const isExpanded = expandedRoleIDs[role.required_role] ?? false;
        const modelBreakdown = role.by_model ?? [];
        return (
          <div key={role.required_role} className="rounded-lg border border-border/70 bg-surface-raised/25">
            <button
              type="button"
              aria-expanded={isExpanded}
              aria-label={buildRoleBreakdownButtonLabel(role)}
              onClick={() => toggleRole(role.required_role)}
              className="flex w-full items-start justify-between gap-3 px-3 py-3 text-left transition-colors hover:bg-surface-raised/50"
            >
              <div className="min-w-0 space-y-1">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold text-foreground">{roleTitle(role.required_role)}</span>
                  <Badge className="text-[10px] uppercase tracking-wide">
                    {formatCompactNumber(role.derived_total_tokens)}
                  </Badge>
                </div>
                <p className="text-xs text-foreground-subtle">{summary}</p>
              </div>
              <div className="flex items-center gap-2">
                <ChevronDown className={`h-4 w-4 shrink-0 text-foreground-subtle transition-transform ${isExpanded ? "rotate-180" : ""}`} />
              </div>
            </button>

            {isExpanded && modelBreakdown.length > 0 && (
              <div className="border-t border-border/70 px-3 py-3">
                <div className="space-y-2">
                  {modelBreakdown.map((model, index) => {
                    const summaryLabel = buildModelUsageSummary(model) || "Execution metadata unavailable";
                    return (
                      <div
                        key={`${role.required_role}-${model.model || "unknown"}-${model.thinking_effort || index}`}
                        className="rounded-md border border-border/60 bg-background/70 px-3 py-2"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0 space-y-1">
                            <div className="flex flex-wrap items-center gap-2">
                              {model.provider_name?.trim() && (
                                <span className="text-xs font-medium text-foreground">{model.provider_name.trim()}</span>
                              )}
                              {(model.model_name?.trim() || model.model?.trim()) && (
                                <span className="text-xs text-foreground-subtle">{model.model_name?.trim() || model.model?.trim()}</span>
                              )}
                              {model.thinking_effort?.trim() && (
                                <Badge className="text-[10px] uppercase tracking-wide">
                                  {`Thinking: ${model.thinking_effort.trim()}`}
                                </Badge>
                              )}
                            </div>
                            {!model.provider_name?.trim() && !model.model_name?.trim() && !model.model?.trim() && (
                              <p className="text-xs font-medium text-foreground">{summaryLabel}</p>
                            )}
                            <p className="text-[11px] text-foreground-subtle">
                              {pluralize(model.run_count, "run")} • {pluralize(model.record_count, "record")}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="font-mono text-xs font-semibold text-foreground">{formatCompactNumber(model.derived_total_tokens)}</p>
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
  icon,
  accentClass,
  titleTooltip,
}: {
  title: string;
  value: number | string;
  icon: React.ReactNode;
  accentClass?: string;
  titleTooltip?: string;
}) {
  return (
    <Card>
      <CardContent className="flex items-center gap-3">
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
          {icon}
        </div>
        <div>
          <p className={`text-2xl font-bold ${accentClass ?? ""}`}>{value}</p>
          <div className="flex items-center gap-1">
            <p className="text-xs text-foreground-muted">{title}</p>
            {titleTooltip && (
              <InfoTooltip label={`${title} definition`} message={titleTooltip} />
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function SkeletonCard() {
  return (
    <Card>
      <CardContent className="flex items-center gap-3">
        <Skeleton className="h-10 w-10 rounded-lg" />
        <div className="space-y-2">
          <Skeleton className="h-6 w-12" />
          <Skeleton className="h-3 w-16" />
        </div>
      </CardContent>
    </Card>
  );
}
