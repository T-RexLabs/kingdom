import { useDeferredValue, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Sparkline } from "@/components/ui/sparkline";
import { Badge } from "@/components/ui/badge";
import { InfoTooltip } from "@/components/ui/info-tooltip";
import {
  useDashboardSnapshot,
} from "@/api/hooks";
import { formatTimestamp } from "@/lib/utils";
import { Link } from "@tanstack/react-router";
import { PageHeader, PageContent } from "@/components/page-layout";
import { describeManualGovernanceState } from "@/features/runs/manual-governance-state";
import { runDisplayBadgeStatus, runDisplayStateLabel } from "@/features/runs/run-display-state";
import {
  Activity,
  CheckCircle,
  XCircle,
  Crown,
  Scroll,
  Timer,
  TrendingUp,
  RefreshCw,
  Coins,
  Hash,
  Brain,
  ShieldCheck,
  Shield,
} from "lucide-react";

export function DashboardPage() {
  const snapshotQuery = useDashboardSnapshot();
  const { data: snapshot, isLoading } = snapshotQuery;

  const windowMinutes = snapshot?.window_minutes ?? 1440;
  const windowLabel = formatDashboardWindowLabel(windowMinutes);
  const runtime = snapshot?.runtime;
  const overview = snapshot?.overview;
  const tokens = snapshot?.tokens;
  const recentRuns = snapshot?.recent_runs ?? [];
  const allTimeRunCount = snapshot?.all_time_run_count ?? 0;
  const trend = useMemo(() => snapshot?.run_trend.map((point) => point.run_count) ?? [], [snapshot?.run_trend]);
  const deferredTrend = useDeferredValue(trend);
  const blockingError = snapshotQuery.error && !snapshot ? snapshotQuery.error : null;
  const warningMessage = snapshot && snapshotQuery.isRefetchError
    ? describeRefreshFailure(snapshotQuery.error, "A refresh failed. Showing the last successful dashboard snapshot.")
    : null;
  const showPrimaryStatSkeletons = isLoading && !snapshot;
  const showRuntimeSkeletons = isLoading && !snapshot;
  const statusDotClass = blockingError
    ? "bg-status-failed"
    : runtime?.status === "ok"
      ? "bg-status-succeeded"
      : runtime
        ? "bg-status-failed animate-pulse"
        : "bg-foreground-subtle/40";
  const statusLabel = blockingError
    ? "Snapshot unavailable"
    : runtime?.status === "ok"
      ? "Realm Online"
      : runtime
        ? "Realm Degraded"
        : isLoading
          ? "Connecting..."
          : "Snapshot unavailable";

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <div className="flex w-full items-center justify-between">
        <div>
          <h1 className="font-display text-2xl font-bold text-gold">
            Throne Room
          </h1>
          <p className="text-sm text-foreground-muted">
            {`Command Center Overview · Last ${windowLabel}`}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {deferredTrend.length > 0 && (
            <div className="hidden items-center gap-2 sm:flex">
                  <span className="text-[10px] uppercase tracking-wider text-foreground-subtle">
	                    {`${windowLabel} trend`}
                  </span>
              <Sparkline
                ariaLabel={`Run trend over the last ${windowLabel}`}
                data={deferredTrend}
                width={100}
                height={28}
              />
            </div>
          )}
            <div className="flex items-center gap-1.5">
              <div
                className={`h-2 w-2 rounded-full ${statusDotClass}`}
              />
              <span className="text-xs text-foreground-subtle">
                {statusLabel}
              </span>
            </div>
        </div>
        </div>
      </PageHeader>

      <PageContent className="space-y-6">
	      {warningMessage ? (
	        <InlineRefreshWarning message={warningMessage} onRetry={() => { void snapshotQuery.refetch(); }} />
	      ) : null}

	      {blockingError ? (
	        <DashboardErrorState onRetry={() => { void snapshotQuery.refetch(); }} />
	      ) : (
	        <>

      {/* Stat cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {showPrimaryStatSkeletons ? (
          <>
            <StatCardSkeleton />
            <StatCardSkeleton />
            <StatCardSkeleton />
            <StatCardSkeleton />
          </>
        ) : (
          <>
            <StatCard
              title="Running Now"
              value={runtime?.running_runs ?? 0}
              icon={<Activity className="h-4 w-4 text-status-running" />}
              accentClass="text-status-running"
            />
            <StatCard
              title={`Succeeded (${windowLabel})`}
              value={overview?.succeeded_runs ?? 0}
              icon={<CheckCircle className="h-4 w-4 text-status-succeeded" />}
              accentClass="text-status-succeeded"
            />
            <StatCard
              title={`Failed (${windowLabel})`}
              value={overview?.failed_runs ?? 0}
              icon={<XCircle className="h-4 w-4 text-status-failed" />}
              accentClass="text-status-failed"
            />
            <StatCard
              title="Avg Duration"
	              value={overview ? formatDuration(overview.avg_duration_seconds) : "--"}
              icon={<Timer className="h-4 w-4 text-status-running" />}
              accentClass="text-status-running"
            />
          </>
        )}
      </div>

      {/* Metrics stat cards (powered by the dashboard snapshot) */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3">
        <StatCard
          title="Success Rate"
          value={overview ? `${overview.success_rate.toFixed(1)}%` : "--"}
          icon={<TrendingUp className="h-4 w-4 text-status-succeeded" />}
          accentClass="text-status-succeeded"
        />
        <StatCard
          title="Review Rework Rate"
          value={overview ? `${overview.review_rework_rate.toFixed(1)}%` : "--"}
          icon={<RefreshCw className="h-4 w-4 text-status-blocked" />}
          accentClass="text-status-blocked"
          titleTooltip="Percentage of runs in the selected window that required at least one reviewer rework loop."
        />
        <StatCard
          title="Validation Rework Rate"
          value={overview ? `${overview.validation_rework_rate.toFixed(1)}%` : "--"}
          icon={<ShieldCheck className="h-4 w-4 text-status-cancelled" />}
          accentClass="text-status-cancelled"
          titleTooltip="Percentage of runs in the selected window that required at least one validation rework loop."
        />
      </div>

      {/* Runtime metrics row */}
      {showRuntimeSkeletons ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-2">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Active Nodes</CardTitle>
            </CardHeader>
            <CardContent>
              <span className="text-2xl font-bold">{runtime?.running_nodes ?? 0}</span>
              {(runtime?.stale_running_nodes ?? 0) > 0 && (
                <span className="ml-2 text-xs text-status-failed">
                  ({runtime?.stale_running_nodes} stale)
                </span>
              )}
            </CardContent>
          </Card>
	          <TokenWindowCard
	            windowLabel={windowLabel}
	            total={tokens?.derived_total_tokens ?? 0}
	            input={tokens?.input ?? 0}
	            output={tokens?.output ?? 0}
	            reasoning={tokens?.reasoning ?? 0}
          />
        </div>
      )}

      {/* Recent runs */}
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
	          {isLoading && recentRuns.length === 0 ? (
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
	          ) : recentRuns.length === 0 ? (
            allTimeRunCount === 0 ? <EmptyRunsState /> : <WindowEmptyRunsState windowLabel={windowLabel} />
          ) : (
            <div className="max-h-[320px] space-y-1 overflow-auto pr-1">
	              {recentRuns.map((run) => {
	                const governanceState = describeManualGovernanceState(run);
	                const isActionRequired = governanceState.isAwaitingPlanApproval || governanceState.isAwaitingFinalApproval;
	                return (
                <Link
                  key={run.run_id}
                  to="/runs/$runId"
                  params={{ runId: run.run_id }}
	                  className="flex items-center justify-between rounded-md px-3 py-2 transition-colors hover:bg-surface-raised"
	                >
	                  <div className="min-w-0 flex-1">
	                    <p className="text-wrap-anywhere text-sm font-medium">
	                      {run.decree_title || run.run_id}
	                    </p>
	                    <p className="mt-1 text-xs text-foreground-subtle">
	                      Created {formatTimestamp(run.created_at)} · Last Activity {formatTimestamp(run.last_activity_at)}
	                    </p>
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
          )}
        </CardContent>
      </Card>
	        </>
	      )}

      </PageContent>
    </div>
  );
}

/* ── Sub-components ── */

function StatCard({
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

function StatCardSkeleton() {
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

function TokenWindowCard({
  windowLabel,
  total,
  input,
  output,
  reasoning,
}: {
  windowLabel: string;
  total: number;
  input: number;
  output: number;
  reasoning: number;
}) {
  return (
    <Card>
      <CardContent className="space-y-2">
        <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-foreground-muted">
          <Coins className="h-3.5 w-3.5 text-gold" />
          <span>{`Token Count (${windowLabel})`}</span>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
            <Hash className="h-4 w-4 text-gold" />
          </div>
          <div>
            <p className="text-lg font-bold font-mono text-gold">{formatCompactNumber(total)}</p>
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
            <p className="text-lg font-bold font-mono text-status-running">{formatCompactNumber(input)}</p>
            <p className="text-xs text-foreground-muted">Input Tokens</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
            <Brain className="h-4 w-4 text-status-cancelled" />
          </div>
          <div>
            <p className="text-lg font-bold font-mono text-status-cancelled">{formatCompactNumber(reasoning)}</p>
            <p className="text-xs text-foreground-muted">Reasoning Tokens</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-surface-raised">
            <Hash className="h-4 w-4 text-status-succeeded" />
          </div>
          <div>
            <p className="text-lg font-bold font-mono text-status-succeeded">{formatCompactNumber(output)}</p>
            <p className="text-xs text-foreground-muted">Output Tokens</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function formatDashboardWindowLabel(windowMinutes: number): string {
  if (windowMinutes % 1440 === 0) {
    const days = windowMinutes / 1440;
    return days === 1 ? "24h" : `${days}d`;
  }
  if (windowMinutes % 60 === 0) {
    return `${windowMinutes / 60}h`;
  }
  return `${windowMinutes}m`;
}

function DashboardErrorState({ onRetry }: { onRetry: () => void }) {
  return (
    <Card className="border-status-failed/40 bg-status-failed/5">
      <CardContent className="flex flex-col gap-3 py-5 sm:flex-row sm:items-center sm:justify-between">
        <div className="space-y-1">
          <p className="text-sm font-semibold text-foreground">Dashboard unavailable</p>
          <p className="text-sm text-foreground-subtle">
            The dashboard snapshot could not be loaded right now. Retry to restore runtime, run, and token metrics.
          </p>
        </div>
        <Button type="button" onClick={onRetry}>
          Retry
        </Button>
      </CardContent>
    </Card>
  );
}

function InlineRefreshWarning({ message, onRetry }: { message: string; onRetry: () => void }) {
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

/** Format seconds into a human-readable duration like "8m 32s" or "2h 15m". */
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

/** Format large numbers compactly: 1234 → "1.2K", 1234567 → "1.2M". */
function formatCompactNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function EmptyRunsState() {
  return (
    <div className="flex flex-col items-center py-12 text-center">
      <div className="mb-4 flex items-center justify-center rounded-full bg-gold/5 p-4">
        <Crown className="h-8 w-8 text-gold/40" />
      </div>
      <p className="text-sm font-medium text-foreground-muted">
        The kingdom awaits your command
      </p>
      <p className="mt-1 text-xs text-foreground-subtle">
        Issue your first decree to set the realm in motion.
      </p>
      <Link
        to="/decrees"
        className="mt-4 inline-flex items-center gap-2 rounded-md bg-gold/10 px-4 py-2 text-sm font-medium text-gold transition-colors hover:bg-gold/20"
      >
        <Scroll className="h-3.5 w-3.5" />
        Issue Decree
      </Link>
    </div>
  );
}

function WindowEmptyRunsState({ windowLabel }: { windowLabel: string }) {
  return (
    <div className="flex flex-col items-center py-12 text-center">
      <div className="mb-4 flex items-center justify-center rounded-full bg-surface-raised p-4">
        <Scroll className="h-8 w-8 text-foreground-subtle" />
      </div>
      <p className="text-sm font-medium text-foreground-muted">
        {`No runs in the last ${windowLabel}`}
      </p>
      <p className="mt-1 text-xs text-foreground-subtle">
        Your run history is still available. Issue a new decree or inspect the full archive.
      </p>
      <Link
        to="/runs"
        className="mt-4 inline-flex items-center gap-2 rounded-md bg-surface-raised px-4 py-2 text-sm font-medium text-foreground transition-colors hover:bg-surface-raised/80"
      >
        <Scroll className="h-3.5 w-3.5" />
        View Run History
      </Link>
    </div>
  );
}
