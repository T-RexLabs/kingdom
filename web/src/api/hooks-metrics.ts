import { useQuery } from "@tanstack/react-query";
import {
  analyticsRangeParams,
  analyticsTimeRangeIdentity,
  DEFAULT_ANALYTICS_TIME_RANGE,
  type AnalyticsTimeRange,
} from "@/lib/analytics-time-range";
import { ApiError, apiFetch, shouldRetryApiQuery } from "./client";
import { pollingRefetchQueryOptions } from "./query-freshness";
import type {
  DashboardSnapshot,
  HealthResponse,
  AnalyticsExecutionDetailSnapshot,
  AnalyticsSnapshot,
  RuntimeMetrics,
} from "./hooks-types";

const HEALTH_REFETCH_INTERVAL_MS = 10_000;
const RUNTIME_METRICS_REFETCH_INTERVAL_MS = 5_000;
const DASHBOARD_SNAPSHOT_REFETCH_INTERVAL_MS = 5_000;
const REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS = 5_000;

export const dashboardSnapshotQueryKey = ["dashboard", "snapshot"] as const;
export const analyticsQueryKey = ["analytics"] as const;
export const runtimeMetricsQueryKey = ["metrics", "runtime"] as const;
export const analyticsSnapshotQueryKey = (range: AnalyticsTimeRange) =>
  ["analytics", "snapshot", analyticsTimeRangeIdentity(range)] as const;
export const analyticsExecutionDetailQueryKey = (range: AnalyticsTimeRange, runId: string | null) =>
  runId
    ? ["analytics", "execution-detail", "run", runId] as const
    : ["analytics", "execution-detail", "window", analyticsTimeRangeIdentity(range)] as const;

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: () => apiFetch<HealthResponse>("/health"),
    ...pollingRefetchQueryOptions,
    refetchInterval: HEALTH_REFETCH_INTERVAL_MS,
    staleTime: HEALTH_REFETCH_INTERVAL_MS,
  });
}

export function useDashboardSnapshot() {
  return useQuery({
    queryKey: dashboardSnapshotQueryKey,
    queryFn: () => apiFetch<DashboardSnapshot>("/dashboard/snapshot"),
    ...pollingRefetchQueryOptions,
    refetchInterval: DASHBOARD_SNAPSHOT_REFETCH_INTERVAL_MS,
    staleTime: DASHBOARD_SNAPSHOT_REFETCH_INTERVAL_MS,
  });
}

export function useRuntimeMetrics() {
  return useQuery({
    queryKey: runtimeMetricsQueryKey,
    queryFn: ({ signal }) => apiFetch<RuntimeMetrics>("/metrics/runtime", { signal }),
    ...pollingRefetchQueryOptions,
    refetchInterval: RUNTIME_METRICS_REFETCH_INTERVAL_MS,
    staleTime: RUNTIME_METRICS_REFETCH_INTERVAL_MS,
  });
}

export function useAnalyticsSnapshot(range: AnalyticsTimeRange = DEFAULT_ANALYTICS_TIME_RANGE) {
  return useQuery({
    queryKey: analyticsSnapshotQueryKey(range),
    queryFn: ({ signal }) => apiFetch<AnalyticsSnapshot>("/analytics/snapshot", {
      params: analyticsRangeParams(range),
      signal,
    }),
    ...pollingRefetchQueryOptions,
    refetchInterval: REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS,
    staleTime: REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS,
  });
}

export function useAnalyticsExecutionDetailSnapshot(range: AnalyticsTimeRange = DEFAULT_ANALYTICS_TIME_RANGE, runId: string | null = null) {
  return useQuery({
    queryKey: analyticsExecutionDetailQueryKey(range, runId),
    queryFn: ({ signal }) => apiFetch<AnalyticsExecutionDetailSnapshot>("/analytics/execution-detail", {
			params: runId
				? { run_id: runId }
				: analyticsRangeParams(range),
      signal,
    }),
    ...pollingRefetchQueryOptions,
    refetchInterval: (query) => {
      if (runId && query.state.error instanceof ApiError && query.state.error.status === 404) {
        return false;
      }
      return REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS;
    },
    staleTime: REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS,
    retry: retryAnalyticsExecutionDetailQuery,
  });
}

function retryAnalyticsExecutionDetailQuery(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && error.status === 404) {
    return false;
  }
  return shouldRetryApiQuery(failureCount, error, 3);
}
