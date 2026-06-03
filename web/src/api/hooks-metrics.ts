import { useQuery } from "@tanstack/react-query";
import { ApiError, apiFetch } from "./client";
import { pollingRefetchQueryOptions } from "./query-freshness";
import type {
  DashboardSnapshot,
  HealthResponse,
  RealmAnalyticsExecutionDetailSnapshot,
  RealmAnalyticsSnapshot,
  RuntimeMetrics,
} from "./hooks-types";

const HEALTH_REFETCH_INTERVAL_MS = 10_000;
const RUNTIME_METRICS_REFETCH_INTERVAL_MS = 5_000;
const DASHBOARD_SNAPSHOT_REFETCH_INTERVAL_MS = 5_000;
const REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS = 5_000;

export const dashboardSnapshotQueryKey = ["dashboard", "snapshot"] as const;
export const realmAnalyticsQueryKey = ["realm-analytics"] as const;
export const runtimeMetricsQueryKey = ["metrics", "runtime"] as const;
export const realmAnalyticsSnapshotQueryKey = (windowMinutes: number) =>
  ["realm-analytics", "snapshot", windowMinutes] as const;
export const realmAnalyticsExecutionDetailQueryKey = (windowMinutes: number, runId: string | null) =>
  runId
    ? ["realm-analytics", "execution-detail", "run", runId] as const
    : ["realm-analytics", "execution-detail", "window", windowMinutes] as const;

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

export function useRealmAnalyticsSnapshot(windowMinutes = 1440) {
  return useQuery({
    queryKey: realmAnalyticsSnapshotQueryKey(windowMinutes),
    queryFn: ({ signal }) => apiFetch<RealmAnalyticsSnapshot>("/realm-analytics/snapshot", {
      params: {
        window_minutes: windowMinutes,
      },
      signal,
    }),
    ...pollingRefetchQueryOptions,
    refetchInterval: REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS,
    staleTime: REALM_ANALYTICS_SNAPSHOT_REFETCH_INTERVAL_MS,
  });
}

export function useRealmAnalyticsExecutionDetailSnapshot(windowMinutes = 1440, runId: string | null = null) {
  return useQuery({
    queryKey: realmAnalyticsExecutionDetailQueryKey(windowMinutes, runId),
    queryFn: ({ signal }) => apiFetch<RealmAnalyticsExecutionDetailSnapshot>("/realm-analytics/execution-detail", {
			params: runId
				? { run_id: runId }
				: { window_minutes: windowMinutes },
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
    retry: retryRealmAnalyticsExecutionDetailQuery,
  });
}

function retryRealmAnalyticsExecutionDetailQuery(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && error.status === 404) {
    return false;
  }
  return failureCount < 3;
}
