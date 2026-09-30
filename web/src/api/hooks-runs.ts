import { skipToken, useInfiniteQuery, useMutation, useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { ApiError, apiFetch, shouldRetryApiQuery } from "./client";
import { historyItems } from "./history-pages";
import { useActivityHistory } from "./hooks-activity";
import { dashboardSnapshotQueryKey, analyticsQueryKey, runtimeMetricsQueryKey } from "./hooks-metrics";
import { manualRefetchQueryOptions, refetchOnMountQueryOptions, streamOwnedCollectionQueryOptions } from "./query-freshness";
import { RUNS_STATE_FILTERS, type RunsStateFilter } from "@/lib/run-filters";
import type {
  AgentsResponse,
  CancelRunRequest,
  CreateWorkOrderRequest,
  CreateWorkOrderResult,
  CreateFinalApprovalRequest,
  CreateFinalApprovalResult,
  CreatePlanApprovalRequest,
  CreatePlanApprovalResult,
  CreateRunRequest,
  WorkOrderDetail,
  GovernanceDecisionsResponse,
  MailboxFilters,
  MailboxMessageDetail,
  MailboxMessagesResponse,
  MutationWithBody,
  MutationWithValue,
  ProviderModelsResponse,
  WorkspaceCapabilities,
  RetryNodeResult,
	RetryExecutionStopResult,
  RerunFromNodeResult,
	RolesResponse,
	RunAgentsResponse,
	RunExecutionDetail,
	RunExecutionsResponse,
	RunNodeAttemptsResponse,
	RunGraphResponse,
	RunDetailSummary,
	RunListResponse,
  RunPlanSnapshot,
  RunPlanReviewsResponse,
  RunSummary,
  RunTimelineResponse,
} from "./hooks-types";

export type { RunsStateFilter } from "@/lib/run-filters";

export interface RunsCollectionQueryDescriptor {
	infinite: boolean;
	limit: number | null;
	search: string | null;
	stateFilter: RunsStateFilter | null;
}

interface RunsQueryOptions {
  stateFilter?: RunsStateFilter;
  limit?: number;
  search?: string;
}

interface InfiniteRunsQueryOptions extends RunsQueryOptions {
  enabled?: boolean;
}

const RUNS_COLLECTION_FALLBACK_REFETCH_INTERVAL_MS = 30_000;
const RUNS_COLLECTION_STATE_FILTERS = new Set<RunsStateFilter>(RUNS_STATE_FILTERS);

export const runQueryKeys = {
  detail: (runId: string) => ["runs", runId] as const,
  plan: (runId: string) => ["runs", runId, "plan"] as const,
	governanceSummary: (runId: string) => ["runs", runId, "governance-summary"] as const,
	governanceHistory: (runId: string) => ["runs", runId, "governance-decisions", "history"] as const,
	graph: (runId: string) => ["runs", runId, "graph"] as const,
	executions: (runId: string) => ["runs", runId, "executions"] as const,
	nodeAttemptsRoot: (runId: string) => ["runs", runId, "nodes"] as const,
	nodeAttempts: (runId: string, nodeId: string) => ["runs", runId, "nodes", nodeId, "attempts"] as const,
	executionDetail: (runId: string, attemptId: string) => ["runs", runId, "executions", "detail", attemptId] as const,
	executionActivity: (runId: string, attemptId: string) => ["runs", runId, "executions", "activity", attemptId] as const,
	timelineRoot: (runId: string) => ["runs", runId, "timeline"] as const,
	timeline: (runId: string, limit: number) => ["runs", runId, "timeline", limit] as const,
	agents: (runId: string) => ["runs", runId, "agents"] as const,
  mailboxRoot: (runId: string) => ["runs", runId, "mailbox"] as const,
  mailboxList: (runId: string, communicationType: string) =>
    ["runs", runId, "mailbox", "list", communicationType] as const,
  mailboxDetail: (runId: string, messageId: string) =>
    ["runs", runId, "mailbox", "detail", messageId] as const,
};

export const providerModelsQueryKey = ["provider-models"] as const;

type RunDetailRecoveryRefetchType = "active" | "inactive" | "all" | "none";
export type RunQuerySurface =
	| "detail"
	| "plan"
	| "governanceSummary"
	| "governanceHistory"
	| "graph"
	| "executions"
	| "timeline"
	| "agents"
	| "mailbox";

interface RunDetailRecoveryInvalidation {
	queryKey: readonly unknown[];
	exact: boolean;
}

const RUN_DETAIL_RECOVERY_SURFACES: readonly RunQuerySurface[] = [
	"detail",
	"plan",
	"governanceSummary",
	"governanceHistory",
	"graph",
	"executions",
	"timeline",
	"agents",
	"mailbox",
];

function runQuerySurfaceInvalidation(runId: string, surface: RunQuerySurface): RunDetailRecoveryInvalidation {
	switch (surface) {
		case "detail":
			return { queryKey: runQueryKeys.detail(runId), exact: true };
		case "plan":
			return { queryKey: runQueryKeys.plan(runId), exact: true };
		case "governanceSummary":
			return { queryKey: runQueryKeys.governanceSummary(runId), exact: true };
		case "governanceHistory":
			return { queryKey: runQueryKeys.governanceHistory(runId), exact: true };
		case "graph":
			return { queryKey: runQueryKeys.graph(runId), exact: true };
		case "executions":
			return { queryKey: runQueryKeys.executions(runId), exact: false };
		case "timeline":
			return { queryKey: runQueryKeys.timelineRoot(runId), exact: false };
		case "agents":
			return { queryKey: runQueryKeys.agents(runId), exact: true };
		case "mailbox":
			return { queryKey: runQueryKeys.mailboxRoot(runId), exact: false };
	}
}

export function runQuerySurfaceInvalidations(runId: string, surfaces: readonly RunQuerySurface[]): RunDetailRecoveryInvalidation[] {
	const uniqueSurfaces = Array.from(new Set(surfaces));
	return uniqueSurfaces.map((surface) => runQuerySurfaceInvalidation(runId, surface));
}

export function runDetailRecoveryInvalidations(runId: string): RunDetailRecoveryInvalidation[] {
	return runQuerySurfaceInvalidations(runId, RUN_DETAIL_RECOVERY_SURFACES);
}

export function invalidateRunQuerySurfaces(
	qc: QueryClient,
	runId: string,
	surfaces: readonly RunQuerySurface[],
	refetchType: RunDetailRecoveryRefetchType = "active",
): void {
	for (const invalidation of runQuerySurfaceInvalidations(runId, surfaces)) {
		void qc.invalidateQueries({ ...invalidation, refetchType });
	}
	if (surfaces.includes("executions")) {
		void qc.invalidateQueries({ queryKey: runQueryKeys.nodeAttemptsRoot(runId), exact: false, refetchType });
	}
}

export function invalidateRunDetailRecoveryQueries(
	qc: QueryClient,
	runId: string,
	refetchType: RunDetailRecoveryRefetchType = "active",
): void {
	invalidateRunQuerySurfaces(qc, runId, RUN_DETAIL_RECOVERY_SURFACES, refetchType);
}

export function runsCollectionQueryKey(options?: RunsQueryOptions) {
  return [
    "runs",
    "collection",
    options?.stateFilter ?? null,
    options?.limit ?? null,
    normalizeRunsSearch(options?.search),
  ] as const;
}

export function infiniteRunsCollectionQueryKey(options?: RunsQueryOptions) {
  return [
    "runs",
    "collection",
    "infinite",
    options?.stateFilter ?? null,
    options?.limit ?? null,
    normalizeRunsSearch(options?.search),
  ] as const;
}

export function isRunsCollectionQueryKey(queryKey: readonly unknown[]): boolean {
	return parseRunsCollectionQueryKey(queryKey) !== null;
}

export function parseRunsCollectionQueryKey(queryKey: readonly unknown[]): RunsCollectionQueryDescriptor | null {
	if (queryKey[0] !== "runs" || queryKey[1] !== "collection") {
		return null;
	}

	const infinite = queryKey[2] === "infinite";
	const offset = infinite ? 1 : 0;
	const stateFilter = queryKey[2 + offset];
	const limit = queryKey[3 + offset];
	const search = queryKey[4 + offset];
	const validStateFilter = stateFilter === null
		|| (typeof stateFilter === "string" && RUNS_COLLECTION_STATE_FILTERS.has(stateFilter as RunsStateFilter));
	const validLimit = limit === null || typeof limit === "number";
	const validSearch = search === null || typeof search === "string";
	if (!validStateFilter || !validLimit || !validSearch) {
		return null;
	}
	return {
		infinite,
		limit: limit as number | null,
		search: search as string | null,
		stateFilter: stateFilter as RunsStateFilter | null,
	};
}

export function useRuns(options?: RunsQueryOptions) {
  const search = normalizeRunsSearch(options?.search) ?? undefined;
  const freshnessOptions = search === undefined ? streamOwnedCollectionQueryOptions : refetchOnMountQueryOptions;
  return useQuery({
    // Only unsearched runs collections are stream-owned. Search results rely on
    // fetch-on-open/search semantics because the collection stream cannot
    // authoritatively decide server-side FTS membership.
    queryKey: runsCollectionQueryKey(options),
    queryFn: () => apiFetch<RunListResponse>("/runs", {
      params: {
        state_filter: options?.stateFilter,
        limit: options?.limit,
        search,
      },
    }),
    refetchInterval: search === undefined ? RUNS_COLLECTION_FALLBACK_REFETCH_INTERVAL_MS : false,
    ...freshnessOptions,
  });
}

export function useInfiniteRuns(options?: InfiniteRunsQueryOptions) {
  const search = normalizeRunsSearch(options?.search) ?? undefined;
  const freshnessOptions = search === undefined ? streamOwnedCollectionQueryOptions : refetchOnMountQueryOptions;
  return useInfiniteQuery({
    queryKey: infiniteRunsCollectionQueryKey(options),
    queryFn: ({ pageParam, signal }: { pageParam?: string; signal: AbortSignal }) =>
      apiFetch<RunListResponse>("/runs", {
        params: {
          state_filter: options?.stateFilter,
          limit: options?.limit,
          search,
          cursor: typeof pageParam === "string" && pageParam.trim() ? pageParam : undefined,
        },
        signal,
      }),
    enabled: options?.enabled ?? true,
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    refetchInterval: search === undefined ? RUNS_COLLECTION_FALLBACK_REFETCH_INTERVAL_MS : false,
    ...freshnessOptions,
  });
}

export function usePendingGovernanceRuns(limit = 200) {
  return useRuns({ stateFilter: "awaiting_human_decision", limit });
}

export function useRun(runId: string) {
  return useQuery({
    queryKey: ["runs", runId],
    queryFn: () => apiFetch<RunDetailSummary>(`/runs/${runId}`),
    enabled: !!runId,
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

interface RunDetailQueryOptions {
  enabled?: boolean;
}

export function useRunPlan(runId: string, options?: RunDetailQueryOptions) {
  return useQuery({
    queryKey: runQueryKeys.plan(runId),
    queryFn: () => apiFetch<RunPlanSnapshot>(`/runs/${runId}/plan`),
    enabled: !!runId && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useRunLatestGovernanceDecision(runId: string, options?: RunDetailQueryOptions) {
  return useQuery({
    queryKey: runQueryKeys.governanceSummary(runId),
    queryFn: () =>
      apiFetch<GovernanceDecisionsResponse>(`/runs/${runId}/governance-decisions`, {
        params: { limit: 1 },
      }),
    enabled: !!runId && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useRunGovernanceDecisions(runId: string, options?: RunDetailQueryOptions) {
	return useInfiniteQuery({
		queryKey: runQueryKeys.governanceHistory(runId),
		queryFn: ({ pageParam, signal }: { pageParam?: string; signal: AbortSignal }) =>
			apiFetch<GovernanceDecisionsResponse>(`/runs/${runId}/governance-decisions`, {
				params: {
					limit: 50,
					cursor: typeof pageParam === "string" && pageParam.trim() ? pageParam : undefined,
				},
				signal,
			}),
		enabled: !!runId && (options?.enabled ?? true),
		initialPageParam: undefined as string | undefined,
		getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
		retry: retryRunDetailQuery,
		...manualRefetchQueryOptions,
	});
}

export function useWorkOrder(workOrderId?: string) {
  return useQuery({
    queryKey: ["work-orders", workOrderId],
    queryFn: workOrderId
      ? () => apiFetch<WorkOrderDetail>(`/work-orders/${workOrderId}`)
      : skipToken,
  });
}

export function useRunGraph(runId: string, options?: RunDetailQueryOptions) {
  return useQuery({
    queryKey: runQueryKeys.graph(runId),
    queryFn: () => apiFetch<RunGraphResponse>(`/runs/${runId}/graph`),
    enabled: !!runId && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useRunExecutions(runId: string, options?: RunDetailQueryOptions) {
	return useQuery({
		queryKey: runQueryKeys.executions(runId),
		queryFn: () => apiFetch<RunExecutionsResponse>(`/runs/${runId}/executions`),
		enabled: !!runId && (options?.enabled ?? true),
		retry: retryRunDetailQuery,
		...manualRefetchQueryOptions,
	});
}

export function useInfiniteRunNodeAttempts(runId: string, nodeId?: string | null, options?: RunDetailQueryOptions) {
	const trimmedNodeId = nodeId?.trim() ?? "";
	return useInfiniteQuery({
		queryKey: runQueryKeys.nodeAttempts(runId, trimmedNodeId),
		queryFn: ({ pageParam, signal }: { pageParam?: string; signal: AbortSignal }) =>
			apiFetch<RunNodeAttemptsResponse>(`/runs/${runId}/nodes/${trimmedNodeId}/attempts`, {
				params: {
					limit: 50,
					cursor: typeof pageParam === "string" && pageParam.trim() ? pageParam : undefined,
				},
				signal,
			}),
		enabled: !!runId && trimmedNodeId !== "" && (options?.enabled ?? true),
		initialPageParam: undefined as string | undefined,
		getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
		retry: retryRunDetailQuery,
		...manualRefetchQueryOptions,
	});
}

export function useRunExecution(runId: string, attemptId?: string | null, options?: RunDetailQueryOptions) {
	const trimmedAttemptId = attemptId?.trim() ?? "";
	return useQuery({
		queryKey: runQueryKeys.executionDetail(runId, trimmedAttemptId),
		queryFn: runId && trimmedAttemptId && (options?.enabled ?? true)
			? () => apiFetch<RunExecutionDetail>(`/runs/${runId}/executions/${trimmedAttemptId}`)
			: skipToken,
		retry: retryRunDetailQuery,
		...manualRefetchQueryOptions,
	});
}

// useRunExecutionActivity fetches the durable provider activity (agent reasoning +
// tool calls) for one node attempt. Live activity arrives over the run event stream;
// this is the durable backstop. Poll while the attempt is in-flight, idle otherwise.
export function useRunExecutionActivity(
  runId: string,
  attemptId?: string | null,
  options?: { enabled?: boolean; refetchIntervalMs?: number | false },
) {
  const trimmedAttemptId = attemptId?.trim() ?? "";
  const enabled = (options?.enabled ?? true) && !!runId && trimmedAttemptId !== "";
  return useActivityHistory(enabled ? `/runs/${runId}/executions/${trimmedAttemptId}/activity` : undefined,
    runQueryKeys.executionActivity(runId, trimmedAttemptId),{...options,enabled});
}

export function useTimeline(runId: string, limit = 50, options?: RunDetailQueryOptions) {
  return useInfiniteQuery({
    queryKey: runQueryKeys.timeline(runId, limit),
    queryFn: ({ pageParam, signal }: { pageParam?: string; signal: AbortSignal }) =>
      apiFetch<RunTimelineResponse>(`/runs/${runId}/timeline`, {
        params: {
          limit,
          cursor: typeof pageParam === "string" && pageParam.trim() ? pageParam : undefined,
        },
        signal,
    }),
    enabled: !!runId && (options?.enabled ?? true),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useRoles() {
  return useQuery({
    queryKey: ["roles"],
    queryFn: () => apiFetch<RolesResponse>("/roles"),
    refetchInterval: 30_000,
  });
}

export function useAgents() {
  return useQuery({
    queryKey: ["agents"],
    queryFn: () => apiFetch<AgentsResponse>("/agents"),
    refetchInterval: 5_000,
  });
}

function fetchProviderModels(refresh: boolean, signal?: AbortSignal) {
	return apiFetch<ProviderModelsResponse>("/provider/models", {
		params: { refresh },
		signal,
	});
}

// The model catalog does not poll. Listing models spawns an OpenCode
// subprocess, and a one-minute poll against a one-minute server cache meant an
// open planner tab guaranteed one spawn a minute forever. The catalog only
// changes when the operator changes their OpenCode installation, which is what
// the explicit Retry control is for; the server keeps its own cache warm.
export function useProviderModels(options?: RunDetailQueryOptions) {
  return useQuery({
	queryKey: providerModelsQueryKey,
	queryFn: ({ signal }) => fetchProviderModels(false, signal),
	enabled: options?.enabled ?? true,
  });
}

export function useRefreshProviderModels() {
	const qc = useQueryClient();
	return useMutation({
		mutationFn: () => fetchProviderModels(true),
		onMutate: async () => {
			await qc.cancelQueries({ queryKey: providerModelsQueryKey, exact: true });
			return { dataUpdatedAt: qc.getQueryState(providerModelsQueryKey)?.dataUpdatedAt ?? 0 };
		},
		onSuccess: async (response) => {
			await qc.cancelQueries({ queryKey: providerModelsQueryKey, exact: true });
			qc.setQueryData(providerModelsQueryKey, response);
		},
	});
}

export function useWorkspaceCapabilities() {
  return useQuery({
    queryKey: ["workspace-capabilities"],
    queryFn: () => apiFetch<WorkspaceCapabilities>("/workspace-capabilities"),
    refetchInterval: 60_000,
  });
}

export function useRunAgents(runId: string, options?: RunDetailQueryOptions) {
	return useQuery({
		queryKey: runQueryKeys.agents(runId),
    queryFn: () => apiFetch<RunAgentsResponse>(`/runs/${runId}/agents`),
    enabled: !!runId && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
	});
}

export function useMailbox(runId: string, filters?: MailboxFilters, options?: RunDetailQueryOptions) {
  const communicationType = filters?.communicationType ?? "";
  return useInfiniteQuery({
    queryKey: runQueryKeys.mailboxList(runId, communicationType),
    queryFn: ({ pageParam, signal }) =>
      apiFetch<MailboxMessagesResponse>(`/runs/${runId}/mailbox/messages`, {
        signal,
        params: {
          limit: 100,
          cursor: pageParam,
          communication_type: filters?.communicationType,
        },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor,
    select: (data) => ({ run_id: runId, items: historyItems(data.pages, (item) => item.message_id) }),
    enabled: !!runId && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useRunPlanReviews(runId: string, options?: RunDetailQueryOptions) {
  return useInfiniteQuery({
    queryKey: ["runs", runId, "plan-reviews"],
    queryFn: ({ pageParam, signal }) => apiFetch<RunPlanReviewsResponse>(`/runs/${runId}/plan-reviews`, {
      params: { limit: 50, cursor: pageParam }, signal,
    }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor,
    enabled: Boolean(runId) && (options?.enabled ?? true),
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

export function useMailboxMessage(runId: string, messageId?: string | null, options?: RunDetailQueryOptions) {
  const trimmedMessageId = messageId?.trim() ?? "";
  return useQuery({
    queryKey: runQueryKeys.mailboxDetail(runId, trimmedMessageId),
    queryFn: runId && trimmedMessageId && (options?.enabled ?? true)
      ? () => apiFetch<MailboxMessageDetail>(`/runs/${runId}/mailbox/messages/${trimmedMessageId}`)
      : skipToken,
    retry: retryRunDetailQuery,
    ...manualRefetchQueryOptions,
  });
}

function retryRunDetailQuery(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && error.status === 404) {
    return false;
  }
  return shouldRetryApiQuery(failureCount, error, 3);
}

function normalizeRunsSearch(search?: string): string | null {
  const trimmed = search?.trim() ?? "";
  return trimmed === "" ? null : trimmed.toLowerCase();
}

function invalidateRunDetailQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return qc.invalidateQueries({ queryKey: runQueryKeys.detail(runId) });
}

function invalidateDashboardSnapshotQueries(qc: ReturnType<typeof useQueryClient>) {
	return qc.invalidateQueries({ queryKey: dashboardSnapshotQueryKey });
}

function invalidateAnalyticsQueries(qc: ReturnType<typeof useQueryClient>) {
	return qc.invalidateQueries({ queryKey: analyticsQueryKey });
}

function invalidateRuntimeMetricsQueries(qc: ReturnType<typeof useQueryClient>) {
	return qc.invalidateQueries({ queryKey: runtimeMetricsQueryKey });
}

function invalidateDashboardAnalyticsQueries(qc: ReturnType<typeof useQueryClient>) {
	return Promise.all([
		invalidateDashboardSnapshotQueries(qc),
		invalidateAnalyticsQueries(qc),
	]);
}

function invalidateRunsCollectionQueries(qc: ReturnType<typeof useQueryClient>) {
	return qc.invalidateQueries({ queryKey: ["runs", "collection"] });
}

function invalidateRunPlanQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return qc.invalidateQueries({ queryKey: runQueryKeys.plan(runId) });
}

function invalidateRunGovernanceQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return Promise.all([
		qc.invalidateQueries({ queryKey: runQueryKeys.governanceSummary(runId) }),
		qc.invalidateQueries({ queryKey: runQueryKeys.governanceHistory(runId) }),
	]);
}

function invalidateRunExecutionAffectedQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return Promise.all(
		runQuerySurfaceInvalidations(runId, ["graph", "executions", "timeline", "agents"]).map((invalidation) =>
			qc.invalidateQueries(invalidation),
		),
	);
}

function invalidateRunMailboxQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return qc.invalidateQueries({ queryKey: runQueryKeys.mailboxRoot(runId) });
}

function invalidateRunGovernanceMutationQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return Promise.all([
		invalidateRunsCollectionQueries(qc),
		invalidateRunDetailQueries(qc, runId),
		invalidateRunPlanQueries(qc, runId),
		invalidateRunGovernanceQueries(qc, runId),
		invalidateRunExecutionAffectedQueries(qc, runId),
	]);
}

function invalidateRunRerunQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return Promise.all([
		invalidateRunGovernanceMutationQueries(qc, runId),
		invalidateRunMailboxQueries(qc, runId),
	]);
}

function invalidateRunCancelQueries(qc: ReturnType<typeof useQueryClient>, runId: string) {
	return Promise.all([
		invalidateRunsCollectionQueries(qc),
		invalidateRunDetailQueries(qc, runId),
		invalidateRunExecutionAffectedQueries(qc, runId),
		invalidateRunMailboxQueries(qc, runId),
	]);
}

export function useCreateWorkOrder() {
  const qc = useQueryClient();
	return useMutation({
		mutationFn: ({ body, key }: MutationWithBody<CreateWorkOrderRequest>) =>
			apiFetch<CreateWorkOrderResult>("/work-orders", {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(body),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunsCollectionQueries(qc),
				invalidateDashboardAnalyticsQueries(qc),
			]);
		},
	});
}

export function useCreateRun() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CreateRunRequest>) =>
      apiFetch<RunSummary>("/runs", {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(body),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunsCollectionQueries(qc),
				invalidateDashboardAnalyticsQueries(qc),
				invalidateRuntimeMetricsQueries(qc),
			]);
		},
	});
}

export function useCancelRun(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CancelRunRequest>) =>
      apiFetch<RunSummary>(`/runs/${runId}/cancel`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(body),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunCancelQueries(qc, runId),
				invalidateDashboardAnalyticsQueries(qc),
				invalidateRuntimeMetricsQueries(qc),
			]);
		},
	});
}

export function useRetryExecutionStop(runId: string) {
	const qc = useQueryClient();
	return useMutation({
		mutationFn: ({ value: attemptId, key }: MutationWithValue<string>) =>
			apiFetch<RetryExecutionStopResult>(`/runs/${runId}/executions/${attemptId}/stop-retry`, {
				method: "POST",
				headers: { "Idempotency-Key": key },
			}),
		onSuccess: () => {
			invalidateRunDetailRecoveryQueries(qc, runId);
		},
	});
}

export function useCreateFinalApproval(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CreateFinalApprovalRequest>) =>
      apiFetch<CreateFinalApprovalResult>(`/runs/${runId}/final-approval`, {
        method: "POST",
        headers: {
          "Idempotency-Key": key,
        },
        body: JSON.stringify(body),
      }),
		onSuccess: async (_data, variables) => {
			const invalidations: Array<Promise<unknown>> = [
				invalidateRunGovernanceMutationQueries(qc, runId),
				invalidateDashboardAnalyticsQueries(qc),
			];
			if (variables.body.action === "rework") {
				invalidations.push(invalidateRuntimeMetricsQueries(qc));
			}
			await Promise.all(invalidations);
		},
	});
}

export function useCreatePlanApproval(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CreatePlanApprovalRequest>) =>
      apiFetch<CreatePlanApprovalResult>(`/runs/${runId}/plan-approval`, {
        method: "POST",
        headers: {
          "Idempotency-Key": key,
        },
        body: JSON.stringify(body),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunGovernanceMutationQueries(qc, runId),
				invalidateDashboardAnalyticsQueries(qc),
				invalidateRuntimeMetricsQueries(qc),
			]);
		},
	});
}

export function useRetryNode(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ value, key }: MutationWithValue<string>) =>
      apiFetch<RetryNodeResult>(`/runs/${runId}/nodes/${value}/retry`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ reason: "manual retry from run detail" }),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunsCollectionQueries(qc),
				invalidateRunDetailQueries(qc, runId),
				invalidateRunExecutionAffectedQueries(qc, runId),
				invalidateDashboardAnalyticsQueries(qc),
				invalidateRuntimeMetricsQueries(qc),
			]);
		},
	});
}

export function useRerunFromNode(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ value, key }: MutationWithValue<string>) =>
      apiFetch<RerunFromNodeResult>(`/runs/${runId}/rerun-from/${value}`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ reason: "manual rerun from run detail" }),
      }),
		onSuccess: async () => {
			await Promise.all([
				invalidateRunRerunQueries(qc, runId),
				invalidateDashboardAnalyticsQueries(qc),
				invalidateRuntimeMetricsQueries(qc),
			]);
		},
	});
}
