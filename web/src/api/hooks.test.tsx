import { focusManager, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { plannerKeys } from "@/features/decrees/planner-query-keys";
import { applyPlanningSessionFields, applyGeneratedPlanningSessionTitleIfPlaceholder, compareAndSwapPlanningSessionTitle } from "@/features/decrees/planner-session-cache";
import {
  SUPERSEDED,
  useCancelRun,
  useCreateDecree,
  useCreateFinalApproval,
  useCreatePlanApproval,
  useCreateRun,
  useMailbox,
  useMailboxMessage,
  ACP_RUN_REFETCH_INTERVAL_MS,
  useProviderModels,
  useRefreshProviderModels,
  useRunExecution,
  useRunExecutions,
	useInfiniteRunNodeAttempts,
  useRealmAnalyticsExecutionDetailSnapshot,
  useRealmAnalyticsSnapshot,
  useRerunFromNode,
  useRun,
  useRunGraph,
  useRunLatestGovernanceDecision,
  useRunPlan,
  useRetryNode,
  useRoles,
  useRuns,
  useTimeline,
  usePlanningOperation,
  usePlanningSessions,
  usePlanningContextEntries,
  useRunPlanReviews,
  useUpdatePlanningSessionTitle,
  type PlanningSessionDetail,
} from "./hooks";
import { dashboardSnapshotQueryKey, realmAnalyticsQueryKey, runtimeMetricsQueryKey } from "./hooks-metrics";
import { providerModelsQueryKey, runQueryKeys } from "./hooks-runs";

// Mock @/api/client so we can drive request/response timing.
const apiFetchMock = vi.fn();
vi.mock("./client", () => ({
  ApiError: class ApiError extends Error {
    public readonly status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
  apiFetch: (...args: unknown[]) => apiFetchMock(...args),
  shouldRetryApiQuery: (failureCount: number, _error: unknown, maxRetries: number) => failureCount < maxRetries,
}));

function buildDetail(overrides: Partial<PlanningSessionDetail> = {}): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    acp_session_id: "8cfa85f2-f95a-53ec-ae58-2c46f21495dc",
    title: "fresh",
    title_state: "custom",
    auto_title_eligible: false,
    lifecycle_state: "active",
    conversation_state: "ready",
    active_leaf_message_id: "msg_1",
    current_run_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "snap_1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
    external_sources: overrides.external_sources ?? [],
  };
}

function buildRun(overrides: Record<string, unknown> = {}) {
  return {
    run_id: "run-1",
    decree_id: "decree-1",
    decree_title: "Fresh run",
    workflow_id: "royal-decree-v1",
    autonomous_approvals: true,
    status: "running",
    display_state: "running",
    plan_state: "approved",
    created_at: "2026-03-01T12:00:00Z",
    updated_at: "2026-03-01T12:00:00Z",
	    tab_counts: { dag_nodes: 1, agents: 1, activity_events: 1, executions: 1, messages: 1 },
    failed_nodes: [],
    ...overrides,
  };
}

function makeWrapper(qc: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}

// Each test gets a fresh QueryClient. The wrapper closure captures it
// so renderHook's wrapper has the right cache.
let qc: QueryClient;

beforeEach(() => {
  apiFetchMock.mockReset();
  qc = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
});

afterEach(() => {
  vi.useRealTimers();
  focusManager.setFocused(undefined);
  qc.clear();
});

describe("cursor history queries", () => {
  it("loads planning sessions beyond 200 and patches titles on older cached pages", async () => {
    const items = Array.from({ length: 245 }, (_, index) => buildDetail({ planning_session_id: `ps_${index}`, title: `Session ${index}`, title_state: "placeholder" }));
    apiFetchMock
      .mockResolvedValueOnce({ items: items.slice(0, 100), next_cursor: "page-2" })
      .mockResolvedValueOnce({ items: items.slice(100, 200), next_cursor: "page-3" })
      .mockResolvedValueOnce({ items: items.slice(200) });
    const { result } = renderHook(() => usePlanningSessions(), { wrapper: makeWrapper(qc) });
    await waitFor(() => expect(result.current.data?.items).toHaveLength(100));
    await act(async () => { await result.current.fetchNextPage(); });
    await act(async () => { await result.current.fetchNextPage(); });
    await waitFor(() => expect(result.current.data?.items).toHaveLength(245));
    expect(result.current.hasNextPage).toBe(false);
    expect(apiFetchMock).toHaveBeenNthCalledWith(3, "/planning-sessions", expect.objectContaining({ params: { limit: 100, cursor: "page-3" } }));
    act(() => applyGeneratedPlanningSessionTitleIfPlaceholder(qc, "ps_244", "Generated"));
    await waitFor(() => expect(result.current.data?.items[244].title).toBe("Generated"));
    act(() => applyPlanningSessionFields(qc, "ps_244", { title: "Custom", title_state: "custom" }));
    await waitFor(() => expect(result.current.data?.items[244].title).toBe("Custom"));
    act(() => compareAndSwapPlanningSessionTitle(qc, "ps_244", "Custom", "custom", "Generated", "generated"));
    await waitFor(() => expect(result.current.data?.items[244].title).toBe("Generated"));
  });

  it("loads mailbox history beyond 200 and starts a fresh cursor for changed filters", async () => {
    const items = Array.from({ length: 245 }, (_, index) => ({ message_id: `message-${index}` }));
    apiFetchMock
      .mockResolvedValueOnce({ run_id: "run-1", items: items.slice(0, 100), next_cursor: "older-1" })
      .mockResolvedValueOnce({ run_id: "run-1", items: items.slice(100, 200), next_cursor: "older-2" })
      .mockResolvedValueOnce({ run_id: "run-1", items: [items[199], ...items.slice(200)] })
      .mockResolvedValueOnce({ run_id: "run-1", items: [] });
    const { result, rerender } = renderHook(({ communicationType }) => useMailbox("run-1", { communicationType }), {
      wrapper: makeWrapper(qc), initialProps: { communicationType: "" },
    });
    await waitFor(() => expect(result.current.hasNextPage).toBe(true));
    await act(async () => { await result.current.fetchNextPage(); });
    await act(async () => { await result.current.fetchNextPage(); });
    await waitFor(() => expect(result.current.data?.items).toHaveLength(245));
    expect(result.current.hasNextPage).toBe(false);
    rerender({ communicationType: "response_received" });
    await waitFor(() => expect(result.current.data?.items).toHaveLength(0));
    expect(apiFetchMock).toHaveBeenLastCalledWith("/runs/run-1/mailbox/messages", expect.objectContaining({
      params: { limit: 100, cursor: undefined, communication_type: "response_received" },
    }));
  });

  it("loads all plan reviews beyond 100", async () => {
    const items = Array.from({ length: 125 }, (_, index) => ({ plan_review_id: `review-${index}` }));
    apiFetchMock
      .mockResolvedValueOnce({ run_id: "run-1", items: items.slice(0, 50), next_cursor: "review-2" })
      .mockResolvedValueOnce({ run_id: "run-1", items: items.slice(50, 100), next_cursor: "review-3" })
      .mockResolvedValueOnce({ run_id: "run-1", items: items.slice(100) });
    const { result } = renderHook(() => useRunPlanReviews("run-1"), { wrapper: makeWrapper(qc) });
    await waitFor(() => expect(result.current.hasNextPage).toBe(true));
    await act(async () => { await result.current.fetchNextPage(); });
    await act(async () => { await result.current.fetchNextPage(); });
    await waitFor(() => expect(result.current.data?.pages.flatMap((page) => page.items)).toHaveLength(125));
    expect(result.current.hasNextPage).toBe(false);
  });

  it("keeps context entry pages scoped to snapshot and kind", async () => {
    apiFetchMock
      .mockResolvedValueOnce({ items: [{ path: "first" }], next_cursor: "entry-cursor" })
      .mockResolvedValueOnce({ items: [{ path: "last" }] })
      .mockResolvedValueOnce({ items: [{ path: "excluded" }] });
    const { result, rerender } = renderHook(({ kind }: { kind: "included" | "excluded" }) => usePlanningContextEntries("ps_1", "snapshot-1", kind), {
      wrapper: makeWrapper(qc), initialProps: { kind: "included" },
    });
    await waitFor(() => expect(result.current.hasNextPage).toBe(true));
    await act(async () => { await result.current.fetchNextPage(); });
    expect(apiFetchMock).toHaveBeenLastCalledWith("/planning-sessions/ps_1/context/snapshot-1/entries", expect.objectContaining({ params: { kind: "included", limit: 100, cursor: "entry-cursor" } }));
    rerender({ kind: "excluded" });
    await waitFor(() => expect(result.current.data?.pages[0].items[0].path).toBe("excluded"));
    expect(apiFetchMock).toHaveBeenLastCalledWith("/planning-sessions/ps_1/context/snapshot-1/entries", expect.objectContaining({ params: { kind: "excluded", limit: 100, cursor: undefined } }));
  });
});

describe("usePlanningOperation polling", () => {
  const acpRun = (status: "created" | "in-progress" | "completed") => ({
    run_id: "acp-run-1",
    session_id: "acp-session-1",
    planning_session_id: "planning-session-1",
    agent_name: "planner-reply" as const,
    status,
    created_at: "2026-08-24T10:00:00Z",
  });

  it("discovers completion by polling and stops once the run is terminal", async () => {
    vi.useFakeTimers();
    apiFetchMock
      .mockResolvedValueOnce(acpRun("in-progress"))
      .mockResolvedValueOnce(acpRun("completed"));

    const { result } = renderHook(() => usePlanningOperation("acp-run-1"), {
      wrapper: makeWrapper(qc),
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.data?.status).toBe("in-progress");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(ACP_RUN_REFETCH_INTERVAL_MS + 1);
    });
    expect(result.current.data?.status).toBe("completed");
    expect(apiFetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(ACP_RUN_REFETCH_INTERVAL_MS * 2);
    });
    expect(apiFetchMock).toHaveBeenCalledTimes(2);
  });

  it("cleans up nonterminal polling when its observer unmounts", async () => {
    vi.useFakeTimers();
    apiFetchMock.mockResolvedValue(acpRun("in-progress"));

    const { unmount } = renderHook(() => usePlanningOperation("acp-run-1"), {
      wrapper: makeWrapper(qc),
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(apiFetchMock).toHaveBeenCalledTimes(1);

    unmount();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(ACP_RUN_REFETCH_INTERVAL_MS * 2);
    });
    expect(apiFetchMock).toHaveBeenCalledTimes(1);
  });

  it("keeps polling after a query error so the run can recover", async () => {
    vi.useFakeTimers();
    const pollingClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    apiFetchMock
      .mockRejectedValueOnce(new Error("connection reset"))
      .mockResolvedValueOnce(acpRun("completed"));

    const { result } = renderHook(() => usePlanningOperation("acp-run-1"), {
      wrapper: makeWrapper(pollingClient),
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(result.current.isError).toBe(true);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(ACP_RUN_REFETCH_INTERVAL_MS + 1);
    });
    expect(result.current.data?.status).toBe("completed");
    expect(result.current.isError).toBe(false);
    expect(apiFetchMock).toHaveBeenCalledTimes(2);

    pollingClient.clear();
  });
});

describe("useUpdatePlanningSessionTitle — same-slice ordering", () => {
  it("serializes PATCHes so the newer response is canonical and applied last", async () => {

    // Pre-seed the detail cache so applyPlanningSessionFields has a
    // live entry to patch.
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildDetail({ title: "starting" }));

    // Each invocation of apiFetchMock returns a deferred Promise.
    // We don't pre-bind the resolvers because mockImplementation
    // closures fire synchronously when invoked, but resolver-binding
    // races with renderHook's async act in the test runner.
    type Deferred = { promise: Promise<PlanningSessionDetail>; resolve: (v: PlanningSessionDetail) => void };
    const pending: Deferred[] = [];
    apiFetchMock.mockImplementation(() => {
      let resolve!: (v: PlanningSessionDetail) => void;
      const promise = new Promise<PlanningSessionDetail>((r) => {
        resolve = r;
      });
      pending.push({ promise, resolve });
      return promise;
    });

    const { result } = renderHook(() => useUpdatePlanningSessionTitle("ps_1"), {
      wrapper: makeWrapper(qc),
    });

    // Queue two rapid edits. Only the first request is dispatched until it
    // settles, preventing canonical server writes from landing out of order.
    act(() => {
      result.current.mutate({ title: "older", key: "k1" });
      result.current.mutate({ title: "newer", key: "k2" });
    });

    await waitFor(() => {
      expect(pending).toHaveLength(1);
    });

    await act(async () => {
      pending[0].resolve(buildDetail({ title: "older" }));
    });

    await waitFor(() => {
      expect(pending).toHaveLength(2);
    });
    await act(async () => {
      pending[1].resolve(buildDetail({ title: "newer" }));
    });

    await waitFor(() => {
      expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("newer");
    });
  });

  it("does not abort a dispatched PATCH when a newer edit queues", async () => {
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildDetail({ title: "starting" }));

    let firstSignal: AbortSignal | null = null;
    let resolveFirst!: (value: PlanningSessionDetail) => void;
    apiFetchMock.mockImplementationOnce(async (_path: string, init: RequestInit | undefined) => {
      firstSignal = init?.signal ?? null;
      return new Promise<PlanningSessionDetail>((resolve) => {
        resolveFirst = resolve;
      });
    });
    apiFetchMock.mockResolvedValueOnce(buildDetail({ title: "newer" }));

    const { result } = renderHook(() => useUpdatePlanningSessionTitle("ps_1"), {
      wrapper: makeWrapper(qc),
    });

    act(() => {
      result.current.mutate({ title: "older", key: "k1" });
      result.current.mutate({ title: "newer", key: "k2" });
    });
	await waitFor(() => expect(firstSignal).not.toBeNull());
	expect((firstSignal as AbortSignal | null)?.aborted).toBe(false);
	await act(async () => resolveFirst(buildDetail({ title: "older" })));

    await waitFor(() => {
      expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("newer");
    });

    expect(result.current.error).toBeNull();
  });

  it("real failures still surface on the hook's error slot", async () => {
    // Regression guard: SUPERSEDED suppression must not mask actual
    // failures. A genuine network error (not an abort) on the latest
    // request still populates hook.error and lights the chip.
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildDetail({ title: "starting" }));

    apiFetchMock.mockRejectedValueOnce(new Error("Server unreachable"));

    const { result } = renderHook(() => useUpdatePlanningSessionTitle("ps_1"), {
      wrapper: makeWrapper(qc),
    });

    act(() => {
      result.current.mutate({ title: "doomed", key: "k1" });
    });

    await waitFor(() => {
      expect(result.current.error).toBeInstanceOf(Error);
    });
    expect(result.current.error?.message).toBe("Server unreachable");
  });

  it("exports SUPERSEDED as a unique symbol consumers can identify", () => {
    // Prevents accidental string-equality comparisons in callers
    // that need to detect the sentinel.
    expect(typeof SUPERSEDED).toBe("symbol");
    expect(SUPERSEDED).toBe(SUPERSEDED);
  });
});

describe("mailbox query keys", () => {
  it("keeps mailbox list refetchable when no detail message is selected", async () => {
    apiFetchMock.mockResolvedValue({ run_id: "run-1", items: [] });

    const { result } = renderHook(() => ({
      mailbox: useMailbox("run-1", {}, { enabled: true }),
      detail: useMailboxMessage("run-1", null, { enabled: true }),
    }), {
      wrapper: makeWrapper(qc),
    });

    await waitFor(() => {
      expect(result.current.mailbox.isSuccess).toBe(true);
    });

    expect(apiFetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await qc.invalidateQueries({ queryKey: ["runs", "run-1"] });
    });

    await waitFor(() => {
      expect(apiFetchMock).toHaveBeenCalledTimes(2);
    });

    const mailboxQueries = qc
      .getQueryCache()
      .findAll({ queryKey: ["runs", "run-1", "mailbox"] })
      .map((query) => query.queryKey);

    expect(mailboxQueries).toContainEqual(["runs", "run-1", "mailbox", "list", ""]);
    expect(mailboxQueries).toContainEqual(["runs", "run-1", "mailbox", "detail", ""]);
  });

	it("creates stable execution list and detail query keys", async () => {
		apiFetchMock
			.mockResolvedValueOnce({ run_id: "run-1", items: [{ attempt_id: "attempt-1" }] })
			.mockResolvedValueOnce({ attempt_id: "attempt-1", node_id: "delegated_execution_berserker" });

		const { result } = renderHook(() => ({
			executions: useRunExecutions("run-1", { enabled: true }),
			detail: useRunExecution("run-1", "attempt-1", { enabled: true }),
		}), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.executions.isSuccess).toBe(true);
			expect(result.current.detail.isSuccess).toBe(true);
		});

		const executionQueries = qc
			.getQueryCache()
			.findAll({ queryKey: ["runs", "run-1", "executions"] })
			.map((query) => query.queryKey);

		expect(executionQueries).toContainEqual(["runs", "run-1", "executions"]);
		expect(executionQueries).toContainEqual(["runs", "run-1", "executions", "detail", "attempt-1"]);
	});

	it("loads selected node attempt history lazily and follows its scoped cursor", async () => {
		apiFetchMock
			.mockResolvedValueOnce({ run_id: "run-1", node_id: "node-1", items: [{ attempt_id: "attempt-2" }], next_cursor: "cursor-2" })
			.mockResolvedValueOnce({ run_id: "run-1", node_id: "node-1", items: [{ attempt_id: "attempt-1" }] });

		const { result, rerender } = renderHook(
			({ enabled }) => useInfiniteRunNodeAttempts("run-1", "node-1", { enabled }),
			{ initialProps: { enabled: false }, wrapper: makeWrapper(qc) },
		);

		expect(apiFetchMock).not.toHaveBeenCalled();
		rerender({ enabled: true });
		await waitFor(() => expect(result.current.isSuccess).toBe(true));
		expect(apiFetchMock).toHaveBeenLastCalledWith("/runs/run-1/nodes/node-1/attempts", {
			params: { limit: 50, cursor: undefined },
			signal: expect.any(AbortSignal),
		});

		await act(async () => {
			await result.current.fetchNextPage();
		});
		expect(apiFetchMock).toHaveBeenLastCalledWith("/runs/run-1/nodes/node-1/attempts", {
			params: { limit: 50, cursor: "cursor-2" },
			signal: expect.any(AbortSignal),
		});
		expect(qc.getQueryCache().find({ queryKey: runQueryKeys.nodeAttempts("run-1", "node-1") })?.queryKey)
			.toEqual(["runs", "run-1", "nodes", "node-1", "attempts"]);
	});

	it("invalidates active analytics queries after creating a run", async () => {
    apiFetchMock.mockResolvedValue({
      run_id: "run-1",
      decree_id: "decree-1",
      decree_title: "Fresh run",
      workflow_id: "royal-decree-v1",
      status: "pending",
      display_state: "pending",
      plan_state: "draft",
      created_at: "2026-03-01T12:00:00Z",
      updated_at: "2026-03-01T12:00:00Z",
    });

    const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => useCreateRun(), {
      wrapper: makeWrapper(qc),
    });

    await act(async () => {
	    await result.current.mutateAsync({
	      body: {
	        decree_id: "decree-1",
	        publish_mode: "direct",
	        model_selection: { global: { model: "github-copilot/gpt-5.4" } },
	      },
        key: "idem-create-run",
      });
    });

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection"] });
	    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: dashboardSnapshotQueryKey });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: realmAnalyticsQueryKey });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("does not invalidate runtime metrics after creating a decree", async () => {
		apiFetchMock.mockResolvedValue({ decree_id: "decree-1" });

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useCreateDecree(), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				body: {
					decree_title: "Fresh decree",
					decree_text: "Stabilize the realm",
				},
				key: "idem-create-decree",
			});
		});

		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("invalidates runtime metrics after cancelling a run", async () => {
		apiFetchMock.mockResolvedValue(buildRun({ status: "cancelled", display_state: "cancelled" }));

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useCancelRun("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				body: { reason: "Operator cancelled the run" },
				key: "idem-cancel-run",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("invalidates runtime metrics after plan approval can resume execution", async () => {
		apiFetchMock.mockResolvedValue({
			approval_id: "approval-1",
			run_id: "run-1",
			status: "approved",
			recorded_at: "2026-03-01T12:00:00Z",
		});

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useCreatePlanApproval("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				body: {
					action: "confirm",
					plan_revision_id: "planrev-1",
				},
				key: "idem-plan-approval",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("lets retry-node mutations own stale run execution queries", async () => {
		apiFetchMock.mockResolvedValue({ node_key: "execution_review_shieldmaiden" });

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useRetryNode("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				value: "execution_review_shieldmaiden",
				key: "idem-retry-node",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.detail("run-1") });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection"] });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: dashboardSnapshotQueryKey });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: realmAnalyticsQueryKey });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("invalidates runtime metrics after rerunning from a node", async () => {
		apiFetchMock.mockResolvedValue({ run_id: "run-2", from_node: "execution_review_shieldmaiden", rerun_enqueued: true });

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useRerunFromNode("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				value: "execution_review_shieldmaiden",
				key: "idem-rerun-node",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("keeps runtime metrics untouched after final approval confirm", async () => {
		apiFetchMock.mockResolvedValue({
			approval_id: "approval-1",
			run_id: "run-1",
			status: "succeeded",
			recorded_at: "2026-03-01T12:00:00Z",
		});

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useCreateFinalApproval("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				body: { action: "confirm" },
				key: "idem-final-approval-confirm",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("invalidates runtime metrics after final approval rework reopens execution", async () => {
		apiFetchMock.mockResolvedValue({
			approval_id: "approval-1",
			run_id: "run-1",
			status: "running",
			recorded_at: "2026-03-01T12:00:00Z",
		});

		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		const { result } = renderHook(() => useCreateFinalApproval("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await act(async () => {
			await result.current.mutateAsync({
				body: {
					action: "rework",
					reason: "Resume execution",
				},
				key: "idem-final-approval-rework",
			});
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: runtimeMetricsQueryKey });
	});

	it("stops run-detail polling once the cached run is terminal", async () => {
	  vi.useFakeTimers({ shouldAdvanceTime: true });
	  apiFetchMock.mockImplementation((path: string) => {
	    if (path === "/runs/run-1") {
	      return Promise.resolve(buildRun({ status: "succeeded", display_state: "succeeded" }));
	      }
	      if (path === "/runs/run-1/graph") {
	        return Promise.resolve({ run_id: "run-1", nodes: [], edges: [] });
	      }
	      if (path === "/runs/run-1/governance-decisions") {
	        return Promise.resolve({ run_id: "run-1", items: [] });
	      }
	      throw new Error(`Unhandled apiFetch request: ${path}`);
	    });

	    const { result } = renderHook(() => ({
	      run: useRun("run-1"),
	      graph: useRunGraph("run-1", { enabled: true }),
	      governance: useRunLatestGovernanceDecision("run-1", { enabled: true }),
	    }), {
	      wrapper: makeWrapper(qc),
	    });

	  await waitFor(() => {
	    expect(result.current.run.isSuccess).toBe(true);
	    expect(result.current.graph.isSuccess).toBe(true);
	    expect(result.current.governance.isSuccess).toBe(true);
	  });

	  apiFetchMock.mockClear();

	  await act(async () => {
	    await vi.advanceTimersByTimeAsync(31_000);
	  });

	  expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("does not refetch stream-owned run detail summary surfaces on window focus", async () => {
		apiFetchMock.mockImplementation((path: string) => {
			switch (path) {
				case "/runs/run-1":
					return Promise.resolve(buildRun());
				case "/runs/run-1/plan":
					return Promise.resolve({
						run_id: "run-1",
						plan_state: "approved",
						revision_status: "ready",
						plan_revision_id: "planrev-1",
						components: [{
							node_id: "plan_drafting",
							planning_output_id: "planout-1",
							created_at: "2026-03-01T12:00:00Z",
							content: "plan",
						}],
						format: "markdown",
						content: "plan",
						created_at: "2026-03-01T12:00:00Z",
						updated_at: "2026-03-01T12:00:00Z",
					});
				case "/runs/run-1/governance-decisions":
					return Promise.resolve({ run_id: "run-1", items: [] });
				default:
					throw new Error(`Unhandled apiFetch request: ${path}`);
			}
		});

		const { result } = renderHook(() => ({
			run: useRun("run-1"),
			plan: useRunPlan("run-1", { enabled: true }),
			governance: useRunLatestGovernanceDecision("run-1", { enabled: true }),
		}), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.run.isSuccess).toBe(true);
			expect(result.current.plan.isSuccess).toBe(true);
			expect(result.current.governance.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(3);

		apiFetchMock.mockClear();

		await act(async () => {
			focusManager.setFocused(false);
			await Promise.resolve();
			focusManager.setFocused(true);
		});

		await act(async () => {
			await Promise.resolve();
		});

		expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("does not refetch heavy run surfaces or stream-owned collections on window focus", async () => {
		apiFetchMock.mockImplementation((path: string) => {
			switch (path) {
				case "/runs/run-1/graph":
					return Promise.resolve({ run_id: "run-1", nodes: [], edges: [] });
				case "/runs/run-1/timeline":
					return Promise.resolve({ run_id: "run-1", items: [], next_cursor: null });
				case "/runs/run-1/mailbox/messages":
					return Promise.resolve({ run_id: "run-1", items: [] });
				case "/runs":
					return Promise.resolve({ items: [] });
				default:
					throw new Error(`Unhandled apiFetch request: ${path}`);
			}
		});

		const { result } = renderHook(() => ({
			graph: useRunGraph("run-1", { enabled: true }),
			timeline: useTimeline("run-1", 50, { enabled: true }),
			mailbox: useMailbox("run-1", {}, { enabled: true }),
			runs: useRuns(),
		}), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.graph.isSuccess).toBe(true);
			expect(result.current.timeline.isSuccess).toBe(true);
			expect(result.current.mailbox.isSuccess).toBe(true);
			expect(result.current.runs.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(4);

		apiFetchMock.mockClear();

		await act(async () => {
			focusManager.setFocused(false);
			await Promise.resolve();
			focusManager.setFocused(true);
		});

		await act(async () => {
			await Promise.resolve();
		});

		expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("refetches polling-owned queries on window focus", async () => {
		apiFetchMock.mockResolvedValue({ items: [] });

		const { result } = renderHook(() => useRoles(), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		apiFetchMock.mockClear();

		await act(async () => {
			focusManager.setFocused(false);
			await Promise.resolve();
			focusManager.setFocused(true);
		});

		await waitFor(() => {
			expect(apiFetchMock).toHaveBeenCalledTimes(1);
		});
		expect(apiFetchMock).toHaveBeenCalledWith("/roles");
	});

	it("passes AbortSignal to analytics snapshot queries", async () => {
		apiFetchMock.mockResolvedValue({
			snapshot_at: "2026-03-22T10:10:00Z",
			window_minutes: 1440,
			all_time_run_count: 0,
			runtime: { status: "ok", running_runs: 0, running_nodes: 0, stale_running_nodes: 0 },
			overview: {
				total_runs: 0,
				succeeded_runs: 0,
				failed_runs: 0,
				cancelled_runs: 0,
				avg_duration_seconds: 0,
				success_rate: 0,
				review_rework_rate: 0,
				validation_rework_rate: 0,
			},
			run_analytics: { bucket_type: "hour", outcome_series: [], duration_by_role: [], recent_runs: [] },
		});

		const { result } = renderHook(() => useRealmAnalyticsSnapshot(1440), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});

		expect(apiFetchMock).toHaveBeenCalledTimes(1);
		expect(apiFetchMock.mock.calls[0][0]).toBe("/realm-analytics/snapshot");
		expect(apiFetchMock.mock.calls[0][1]?.signal).toBeInstanceOf(AbortSignal);
	});

	it("passes AbortSignal to selected-run execution detail queries", async () => {
		apiFetchMock.mockResolvedValue({
			snapshot_at: "2026-03-22T10:10:00Z",
			window_minutes: 1440,
			execution_detail: {
				selected_run: {
					run_id: "run-1",
					decree_title: "Run 1",
					status: "succeeded",
					created_at: "2026-03-22T10:10:00Z",
				},
				tokens: {
					provider_total_tokens: 0,
					derived_total_tokens: 0,
					input: 0,
					output: 0,
					reasoning: 0,
					cache_read: 0,
					cache_write: 0,
					by_role: [],
				},
				artifacts: { total_count: 0, total_size_bytes: 0 },
				nodes: { total_executed: 0 },
			},
		});

		const { result } = renderHook(() => useRealmAnalyticsExecutionDetailSnapshot(1440, "run-1"), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});

		expect(apiFetchMock).toHaveBeenCalledTimes(1);
		expect(apiFetchMock.mock.calls[0][0]).toBe("/realm-analytics/execution-detail");
		expect(apiFetchMock.mock.calls[0][1]?.params).toEqual({ run_id: "run-1" });
		expect(apiFetchMock.mock.calls[0][1]?.signal).toBeInstanceOf(AbortSignal);
	});

	it("loads provider models from the cached catalog path by default", async () => {
		apiFetchMock.mockResolvedValue({ catalog_status: "fresh", items: [] });

		const { result } = renderHook(() => useProviderModels(), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});

		expect(apiFetchMock).toHaveBeenCalledWith("/provider/models", expect.objectContaining({ params: { refresh: false } }));
	});

	it("refreshes provider models live and updates the canonical cache", async () => {
		const staleResponse = {
			catalog_status: "stale",
			items: [],
			failure_reason: "command_failed",
			failure_message: "cached fallback",
		};
		const freshResponse = {
			catalog_status: "fresh",
			items: [
				{
					id: "github-copilot/gpt-5.4",
					provider_id: "github-copilot",
					provider_name: "GitHub Copilot",
					model_name: "GPT-5.4",
					thinking_effort_options: [{ value: "medium", label: "Medium" }],
				},
			],
		};
		apiFetchMock.mockResolvedValueOnce(staleResponse).mockResolvedValueOnce(freshResponse);

		const { result } = renderHook(() => ({
			query: useProviderModels(),
			refresh: useRefreshProviderModels(),
		}), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.query.isSuccess).toBe(true);
		});

		await act(async () => {
			await result.current.refresh.mutateAsync();
		});

		expect(apiFetchMock.mock.calls[1][0]).toBe("/provider/models");
		expect(apiFetchMock.mock.calls[1][1]?.params).toEqual({ refresh: true });
		expect(qc.getQueryData(providerModelsQueryKey)).toEqual(freshResponse);
		await waitFor(() => {
			expect(result.current.query.data).toEqual(freshResponse);
		});
	});

	it("does not let an older provider-model poll overwrite a forced refresh", async () => {
		const staleResponse = {
			catalog_status: "stale",
			items: [],
			failure_reason: "command_failed",
			failure_message: "older cached fallback",
		};
		const freshResponse = {
			catalog_status: "fresh",
			items: [{
				id: "github-copilot/gpt-5.4",
				provider_id: "github-copilot",
				provider_name: "GitHub Copilot",
				model_name: "GPT-5.4",
				thinking_effort_options: [{ value: "medium", label: "Medium" }],
			}],
		};
		let resolvePoll!: (value: typeof staleResponse) => void;
		let resolveRefresh!: (value: typeof freshResponse) => void;
		apiFetchMock.mockImplementation((_path: string, options: { params: { refresh: boolean } }) => (
			options.params.refresh
				? new Promise<typeof freshResponse>((resolve) => { resolveRefresh = resolve; })
				: new Promise<typeof staleResponse>((resolve) => { resolvePoll = resolve; })
		));

		const { result } = renderHook(() => ({
			query: useProviderModels(),
			refresh: useRefreshProviderModels(),
		}), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(apiFetchMock).toHaveBeenCalledTimes(1);
		});
		act(() => result.current.refresh.mutate());
		await waitFor(() => {
			expect(apiFetchMock).toHaveBeenCalledTimes(2);
		});

		await act(async () => resolveRefresh(freshResponse));
		expect(qc.getQueryData(providerModelsQueryKey)).toEqual(freshResponse);

		await act(async () => resolvePoll(staleResponse));
		expect(qc.getQueryData(providerModelsQueryKey)).toEqual(freshResponse);
	});

});
