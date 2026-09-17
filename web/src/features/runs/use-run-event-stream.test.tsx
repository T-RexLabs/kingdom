import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  RUN_STREAM_RECONNECT_DELAY_MS,
  useRunEventStream,
} from "./use-run-event-stream";
import { runQueryKeys } from "@/api/hooks-runs";
import { liveExecutionProjectionKey } from "./run-live-execution-projection";
import { liveTimelineQueryKey, liveTimelineSyncQueryKey } from "./run-live-timeline";
import { runSurfaceFreshnessQueryKey } from "./run-surface-freshness";

const subscribeSSEMock = vi.fn();
vi.mock("@/lib/sse-stream", () => ({
  subscribeSSE: (args: unknown) => subscribeSSEMock(args),
}));

const buildAuthHeadersMock = vi.fn((headers?: HeadersInit) => ({ ...(headers as object) }));
vi.mock("@/api/client", () => ({
  API_BASE: "/api/v1",
  buildAuthHeaders: (headers?: HeadersInit) => buildAuthHeadersMock(headers),
}));

let queryClient: QueryClient;

function makeWrapper() {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

async function flush(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe("useRunEventStream", () => {
  beforeEach(() => {
    subscribeSSEMock.mockReset();
    buildAuthHeadersMock.mockClear();
    vi.useFakeTimers();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    vi.useRealTimers();
    queryClient.clear();
  });

  it("does not advance Last-Event-ID after a parse failure, so the retry resumes from the prior applied event", async () => {
    const invocations: Array<{
      buildHeaders: (lastEventId: string | null) => HeadersInit;
      onConnectionChange: (state: { state: string; attempt?: number; lastError?: string }) => void;
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];
    const resolvers: Array<() => void> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>((resolve) => {
        resolvers.push(resolve);
      });
    });

    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

    expect(invocations).toHaveLength(1);
    await expect(
      invocations[0].onEvent({
        id: "evt-bad",
        type: "attempt.started",
        data: "{not-json",
      }),
    ).resolves.toBe(false);
    expect(invalidateSpy).not.toHaveBeenCalled();

    await act(async () => {
      invocations[0].onConnectionChange({ state: "failed", attempt: 6, lastError: "EOF" });
      resolvers[0]();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(RUN_STREAM_RECONNECT_DELAY_MS);
    });
    await flush();

    expect(invocations).toHaveLength(2);
    expect(invocations[1].buildHeaders(null)).toEqual({});
  });

  it("advances Last-Event-ID only after a successful apply and keeps run SSE scoped to the run subtree", async () => {
    const invocations: Array<{
      buildHeaders: (lastEventId: string | null) => HeadersInit;
      onConnectionChange: (state: { state: string; attempt?: number; lastError?: string }) => void;
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];
    const resolvers: Array<() => void> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>((resolve) => {
        resolvers.push(resolve);
      });
    });

    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    queryClient.setQueryData(runQueryKeys.detail("run-1"), {
      status: "pending",
      display_state: "pending",
      plan_state: "draft",
      updated_at: "2026-03-01T10:00:00Z",
	      tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
    });
    queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), {
      pageParams: [undefined],
      pages: [{ run_id: "run-1", items: [] }],
    });

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

    expect(invocations).toHaveLength(1);
	    await expect(
	      invocations[0].onEvent({
	        id: "evt-good",
	        type: "message",
	        data: JSON.stringify({
	          event_id: "evt-good",
	          event_type: "run.state_changed",
	          run_event_seq: 1,
	          attempt_seq: 1,
	          created_at: "2026-03-01T10:05:00Z",
	          status: "running",
          display_state: "running",
          plan_state: "approved",
        }),
      }),
    ).resolves.toBe(true);

    await act(async () => {
      invocations[0].onConnectionChange({ state: "failed", attempt: 6, lastError: "EOF" });
      resolvers[0]();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(RUN_STREAM_RECONNECT_DELAY_MS);
    });
    await flush();

    expect(invocations).toHaveLength(2);
    expect(invocations[1].buildHeaders(null)).toEqual({ "Last-Event-ID": "evt-good" });

    expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
      status: "running",
      display_state: "running",
      plan_state: "approved",
      updated_at: "2026-03-01T10:05:00Z",
      tab_counts: { activity_events: 1 },
    });
    expect(queryClient.getQueryData(runQueryKeys.timeline("run-1", 50))).toMatchObject({
      pages: [{ items: [] }],
    });
	    expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	      { event_id: "evt-good", event_type: "run.state_changed", run_event_seq: 1, attempt_seq: 1 },
	    ]);

    const invalidatedKeys = invalidateSpy.mock.calls
      .map((call) => ({
        queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
        exact: (call[0] as { exact?: boolean } | undefined)?.exact,
      }))
      .filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

    expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.plan("run-1"), exact: true });
    expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
    expect(invalidatedKeys).not.toContainEqual({ queryKey: ["runs", "run-1", "timeline"], exact: undefined });
    expect(invalidatedKeys).not.toContainEqual({ queryKey: ["runs"], exact: undefined });
    expect(invalidatedKeys).not.toContainEqual({ queryKey: ["metrics"], exact: undefined });
  });

	it("reuses the persisted run resume cursor after the hook remounts", async () => {
		const invocations: Array<{
			buildHeaders: (lastEventId: string | null) => HeadersInit;
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const firstMount = renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-remount",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-remount",
					event_type: "run.state_changed",
					run_event_seq: 1,
					attempt_seq: 1,
					created_at: "2026-03-01T10:05:00Z",
					status: "running",
					display_state: "running",
					plan_state: "approved",
				}),
			}),
		).resolves.toBe(true);

		firstMount.unmount();

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		expect(invocations).toHaveLength(2);
		expect(invocations[1].buildHeaders(null)).toEqual({ "Last-Event-ID": "evt-remount" });
	});

	it("clears only the invalid resume token and does not reapply replayed events after HTTP 400 recovery", async () => {
		const invocations: Array<{
			buildHeaders: (lastEventId: string | null) => HeadersInit;
			onConnectionChange: (state: { state: string; attempt?: number; lastError?: string }) => void;
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			status: "running",
			display_state: "running",
			plan_state: "approved",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 5, executions: 0, messages: 1 },
			stream_cursor: { event_id: "evt-live-5", run_event_seq: 5 },
		});
		queryClient.setQueryData(runQueryKeys.plan("run-1"), { content: "plan" });
		queryClient.setQueryData(runQueryKeys.governanceSummary("run-1"), { items: [] });
		queryClient.setQueryData(runQueryKeys.governanceHistory("run-1"), { pages: [{ items: [] }], pageParams: [undefined] });
		queryClient.setQueryData(runQueryKeys.graph("run-1"), { run_id: "run-1", nodes: [], edges: [] });
		queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), { pages: [{ items: [] }], pageParams: [undefined] });
		queryClient.setQueryData(runQueryKeys.agents("run-1"), { run_id: "run-1", items: [] });
		queryClient.setQueryData(runQueryKeys.mailboxList("run-1", ""), { run_id: "run-1", items: [] });
		queryClient.setQueryData(liveTimelineQueryKey("run-1"), [
			{ event_id: "evt-live-5", event_type: "attempt.progress", run_event_seq: 5, attempt_seq: 5, emitted_at: "2026-03-01T10:05:00Z", payload_json: {} },
		]);

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await act(async () => {
			invocations[0].onConnectionChange({ state: "failed", attempt: 1, lastError: "HTTP 400" });
		});
		await flush();

		expect(invocations).toHaveLength(2);
		expect(invocations[1].buildHeaders(null)).toEqual({});

		await expect(
			invocations[1].onEvent({
				id: "evt-replayed-4",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-replayed-4",
					event_type: "attempt.started",
					run_event_seq: 4,
					attempt_seq: 4,
					created_at: "2026-03-01T10:04:00Z",
				}),
			}),
		).resolves.toBe(true);
		await expect(
			invocations[1].onEvent({
				id: "evt-live-5",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-live-5",
					event_type: "mailbox.message_recorded",
					run_event_seq: 5,
					attempt_seq: 5,
					created_at: "2026-03-01T10:05:00Z",
					visible_to_operator: true,
				}),
			}),
		).resolves.toBe(true);
		await expect(
			invocations[1].onEvent({
				id: "evt-live-6",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-live-6",
					event_type: "mailbox.message_recorded",
					run_event_seq: 6,
					attempt_seq: 6,
					created_at: "2026-03-01T10:06:00Z",
					visible_to_operator: true,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			tab_counts: { activity_events: 6, messages: 2 },
		});
		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
			{ event_id: "evt-live-5", run_event_seq: 5 },
			{ event_id: "evt-live-6", run_event_seq: 6 },
		]);
		expect(queryClient.getQueryState(runQueryKeys.plan("run-1"))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.governanceSummary("run-1"))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.governanceHistory("run-1"))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.graph("run-1"))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.timeline("run-1", 50))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.agents("run-1"))?.isInvalidated).toBe(true);
		expect(queryClient.getQueryState(runQueryKeys.mailboxList("run-1", ""))?.isInvalidated).toBe(true);
	});

	  it("caches the live timeline tail without marking canonical sync while activity is visible", async () => {
    const invocations: Array<{
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>(() => undefined);
    });

    queryClient.setQueryData(runQueryKeys.detail("run-1"), {
	      tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
    });

	    renderHook(() => useRunEventStream("run-1", true, false), {
	      wrapper: makeWrapper(),
	    });
    await flush();

    await expect(
      invocations[0].onEvent({
        id: "evt-hidden-activity",
        type: "message",
	        data: JSON.stringify({
	          event_id: "evt-hidden-activity",
	          event_type: "attempt.started",
	          run_event_seq: 1,
	          attempt_seq: 1,
	          created_at: "2026-03-01T10:05:00Z",
	        }),
	      }),
	    ).resolves.toBe(true);

	    expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	      { event_id: "evt-hidden-activity", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1 },
	    ]);
	    expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toBeUndefined();
	  });

	it("marks hidden activity for canonical reconciliation when the timeline is not visible", async () => {
	  const invocations: Array<{
	    onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
	  }> = [];

	  subscribeSSEMock.mockImplementation((args) => {
	    invocations.push(args);
	    return new Promise<void>(() => undefined);
	  });

	  queryClient.setQueryData(runQueryKeys.detail("run-1"), {
	    tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
	  });

	  renderHook(() => useRunEventStream("run-1", true, true), {
	    wrapper: makeWrapper(),
	  });
	  await flush();

	  await expect(
	    invocations[0].onEvent({
	      id: "evt-hidden-dirty",
	      type: "message",
	      data: JSON.stringify({
	        event_id: "evt-hidden-dirty",
	        event_type: "attempt.started",
	        run_event_seq: 1,
	        attempt_seq: 1,
	        created_at: "2026-03-01T10:05:00Z",
	      }),
	    }),
	  ).resolves.toBe(true);

	  expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	    { event_id: "evt-hidden-dirty", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1 },
	  ]);
	  expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toEqual({
	    dirtyRanges: [{ startSeq: 1, endSeq: 1 }],
	    dirtyStartSeq: 1,
	    dirtyEndSeq: 1,
	    reconciliationAttempt: null,
	    reconciliationRetryCount: 0,
	    nextRetryAtMs: null,
	  });
	  });

	it("marks hidden execution-related surfaces dirty when execution events arrive off-tab", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1", true, false, ["graph", "executions", "agents"]), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-hidden-execution",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-hidden-execution",
					event_type: "attempt.started",
					run_event_seq: 1,
					attempt_seq: 1,
					created_at: "2026-03-01T10:05:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runSurfaceFreshnessQueryKey("run-1"))).toEqual({
			dirtySurfaces: ["graph", "executions", "agents"],
		});
	});

	it("marks the hidden mailbox surface dirty for operator-visible mailbox events", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1", true, false, ["mailbox"]), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-hidden-mailbox",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-hidden-mailbox",
					event_type: "mailbox.message_recorded",
					run_event_seq: 1,
					attempt_seq: 1,
					created_at: "2026-03-01T10:05:00Z",
					visible_to_operator: true,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runSurfaceFreshnessQueryKey("run-1"))).toEqual({
			dirtySurfaces: ["mailbox"],
		});
	});

  it("keeps cursor-paginated timeline pages immutable while appending live events separately", async () => {
    const invocations: Array<{
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>(() => undefined);
    });

    queryClient.setQueryData(runQueryKeys.detail("run-1"), {
	      tab_counts: { dag_nodes: 0, agents: 0, activity_events: 2, executions: 0, messages: 0 },
    });
	    queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), {
	      pageParams: [undefined, "cursor-2"],
	      pages: [{ items: [{ event_id: "evt-good", event_type: "run.state_changed", run_event_seq: 1, attempt_seq: 1 }] }],
	    });

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

	    await expect(
	      invocations[0].onEvent({
	        id: "evt-live-tail",
        type: "message",
	        data: JSON.stringify({
	          event_id: "evt-live-tail",
	          event_type: "node.state_changed",
	          run_event_seq: 3,
	          attempt_seq: 1,
	          created_at: "2026-03-01T10:06:00Z",
	          node_id: "plan_drafting",
	        }),
      }),
    ).resolves.toBe(true);

	    expect(queryClient.getQueryData(runQueryKeys.timeline("run-1", 50))).toMatchObject({
	      pageParams: [undefined, "cursor-2"],
	      pages: [{ items: [{ event_id: "evt-good", event_type: "run.state_changed", run_event_seq: 1, attempt_seq: 1 }] }],
	    });
	    expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	      { event_id: "evt-live-tail", event_type: "node.state_changed", run_event_seq: 3, attempt_seq: 1 },
	    ]);
  });

  it("invalidates governance summary queries that live under the run detail subtree", async () => {
    const invocations: Array<{
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>(() => undefined);
    });

    const detailKey = runQueryKeys.detail("run-1");
    const planKey = runQueryKeys.plan("run-1");
    const governanceKey = runQueryKeys.governanceSummary("run-1");
    const governanceHistoryKey = runQueryKeys.governanceHistory("run-1");
    queryClient.setQueryData(detailKey, { run_id: "run-1", status: "awaiting_human_confirmation" });
    queryClient.setQueryData(planKey, { run_id: "run-1", plan_state: "awaiting_king_decision" });
    queryClient.setQueryData(governanceKey, { items: [] });
    queryClient.setQueryData(governanceHistoryKey, { pages: [] });

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

    await expect(
      invocations[0].onEvent({
        id: "evt-governance",
        type: "message",
        data: JSON.stringify({ event_type: "approval.recorded" }),
      }),
    ).resolves.toBe(true);

    await flush();

    expect(queryClient.getQueryState(detailKey)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(planKey)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(governanceKey)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(governanceHistoryKey)?.isInvalidated).toBe(true);
  });

	it("patches timeline activity while invalidating execution surfaces for node state changes", async () => {
    const invocations: Array<{
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>(() => undefined);
    });

    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    queryClient.setQueryData(runQueryKeys.detail("run-1"), {
      updated_at: "2026-03-01T10:00:00Z",
	      tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
    });
    queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), {
      pageParams: [undefined],
      pages: [{ run_id: "run-1", items: [] }],
    });

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

	    await expect(
	      invocations[0].onEvent({
	        id: "evt-node",
        type: "message",
	        data: JSON.stringify({
	          event_id: "evt-node",
	          event_type: "node.state_changed",
	          run_event_seq: 1,
	          attempt_seq: 3,
	          created_at: "2026-03-01T10:06:00Z",
	          node_id: "plan_drafting",
	        }),
      }),
    ).resolves.toBe(true);

    expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
	      updated_at: "2026-03-01T10:00:00Z",
	      last_activity_at: "2026-03-01T10:06:00Z",
      tab_counts: { activity_events: 1 },
    });
    expect(queryClient.getQueryData(runQueryKeys.timeline("run-1", 50))).toMatchObject({
      pages: [{ items: [] }],
    });
	    expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	      { event_id: "evt-node", event_type: "node.state_changed", run_event_seq: 1, attempt_seq: 3 },
	    ]);

    const invalidatedKeys = invalidateSpy.mock.calls
      .map((call) => ({
        queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
        exact: (call[0] as { exact?: boolean } | undefined)?.exact,
      }))
      .filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.executions("run-1"), exact: false });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: ["runs", "run-1", "timeline"], exact: undefined });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.plan("run-1"), exact: true });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.governanceSummary("run-1"), exact: true });
  });

  it("invalidates graph and agents when execution starts running", async () => {
    const invocations: Array<{
      onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
    }> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>(() => undefined);
    });

    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    queryClient.setQueryData(runQueryKeys.detail("run-1"), {
	      tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
    });
    queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), {
      pageParams: [undefined],
      pages: [{ run_id: "run-1", items: [] }],
    });

    renderHook(() => useRunEventStream("run-1"), {
      wrapper: makeWrapper(),
    });
    await flush();

	    await expect(
	      invocations[0].onEvent({
	        id: "evt-started",
        type: "message",
	        data: JSON.stringify({
	          event_id: "evt-started",
	          event_type: "attempt.started",
	          run_event_seq: 1,
	          attempt_seq: 4,
	          created_at: "2026-03-01T10:07:00Z",
	          node_id: "delegated_execution_berserker",
	        }),
      }),
    ).resolves.toBe(true);

    expect(queryClient.getQueryData(runQueryKeys.timeline("run-1", 50))).toMatchObject({
      pages: [{ items: [] }],
    });
	    expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
	      { event_id: "evt-started", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 4 },
	    ]);

    const invalidatedKeys = invalidateSpy.mock.calls
      .map((call) => ({
        queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
        exact: (call[0] as { exact?: boolean } | undefined)?.exact,
      }))
      .filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

    expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
    expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.graph("run-1"), exact: true });
    expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
  });

	it("patches Last Activity for meaningful attempt progress without invalidating detail", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			last_activity_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-progress-meaningful",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-progress-meaningful",
					event_type: "attempt.progress",
					run_event_seq: 1,
					attempt_seq: 1,
					created_at: "2026-03-01T10:09:00Z",
					reason_code: "stream_event",
					text: "builder produced progress output",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			updated_at: "2026-03-01T10:00:00Z",
			last_activity_at: "2026-03-01T10:09:00Z",
			tab_counts: { activity_events: 1 },
		});
		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
	});

	it("keeps Last Activity stable for synthetic attempt progress telemetry", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			last_activity_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, executions: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-progress-observation",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-progress-observation",
					event_type: "attempt.progress",
					run_event_seq: 1,
					attempt_seq: 1,
					created_at: "2026-03-01T10:10:00Z",
					reason_code: "runtime_observation",
					execution_diagnostics: { event_count: 1 },
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			updated_at: "2026-03-01T10:00:00Z",
			last_activity_at: "2026-03-01T10:00:00Z",
			tab_counts: { activity_events: 1 },
		});
	});

	it("invalidates detail, executions, graph, and agents when execution reaches a terminal attempt event", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-succeeded",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-succeeded",
					event_type: "attempt.succeeded",
					run_event_seq: 1,
					attempt_seq: 5,
					created_at: "2026-03-01T10:08:00Z",
					node_id: "delegated_execution_berserker",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			updated_at: "2026-03-01T10:00:00Z",
			last_activity_at: "2026-03-01T10:08:00Z",
			tab_counts: { activity_events: 1 },
		});

		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.executions("run-1"), exact: false });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
	});

	it("patches live execution projection from terminal SSE lineage", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-terminal-lineage",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-terminal-lineage",
					event_type: "attempt.succeeded",
					run_event_seq: 1,
					attempt_seq: 5,
					attempt_id: "attempt-run-1",
					node_run_id: "nr-run-1",
					node_id: "delegated_execution_berserker",
					attempt: 1,
					required_role: "builder-berserker",
					execution_mode: "provider_backed",
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-run-1": {
				attempt_id: "attempt-run-1",
				node_id: "delegated_execution_berserker",
				attempt: 1,
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				node_run_id: "nr-run-1",
				activity_at: "2026-03-01T10:08:00Z",
				status: "succeeded",
				terminal_observed_at: "2026-03-01T10:08:00Z",
				live_label: "Execution succeeded",
			},
		});
	});

	it("does not regress a terminal live execution from synthetic completion telemetry", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-terminal-lineage",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-terminal-lineage",
					event_type: "attempt.succeeded",
					run_event_seq: 1,
					attempt_seq: 5,
					attempt_id: "attempt-run-1",
					node_run_id: "nr-run-1",
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		await expect(
			invocations[0].onEvent({
				id: "evt-terminal-observation",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-terminal-observation",
					event_type: "attempt.progress",
					reason_code: "runtime_completion_observation",
					run_event_seq: 2,
					attempt_seq: 6,
					attempt_id: "attempt-run-1",
					node_run_id: "nr-run-1",
					created_at: "2026-03-01T10:09:00Z",
					phase: "complete_execution_exited",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-run-1": {
				node_run_id: "nr-run-1",
				activity_at: "2026-03-01T10:08:00Z",
				status: "succeeded",
				terminal_observed_at: "2026-03-01T10:08:00Z",
				live_label: "Execution succeeded",
			},
		});
	});

	it("projects completion observation as verifying state for in-flight work", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-runtime-started",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-runtime-started",
					event_type: "attempt.started",
					run_event_seq: 1,
					attempt_seq: 1,
					attempt_id: "attempt-runtime-1",
					node_run_id: "nr-runtime-1",
					node_id: "delegated_execution_berserker",
					attempt: 1,
					required_role: "builder-berserker",
					execution_mode: "provider_backed",
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		await expect(
			invocations[0].onEvent({
				id: "evt-runtime-completion-observed",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-runtime-completion-observed",
					event_type: "attempt.progress",
					reason_code: "runtime_completion_observation",
					run_event_seq: 2,
					attempt_seq: 2,
					attempt_id: "attempt-runtime-1",
					node_run_id: "nr-runtime-1",
					node_id: "delegated_execution_berserker",
					attempt: 1,
					required_role: "builder-berserker",
					execution_mode: "provider_backed",
					created_at: "2026-03-01T10:08:05Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-runtime-1": {
				node_run_id: "nr-runtime-1",
				activity_at: "2026-03-01T10:08:05Z",
				status: "verifying",
				live_label: "Completion observed",
				live_reason_code: "runtime_completion_observation",
			},
		});
	});

	it("prefers same-timestamp terminal runtime-owned events over earlier running telemetry", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-runtime-started",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-runtime-started",
					event_type: "attempt.started",
					run_event_seq: 140,
					attempt_seq: 1,
					attempt_id: "attempt-runtime-1",
					node_run_id: "nr-runtime-1",
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		await expect(
			invocations[0].onEvent({
				id: "evt-runtime-succeeded",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-runtime-succeeded",
					event_type: "attempt.succeeded",
					run_event_seq: 143,
					attempt_seq: 2,
					attempt_id: "attempt-runtime-1",
					node_run_id: "nr-runtime-1",
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-runtime-1": {
				node_run_id: "nr-runtime-1",
				activity_at: "2026-03-01T10:08:00Z",
				status: "succeeded",
				terminal_observed_at: "2026-03-01T10:08:00Z",
			},
		});
	});

	it("invalidates detail, executions, graph, and agents when a retry is queued", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-retry-queued",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-retry-queued",
					event_type: "attempt.retry_queued",
					run_event_seq: 1,
					attempt_seq: 4,
					created_at: "2026-03-01T10:07:00Z",
					attempt_id: "attempt-retry-queued",
					node_run_id: "nr-retry-queued",
					node_id: "delegated_execution_berserker",
					attempt: 4,
					required_role: "builder-berserker",
					execution_mode: "provider_backed",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-retry-queued": {
				attempt_id: "attempt-retry-queued",
				node_id: "delegated_execution_berserker",
				attempt: 4,
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				node_run_id: "nr-retry-queued",
				activity_at: "2026-03-01T10:07:00Z",
				status: "queued",
				live_label: "Retry queued",
			},
		});

		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
			{ event_id: "evt-retry-queued", event_type: "attempt.retry_queued", run_event_seq: 1, attempt_seq: 4 },
		]);

		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.executions("run-1"), exact: false });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.graph("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
	});

	it("keeps rerun-queued activity queued until execution actually starts", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-rerun-queued",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-rerun-queued",
					event_type: "attempt.rerun_queued",
					run_event_seq: 1,
					attempt_seq: 9,
					node_run_id: "nr-rerun-queued",
					created_at: "2026-03-01T10:09:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-rerun-queued": {
				node_run_id: "nr-rerun-queued",
				activity_at: "2026-03-01T10:09:00Z",
				status: "queued",
				live_label: "Rerun queued",
			},
		});

		await expect(
			invocations[0].onEvent({
				id: "evt-rerun-started",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-rerun-started",
					event_type: "attempt.started",
					run_event_seq: 2,
					attempt_seq: 10,
					node_run_id: "nr-rerun-queued",
					created_at: "2026-03-01T10:10:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-rerun-queued": {
				node_run_id: "nr-rerun-queued",
				activity_at: "2026-03-01T10:10:00Z",
				status: "running",
				started_at: "2026-03-01T10:10:00Z",
				live_label: "Execution started",
			},
		});

		await expect(
			invocations[0].onEvent({
				id: "evt-rerun-running",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-rerun-running",
					event_type: "node.state_changed",
					run_event_seq: 3,
					attempt_seq: 11,
					node_run_id: "nr-rerun-queued",
					created_at: "2026-03-01T10:10:02Z",
					payload_json: { state: "running" },
					state: "running",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-rerun-queued": {
				node_run_id: "nr-rerun-queued",
				activity_at: "2026-03-01T10:10:02Z",
				status: "running",
				started_at: "2026-03-01T10:10:00Z",
				live_label: "Execution started",
			},
		});
	});

	it("keeps workspace preparation progress out of the running state", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-workspace-prepared",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-workspace-prepared",
					event_type: "attempt.progress",
					reason_code: "workspace_prepared",
					run_event_seq: 1,
					attempt_seq: 1,
					node_run_id: "nr-workspace-prepared",
					created_at: "2026-03-01T10:09:00Z",
					node_id: "plan_drafting",
					attempt_id: "run-1:plan_drafting:1",
					attempt: 1,
				}),
			}),
		).resolves.toBe(true);

		const projection = queryClient.getQueryData<Record<string, { status?: string; live_label?: string }>>(liveExecutionProjectionKey("run-1"));
		expect(projection?.["nr-workspace-prepared"]?.status).toBeUndefined();
		expect(projection?.["nr-workspace-prepared"]?.live_label).toBe("Workspace prepared");
	});

	it("maps scheduled retries without replacing the source attempt status", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-retry-scheduled",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-retry-scheduled",
					event_type: "attempt.retry_scheduled",
					reason_code: "provider_empty_output",
					run_event_seq: 1,
					attempt_seq: 4,
					node_run_id: "nr-retry-scheduled",
					created_at: "2026-03-01T10:11:00Z",
					payload_json: {
						node_id: "delegated_execution_berserker",
						node_run_id: "nr-retry-scheduled",
						attempt_id: "run-1:delegated_execution_berserker:1",
						attempt: 1,
						recovery_trigger: "automatic_retry",
						recovery_phase: "scheduled",
						error_code: "provider_empty_output",
						retry_index: 2,
						retry_max: 3,
						scheduled_at: "2026-03-01T10:11:10Z",
					},
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-retry-scheduled": {
				node_run_id: "nr-retry-scheduled",
				activity_at: "2026-03-01T10:11:00Z",
				live_label: "Retry scheduled",
			},
		});
	});

	it("projects stale recovery as blocked instead of running", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-attempt-blocked",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-attempt-blocked",
					event_type: "node.state_changed",
					reason_code: "agent_unresponsive",
					run_event_seq: 1,
					attempt_seq: 4,
					node_run_id: "nr-attempt-blocked",
					created_at: "2026-03-01T10:12:00Z",
					payload_json: {
						state: "blocked",
						node_id: "plan_drafting",
						node_run_id: "nr-attempt-blocked",
						attempt_id: "run-1:plan_drafting:1",
						attempt: 1,
					},
					state: "blocked",
					node_id: "plan_drafting",
					attempt_id: "run-1:plan_drafting:1",
					attempt: 1,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(liveExecutionProjectionKey("run-1"))).toMatchObject({
			"nr-attempt-blocked": {
				node_run_id: "nr-attempt-blocked",
				activity_at: "2026-03-01T10:12:00Z",
				status: "blocked",
				live_label: "Blocked",
			},
		});
	});

	it("invalidates executions when a run reopens from retry", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-run-retry-reopened",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-run-retry-reopened",
					event_type: "run.retry_reopened",
					run_event_seq: 1,
					attempt_seq: 5,
					created_at: "2026-03-01T10:08:00Z",
				}),
			}),
		).resolves.toBe(true);

		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.executions("run-1"), exact: false });
	});

	it("dedupes replayed events from the authoritative stream cursor before mutating cache state", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 5, executions: 0, messages: 0 },
			stream_cursor: { event_id: "evt-authoritative", run_event_seq: 5 },
		});
		queryClient.setQueryData(runQueryKeys.timeline("run-1", 50), {
			pageParams: [undefined],
			pages: [{ run_id: "run-1", items: [] }],
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-replayed",
				type: "message",
				data: JSON.stringify({
					event_id: "evt-replayed",
					event_type: "node.state_changed",
					run_event_seq: 5,
					attempt_seq: 9,
					created_at: "2026-03-01T10:06:00Z",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			tab_counts: { activity_events: 5 },
		});
		expect(queryClient.getQueryData(runQueryKeys.timeline("run-1", 50))).toMatchObject({
			pages: [{ items: [] }],
		});
		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1")) ?? []).toHaveLength(0);
	});

	it("patches mailbox counters only for operator-visible mailbox events", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 1, executions: 0, messages: 0 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-mailbox",
				type: "message",
				data: JSON.stringify({
					event_type: "mailbox.message_recorded",
					run_event_seq: 2,
					created_at: "2026-03-01T10:08:00Z",
					visible_to_operator: true,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			tab_counts: { activity_events: 2, messages: 1 },
		});

		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidatedKeys).toContainEqual({ queryKey: ["runs", "run-1", "mailbox"], exact: false });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: runQueryKeys.detail("run-1"), exact: true });
	});

	it("does not inflate mailbox counts for visible message revisions", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			updated_at: "2026-03-01T10:00:00Z",
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 1, executions: 0, messages: 1 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-mailbox-revision",
				type: "message",
				data: JSON.stringify({
					event_type: "mailbox.message_recorded",
					run_event_seq: 2,
					created_at: "2026-03-01T10:08:00Z",
					visible_to_operator: true,
					message_id: "msg-1",
					message_version: 2,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			tab_counts: { activity_events: 2, messages: 1 },
		});
	});

	it("does not bump mailbox counts for non-user-facing mailbox events", async () => {
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		queryClient.setQueryData(runQueryKeys.detail("run-1"), {
			tab_counts: { dag_nodes: 0, agents: 0, activity_events: 1, executions: 0, messages: 4 },
		});

		renderHook(() => useRunEventStream("run-1"), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-mailbox-hidden",
				type: "message",
				data: JSON.stringify({
					event_type: "mailbox.message_recorded",
					run_event_seq: 2,
					visible_to_operator: false,
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(runQueryKeys.detail("run-1"))).toMatchObject({
			tab_counts: { activity_events: 2, messages: 4 },
		});

		const invalidatedKeys = invalidateSpy.mock.calls
			.map((call) => ({
				queryKey: (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey,
				exact: (call[0] as { exact?: boolean } | undefined)?.exact,
			}))
			.filter((call): call is { queryKey: readonly unknown[]; exact: boolean | undefined } => Array.isArray(call.queryKey));

		expect(invalidatedKeys).toContainEqual({ queryKey: runQueryKeys.agents("run-1"), exact: true });
		expect(invalidatedKeys).not.toContainEqual({ queryKey: ["runs", "run-1", "mailbox"], exact: false });
	});

  it("exposes retrying stream state during reconnect backoff", async () => {
    const invocations: Array<{
      onConnectionChange: (state: { state: string; attempt?: number; nextDelayMs?: number; lastError?: string }) => void;
    }> = [];
    const resolvers: Array<() => void> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>((resolve) => {
        resolvers.push(resolve);
      });
    });

    const { result } = renderHook(() => useRunEventStream("run-1", true), {
      wrapper: makeWrapper(),
    });
    await flush();

    expect(result.current).toBe("connecting");

    await act(async () => {
      invocations[0].onConnectionChange({ state: "retrying", attempt: 1, nextDelayMs: 500, lastError: "EOF" });
    });

    expect(result.current).toBe("retrying");

    await act(async () => {
      invocations[0].onConnectionChange({ state: "failed", attempt: 6, lastError: "EOF" });
      resolvers[0]();
    });

    expect(result.current).toBe("failed");
  });

  it("does not resubscribe after a fatal not-found stream failure", async () => {
    const invocations: Array<{
      onConnectionChange: (state: { state: string; attempt?: number; lastError?: string }) => void;
    }> = [];
    const resolvers: Array<() => void> = [];

    subscribeSSEMock.mockImplementation((args) => {
      invocations.push(args);
      return new Promise<void>((resolve) => {
        resolvers.push(resolve);
      });
    });

    renderHook(() => useRunEventStream("run-1", true), {
      wrapper: makeWrapper(),
    });
    await flush();

    expect(invocations).toHaveLength(1);

    await act(async () => {
      invocations[0].onConnectionChange({ state: "failed", attempt: 1, lastError: "HTTP 404" });
      resolvers[0]();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(RUN_STREAM_RECONNECT_DELAY_MS);
    });
    await flush();

    expect(invocations).toHaveLength(1);
  });
});
