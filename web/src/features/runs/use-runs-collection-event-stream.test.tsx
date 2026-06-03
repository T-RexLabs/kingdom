import { QueryClient, QueryClientProvider, QueryObserver } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	RUN_COLLECTION_INVALIDATION_COALESCE_MS,
  RUN_COLLECTION_STREAM_RECONNECT_DELAY_MS,
	applyRunsCollectionStreamEvent,
  useRunsCollectionEventStream,
} from "./use-runs-collection-event-stream";

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
let observerCleanups: Array<() => void>;

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

describe("useRunsCollectionEventStream", () => {
  beforeEach(() => {
    subscribeSSEMock.mockReset();
    buildAuthHeadersMock.mockClear();
    vi.useFakeTimers();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    observerCleanups = [];
  });

  afterEach(() => {
    for (const cleanup of observerCleanups) {
      cleanup();
    }
    vi.useRealTimers();
    queryClient.clear();
  });

	function observeCollectionQuery(queryKey: readonly unknown[], data: unknown) {
		queryClient.setQueryData(queryKey, data);
		const observer = new QueryObserver(queryClient, {
			queryKey: [...queryKey],
			queryFn: async () => queryClient.getQueryData(queryKey),
			initialData: data,
			staleTime: Number.POSITIVE_INFINITY,
		});
		const unsubscribe = observer.subscribe(() => undefined);
		observerCleanups.push(unsubscribe);
		return unsubscribe;
	}

	it("patches stable-order infinite collections in place without refetching searched queries", async () => {
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
	    observeCollectionQuery(["runs", "collection", null, null, null], {
	      items: [{
	        run_id: "run-1",
        workflow_id: "wf-1",
        decree_id: "decree-1",
        decree_title: "Original",
        status: "pending",
        plan_state: "draft",
        display_state: "pending",
        created_at: "2026-03-01T10:00:00Z",
        updated_at: "2026-03-01T10:00:00Z",
        last_activity_at: "2026-03-01T10:00:00Z",
        autonomous_approvals: false,
        publish_mode: "direct",
      }],
	    });
	    observeCollectionQuery(["runs", "collection", "infinite", null, 50, null], {
	      pageParams: [undefined],
	      pages: [{ items: [{
        run_id: "run-1",
        workflow_id: "wf-1",
        decree_id: "decree-1",
        decree_title: "Original",
        status: "pending",
        plan_state: "draft",
        display_state: "pending",
        created_at: "2026-03-01T10:00:00Z",
        updated_at: "2026-03-01T10:00:00Z",
        last_activity_at: "2026-03-01T10:00:00Z",
        autonomous_approvals: false,
        publish_mode: "direct",
	      }] }],
	    });
	    observeCollectionQuery(["runs", "collection", null, 25, "stale search"], {
	      items: [{
	        run_id: "run-1",
	        workflow_id: "wf-1",
	        decree_id: "decree-1",
	        decree_title: "Search cached",
	        status: "pending",
	        plan_state: "draft",
	        display_state: "pending",
	        created_at: "2026-03-01T10:00:00Z",
	        updated_at: "2026-03-01T10:00:00Z",
	        last_activity_at: "2026-03-01T10:00:00Z",
	      }],
	    });
	    queryClient.setQueryData(["unrelated"], { ok: true });

    renderHook(() => useRunsCollectionEventStream(true), {
      wrapper: makeWrapper(),
    });
    await flush();

    expect(invocations).toHaveLength(1);
    await expect(
      invocations[0].onEvent({
        id: "evt-collection-1",
        type: "message",
        data: JSON.stringify({
          event_type: "run.state_changed",
          run_id: "run-1",
          workflow_id: "wf-1",
          decree_id: "decree-1",
          decree_title: "Updated",
          status: "running",
          plan_state: "approved",
          display_state: "running",
          created_at: "2026-03-01T10:05:00Z",
          updated_at: "2026-03-01T10:05:00Z",
          last_activity_at: "2026-03-01T10:05:00Z",
          autonomous_approvals: false,
          publish_mode: "direct",
        }),
      }),
	    ).resolves.toBe(true);

	    const invalidationCalls = invalidateSpy.mock.calls.map((call) => call[0]);
	    expect(invalidationCalls).not.toContainEqual({ queryKey: ["runs", "collection", "infinite", null, 50, null], exact: true, refetchType: "active" });
	    expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "collection", null, 25, "stale search"], exact: true, refetchType: "active" });
	    expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "collection", null, 25, "stale search"], exact: true, refetchType: "none" });

	    await act(async () => {
	      await vi.advanceTimersByTimeAsync(RUN_COLLECTION_INVALIDATION_COALESCE_MS);
	    });

	    await act(async () => {
	      invocations[0].onConnectionChange({ state: "failed", attempt: 3, lastError: "EOF" });
      resolvers[0]();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(RUN_COLLECTION_STREAM_RECONNECT_DELAY_MS);
    });
    await flush();

	    expect(invocations).toHaveLength(2);
	    expect(invocations[1].buildHeaders(null)).toEqual({ "Last-Event-ID": "evt-collection-1" });

	    expect(queryClient.getQueryData(["runs", "collection", null, null, null])).toMatchObject({
	      items: [{ run_id: "run-1", decree_title: "Updated", status: "running", display_state: "running" }],
	    });
	    expect(queryClient.getQueryData(["runs", "collection", "infinite", null, 50, null])).toMatchObject({
	      pages: [{ items: [{ run_id: "run-1", decree_title: "Updated", status: "running", display_state: "running" }] }],
	    });
	    expect(queryClient.getQueryData(["runs", "collection", null, 25, "stale search"])).toMatchObject({
	      items: [{ run_id: "run-1", decree_title: "Search cached", status: "pending", display_state: "pending" }],
	    });
	    expect(queryClient.getQueryState(["runs", "collection", null, 25, "stale search"])?.isInvalidated).not.toBe(true);
	    expect(queryClient.getQueryState(["unrelated"])?.isInvalidated).not.toBe(true);
	  });

	it("applies retry and rerun reopen collection events as first-class summary updates", async () => {
		observeCollectionQuery(["runs", "collection", null, null, null], {
			items: [{
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Original",
				status: "failed",
				plan_state: "approved",
				display_state: "failed",
				created_at: "2026-03-01T10:00:00Z",
				updated_at: "2026-03-01T10:00:00Z",
				last_activity_at: "2026-03-01T10:00:00Z",
			}],
		});

		await expect(applyRunsCollectionStreamEvent(queryClient, {
			id: "evt-retry-reopened",
			type: "message",
			data: JSON.stringify({
				event_type: "run.retry_reopened",
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Original",
				status: "running",
				plan_state: "approved",
				display_state: "running",
				created_at: "2026-03-01T10:00:00Z",
				updated_at: "2026-03-01T10:05:00Z",
				last_activity_at: "2026-03-01T10:05:00Z",
			}),
		})).resolves.toBe(true);

		expect(queryClient.getQueryData(["runs", "collection", null, null, null])).toMatchObject({
			items: [{ run_id: "run-1", status: "running", display_state: "running" }],
		});

		await expect(applyRunsCollectionStreamEvent(queryClient, {
			id: "evt-rerun-reopened",
			type: "message",
			data: JSON.stringify({
				event_type: "run.rerun_reopened",
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Original",
				status: "running",
				plan_state: "draft",
				display_state: "running",
				created_at: "2026-03-01T10:00:00Z",
				updated_at: "2026-03-01T10:06:00Z",
				last_activity_at: "2026-03-01T10:06:00Z",
			}),
		})).resolves.toBe(true);

		expect(queryClient.getQueryData(["runs", "collection", null, null, null])).toMatchObject({
			items: [{ run_id: "run-1", status: "running", plan_state: "draft", display_state: "running" }],
		});
	});

	it("preserves pending approval identity from collection events", async () => {
		observeCollectionQuery(["runs", "collection", null, null, null], {
			items: [{
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Original",
				status: "running",
				plan_state: "draft",
				display_state: "running",
				created_at: "2026-03-01T10:00:00Z",
				updated_at: "2026-03-01T10:00:00Z",
				last_activity_at: "2026-03-01T10:00:00Z",
				autonomous_approvals: false,
			}],
		});

		await expect(applyRunsCollectionStreamEvent(queryClient, {
			id: "evt-pending-approval-key",
			type: "message",
			data: JSON.stringify({
				event_type: "run.state_changed",
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Original",
				status: "running",
				plan_state: "awaiting_king_decision",
				display_state: "awaiting_plan_decision",
				created_at: "2026-03-01T10:00:00Z",
				updated_at: "2026-03-01T10:05:00Z",
				last_activity_at: "2026-03-01T10:05:00Z",
				autonomous_approvals: false,
				pending_approval_key: "plan:planrev-2",
			}),
		})).resolves.toBe(true);

		expect(queryClient.getQueryData(["runs", "collection", null, null, null])).toMatchObject({
			items: [{ run_id: "run-1", display_state: "awaiting_plan_decision", pending_approval_key: "plan:planrev-2" }],
		});
	});

	it("keeps multi-page infinite collections immutable so cursor boundaries stay server-owned", async () => {
		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		const invocations: Array<{
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		observeCollectionQuery(["runs", "collection", "infinite", null, 50, null], {
			pageParams: [undefined, "cursor-2"],
			pages: [
				{
					items: [{
						run_id: "run-2",
						workflow_id: "wf-1",
						decree_id: "decree-2",
						decree_title: "Second",
						status: "pending",
						plan_state: "draft",
						display_state: "pending",
						created_at: "2026-03-01T10:01:00Z",
						updated_at: "2026-03-01T10:01:00Z",
						last_activity_at: "2026-03-01T10:01:00Z",
					}],
					next_cursor: "cursor-2",
				},
				{
					items: [{
						run_id: "run-1",
						workflow_id: "wf-1",
						decree_id: "decree-1",
						decree_title: "Original",
						status: "pending",
						plan_state: "draft",
						display_state: "pending",
						created_at: "2026-03-01T10:00:00Z",
						updated_at: "2026-03-01T10:00:00Z",
						last_activity_at: "2026-03-01T10:00:00Z",
					}],
				},
			],
		});

		renderHook(() => useRunsCollectionEventStream(true), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-collection-2",
				type: "message",
				data: JSON.stringify({
					event_type: "run.created",
					run_id: "run-3",
					workflow_id: "wf-1",
					decree_id: "decree-3",
					decree_title: "Newest",
					status: "pending",
					plan_state: "draft",
					display_state: "pending",
					created_at: "2026-03-01T10:02:00Z",
					updated_at: "2026-03-01T10:02:00Z",
					last_activity_at: "2026-03-01T10:02:00Z",
				}),
			}),
		).resolves.toBe(true);

		await act(async () => {
			await vi.advanceTimersByTimeAsync(RUN_COLLECTION_INVALIDATION_COALESCE_MS);
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection", "infinite", null, 50, null], exact: true, refetchType: "active" });
		expect(queryClient.getQueryData(["runs", "collection", "infinite", null, 50, null])).toMatchObject({
			pageParams: [undefined, "cursor-2"],
			pages: [
				{ items: [{ run_id: "run-2" }], next_cursor: "cursor-2" },
				{ items: [{ run_id: "run-1" }] },
			],
		});
	});

	it("still invalidates awaiting-human-decision infinite collections when updates can reorder pages", async () => {
		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		observeCollectionQuery(["runs", "collection", "infinite", "awaiting_human_decision", 50, null], {
			pageParams: [undefined],
			pages: [{
				items: [{
					run_id: "run-1",
					workflow_id: "wf-1",
					decree_id: "decree-1",
					decree_title: "Original",
					status: "running",
					plan_state: "awaiting_king_decision",
					display_state: "awaiting_plan_decision",
					created_at: "2026-03-01T10:00:00Z",
					updated_at: "2026-03-01T10:00:00Z",
					last_activity_at: "2026-03-01T10:00:00Z",
					autonomous_approvals: false,
					publish_mode: "direct",
				}],
			}],
		});

		await expect(
			applyRunsCollectionStreamEvent(queryClient as ReturnType<typeof import("@tanstack/react-query").useQueryClient>, {
				id: "evt-awaiting-update",
				type: "message",
				data: JSON.stringify({
					event_type: "approval.recorded",
					run_id: "run-1",
					workflow_id: "wf-1",
					decree_id: "decree-1",
					decree_title: "Updated",
					status: "running",
					plan_state: "awaiting_king_decision",
					display_state: "awaiting_plan_decision",
					created_at: "2026-03-01T10:00:00Z",
					updated_at: "2026-03-01T10:05:00Z",
					last_activity_at: "2026-03-01T10:05:00Z",
					autonomous_approvals: false,
					publish_mode: "direct",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(["runs", "collection", "infinite", "awaiting_human_decision", 50, null])).toMatchObject({
			pages: [{ items: [{ run_id: "run-1", decree_title: "Updated", updated_at: "2026-03-01T10:05:00Z" }] }],
		});

		await act(async () => {
			await vi.advanceTimersByTimeAsync(RUN_COLLECTION_INVALIDATION_COALESCE_MS);
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection", "infinite", "awaiting_human_decision", 50, null], exact: true, refetchType: "active" });
	});

	it("invalidates a full finite capped collection when a removal needs server backfill", async () => {
		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		observeCollectionQuery(["runs", "collection", "running", 1, null], {
			items: [{
				run_id: "run-1",
				workflow_id: "wf-1",
				decree_id: "decree-1",
				decree_title: "Only active run",
				status: "running",
				plan_state: "approved",
					display_state: "running",
					created_at: "2026-03-01T10:00:00Z",
					updated_at: "2026-03-01T10:00:00Z",
					last_activity_at: "2026-03-01T10:00:00Z",
					autonomous_approvals: false,
					publish_mode: "direct",
			}],
		});

		await expect(
			applyRunsCollectionStreamEvent(queryClient as ReturnType<typeof import("@tanstack/react-query").useQueryClient>, {
				id: "evt-removal-1",
				type: "message",
				data: JSON.stringify({
					event_type: "run.state_changed",
					run_id: "run-1",
					workflow_id: "wf-1",
					decree_id: "decree-1",
					decree_title: "Only active run",
					status: "succeeded",
					plan_state: "approved",
					display_state: "succeeded",
					created_at: "2026-03-01T10:00:00Z",
					updated_at: "2026-03-01T10:05:00Z",
					last_activity_at: "2026-03-01T10:05:00Z",
					autonomous_approvals: false,
					publish_mode: "direct",
				}),
			}),
		).resolves.toBe(true);

		expect(queryClient.getQueryData(["runs", "collection", "running", 1, null])).toMatchObject({ items: [] });

		await act(async () => {
			await vi.advanceTimersByTimeAsync(RUN_COLLECTION_INVALIDATION_COALESCE_MS);
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection", "running", 1, null], exact: true, refetchType: "active" });
	});

	it("keeps finite collections referentially stable on semantically identical updates", async () => {
		const queryKey = ["runs", "collection", null, 25, null] as const;
		const item = {
			run_id: "run-1",
			workflow_id: "wf-1",
			decree_id: "decree-1",
			decree_title: "Original",
			status: "running",
			plan_state: "approved",
			display_state: "running",
			created_at: "2026-03-01T10:00:00Z",
			updated_at: "2026-03-01T10:05:00Z",
			last_activity_at: "2026-03-01T10:05:00Z",
			autonomous_approvals: false,
			publish_mode: "direct",
		};
		const initial = { items: [item] };
		observeCollectionQuery(queryKey, initial);

		await expect(
			applyRunsCollectionStreamEvent(queryClient as ReturnType<typeof import("@tanstack/react-query").useQueryClient>, {
				id: "evt-noop-finite",
				type: "message",
				data: JSON.stringify({
					event_type: "run.state_changed",
					...item,
				}),
			}),
		).resolves.toBe(true);

		const next = queryClient.getQueryData<typeof initial>(queryKey);
		expect(next).toBe(initial);
		expect(next?.items[0]).toBe(item);
		expect(queryClient.getQueryState(queryKey)?.isInvalidated).not.toBe(true);
	});

	it("treats semantically identical model_selection payloads as collection no-ops", async () => {
		const queryKey = ["runs", "collection", null, 25, null] as const;
		const item = {
			run_id: "run-1",
			workflow_id: "wf-1",
			decree_id: "decree-1",
			decree_title: "Original",
			status: "running",
			plan_state: "approved",
			display_state: "running",
			created_at: "2026-03-01T10:00:00Z",
			updated_at: "2026-03-01T10:05:00Z",
			last_activity_at: "2026-03-01T10:05:00Z",
			autonomous_approvals: false,
			publish_mode: "direct",
			model_selection: {
				global: { model: "github-copilot/gpt-5.4", thinking_effort: "low" },
				role_overrides: {
					"council-member": { model: "github-copilot/gpt-5.4-mini", thinking_effort: "low" },
				},
			},
		};
		const initial = { items: [item] };
		observeCollectionQuery(queryKey, initial);
		const setQueryDataSpy = vi.spyOn(queryClient, "setQueryData");

		await expect(
			applyRunsCollectionStreamEvent(queryClient as ReturnType<typeof import("@tanstack/react-query").useQueryClient>, {
				id: "evt-noop-model-selection",
				type: "message",
				data: JSON.stringify({
					event_type: "run.state_changed",
					...item,
					model_selection: {
						global: { model: "github-copilot/gpt-5.4", thinking_effort: "low" },
						role_overrides: {
							"council-member": { model: "github-copilot/gpt-5.4-mini", thinking_effort: "low" },
						},
					},
				}),
			}),
		).resolves.toBe(true);

		const next = queryClient.getQueryData<typeof initial>(queryKey);
		expect(setQueryDataSpy).not.toHaveBeenCalled();
		expect(next).toBe(initial);
		expect(next?.items[0]).toBe(item);
		expect(queryClient.getQueryState(queryKey)?.isInvalidated).not.toBe(true);
	});

	it("acknowledges malformed collection events and falls back to invalidation", async () => {
	    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
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

			observeCollectionQuery(["runs", "collection", null, null, null], {
				items: [{
					run_id: "run-1",
					workflow_id: "wf-1",
					decree_id: "decree-1",
					decree_title: "Original",
					status: "pending",
					plan_state: "draft",
					display_state: "pending",
					created_at: "2026-03-01T10:00:00Z",
					updated_at: "2026-03-01T10:00:00Z",
					last_activity_at: "2026-03-01T10:00:00Z",
				}],
			});

			renderHook(() => useRunsCollectionEventStream(true), {
		      wrapper: makeWrapper(),
		    });
		    await flush();

	    await expect(
	      invocations[0].onEvent({
	        id: "evt-ignore",
	        type: "message",
	        data: JSON.stringify({ event_type: "run.state_changed", run_id: "run-1" }),
	      }),
	    ).resolves.toBe(true);

			expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection", null, null, null], exact: true, refetchType: "active" });

	    await act(async () => {
	      invocations[0].onConnectionChange({ state: "failed", attempt: 2, lastError: "EOF" });
      resolvers[0]();
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(RUN_COLLECTION_STREAM_RECONNECT_DELAY_MS);
	    });
	    await flush();

	    expect(invocations[1].buildHeaders(null)).toEqual({ "Last-Event-ID": "evt-ignore" });
	  });

	it("clears a bad collection cursor on HTTP 400 and reconnects without Last-Event-ID", async () => {
		const invocations: Array<{
			buildHeaders: (lastEventId: string | null) => HeadersInit;
			onConnectionChange: (state: { state: string; attempt?: number; lastError?: string }) => void;
			onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
		}> = [];

		subscribeSSEMock.mockImplementation((args) => {
			invocations.push(args);
			return new Promise<void>(() => undefined);
		});

		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
		observeCollectionQuery(["runs", "collection", null, null, null], { items: [] });

		renderHook(() => useRunsCollectionEventStream(true), {
			wrapper: makeWrapper(),
		});
		await flush();

		await expect(
			invocations[0].onEvent({
				id: "evt-collection-400",
				type: "message",
				data: JSON.stringify({
					event_type: "run.state_changed",
					run_id: "run-1",
					workflow_id: "wf-1",
					decree_id: "decree-1",
					decree_title: "Updated",
					status: "running",
					plan_state: "approved",
					display_state: "running",
					created_at: "2026-03-01T10:05:00Z",
					updated_at: "2026-03-01T10:05:00Z",
					last_activity_at: "2026-03-01T10:05:00Z",
					autonomous_approvals: false,
					publish_mode: "direct",
				}),
			}),
		).resolves.toBe(true);

		await act(async () => {
			invocations[0].onConnectionChange({ state: "failed", attempt: 1, lastError: "HTTP 400" });
		});
		await flush();

		expect(invocations).toHaveLength(2);
		expect(invocations[1].buildHeaders(null)).toEqual({});
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "collection", null, null, null], exact: true, refetchType: "active" });
	});
});
