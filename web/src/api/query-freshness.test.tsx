import { onlineManager, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	infiniteRunsCollectionQueryKey,
	runQueryKeys,
	useInfiniteRuns,
	useRun,
	useRunGovernanceDecisions,
	useRunGraph,
	useRuns,
} from "./hooks-runs";

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
}));

function buildRun(overrides: Record<string, unknown> = {}) {
	return {
		run_id: "run-1",
		work_order_id: "work order-1",
		work_order_title: "Fresh run",
		workflow_id: "work-order-v1",
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

let qc: QueryClient;

beforeEach(() => {
	apiFetchMock.mockReset();
	qc = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
});

afterEach(() => {
	vi.useRealTimers();
	onlineManager.setOnline(true);
	qc.clear();
});

async function sleep(ms: number): Promise<void> {
	await new Promise((resolve) => window.setTimeout(resolve, ms));
}

describe("query freshness ownership", () => {
	it("does not refetch stream-owned run detail on remount after the stale window elapses", async () => {
		apiFetchMock.mockImplementation((path: string) => {
			if (path === "/runs/run-1") {
				return Promise.resolve(buildRun());
			}
			throw new Error(`Unhandled apiFetch request: ${path}`);
		});

		const firstMount = renderHook(() => useRun("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(firstMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		firstMount.unmount();
		await sleep(2_100);
		apiFetchMock.mockClear();

		const secondMount = renderHook(() => useRun("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(secondMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("does not refetch stream-owned run detail on reconnect after the stale window elapses", async () => {
		apiFetchMock.mockImplementation((path: string) => {
			if (path === "/runs/run-1") {
				return Promise.resolve(buildRun());
			}
			throw new Error(`Unhandled apiFetch request: ${path}`);
		});

		const { result } = renderHook(() => useRun("run-1"), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		await sleep(2_100);
		apiFetchMock.mockClear();

		await act(async () => {
			onlineManager.setOnline(false);
			await Promise.resolve();
			onlineManager.setOnline(true);
		});

		await act(async () => {
			await Promise.resolve();
		});

		expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("does not refetch heavy-surface run graph on reconnect after the stale window elapses", async () => {
		apiFetchMock.mockImplementation((path: string) => {
			if (path === "/runs/run-1/graph") {
				return Promise.resolve({ run_id: "run-1", nodes: [], edges: [] });
			}
			throw new Error(`Unhandled apiFetch request: ${path}`);
		});

		const { result } = renderHook(() => useRunGraph("run-1", { enabled: true }), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		await sleep(2_100);
		apiFetchMock.mockClear();

		await act(async () => {
			onlineManager.setOnline(false);
			await Promise.resolve();
			onlineManager.setOnline(true);
		});

		await act(async () => {
			await Promise.resolve();
		});

		expect(apiFetchMock).not.toHaveBeenCalled();
	});

	it("refetches cached searched infinite runs once they are re-enabled", async () => {
		const queryKey = infiniteRunsCollectionQueryKey({ limit: 25, search: "stale search" });
		qc.setQueryData(queryKey, {
			pageParams: [undefined],
			pages: [{ items: [{ run_id: "run-1" }], next_cursor: null }],
		});

		apiFetchMock.mockResolvedValue({ items: [{ run_id: "run-2" }], next_cursor: null });

		const hook = renderHook(({ enabled }: { enabled: boolean }) => useInfiniteRuns({ enabled, limit: 25, search: "stale search" }), {
			initialProps: { enabled: false },
			wrapper: makeWrapper(qc),
		});

		expect(apiFetchMock).not.toHaveBeenCalled();

		hook.rerender({ enabled: true });

		await waitFor(() => {
			expect(apiFetchMock).toHaveBeenCalledTimes(1);
		});
	});

	it("refetches cached searched runs collections on remount", async () => {
		apiFetchMock.mockResolvedValue({ items: [{ run_id: "run-2" }], next_cursor: null });

		const firstMount = renderHook(() => useRuns({ limit: 25, search: "stale search" }), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(firstMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		firstMount.unmount();
		apiFetchMock.mockClear();

		const secondMount = renderHook(() => useRuns({ limit: 25, search: "stale search" }), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(secondMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);
	});

	it("refetches cached searched infinite runs collections on remount", async () => {
		apiFetchMock.mockResolvedValue({ items: [{ run_id: "run-2" }], next_cursor: null });

		const firstMount = renderHook(() => useInfiniteRuns({ limit: 25, search: "stale search" }), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(firstMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);

		firstMount.unmount();
		apiFetchMock.mockClear();

		const secondMount = renderHook(() => useInfiniteRuns({ limit: 25, search: "stale search" }), {
			wrapper: makeWrapper(qc),
		});

		await waitFor(() => {
			expect(secondMount.result.current.isSuccess).toBe(true);
		});
		expect(apiFetchMock).toHaveBeenCalledTimes(1);
	});

	it("refetches invalidated governance history once it is re-enabled", async () => {
		const queryKey = runQueryKeys.governanceHistory("run-1");
		qc.setQueryData(queryKey, {
			pageParams: [undefined],
			pages: [{ items: [{ decision_id: "decision-1" }], next_cursor: null }],
		});
		await qc.invalidateQueries({ queryKey, exact: true, refetchType: "none" });

		apiFetchMock.mockResolvedValue({ items: [{ decision_id: "decision-2" }], next_cursor: null });

		const hook = renderHook(({ enabled }: { enabled: boolean }) => useRunGovernanceDecisions("run-1", { enabled }), {
			initialProps: { enabled: false },
			wrapper: makeWrapper(qc),
		});

		expect(apiFetchMock).not.toHaveBeenCalled();

		hook.rerender({ enabled: true });

		await waitFor(() => {
			expect(apiFetchMock).toHaveBeenCalledTimes(1);
		});
	});
});
