import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import type { ComponentProps, ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DashboardPage } from "./dashboard-page";

type RequestParams = Record<string, string | number | boolean | undefined>;

const apiFetchMock = vi.fn();
const sparklineRenderSpy = vi.fn();

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to, ...props }: Omit<ComponentProps<"a">, "href"> & { to?: string }) => (
    <a href={to} {...props}>{children}</a>
  ),
}));

vi.mock("@/components/ui/sparkline", async () => {
  const React = await vi.importActual<typeof import("react")>("react");
  return {
    Sparkline: React.memo((props: unknown) => {
      sparklineRenderSpy(props);
      return <div data-testid="sparkline" />;
    }),
  };
});

vi.mock("@/api/client", async () => {
  const actual = await vi.importActual<typeof import("@/api/client")>("@/api/client");
  return {
    ...actual,
    apiFetch: (...args: unknown[]) => apiFetchMock(...args),
  };
});

const defaultResponses = new Map<string, unknown>();
const queuedResponses = new Map<string, unknown[]>();

function requestKey(path: string, params?: RequestParams): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params ?? {}).sort(([left], [right]) => left.localeCompare(right))) {
    if (value !== undefined) {
      searchParams.set(key, String(value));
    }
  }
  const query = searchParams.toString();
  return query ? `${path}?${query}` : path;
}

function setDefaultResponse(path: string, response: unknown, params?: RequestParams) {
  defaultResponses.set(requestKey(path, params), response);
}

function queueResponse(path: string, response: unknown, params?: RequestParams) {
  const key = requestKey(path, params);
  const queued = queuedResponses.get(key) ?? [];
  queued.push(response);
  queuedResponses.set(key, queued);
}

function createDeferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
}

function makeWrapper(queryClient: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

const dashboardSnapshot = {
  snapshot_at: "2026-03-22T10:10:00Z",
  window_minutes: 1440,
  all_time_run_count: 2,
  runtime: {
    status: "ok",
    running_runs: 3,
    running_nodes: 5,
    stale_running_nodes: 0,
  },
  overview: {
    total_runs: 10,
    succeeded_runs: 8,
    failed_runs: 2,
    cancelled_runs: 0,
    avg_duration_seconds: 30,
    success_rate: 100,
    review_rework_rate: 0,
    validation_rework_rate: 100,
  },
  tokens: {
    derived_total_tokens: 2600,
    input: 1800,
    output: 600,
    reasoning: 200,
  },
  recent_runs: [
    {
      run_id: "run_1",
      decree_id: "decree_1",
      decree_title: "Royal decree",
      workflow_id: "royal-decree-v1",
      status: "running",
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_king_decision",
      autonomous_approvals: false,
      created_at: "2026-03-22T09:55:00Z",
      updated_at: "2026-03-22T10:10:00Z",
      last_activity_at: "2026-03-22T10:10:00Z",
    },
    {
      run_id: "run_2",
      decree_id: "decree_2",
      decree_title: "Broken compact",
      workflow_id: "royal-decree-v1",
      status: "failed",
      display_state: "failed",
      plan_state: "approved",
      autonomous_approvals: true,
      created_at: "2026-03-22T08:45:00Z",
      updated_at: "2026-03-22T09:15:00Z",
      last_activity_at: "2026-03-22T09:15:00Z",
    },
  ],
  run_trend: [
    { bucket_start_at: "2026-03-21T12:00:00Z", run_count: 1 },
    { bucket_start_at: "2026-03-21T14:00:00Z", run_count: 0 },
  ],
};

const degradedDashboardSnapshot = {
  ...dashboardSnapshot,
  runtime: {
    status: "degraded",
    running_runs: 3,
    running_nodes: 5,
    stale_running_nodes: 1,
  },
};

afterEach(() => {
  defaultResponses.clear();
  queuedResponses.clear();
  apiFetchMock.mockReset();
  sparklineRenderSpy.mockReset();
});

describe("DashboardPage", () => {
  it("renders the dashboard from a single snapshot query", async () => {
    setDefaultResponse("/dashboard/snapshot", dashboardSnapshot);

    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
      const key = requestKey(path, options?.params);
      const queued = queuedResponses.get(key);
      if (queued && queued.length > 0) {
        return queued.shift();
      }
      if (!defaultResponses.has(key)) {
        throw new Error(`Unhandled apiFetch request: ${key}`);
      }
      return defaultResponses.get(key);
    });

    const queryClient = createQueryClient();
    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Royal decree");

    expect(screen.getByText("Command Center Overview · Last 24h")).toBeInTheDocument();
    expect(screen.getByText("Realm Online")).toBeInTheDocument();
    expect(screen.getByText("Running Now")).toBeInTheDocument();
    expect(screen.getByText("Succeeded (24h)")).toBeInTheDocument();
    expect(screen.getByText("Failed (24h)")).toBeInTheDocument();
    expect(screen.getByText("Avg Duration")).toBeInTheDocument();
    expect(screen.getByText("Success Rate")).toBeInTheDocument();
    expect(screen.getByLabelText("Review Rework Rate definition")).toBeInTheDocument();
    expect(screen.getByLabelText("Validation Rework Rate definition")).toBeInTheDocument();
    expect(screen.getByText("Active Nodes")).toBeInTheDocument();
    expect(screen.getByText("Token Count (24h)")).toBeInTheDocument();
    expect(screen.getByText("2.6K")).toBeInTheDocument();
    expect(screen.getByText("1.8K")).toBeInTheDocument();
    expect(screen.getByText("600")).toBeInTheDocument();
    expect(screen.getByText("200")).toBeInTheDocument();
	    expect(screen.getByText("Broken compact")).toBeInTheDocument();
	    expect(screen.getByText("Awaiting Plan Decision")).toBeInTheDocument();
	    expect(screen.getByText("Action Required")).toBeInTheDocument();
	    expect(screen.getAllByText(/Created /)).toHaveLength(2);
	    expect(screen.getAllByText(/Last Activity/)).toHaveLength(2);
	    expect(screen.queryByText(/royal-decree-v1/)).not.toBeInTheDocument();
	    await screen.findByTestId("sparkline");
    expect(screen.queryByText("Refreshing dashboard metrics...")).not.toBeInTheDocument();
    expect(apiFetchMock).toHaveBeenCalledTimes(1);
	    expect(apiFetchMock).toHaveBeenCalledWith("/dashboard/snapshot");
  });

  it("keeps the current dashboard content visible during background snapshot refresh", async () => {
    setDefaultResponse("/dashboard/snapshot", dashboardSnapshot);

    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
      const key = requestKey(path, options?.params);
      const queued = queuedResponses.get(key);
      if (queued && queued.length > 0) {
        return queued.shift();
      }
      if (!defaultResponses.has(key)) {
        throw new Error(`Unhandled apiFetch request: ${key}`);
      }
      return defaultResponses.get(key);
    });

    const queryClient = createQueryClient();
    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Royal decree");

    const snapshotRefresh = createDeferred<typeof dashboardSnapshot>();
    queueResponse("/dashboard/snapshot", snapshotRefresh.promise);

    await act(async () => {
      void queryClient.invalidateQueries({ queryKey: ["dashboard", "snapshot"] });
    });

    expect(screen.getByText("Royal decree")).toBeInTheDocument();
    expect(screen.getByText("Token Count (24h)")).toBeInTheDocument();
    expect(screen.queryByText("Refreshing dashboard metrics...")).not.toBeInTheDocument();

    await act(async () => {
      snapshotRefresh.resolve(dashboardSnapshot);
    });

    await waitFor(() => {
      expect(
        apiFetchMock.mock.calls.filter(([path]) => path === "/dashboard/snapshot").length,
      ).toBe(2);
    });
    expect(screen.getByText("Royal decree")).toBeInTheDocument();
  });

  it("does not rerender the dashboard sparkline while retained trend data is unchanged", async () => {
	    setDefaultResponse("/dashboard/snapshot", dashboardSnapshot);

	    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
	      const key = requestKey(path, options?.params);
	      const queued = queuedResponses.get(key);
	      if (queued && queued.length > 0) {
	        return queued.shift();
	      }
	      if (!defaultResponses.has(key)) {
	        throw new Error(`Unhandled apiFetch request: ${key}`);
	      }
	      return defaultResponses.get(key);
	    });

	    const queryClient = createQueryClient();
	    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

	    await screen.findByText("Royal decree");
	    await waitFor(() => expect(sparklineRenderSpy).toHaveBeenCalled());
	    sparklineRenderSpy.mockClear();

	    const snapshotRefresh = createDeferred<typeof dashboardSnapshot>();
	    queueResponse("/dashboard/snapshot", snapshotRefresh.promise);

	    await act(async () => {
	      void queryClient.invalidateQueries({ queryKey: ["dashboard", "snapshot"] });
	    });

	    expect(sparklineRenderSpy).not.toHaveBeenCalled();

	    await act(async () => {
	      snapshotRefresh.resolve(dashboardSnapshot);
	    });

	    await waitFor(() => {
	      expect(
	        apiFetchMock.mock.calls.filter(([path]) => path === "/dashboard/snapshot").length,
	      ).toBe(2);
	    });
	  });

	  it("shows an explicit error state when the initial snapshot load fails", async () => {
	    setDefaultResponse("/dashboard/snapshot", new Error("Server unreachable"));

	    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
	      const key = requestKey(path, options?.params);
	      const response = defaultResponses.get(key);
	      if (response === undefined) {
	        throw new Error(`Unhandled apiFetch request: ${key}`);
	      }
	      if (response instanceof Error) {
	        throw response;
	      }
	      return response;
	    });

	    const queryClient = createQueryClient();
	    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

	    await screen.findByText("Dashboard unavailable");
	    expect(screen.getByText("Snapshot unavailable")).toBeInTheDocument();
	    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
	    expect(screen.queryByText("Running Now")).not.toBeInTheDocument();
	  });

	  it("shows a warning and keeps stale data visible when a background refresh fails", async () => {
	    setDefaultResponse("/dashboard/snapshot", dashboardSnapshot);

	    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
	      const key = requestKey(path, options?.params);
	      const queued = queuedResponses.get(key);
	      if (queued && queued.length > 0) {
	        const response = queued.shift();
	        if (response instanceof Error) {
	          throw response;
	        }
	        return response;
	      }
	      if (!defaultResponses.has(key)) {
	        throw new Error(`Unhandled apiFetch request: ${key}`);
	      }
	      return defaultResponses.get(key);
	    });

	    const queryClient = createQueryClient();
	    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

	    await screen.findByText("Royal decree");
	    queueResponse("/dashboard/snapshot", new Error("Server unreachable"));

	    await act(async () => {
	      await queryClient.invalidateQueries({ queryKey: ["dashboard", "snapshot"] });
	    });

	    await screen.findByText("A refresh failed. Showing the last successful dashboard snapshot. Server unreachable");
	    expect(screen.getByText("Royal decree")).toBeInTheDocument();
	    expect(screen.getByText("Running Now")).toBeInTheDocument();
	  });

	  it("renders a window-scoped empty state when history exists outside the snapshot window", async () => {
	    setDefaultResponse("/dashboard/snapshot", {
	      ...dashboardSnapshot,
	      recent_runs: [],
	      all_time_run_count: 4,
	    });

	    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
	      const key = requestKey(path, options?.params);
	      if (!defaultResponses.has(key)) {
	        throw new Error(`Unhandled apiFetch request: ${key}`);
	      }
	      return defaultResponses.get(key);
	    });

	    const queryClient = createQueryClient();
	    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

	    await screen.findByText("No runs in the last 24h");
	    expect(screen.queryByText("Issue your first decree to set the realm in motion.")).not.toBeInTheDocument();
	    expect(screen.getByText("View Run History")).toBeInTheDocument();
	  });

	  it("renders degraded runtime health from the snapshot status", async () => {
	    setDefaultResponse("/dashboard/snapshot", degradedDashboardSnapshot);

	    apiFetchMock.mockImplementation((path: string, options?: { params?: RequestParams }) => {
	      const key = requestKey(path, options?.params);
	      if (!defaultResponses.has(key)) {
	        throw new Error(`Unhandled apiFetch request: ${key}`);
	      }
	      return defaultResponses.get(key);
	    });

	    const queryClient = createQueryClient();
	    render(<DashboardPage />, { wrapper: makeWrapper(queryClient) });

	    await screen.findByText("Realm Degraded");
	    expect(screen.getByText("(1 stale)")).toBeInTheDocument();
	  });
});
