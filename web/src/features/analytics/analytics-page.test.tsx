import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { AnchorHTMLAttributes, ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import { AnalyticsPage } from "./analytics-page";

type RequestParams = Record<string, string | number | boolean | undefined>;

const apiFetchMock = vi.fn();

const routerState = vi.hoisted(() => ({
  search: {} as Record<string, unknown>,
  listeners: new Set<() => void>(),
}));

const chartRenderSpies = vi.hoisted(() => ({
  horizontal: vi.fn(),
  stacked: vi.fn(),
}));

vi.mock("@tanstack/react-router", async () => {
  const React = await vi.importActual<typeof import("react")>("react");

  return {
    Link: ({ children, ...props }: AnchorHTMLAttributes<HTMLAnchorElement>) => (
      <a {...props}>{children}</a>
    ),
    useSearch: ({
      select,
    }: { select?: (search: Record<string, unknown>) => unknown } = {}) =>
      React.useSyncExternalStore(
        (listener) => {
          routerState.listeners.add(listener);
          return () => routerState.listeners.delete(listener);
        },
        () => (select ? select(routerState.search) : routerState.search),
        () => (select ? select(routerState.search) : routerState.search),
      ),
    useNavigate:
      () =>
      ({
        search,
      }: {
        search:
          | Record<string, unknown>
          | ((previous: Record<string, unknown>) => Record<string, unknown>);
      }) => {
        routerState.search =
          typeof search === "function" ? search(routerState.search) : search;
        for (const listener of Array.from(routerState.listeners)) {
          listener();
        }
      },
  };
});

vi.mock("@/components/ui/horizontal-bar-chart", async () => {
  const React = await vi.importActual<typeof import("react")>("react");
  return {
    HorizontalBarChart: React.memo((props: unknown) => {
      chartRenderSpies.horizontal(props);
      return <div data-testid="horizontal-bar-chart" />;
    }),
  };
});

vi.mock("@/components/ui/stacked-bar-chart", async () => {
  const React = await vi.importActual<typeof import("react")>("react");
  return {
    StackedBarChart: React.memo((props: unknown) => {
      chartRenderSpies.stacked(props);
      return <div data-testid="stacked-bar-chart" />;
    }),
  };
});

vi.mock("@/api/client", async () => {
  const actual =
    await vi.importActual<typeof import("@/api/client")>("@/api/client");
  return {
    ...actual,
    apiFetch: (...args: unknown[]) => apiFetchMock(...args),
  };
});

vi.mock("@/theme", async () => {
  const { roleTitle } = await import("@/lib/utils");
  const theme = {};
  return {
    useTheme: () => ({ theme }),
    roleIdentity: (_theme: unknown, role: string) => ({
      name: roleTitle(role),
    }),
  };
});

const defaultResponses = new Map<string, unknown>();
const queuedResponses = new Map<string, unknown[]>();

function requestKey(path: string, params?: RequestParams): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params ?? {}).sort(
    ([left], [right]) => left.localeCompare(right),
  )) {
    if (value !== undefined) {
      searchParams.set(key, String(value));
    }
  }
  const query = searchParams.toString();
  return query ? `${path}?${query}` : path;
}

function setDefaultResponse(
  path: string,
  response: unknown,
  params?: RequestParams,
) {
  defaultResponses.set(requestKey(path, params), response);
}

function queueResponse(
  path: string,
  response: unknown,
  params?: RequestParams,
) {
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
      queries: { retry: false, retryDelay: 1 },
      mutations: { retry: false },
    },
  });
}

function installApiFetchMock() {
  apiFetchMock.mockImplementation(
    (path: string, options?: { params?: RequestParams }) => {
      const key = requestKey(path, options?.params);
      const queued = queuedResponses.get(key);
      const response =
        queued && queued.length > 0
          ? queued.shift()
          : defaultResponses.get(key);

      if (response === undefined) {
        throw new Error(`Unhandled apiFetch request: ${key}`);
      }
      if (response instanceof Error) {
        throw response;
      }
      return response;
    },
  );
}

function makeWrapper(queryClient: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

function buildRecentRun(overrides: Record<string, unknown> = {}) {
  return {
    run_id: "run_recent_24h",
    work_order_id: "work_order_recent_24h",
    work_order_title: "Windowed work order (24h)",
    workflow_id: "work-order-v1",
    status: "running",
    display_state: "awaiting_plan_decision",
    plan_state: "awaiting_plan_decision",
    autonomous_approvals: false,
    created_at: "2026-03-22T10:00:00Z",
    updated_at: "2026-03-22T10:05:00Z",
    last_activity_at: "2026-03-22T10:10:00Z",
    ...overrides,
  };
}

const runtimeMetrics = {
  status: "ok",
  running_runs: 11,
  running_nodes: 17,
  stale_running_nodes: 0,
  planner: {
    queued_operations: 1,
    running_operations: 2,
    failed_operations: 3,
    active_sessions: 4,
    processing_sessions: 5,
    planning_stream_replay_count: 6,
    planning_stream_resume_count: 7,
    planning_operation_recovery_count: 8,
    planning_operation_failure_count: 9,
  },
};

const aggregateSnapshot24h = {
  snapshot_at: "2026-03-22T10:10:00Z",
  window_minutes: 1440,
  all_time_run_count: 1,
  overview: {
    total_runs: 2,
    succeeded_runs: 2,
    failed_runs: 0,
    cancelled_runs: 0,
    avg_duration_seconds: 120,
    success_rate: 100,
    review_rework_rate: 50,
    validation_rework_rate: 0,
  },
  run_analytics: {
    bucket_type: "hour",
    outcome_series: [
      { bucket: "2026-03-22T10:00:00Z", succeeded: 1, failed: 0, cancelled: 0 },
    ],
    duration_by_role: [{ role: "builder", avg_duration_seconds: 120 }],
    recent_runs: [buildRecentRun()],
  },
};

const aggregateSnapshot30m = {
  ...aggregateSnapshot24h,
  snapshot_at: "2026-03-22T10:30:00Z",
  window_minutes: 30,
  overview: {
    total_runs: 1,
    succeeded_runs: 0,
    failed_runs: 1,
    cancelled_runs: 0,
    avg_duration_seconds: 42,
    success_rate: 0,
    review_rework_rate: 100,
    validation_rework_rate: 100,
  },
  run_analytics: {
    bucket_type: "hour",
    outcome_series: [
      { bucket: "2026-03-22T10:30:00Z", succeeded: 0, failed: 1, cancelled: 0 },
    ],
    duration_by_role: [{ role: "builder", avg_duration_seconds: 42 }],
    recent_runs: [
      buildRecentRun({
        run_id: "run_recent_30m",
        work_order_id: "work_order_recent_30m",
        work_order_title: "Windowed work order (30m)",
        status: "failed",
        display_state: "failed",
        plan_state: "approved",
        autonomous_approvals: true,
        created_at: "2026-03-22T10:30:00Z",
        updated_at: "2026-03-22T10:32:00Z",
        last_activity_at: "2026-03-22T10:35:00Z",
      }),
    ],
  },
};

const aggregateExecutionDetailSnapshot = {
  snapshot_at: "2026-03-22T10:10:00Z",
  window_minutes: 1440,
  execution_detail: {
    tokens: {
      provider_total_tokens: 1000,
      derived_total_tokens: 1100,
      input: 700,
      output: 300,
      reasoning: 100,
      cache_read: 0,
      cache_write: 0,
      by_role: [
        {
          required_role: "builder",
          mixed_model: false,
          mixed_thinking_effort: false,
          provider_total_tokens: 1000,
          derived_total_tokens: 1100,
          input_tokens: 700,
          output_tokens: 300,
          reasoning_tokens: 100,
          record_count: 2,
          by_model: [
            {
              provider_id: "github-copilot",
              provider_name: "GitHub Copilot",
              model: "github-copilot/gpt-5.4-mini",
              model_name: "GPT-5.4 Mini",
              thinking_effort: "low",
              provider_total_tokens: 1000,
              derived_total_tokens: 1100,
              input_tokens: 700,
              output_tokens: 300,
              reasoning_tokens: 100,
              record_count: 2,
              run_count: 2,
            },
          ],
        },
      ],
    },
    artifacts: { total_count: 6, total_size_bytes: 1024 },
    nodes: { total_executed: 12 },
  },
};

const selectedRunExecutionDetailSnapshot = {
  snapshot_at: "2026-03-22T10:10:00Z",
  window_minutes: 1440,
  execution_detail: {
    selected_run: {
      run_id: "run_1",
      work_order_title: "Royal work order",
      status: "succeeded",
      created_at: "2026-03-22T10:00:00Z",
    },
    tokens: {
      provider_total_tokens: 250,
      derived_total_tokens: 260,
      input: 200,
      output: 50,
      reasoning: 10,
      cache_read: 25,
      cache_write: 5,
      by_role: [
        {
          required_role: "builder",
          mixed_model: true,
          mixed_thinking_effort: true,
          provider_total_tokens: 250,
          derived_total_tokens: 260,
          input_tokens: 200,
          output_tokens: 50,
          reasoning_tokens: 10,
          record_count: 2,
          by_model: [
            {
              provider_id: "anthropic",
              provider_name: "Anthropic",
              model: "anthropic/opus-4.6",
              model_name: "Opus 4.6",
              thinking_effort: "high",
              provider_total_tokens: 150,
              derived_total_tokens: 160,
              input_tokens: 120,
              output_tokens: 30,
              reasoning_tokens: 10,
              record_count: 1,
              run_count: 1,
            },
            {
              provider_id: "github-copilot",
              provider_name: "GitHub Copilot",
              model: "github-copilot/gpt-4.1",
              model_name: "GPT-4.1",
              thinking_effort: "medium",
              provider_total_tokens: 100,
              derived_total_tokens: 100,
              input_tokens: 80,
              output_tokens: 20,
              reasoning_tokens: 0,
              record_count: 1,
              run_count: 1,
            },
          ],
        },
      ],
    },
    artifacts: { total_count: 2, total_size_bytes: 512 },
    nodes: { total_executed: 4 },
  },
};

const allRuns = {
  items: [
    {
      run_id: "run_1",
      work_order_id: "work_order_1",
      work_order_title: "Royal work order",
      workflow_id: "work-order-v1",
      status: "succeeded",
      display_state: "succeeded",
      plan_state: "approved",
      autonomous_approvals: true,
      created_at: "2026-03-22T10:00:00Z",
      updated_at: "2026-03-22T10:10:00Z",
      last_activity_at: "2026-03-22T10:10:00Z",
    },
  ],
};

beforeEach(() => {
  routerState.search = {};
  setDefaultResponse("/metrics/runtime", runtimeMetrics);
});

afterEach(() => {
  vi.useRealTimers();
  defaultResponses.clear();
  queuedResponses.clear();
  apiFetchMock.mockReset();
  chartRenderSpies.horizontal.mockReset();
  chartRenderSpies.stacked.mockReset();
  routerState.search = {};
  routerState.listeners.clear();
});

describe("AnalyticsPage", () => {
  it("keeps planning reliability details available without a divider", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    installApiFetchMock();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });

    await screen.findByText("Windowed work order (24h)");
    await userEvent.click(screen.getByRole("button", { name: "Reliability" }));

    const summary = await screen.findByText("Planning Reliability");
    const disclosure = summary.closest("details");
    expect(disclosure).not.toHaveClass("border-t");
    expect(disclosure).not.toHaveClass("pt-6");

    await userEvent.click(summary);
    expect(await screen.findByText("Queued Operations")).toBeInTheDocument();
  });

  it("restores an exact calendar range from the URL and clears both dates when a preset is selected", async () => {
    const dateParams = { start_at: "2026-03-01T23:00:00.000Z", end_at: "2026-03-08T23:00:00.000Z" };
    routerState.search = { ...dateParams, view: "usage" };
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, dateParams);
    setDefaultResponse("/analytics/execution-detail", aggregateExecutionDetailSnapshot, dateParams);
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot30m, { window_minutes: 30 });
    setDefaultResponse("/analytics/execution-detail", aggregateExecutionDetailSnapshot, { window_minutes: 30 });
    installApiFetchMock();
    const user = userEvent.setup();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });
    await screen.findByText("Windowed work order (24h)");
    for (const path of ["/analytics/snapshot", "/analytics/execution-detail"]) {
      expect(apiFetchMock).toHaveBeenCalledWith(path, expect.objectContaining({ params: dateParams }));
    }
    expect(screen.getByRole("button", { name: "Usage" })).toHaveAttribute("aria-pressed", "true");
    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));
    await screen.findByText("Windowed work order (30m)");
    expect(routerState.search).toEqual({ view: "usage", window_minutes: 30 });
  });

  it("uses a date-range empty state and never labels historical dates as the last rolling window", async () => {
    const dateParams = { start_at: "2025-12-31T23:00:00.000Z", end_at: "2026-01-01T23:00:00.000Z" };
    routerState.search = dateParams;
    setDefaultResponse("/analytics/snapshot", { ...aggregateSnapshot24h, run_analytics: { ...aggregateSnapshot24h.run_analytics, recent_runs: [] } }, dateParams);
    setDefaultResponse("/analytics/execution-detail", aggregateExecutionDetailSnapshot, dateParams);
    installApiFetchMock();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });
    await screen.findByText("No completed runs in the selected date range.");
    expect(screen.queryByText("No completed runs in the last 24h.")).not.toBeInTheDocument();
  });

  it("clears aggregate execution data when a different date range loads", async () => {
    routerState.search = { view: "usage" };
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, { window_minutes: 1440 });
    setDefaultResponse("/analytics/execution-detail", aggregateExecutionDetailSnapshot, { window_minutes: 1440 });
    const dateParams = { start_at: "2026-03-01T23:00:00.000Z", end_at: "2026-03-08T23:00:00.000Z" };
    queueResponse("/analytics/snapshot", createDeferred().promise, dateParams);
    queueResponse("/analytics/execution-detail", createDeferred().promise, dateParams);
    installApiFetchMock();
    const { container } = render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });
    await screen.findByText("Windowed work order (24h)");
    expect(container.querySelectorAll('[data-testid="horizontal-bar-chart"]').length).toBeGreaterThan(1);
    await act(async () => {
      routerState.search = { ...dateParams, view: "usage" };
      for (const listener of routerState.listeners) listener();
    });
    expect(screen.queryByText("Windowed work order (24h)")).not.toBeInTheDocument();
    // The only chart left is the empty run-duration plot; usage is a loading skeleton.
    expect(container.querySelectorAll('[data-testid="horizontal-bar-chart"]')).toHaveLength(1);
    expect(screen.queryByText("Updating execution detail...")).not.toBeInTheDocument();
  });

  it("keeps chart headings and plots inside the shared card inset", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    installApiFetchMock();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });

    for (const title of ["Run Outcomes Over Time", "Execution Duration by Role"]) {
      const heading = await screen.findByText(title);
      const card = heading.closest(".ui-card");
      expect(card).toHaveClass("p-4");
      expect(card).not.toHaveClass("p-0", "px-0");
    }
  });

  it("gives the role plots room to fill their panels without an oversized label gutter", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    installApiFetchMock();
    const user = userEvent.setup();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });

    await screen.findByText("Execution Duration by Role");
    expect(screen.getByTestId("horizontal-bar-chart").parentElement).toHaveClass(
      "h-[320px]",
    );
    expect(screen.getByTestId("stacked-bar-chart").parentElement).toHaveClass(
      "h-[320px]",
    );

    await user.click(screen.getByRole("button", { name: "Usage" }));
    const tokenHeading = await screen.findByText("Tokens by Role");
    const tokenCard = tokenHeading.closest(".ui-card") as HTMLElement;
    const tokenPlot = within(tokenCard).getByTestId(
      "horizontal-bar-chart",
    ).parentElement;
    expect(tokenPlot).toHaveClass("max-h-[360px]", "overflow-y-auto");
    expect(tokenPlot).not.toHaveClass("min-h-[250px]");
    expect(tokenPlot?.parentElement).toHaveClass("items-start");
  });

  it("renders recent runs with canonical awaiting-decision state and action-required cues", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    expect(screen.getByText("Awaiting Plan Decision")).toBeInTheDocument();
    expect(screen.getByText("Action Required")).toBeInTheDocument();
    expect(screen.getByText(/work-order-v1/)).toBeInTheDocument();
  });

  it("does not fetch run scope options until the picker is opened", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    expect(apiFetchMock.mock.calls.some(([path]) => path === "/runs")).toBe(
      false,
    );

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));

    await screen.findByRole("option", { name: /Royal work order/i });
    expect(apiFetchMock.mock.calls.some(([path]) => path === "/runs")).toBe(
      true,
    );
  });

  it("debounces run-scope search before issuing server queries", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });
    setDefaultResponse("/runs", allRuns, { limit: 25, search: "royal" });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await screen.findByRole("option", { name: /Royal work order/i });

    vi.useFakeTimers();
    const searchInput = screen.getByRole("textbox", { name: "Search runs" });
    fireEvent.change(searchInput, { target: { value: "R" } });
    fireEvent.change(searchInput, { target: { value: "Ro" } });
    fireEvent.change(searchInput, { target: { value: "Roy" } });
    fireEvent.change(searchInput, { target: { value: "Royal" } });

    expect(
      apiFetchMock.mock.calls.filter(([path]) => path === "/runs"),
    ).toHaveLength(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(249);
    });
    expect(
      apiFetchMock.mock.calls.filter(([path]) => path === "/runs"),
    ).toHaveLength(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(
      apiFetchMock.mock.calls.some(
        ([path, options]) =>
          path === "/runs" && options?.params?.search === "royal",
      ),
    ).toBe(true);
  });

  it("can discover and select runs beyond the first page via server-backed search", async () => {
    const firstPageRuns = {
      items: Array.from({ length: 25 }).map((_, index) => ({
        run_id: `run_${index + 1}`,
        work_order_id: `work_order_${index + 1}`,
        work_order_title: `Recent work order ${index + 1}`,
        workflow_id: "work-order-v1",
        status: "succeeded",
        display_state: "succeeded",
        plan_state: "approved",
        created_at: `2026-03-22T${String(index).padStart(2, "0")}:00:00Z`,
        updated_at: `2026-03-22T${String(index).padStart(2, "0")}:10:00Z`,
      })),
      next_cursor: "page-2",
    };
    const olderRun = {
      run_id: "run_older",
      work_order_id: "work_order_older",
      work_order_title: "Older historical run",
      workflow_id: "work-order-v1",
      status: "succeeded",
      display_state: "succeeded",
      plan_state: "approved",
      created_at: "2026-02-01T10:00:00Z",
      updated_at: "2026-02-01T10:10:00Z",
    };

    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      {
        ...selectedRunExecutionDetailSnapshot,
        execution_detail: {
          ...selectedRunExecutionDetailSnapshot.execution_detail,
          selected_run: {
            run_id: olderRun.run_id,
            work_order_title: olderRun.work_order_title,
            status: olderRun.status,
            created_at: olderRun.created_at,
          },
        },
      },
      { run_id: olderRun.run_id },
    );
    setDefaultResponse("/runs", firstPageRuns, { limit: 25 });
    setDefaultResponse(
      "/runs",
      { items: [olderRun] },
      { limit: 25, search: "older historical run" },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.type(
      screen.getByRole("textbox", { name: "Search runs" }),
      "Older historical run",
    );
    await user.click(
      await screen.findByRole("option", { name: /Older historical run/i }),
    );

    await waitFor(() => {
      expect(
        apiFetchMock.mock.calls.some(
          ([path, options]) =>
            path === "/analytics/execution-detail" &&
            options?.params?.run_id === "run_older",
        ),
      ).toBe(true);
    });
    expect(
      screen.getByRole("combobox", { name: "Run scope" }),
    ).toHaveTextContent("Older historical run");
  });

  it("supports keyboard navigation through the run-scope picker", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      selectedRunExecutionDetailSnapshot,
      { run_id: "run_1" },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    const picker = screen.getByRole("combobox", { name: "Run scope" });
    picker.focus();
    expect(picker).toHaveAttribute("aria-expanded", "false");

    await user.keyboard("[Enter]");
    expect(picker).toHaveAttribute("aria-expanded", "true");
    await user.keyboard("[ArrowDown]");
    await user.keyboard("[Enter]");

    await waitFor(() => {
      expect(
        apiFetchMock.mock.calls.some(
          ([path, options]) =>
            path === "/analytics/execution-detail" &&
            options?.params?.run_id === "run_1",
        ),
      ).toBe(true);
    });
    expect(
      screen.getByRole("combobox", { name: "Run scope" }),
    ).toHaveTextContent("Royal work order");
  });

  it("lets the footer Retry button handle Enter without listbox interception", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    queueResponse("/runs", new Error("temporary outage"), { limit: 25 });
    queueResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));

    const retryButton = await screen.findByRole("button", { name: "Retry" });
    retryButton.focus();
    await user.keyboard("[Enter]");

    await screen.findByRole("option", { name: /Royal work order/i });
    expect(
      apiFetchMock.mock.calls.filter(([path]) => path === "/runs"),
    ).toHaveLength(2);
  });

  it("lets the footer Load More Runs button handle Enter without selecting the list item", async () => {
    const paginatedRuns = {
      items: [allRuns.items[0]],
      next_cursor: "page-2",
    };
    const olderRun = {
      run_id: "run_older",
      work_order_id: "work_order_older",
      work_order_title: "Loaded from footer",
      workflow_id: "work-order-v1",
      status: "succeeded",
      display_state: "succeeded",
      plan_state: "approved",
      created_at: "2026-02-01T10:00:00Z",
      updated_at: "2026-02-01T10:10:00Z",
    };

    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse("/runs", paginatedRuns, { limit: 25 });
    setDefaultResponse(
      "/runs",
      { items: [olderRun] },
      { limit: 25, cursor: "page-2" },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));

    const loadMoreButton = await screen.findByRole("button", {
      name: "Load More Runs",
    });
    loadMoreButton.focus();
    await user.keyboard("[Enter]");

    await screen.findByRole("option", { name: /Loaded from footer/i });
    expect(
      screen.getByRole("combobox", { name: "Run scope" }),
    ).toHaveTextContent("All Runs");
  });

  it("drives the page from the selected time window snapshot", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot30m, {
      window_minutes: 30,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      {
        ...aggregateExecutionDetailSnapshot,
        snapshot_at: "2026-03-22T10:30:00Z",
        window_minutes: 30,
        execution_detail: {
          ...aggregateExecutionDetailSnapshot.execution_detail,
          tokens: {
            ...aggregateExecutionDetailSnapshot.execution_detail.tokens,
            provider_total_tokens: 120,
            derived_total_tokens: 150,
            input: 100,
            output: 35,
            reasoning: 15,
          },
          artifacts: { total_count: 1, total_size_bytes: 128 },
          nodes: { total_executed: 2 },
        },
      },
      { window_minutes: 30 },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));

    await screen.findByText("Windowed work order (30m)");

    expect(
      screen.queryByText("Windowed work order (24h)"),
    ).not.toBeInTheDocument();
    expect(
      apiFetchMock.mock.calls.some(
        ([path, options]) =>
          path === "/analytics/snapshot" &&
          options?.params?.window_minutes === 30,
      ),
    ).toBe(true);
  });

  it("uses a window-scoped completed-runs empty state when history exists outside the selected window", async () => {
    setDefaultResponse(
      "/analytics/snapshot",
      {
        ...aggregateSnapshot24h,
        all_time_run_count: 5,
        run_analytics: {
          ...aggregateSnapshot24h.run_analytics,
          recent_runs: [],
        },
      },
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("No completed runs in the last 24h.");
    expect(screen.queryByText("No runs yet.")).not.toBeInTheDocument();
  });

  it("hides the previous window snapshot while the next window is loading", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    const nextSnapshot = createDeferred<typeof aggregateSnapshot30m>();
    queueResponse("/analytics/snapshot", nextSnapshot.promise, {
      window_minutes: 30,
    });
    queueResponse(
      "/analytics/execution-detail",
      createDeferred<typeof aggregateExecutionDetailSnapshot>().promise,
      { window_minutes: 30 },
    );

    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));

    expect(screen.queryByText("Windowed work order (24h)")).not.toBeInTheDocument();
    expect(screen.getByText("Loading analytics for the selected time window…")).toBeInTheDocument();
    expect(
      screen.queryByText("Windowed work order (30m)"),
    ).not.toBeInTheDocument();

    await act(async () => {
      nextSnapshot.resolve(aggregateSnapshot30m);
    });

    await screen.findByText("Windowed work order (30m)");
    expect(
      screen.queryByText("Windowed work order (24h)"),
    ).not.toBeInTheDocument();
  });

  it("keeps runtime health stable and avoids refetching it when only the time window changes", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot30m, {
      window_minutes: 30,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 30 },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");
    expect(screen.getByText("17")).toBeInTheDocument();

    apiFetchMock.mockClear();
    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));

    await screen.findByText("Windowed work order (30m)");
    expect(screen.getByText("17")).toBeInTheDocument();
    expect(
      apiFetchMock.mock.calls.some(([path]) => path === "/metrics/runtime"),
    ).toBe(false);
  });

  it("keeps same-window analytics refreshes quiet while the current snapshot stays visible", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    apiFetchMock.mockClear();
    const snapshotRefresh = createDeferred<typeof aggregateSnapshot24h>();
    queueResponse("/analytics/snapshot", snapshotRefresh.promise, {
      window_minutes: 1440,
    });

    await act(async () => {
      void queryClient.invalidateQueries({
        queryKey: ["analytics", "snapshot", "last:1440"],
      });
    });

    await waitFor(() => {
      expect(
        apiFetchMock.mock.calls.some(
          ([path, options]) =>
            path === "/analytics/snapshot" &&
            options?.params?.window_minutes === 1440,
        ),
      ).toBe(true);
    });
    expect(screen.getByText("Windowed work order (24h)")).toBeInTheDocument();
    expect(
      screen.queryByText("Refreshing analytics..."),
    ).not.toBeInTheDocument();

    await act(async () => {
      snapshotRefresh.resolve(aggregateSnapshot24h);
    });
  });

  it("clears both run charts while a different time range loads", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");
    await waitFor(() => {
      expect(chartRenderSpies.horizontal).toHaveBeenCalled();
      expect(chartRenderSpies.stacked).toHaveBeenCalled();
    });
    chartRenderSpies.horizontal.mockClear();
    chartRenderSpies.stacked.mockClear();

    const nextSnapshot = createDeferred<typeof aggregateSnapshot30m>();
    queueResponse("/analytics/snapshot", nextSnapshot.promise, {
      window_minutes: 30,
    });
    queueResponse(
      "/analytics/execution-detail",
      createDeferred<typeof aggregateExecutionDetailSnapshot>().promise,
      { window_minutes: 30 },
    );

    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));

    expect(chartRenderSpies.horizontal).toHaveBeenLastCalledWith(expect.objectContaining({ data: [] }));
    expect(chartRenderSpies.stacked).toHaveBeenLastCalledWith(expect.objectContaining({ data: [] }));
    chartRenderSpies.horizontal.mockClear();
    chartRenderSpies.stacked.mockClear();

    await act(async () => {
      nextSnapshot.resolve(aggregateSnapshot30m);
    });

    await screen.findByText("Windowed work order (30m)");
    expect(chartRenderSpies.horizontal).toHaveBeenCalledTimes(1);
    expect(chartRenderSpies.stacked).toHaveBeenCalledTimes(1);
  });

  it("does not rerender the execution detail role chart while retained chart data is unchanged", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");
    await waitFor(() => expect(chartRenderSpies.horizontal).toHaveBeenCalled());
    chartRenderSpies.horizontal.mockClear();

    const nextExecutionDetail =
      createDeferred<typeof selectedRunExecutionDetailSnapshot>();
    queueResponse("/analytics/execution-detail", nextExecutionDetail.promise, {
      run_id: "run_1",
    });

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    expect(
      chartRenderSpies.horizontal.mock.calls.filter(
        ([props]) =>
          (props as { ariaLabel?: string }).ariaLabel === "Tokens by role",
      ),
    ).toHaveLength(0);

    await act(async () => {
      nextExecutionDetail.resolve(selectedRunExecutionDetailSnapshot);
    });

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });
    expect(
      chartRenderSpies.horizontal.mock.calls.filter(
        ([props]) =>
          (props as { ariaLabel?: string }).ariaLabel === "Tokens by role",
      ),
    ).toHaveLength(1);
  });

  it("keeps aggregate execution detail visible while switching to a selected run", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");
    expect(screen.getAllByText("1.1K").length).toBeGreaterThan(0);

    const nextExecutionDetail =
      createDeferred<typeof selectedRunExecutionDetailSnapshot>();
    queueResponse("/analytics/execution-detail", nextExecutionDetail.promise, {
      run_id: "run_1",
    });

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    expect(screen.getAllByText("1.1K").length).toBeGreaterThan(0);
    expect(screen.queryAllByText("260")).toHaveLength(0);

    await act(async () => {
      nextExecutionDetail.resolve(selectedRunExecutionDetailSnapshot);
    });

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });
  });

  it("switches execution detail to a selected run and keeps that detail visible during refetch", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      selectedRunExecutionDetailSnapshot,
      { run_id: "run_1" },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");
    expect(screen.getAllByText("1.1K").length).toBeGreaterThan(0);

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });

    const breakdownButton = screen.getByRole("button", {
      name: /Builder.*2 models.*2 records/i,
    });
    await user.click(breakdownButton);

    expect(screen.getByText("Anthropic")).toBeInTheDocument();
    expect(screen.getByText("Opus 4.6")).toBeInTheDocument();
    expect(screen.getByText("GPT-4.1")).toBeInTheDocument();
    expect(screen.getByText("Windowed work order (24h)")).toBeInTheDocument();

    const snapshotRefresh =
      createDeferred<typeof selectedRunExecutionDetailSnapshot>();
    queueResponse("/analytics/execution-detail", snapshotRefresh.promise, {
      run_id: "run_1",
    });

    await act(async () => {
      void queryClient.invalidateQueries({
        queryKey: ["analytics", "execution-detail", "run", "run_1"],
      });
    });

    expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    expect(
      screen.queryByText("Updating execution detail..."),
    ).not.toBeInTheDocument();

    await act(async () => {
      snapshotRefresh.resolve(selectedRunExecutionDetailSnapshot);
    });

    await waitFor(() => {
      expect(
        apiFetchMock.mock.calls.some(
          ([path, options]) =>
            path === "/analytics/execution-detail" &&
            options?.params?.run_id === "run_1",
        ),
      ).toBe(true);
    });
    expect(screen.getAllByText("260").length).toBeGreaterThan(0);
  });

  it("does not refetch selected-run execution detail when only the time window changes", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot30m, {
      window_minutes: 30,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 30 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      selectedRunExecutionDetailSnapshot,
      { run_id: "run_1" },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });

    apiFetchMock.mockClear();
    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));

    await screen.findByText("Windowed work order (30m)");
    expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    expect(
      apiFetchMock.mock.calls.some(
        ([path, options]) =>
          path === "/analytics/execution-detail" &&
          options?.params?.run_id === "run_1",
      ),
    ).toBe(false);
  });

  it("keeps cached execution detail visible when a background refresh fails transiently", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      selectedRunExecutionDetailSnapshot,
      { run_id: "run_1" },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });

    for (let attempt = 0; attempt < 4; attempt += 1) {
      queueResponse(
        "/analytics/execution-detail",
        new Error("Server unreachable"),
        { run_id: "run_1" },
      );
    }

    await act(async () => {
      await queryClient.invalidateQueries({
        queryKey: ["analytics", "execution-detail", "run", "run_1"],
      });
    });

    await screen.findByText(
      "A refresh failed. Showing the last successful execution detail. Server unreachable",
    );
    expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    expect(screen.getByText("Tokens by Role")).toBeInTheDocument();
    expect(
      screen.queryByText("Selected run not found"),
    ).not.toBeInTheDocument();
  });

  it("replaces cached execution detail with a not-found state when the selected run disappears", async () => {
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      selectedRunExecutionDetailSnapshot,
      { run_id: "run_1" },
    );
    setDefaultResponse("/runs", allRuns, { limit: 25 });

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Windowed work order (24h)");

    await user.click(screen.getByRole("button", { name: "Usage" }));
    await user.click(screen.getByRole("combobox", { name: "Run scope" }));
    await user.click(
      await screen.findByRole("option", { name: /Royal work order/i }),
    );

    await waitFor(() => {
      expect(screen.getAllByText("260").length).toBeGreaterThan(0);
    });

    queueResponse(
      "/analytics/execution-detail",
      new ApiError(404, {
        message: "run not found",
        code: "dependency_blocked",
        retryable: false,
        category: "core",
        details: {},
        correlationId: "api.get_realm_analytics_execution_detail_snapshot",
      }),
      {
        run_id: "run_1",
      },
    );

    await act(async () => {
      await queryClient.invalidateQueries({
        queryKey: ["analytics", "execution-detail", "run", "run_1"],
      });
    });

    await screen.findByText("Selected run not found");
    expect(screen.queryByText("Tokens by Role")).not.toBeInTheDocument();
    expect(screen.queryAllByText("260")).toHaveLength(0);
    expect(
      screen.getByRole("button", { name: "Show All Runs" }),
    ).toBeInTheDocument();
  });

  it("renders an inline not-found state for a stale selected run and lets the operator clear the scope", async () => {
    routerState.search = { run_id: "run_missing", view: "usage" };
    setDefaultResponse("/analytics/snapshot", aggregateSnapshot24h, {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      new ApiError(404, {
        message: "run not found",
        code: "dependency_blocked",
        retryable: false,
        category: "core",
        details: {},
        correlationId: "api.get_realm_analytics_execution_detail_snapshot",
      }),
      {
        run_id: "run_missing",
      },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const user = userEvent.setup();
    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Selected run not found");
    expect(
      screen.getByRole("button", { name: "Show All Runs" }),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Show All Runs" }));

    await waitFor(() => {
      expect(
        screen.queryByText("Selected run not found"),
      ).not.toBeInTheDocument();
    });
    expect(screen.getAllByText("1.1K").length).toBeGreaterThan(0);
    expect(
      screen.getByRole("combobox", { name: "Run scope" }),
    ).toHaveTextContent("All Runs");
  });

  it("shows an explicit aggregate snapshot error state when the analytics snapshot fails", async () => {
    setDefaultResponse("/analytics/snapshot", new Error("Server unreachable"), {
      window_minutes: 1440,
    });
    setDefaultResponse(
      "/analytics/execution-detail",
      aggregateExecutionDetailSnapshot,
      { window_minutes: 1440 },
    );

    installApiFetchMock();

    const queryClient = createQueryClient();
    render(<AnalyticsPage />, { wrapper: makeWrapper(queryClient) });

    await screen.findByText("Analytics snapshot unavailable");
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByText("Execution Detail")).toBeInTheDocument();
    expect(screen.queryByText("Run Analytics")).not.toBeInTheDocument();
  });
  it("does not present missing duration, rate denominators or token reports as measured zero", async () => {
    setDefaultResponse(
      "/analytics/snapshot",
      {
        ...aggregateSnapshot24h,
        overview: {
          ...aggregateSnapshot24h.overview,
          succeeded_runs: 0,
          failed_runs: 0,
          cancelled_runs: 0,
          avg_duration_seconds: 0,
          success_rate: 0,
        },
      },
      { window_minutes: 1440 },
    );
    setDefaultResponse(
      "/analytics/execution-detail",
      {
        ...aggregateExecutionDetailSnapshot,
        execution_detail: {
          ...aggregateExecutionDetailSnapshot.execution_detail,
          tokens: {
            ...aggregateExecutionDetailSnapshot.execution_detail.tokens,
            by_role: [],
          },
        },
      },
      { window_minutes: 1440 },
    );
    installApiFetchMock();
    render(<AnalyticsPage />, { wrapper: makeWrapper(createQueryClient()) });
    await screen.findByText("Windowed work order (24h)");
    expect(
      within(screen.getByText("Success Rate").closest("dl")!).getByText("—"),
    ).toBeInTheDocument();
    expect(
      within(screen.getByText("Average Duration").closest("dl")!).getByText(
        "—",
      ),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Usage" }));
    expect(
      await screen.findByText(
        "No token usage has been reported for this scope.",
      ),
    ).toBeVisible();
  });
});
