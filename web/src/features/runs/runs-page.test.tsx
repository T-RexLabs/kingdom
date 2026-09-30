import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { RunsPage } from "./runs-page";


vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return { ...actual, useTheme: () => ({ theme: actual.builtInThemes[3] }), useThemeCopy: (slot: keyof (typeof actual.builtInThemes)[number]["copy"]) => actual.builtInThemes[0].copy[slot], useRoleIdentity: (role: string) => actual.roleIdentity(actual.builtInThemes[3], role), ThemeIcon: () => <span aria-hidden="true" /> };
});

vi.mock("react-virtuoso", () => ({ Virtuoso: ({ data, itemContent }: { data: unknown[]; itemContent: (index: number, item: unknown) => ReactNode }) => <div>{data.map((item, index) => <div key={index}>{itemContent(index, item)}</div>)}</div> }));

const useInfiniteRunsMock = vi.fn();

const routerState = vi.hoisted(() => ({
  search: {} as Record<string, unknown>,
  listeners: new Set<() => void>(),
}));

const queryState = vi.hoisted(() => ({
  byFilter: {} as Record<string, Record<string, unknown>>,
}));

const runsByFilter = {
  all: [
    {
      run_id: "run-running",
      work_order_id: "work-order-running",
      work_order_title: "Running Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "running",
      display_state: "running",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:01:00.000Z",
    },
    {
      run_id: "run-plan",
      work_order_id: "work-order-plan",
      work_order_title: "Plan Decision Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: false,
      status: "running",
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_plan_decision",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:02:00.000Z",
    },
    {
      run_id: "run-final",
      work_order_id: "work-order-final",
      work_order_title: "Final Decision Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: false,
      status: "awaiting_human_confirmation",
      display_state: "awaiting_final_decision",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:03:00.000Z",
    },
  ],
  running: [
    {
      run_id: "run-running",
      work_order_id: "work-order-running",
      work_order_title: "Running Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "running",
      display_state: "running",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:01:00.000Z",
    },
  ],
  awaiting_human_decision: [
    {
      run_id: "run-plan",
      work_order_id: "work-order-plan",
      work_order_title: "Plan Decision Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: false,
      status: "running",
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_plan_decision",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:02:00.000Z",
    },
    {
      run_id: "run-final",
      work_order_id: "work-order-final",
      work_order_title: "Final Decision Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: false,
      status: "awaiting_human_confirmation",
      display_state: "awaiting_final_decision",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:03:00.000Z",
    },
  ],
  succeeded: [
    {
      run_id: "run-succeeded",
      work_order_id: "work-order-succeeded",
      work_order_title: "Succeeded Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "succeeded",
      display_state: "succeeded",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:04:00.000Z",
    },
  ],
  failed: [
    {
      run_id: "run-failed",
      work_order_id: "work-order-failed",
      work_order_title: "Failed Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "failed",
      display_state: "failed",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:05:00.000Z",
    },
  ],
  pending: [
    {
      run_id: "run-pending",
      work_order_id: "work-order-pending",
      work_order_title: "Pending Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "pending",
      display_state: "pending",
      plan_state: "draft",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:06:00.000Z",
    },
  ],
  cancelled: [
    {
      run_id: "run-cancelled",
      work_order_id: "work-order-cancelled",
      work_order_title: "Cancelled Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "cancelled",
      display_state: "cancelled",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:07:00.000Z",
    },
  ],
} as const;

function buildInfiniteRunsState(items: readonly Record<string, unknown>[], overrides: Record<string, unknown> = {}) {
  return {
    data: {
      pages: [{ items }],
    },
    error: null,
    isError: false,
    isPending: false,
    isLoading: false,
    isFetching: false,
    isFetchNextPageError: false,
    hasNextPage: false,
    isFetchingNextPage: false,
    fetchNextPage: vi.fn(),
    refetch: vi.fn(),
    ...overrides,
  };
}

vi.mock("@/api/hooks", () => ({
  useInfiniteRuns: (options?: { stateFilter?: keyof typeof runsByFilter; limit?: number }) => {
    useInfiniteRunsMock(options);
    const filterKey = options?.stateFilter ?? "__all__";
    return queryState.byFilter[filterKey] ?? buildInfiniteRunsState(options?.stateFilter ? runsByFilter[options.stateFilter] : runsByFilter.all);
  },
}));

vi.mock("@tanstack/react-router", async () => {
  const React = await vi.importActual<typeof import("react")>("react");

  return {
    Link: ({ children, to }: { children: ReactNode; to: string }) => <a href={to}>{children}</a>,
    useSearch: ({ select }: { select?: (search: Record<string, unknown>) => unknown } = {}) =>
      React.useSyncExternalStore(
        (listener) => {
          routerState.listeners.add(listener);
          return () => routerState.listeners.delete(listener);
        },
        () => (select ? select(routerState.search) : routerState.search),
        () => (select ? select(routerState.search) : routerState.search),
      ),
    useNavigate: () =>
      ({ search }: { search: Record<string, unknown> | ((previous: Record<string, unknown>) => Record<string, unknown>) }) => {
        routerState.search = typeof search === "function" ? search(routerState.search) : search;
        for (const listener of Array.from(routerState.listeners)) {
          listener();
        }
      },
  };
});

beforeEach(() => {
  routerState.search = {};
  queryState.byFilter.__all__ = buildInfiniteRunsState(runsByFilter.all);
  queryState.byFilter.running = buildInfiniteRunsState(runsByFilter.running);
  queryState.byFilter.awaiting_human_decision = buildInfiniteRunsState(runsByFilter.awaiting_human_decision);
  queryState.byFilter.succeeded = buildInfiniteRunsState(runsByFilter.succeeded);
  queryState.byFilter.failed = buildInfiniteRunsState(runsByFilter.failed);
  queryState.byFilter.pending = buildInfiniteRunsState(runsByFilter.pending);
  queryState.byFilter.cancelled = buildInfiniteRunsState(runsByFilter.cancelled);
});

afterEach(() => {
  useInfiniteRunsMock.mockReset();
  routerState.search = {};
  routerState.listeners.clear();
});

describe("runs page quick filters", () => {
  it("uses the active theme's work order noun in the create action", () => {
    render(<RunsPage />);
    expect(screen.getByRole("button", { name: "New Decree" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "New Decree" })).toHaveAttribute("href", "/work-orders/new");
  });
  it("drives the active filter from URL search state and labels decision stages clearly", async () => {
    routerState.search = { state_filter: "awaiting_human_decision" };

    const user = userEvent.setup();
    render(<RunsPage />);

    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: "awaiting_human_decision", limit: 50, search: undefined });
    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();
    expect(screen.getByText("Final Decision Run")).toBeInTheDocument();
    expect(screen.getByText("Awaiting Plan Decision")).toBeInTheDocument();
    expect(screen.getByText("Awaiting Final Decision")).toBeInTheDocument();

    const planRow = screen.getByText("Plan Decision Run").closest("a");
    const finalRow = screen.getByText("Final Decision Run").closest("a");
    expect(planRow).not.toBeNull();
    expect(finalRow).not.toBeNull();
    expect(within(planRow as HTMLElement).getByText("Plan Decision Required")).toBeInTheDocument();
    expect(within(finalRow as HTMLElement).getByText("Final Decision Required")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Running" }));
    expect(routerState.search).toEqual({ state_filter: "running" });
    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: "running", limit: 50, search: undefined });
    expect(screen.getByText("Running Run")).toBeInTheDocument();
    expect(screen.queryByText("Plan Decision Run")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "All" }));
    expect(routerState.search).toEqual({});
    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: undefined, limit: 50, search: undefined });
    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Succeeded" }));
    expect(routerState.search).toEqual({ state_filter: "succeeded" });
    expect(screen.getByText("Succeeded Run")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Failed" }));
    expect(routerState.search).toEqual({ state_filter: "failed" });
    expect(screen.getByText("Failed Run")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Pending" }));
    expect(routerState.search).toEqual({ state_filter: "pending" });
    expect(screen.getByText("Pending Run")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Cancelled" }));
    expect(routerState.search).toEqual({ state_filter: "cancelled" });
    expect(screen.getByText("Cancelled Run")).toBeInTheDocument();
  });

  it("keeps the previous run list visible while the next filter is loading", async () => {
    queryState.byFilter.running = buildInfiniteRunsState([], {
      data: undefined,
      isPending: true,
      isLoading: true,
      isFetching: true,
    });

    const user = userEvent.setup();
    render(<RunsPage />);

    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();
    expect(screen.getByText("3 Loaded")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Running" }));

    expect(routerState.search).toEqual({ state_filter: "running" });
    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();
    expect(screen.getByText("3 Loaded")).toBeInTheDocument();
    expect(screen.queryByText("Unable to load runs")).not.toBeInTheDocument();
  });

  it("keeps same-filter refreshes quiet while current runs stay visible", () => {
    routerState.search = { state_filter: "running" };
    queryState.byFilter.running = buildInfiniteRunsState(runsByFilter.running, {
      isFetching: true,
    });

    render(<RunsPage />);

    expect(screen.getByText("Running Run")).toBeInTheDocument();
    expect(screen.getByText("1 Loaded")).toBeInTheDocument();
    expect(screen.queryByText("Updating runs…")).not.toBeInTheDocument();
    expect(screen.getByText("1 Loaded").closest("[aria-busy]")).toHaveAttribute("aria-busy", "false");
  });

	it("shows truthful execution start and meaningful activity metadata beneath each title", () => {
		render(<RunsPage />);

		const runRow = screen.getByText("Running Run").closest("a");
		expect(runRow).not.toBeNull();

		const row = runRow as HTMLElement;
		expect(within(row).queryByText("run-running")).not.toBeInTheDocument();
		expect(within(row).queryByText("Workflow: work-order-v1")).not.toBeInTheDocument();
		expect(row).toHaveTextContent("Started Not Started");
		expect(within(row).getByText(/Updated/)).toBeInTheDocument();
	});
});
