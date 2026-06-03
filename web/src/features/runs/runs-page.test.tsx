import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { RunsPage } from "./runs-page";

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
      decree_id: "decree-running",
      decree_title: "Running Run",
      workflow_id: "royal-decree-v1",
      autonomous_approvals: true,
      status: "running",
      display_state: "running",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:01:00.000Z",
    },
    {
      run_id: "run-plan",
      decree_id: "decree-plan",
      decree_title: "Plan Decision Run",
      workflow_id: "royal-decree-v1",
      autonomous_approvals: false,
      status: "running",
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_king_decision",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:02:00.000Z",
    },
    {
      run_id: "run-final",
      decree_id: "decree-final",
      decree_title: "Final Decision Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-running",
      decree_title: "Running Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-plan",
      decree_title: "Plan Decision Run",
      workflow_id: "royal-decree-v1",
      autonomous_approvals: false,
      status: "running",
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_king_decision",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:02:00.000Z",
    },
    {
      run_id: "run-final",
      decree_id: "decree-final",
      decree_title: "Final Decision Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-succeeded",
      decree_title: "Succeeded Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-failed",
      decree_title: "Failed Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-pending",
      decree_title: "Pending Run",
      workflow_id: "royal-decree-v1",
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
      decree_id: "decree-cancelled",
      decree_title: "Cancelled Run",
      workflow_id: "royal-decree-v1",
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
    Link: ({ children, ...props }: { children: ReactNode }) => <a {...props}>{children}</a>,
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
  it("drives the active filter from URL search state and labels decision stages clearly", async () => {
    routerState.search = { state_filter: "awaiting_human_decision" };

    const user = userEvent.setup();
    render(<RunsPage />);

    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: "awaiting_human_decision", limit: 50 });
    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();
    expect(screen.getByText("Final Decision Run")).toBeInTheDocument();
    expect(screen.getByText("Awaiting Plan Decision")).toBeInTheDocument();
    expect(screen.getByText("Awaiting Final Decision")).toBeInTheDocument();

    const planRow = screen.getByText("Plan Decision Run").closest("a");
    const finalRow = screen.getByText("Final Decision Run").closest("a");
    expect(planRow).not.toBeNull();
    expect(finalRow).not.toBeNull();
    expect(within(planRow as HTMLElement).getByText("Action Required")).toBeInTheDocument();
    expect(within(finalRow as HTMLElement).getByText("Action Required")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Running" }));
    expect(routerState.search).toEqual({ state_filter: "running" });
    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: "running", limit: 50 });
    expect(screen.getByText("Running Run")).toBeInTheDocument();
    expect(screen.queryByText("Plan Decision Run")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "All" }));
    expect(routerState.search).toEqual({});
    expect(useInfiniteRunsMock).toHaveBeenLastCalledWith({ stateFilter: undefined, limit: 50 });
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
    expect(screen.getByText("3 runs")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Running" }));

    expect(routerState.search).toEqual({ state_filter: "running" });
    expect(screen.getByText("Plan Decision Run")).toBeInTheDocument();
    expect(screen.getByText("3 runs")).toBeInTheDocument();
    expect(screen.queryByText("Unable to load runs")).not.toBeInTheDocument();
  });

  it("keeps same-filter refreshes quiet while current runs stay visible", () => {
    routerState.search = { state_filter: "running" };
    queryState.byFilter.running = buildInfiniteRunsState(runsByFilter.running, {
      isFetching: true,
    });

    render(<RunsPage />);

    expect(screen.getByText("Running Run")).toBeInTheDocument();
    expect(screen.getByText("1 run")).toBeInTheDocument();
    expect(screen.queryByText("Updating runs...")).not.toBeInTheDocument();
    expect(screen.getByText("1 run").closest("[aria-busy]")).toHaveAttribute("aria-busy", "false");
  });

	it("shows only created and last activity metadata beneath each decree title", () => {
		render(<RunsPage />);

		const runRow = screen.getByText("Running Run").closest("a");
		expect(runRow).not.toBeNull();

		const row = runRow as HTMLElement;
		expect(within(row).queryByText("run-running")).not.toBeInTheDocument();
		expect(within(row).queryByText("Workflow: royal-decree-v1")).not.toBeInTheDocument();
		expect(within(row).getByText(/Created:/)).toBeInTheDocument();
		expect(within(row).getByText(/Last Activity:/)).toBeInTheDocument();
	});
});
