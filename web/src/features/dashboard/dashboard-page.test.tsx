import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentProps } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DashboardPage } from "./dashboard-page";

const api = vi.fn();
vi.mock("@/api/client", async () => ({
  ...(await vi.importActual("@/api/client")),
  apiFetch: (...args: unknown[]) => api(...args),
}));
vi.mock("@/theme", () => ({ ThemeArtwork: () => null, useThemeCopy: (slot: string) => slot === "workOrder.singular" ? "Decree" : "Your Workspace" }));
vi.mock("@tanstack/react-router", () => ({
  Link: ({
    children,
    to,
    params,
    search,
    ...props
  }: ComponentProps<"a"> & {
    to: string;
    params?: { runId?: string; sessionId?: string };
    search?: Record<string, string>;
  }) => (
    <a
      href={`${to.replace("$runId", params?.runId ?? "").replace("$sessionId", params?.sessionId ?? "")}${search ? `?${new URLSearchParams(search)}` : ""}`}
      {...props}
    >
      {children}
    </a>
  ),
}));
const run = (
  id: string,
  status = "running",
  display_state = "awaiting_plan_decision",
) => ({
  run_id: id,
  work_order_title: `Work ${id}`,
  status,
  display_state,
  plan_state: display_state,
  created_at: "2025-01-01T00:00:00Z",
  last_activity_at: "2026-09-30T01:00:00Z",
  updated_at: "2026-09-30T01:00:00Z",
});
const snapshot = {
  snapshot_at: "2026-09-30T01:00:00Z",
  window_minutes: 1440,
  all_time_run_count: 100,
  runtime: {
    status: "ok",
    running_runs: 42,
    running_nodes: 60,
    stale_running_nodes: 2,
  },
  overview: { succeeded_runs: 24, failed_runs: 3 },
  tokens: { derived_total_tokens: 200000 },
  recent_runs: [run("recent-only")],
};
function setup() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={client}>
      <DashboardPage />
    </QueryClientProvider>,
  );
  return client;
}
let failedQueue = false;
beforeEach(() => {
  failedQueue = false;
  api
    .mockReset()
    .mockImplementation(
      (path: string, options?: { params?: Record<string, unknown> }) => {
        if (path === "/dashboard/snapshot") return snapshot;
        if (path === "/planning-sessions")
          return {
            items: [
              {
                planning_session_id: "session-a",
                title: "Plan an Export",
                lifecycle_state: "active",
                conversation_state: "processing",
                updated_at: "2026-09-30T01:00:00Z",
              },
            ],
          };
        if (path === "/runs") {
          const filter = options?.params?.state_filter;
          if (filter === "awaiting_human_decision") {
            if (failedQueue) throw new Error("unreachable");
            return options?.params?.cursor
              ? {
                  items: [
                    run("old-approval"),
                    run(
                      "older-final",
                      "awaiting_human_confirmation",
                      "awaiting_final_decision",
                    ),
                  ],
                }
              : { items: [run("old-approval")], next_cursor: "next" };
          }
          return {
            items:
              filter === "failed"
                ? [run("failed-check", "failed", "failed")]
                : [],
          };
        }
        throw new Error(`Unexpected query ${path}`);
      },
    );
});

describe("operational Overview", () => {
  it("finds old pending decisions independently of the recent-created snapshot and pages all results", async () => {
    setup();
    const queue = within(
      screen.getByRole("region", { name: "Needs Your Decision" }),
    );
    expect(await queue.findByText("Work old-approval")).toBeInTheDocument();
    expect(screen.queryByText("Work recent-only")).not.toBeInTheDocument();
    await userEvent.click(
      queue.getByRole("button", { name: "Load More Needs Your Decision" }),
    );
    expect(await queue.findByText("Work older-final")).toBeInTheDocument();
    expect(queue.getAllByText("Work old-approval")).toHaveLength(1);
    expect(
      queue.getByRole("link", { name: /Work older-final/ }),
    ).toHaveAttribute("href", "/runs/older-final");
  });
  it("keeps missing data distinct from a genuinely empty work queue", async () => {
    failedQueue = true;
    setup();
    const queue = within(
      screen.getByRole("region", { name: "Needs Your Decision" }),
    );
    expect(await queue.findByRole("alert")).toHaveTextContent(
      "This information is unavailable.",
    );
    expect(
      queue.queryByText("No decisions waiting for you."),
    ).not.toBeInTheDocument();
    expect(
      await screen.findByText("No work is running right now."),
    ).toBeInTheDocument();
    failedQueue = false;
    await userEvent.click(queue.getByRole("button", { name: "Retry" }));
    expect(await queue.findByText("Work old-approval")).toBeInTheDocument();
  });
  it("retains a visible old approval when refreshing its queue fails", async () => {
    const client = setup();
    const queue = within(
      screen.getByRole("region", { name: "Needs Your Decision" }),
    );
    await queue.findByText("Work old-approval");
    failedQueue = true;
    await act(async () => {
      await client.invalidateQueries({
        predicate: (query) =>
          query.queryKey.includes("awaiting_human_decision") ||
          JSON.stringify(query.queryKey).includes("awaiting_human_decision"),
      });
    });
    expect(await queue.findByRole("alert")).toHaveTextContent(
      "Showing the last successful result.",
    );
    expect(queue.getByText("Work old-approval")).toBeInTheDocument();
  });
  it("uses authoritative counts and links planning/current queues to their exact destinations", async () => {
    setup();
    expect(await screen.findByText(/42 Running/)).toBeInTheDocument();
    expect(
      screen.getByText("Runtime Online · 42 Running · 2 Stale Nodes"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("img", { name: "Run trend over the last 24h" }),
    ).not.toBeInTheDocument();
    const runtimeSummary = screen.getByText(
      "Runtime Online · 42 Running · 2 Stale Nodes",
    ).parentElement;
    expect(
      within(runtimeSummary as HTMLElement).getByText(/Updated/),
    ).toBeInTheDocument();
    expect(screen.getByText("24")).toBeInTheDocument();
    expect(screen.getByText("200,000")).toBeInTheDocument();
    const sessions = within(
      screen.getByRole("region", { name: "Continue Planning" }),
    );
    expect(
      await sessions.findByRole("link", { name: /Plan an Export/ }),
    ).toHaveAttribute("href", "/work-orders/planning/session-a");
    expect(
      within(screen.getByRole("region", { name: "Failed Runs" })).getByRole(
        "link",
        { name: "View All" },
      ),
    ).toHaveAttribute("href", "/runs?state_filter=failed");
    expect(
      screen.getByRole("link", { name: "New Decree" }),
    ).toHaveAttribute("href", "/work-orders/new");
  });
});
