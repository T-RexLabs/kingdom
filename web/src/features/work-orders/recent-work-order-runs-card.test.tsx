import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { RecentWorkOrderRunsCard } from "./recent-work-order-runs-card";

const mockRuns = {
  items: [
    {
      run_id: "run-manual",
      work_order_id: "work-order-manual",
      work_order_title: "Manual Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: false,
      status: "awaiting_human_confirmation",
      display_state: "awaiting_final_decision",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:01:00.000Z",
    },
    {
      run_id: "run-autonomous",
      work_order_id: "work-order-autonomous",
      work_order_title: "Autonomous Run",
      workflow_id: "work-order-v1",
      autonomous_approvals: true,
      status: "awaiting_human_confirmation",
      display_state: "awaiting_final_decision",
      plan_state: "approved",
      created_at: "2026-03-06T10:00:00.000Z",
      updated_at: "2026-03-06T10:02:00.000Z",
    },
  ],
};

vi.mock("@/api/hooks", () => ({
  useRuns: () => ({ data: mockRuns, isLoading: false }),
}));

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children }: { children: ReactNode }) => <a href="#">{children}</a>,
}));

describe("recent work order runs card governance badges", () => {
  it("does not mark autonomous inconsistent rows as action required", () => {
    render(<RecentWorkOrderRunsCard />);

    const manualRow = screen.getByText("Manual Run").closest("a");
    const autonomousRow = screen.getByText("Autonomous Run").closest("a");

    expect(manualRow).not.toBeNull();
    expect(autonomousRow).not.toBeNull();
    expect(within(manualRow as HTMLElement).getByText("Action Required")).toBeInTheDocument();
    expect(within(autonomousRow as HTMLElement).queryByText("Action Required")).not.toBeInTheDocument();
  });
});
