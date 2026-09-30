import { render } from "./planning-test-render";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { WorkOrdersWorkspaceLayout } from "./work-orders-workspace-layout";

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, ...props }: { children: ReactNode }) => (
    <a href="#" {...props}>
      {children}
    </a>
  ),
  Outlet: () => <div data-testid="workspace-outlet">Outlet</div>,
  useRouterState: ({
    select,
  }: {
    select: (state: { location: { pathname: string } }) => string;
  }) => select({ location: { pathname: "/work-orders/planning" } }),
  useNavigate: () => vi.fn(),
}));

vi.mock("@/api/hooks", () => ({
  useWorkspaceCapabilities: () => ({ data: { project_root: "/tmp/project" } }),
  usePlanningSessions: () => ({
    data: { items: [] },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useDeletePlanningSession: () => ({
    mutateAsync: vi.fn(),
    isPending: false,
    error: null,
  }),
}));

describe("work orders workspace layout", () => {
  it("renders left rail, page header, and outlet in 2-column layout", () => {
    render(<WorkOrdersWorkspaceLayout />);

    expect(screen.getByRole("heading", { name: "Planning" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Planning sessions" })).toBeInTheDocument();
    expect(screen.getByTestId("workspace-outlet")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "New Work Order" })).toBeInTheDocument();
  });

  it("keeps the conversation in a bounded viewport without a second page scroll", () => {
    render(<WorkOrdersWorkspaceLayout />);
    const grid = screen.getByTestId("workspace-outlet").closest(".grid");
    expect(grid).toHaveClass("flex-1", "min-h-0");
    expect(screen.getByRole("main")).toHaveClass("min-h-0", "min-w-0");
  });

  it("opens an accessible mobile session switcher with search and a new-session action", async () => {
    render(<WorkOrdersWorkspaceLayout />);
    await userEvent.click(screen.getByRole("button", { name: "Sessions" }));
    const dialog = screen.getByRole("dialog", { name: "Planning Sessions" });
    expect(dialog).toHaveTextContent("New Session");
    expect(dialog.querySelector('input[type="search"]')).toHaveAttribute("aria-label", "Search planning sessions");
    await userEvent.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("does not render a right rail draft panel", () => {
    render(<WorkOrdersWorkspaceLayout />);

    expect(screen.queryByText("Current Plan")).not.toBeInTheDocument();
    expect(screen.queryByText("Recent Work Order Runs")).not.toBeInTheDocument();
  });
});

vi.mock("@/theme", () => ({ useThemeCopy: () => "Work Order" }));
