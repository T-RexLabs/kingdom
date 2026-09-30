import { render } from "./planning-test-render";
import { screen } from "@testing-library/react";
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
  }) => select({ location: { pathname: "/work-orders/new" } }),
  useSearch: () => ({}),
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

describe("mobile workspace", () => {
  it("shows planning and issue work order actions", () => {
    render(<WorkOrdersWorkspaceLayout />);

    expect(screen.getByRole("link", { name: "Planning" })).toBeInTheDocument();
    expect(screen.getByTestId("workspace-outlet")).toBeInTheDocument();
  });

  it("does not show left rail in issue mode", () => {
    render(<WorkOrdersWorkspaceLayout />);
    // In issue mode, the planning session list should not be rendered
    expect(screen.queryByText("No sessions yet")).not.toBeInTheDocument();
  });
});

vi.mock("@/theme", () => ({ useThemeCopy: () => "Work Order" }));
