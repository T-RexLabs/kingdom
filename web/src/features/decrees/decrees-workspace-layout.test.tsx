import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { DecreesWorkspaceLayout } from "./decrees-workspace-layout";

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
  }) => select({ location: { pathname: "/decrees/planning" } }),
  useNavigate: () => vi.fn(),
}));

vi.mock("@/api/hooks", () => ({
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

describe("decrees workspace layout", () => {
  it("renders left rail, page header, and outlet in 2-column layout", () => {
    render(<DecreesWorkspaceLayout />);

    expect(screen.getByRole("heading", { name: "Royal Decrees" })).toBeInTheDocument();
    expect(screen.getAllByText("Planning Sessions").length).toBeGreaterThan(0);
    expect(screen.getByTestId("workspace-outlet")).toBeInTheDocument();
    expect(screen.getAllByText("Issue Decree").length).toBeGreaterThan(0);
  });

  it("keeps decree workspace content inside its own scroll viewport", () => {
    render(<DecreesWorkspaceLayout />);

    const outlet = screen.getByTestId("workspace-outlet");
    const scrollViewport = outlet.closest(".overflow-auto");
    expect(scrollViewport).toHaveClass("overflow-auto");
    expect(scrollViewport).toContainElement(outlet);
  });

  it("does not render a right rail draft panel", () => {
    render(<DecreesWorkspaceLayout />);

    expect(screen.queryByText("Current Plan")).not.toBeInTheDocument();
    expect(screen.queryByText("Recent Decree Runs")).not.toBeInTheDocument();
  });
});
