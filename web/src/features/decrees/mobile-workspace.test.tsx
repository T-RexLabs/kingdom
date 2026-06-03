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
  }) => select({ location: { pathname: "/decrees/new" } }),
  useSearch: () => ({}),
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

describe("mobile workspace", () => {
  it("shows planning and issue decree actions", () => {
    render(<DecreesWorkspaceLayout />);

    expect(screen.getAllByText("Planning Sessions").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Issue Decree").length).toBeGreaterThan(0);
  });

  it("does not show left rail in issue mode", () => {
    render(<DecreesWorkspaceLayout />);
    // In issue mode, the planning session list should not be rendered
    expect(screen.queryByText("No sessions yet")).not.toBeInTheDocument();
  });
});
