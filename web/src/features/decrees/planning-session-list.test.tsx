import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { PlanningSessionList } from "./planning-session-list";

const navigate = vi.fn();
const deleteMutate = vi.fn(async () => undefined);
const pagination = { hasNextPage: false, isFetchingNextPage: false, fetchNextPage: vi.fn() };
beforeEach(() => {
  pagination.hasNextPage = false;
  pagination.isFetchingNextPage = false;
  pagination.fetchNextPage.mockReset();
});

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, ...props }: { children: ReactNode }) => <a href="#" {...props}>{children}</a>,
  useRouterState: ({ select }: { select: (state: { location: { pathname: string } }) => string }) => select({ location: { pathname: "/decrees/planning/session-2" } }),
  useNavigate: () => navigate,
}));

vi.mock("@/api/hooks", () => ({
  usePlanningSessions: () => ({
    ...pagination,
    data: {
      items: [
        {
          planning_session_id: "session-1",
          title: "Session one",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: undefined,
          current_operation_id: undefined,
          current_draft_id: undefined,
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:00:00Z",
        },
        {
          planning_session_id: "session-2",
          title: "Session two",
          title_state: "generated",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: undefined,
          current_operation_id: undefined,
          current_draft_id: "planning-draft-2",
          created_at: "2026-04-21T11:00:00Z",
          updated_at: "2026-04-21T11:00:00Z",
        },
      ],
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useDeletePlanningSession: () => ({ mutateAsync: deleteMutate, isPending: false, error: null }),
  newIdempotencyKey: (scope: string) => `${scope}-test`,
}));

describe("planning session list", () => {
  it("loads older sessions inside the rail and disables duplicate requests", async () => {
    pagination.hasNextPage = true;
    const { rerender } = render(<PlanningSessionList />);
    await userEvent.click(screen.getByRole("button", { name: "Load More Sessions" }));
    expect(pagination.fetchNextPage).toHaveBeenCalledOnce();
    expect(screen.getByText("Session one")).toBeInTheDocument();
    pagination.isFetchingNextPage = true;
    rerender(<PlanningSessionList />);
    expect(screen.getByRole("button", { name: "Loading sessions..." })).toBeDisabled();
    pagination.isFetchingNextPage = false;
    pagination.hasNextPage = false;
    rerender(<PlanningSessionList />);
    expect(screen.queryByRole("button", { name: "Load More Sessions" })).not.toBeInTheDocument();
  });
  it("renders session titles", () => {
    render(<PlanningSessionList />);

    expect(screen.getByText("Session one")).toBeInTheDocument();
    expect(screen.getByText("Session two")).toBeInTheDocument();
  });

  it("renders links for resuming sessions from the left rail", () => {
    render(<PlanningSessionList />);

    const links = screen.getAllByRole("link");
    expect(links.some((link) => link.textContent?.includes("Session two"))).toBe(true);
  });

  it("confirms deletion for the active session and redirects to planning index", async () => {
    const user = userEvent.setup();
    navigate.mockClear();
    deleteMutate.mockClear();

    render(<PlanningSessionList />);

    await user.click(screen.getByRole("button", { name: "Delete Session two" }));
    expect(screen.getByRole("dialog", { name: "Session two" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Delete Session" }));

    expect(deleteMutate).toHaveBeenCalledWith({
      value: "session-2",
      key: expect.stringMatching(/^kd1\./),
    });
    expect(navigate).toHaveBeenCalledWith({ to: "/decrees/planning" });
  });

	it("keeps the operator on the active session when deletion fails", async () => {
		const user = userEvent.setup();
		navigate.mockClear();
		deleteMutate.mockReset();
		deleteMutate.mockRejectedValueOnce(new Error("Delete failed"));

		render(<PlanningSessionList />);

		await user.click(screen.getByRole("button", { name: "Delete Session two" }));
		await user.click(screen.getByRole("button", { name: "Delete Session" }));

		await waitFor(() => expect(screen.getByRole("dialog", { name: "Session two" })).toBeInTheDocument());
		expect(navigate).not.toHaveBeenCalled();
	});

  it("moves focus into the delete dialog and restores it to the trigger on close", async () => {
    const user = userEvent.setup();

    render(<PlanningSessionList />);

    const trigger = screen.getByRole("button", { name: "Delete Session two" });
    await user.click(trigger);

    const closeButton = screen.getByRole("button", { name: "Close dialog" });
    await waitFor(() => expect(closeButton).toHaveFocus());

    await user.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it("traps keyboard focus inside the delete dialog", async () => {
    const user = userEvent.setup();

    render(<PlanningSessionList />);

    await user.click(screen.getByRole("button", { name: "Delete Session two" }));

    const closeButton = screen.getByRole("button", { name: "Close dialog" });
    const cancelButton = screen.getByRole("button", { name: "Cancel" });
    const deleteButton = screen.getByRole("button", { name: "Delete Session" });

    await waitFor(() => expect(closeButton).toHaveFocus());

    await user.tab();
    expect(cancelButton).toHaveFocus();

    await user.tab();
    expect(deleteButton).toHaveFocus();

    await user.tab();
    expect(closeButton).toHaveFocus();
  });
});
