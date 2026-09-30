import { render as renderPlanning } from "./planning-test-render";
import { VirtuosoMockContext } from "react-virtuoso";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement, ReactNode } from "react";
import { PlanningSessionList } from "./planning-session-list";
import { setCSRFToken } from "@/api/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { plannerKeys } from "./planner-query-keys";

function render(ui: ReactElement) {
  return renderPlanning(<VirtuosoMockContext.Provider value={{ viewportHeight: 600, itemHeight: 76 }}>{ui}</VirtuosoMockContext.Provider>);
}
let largeFixture = false;
const planningSearch = vi.fn();
const navigate = vi.fn();
const deleteMutate = vi.fn<(request: { value: string; key: string }) => Promise<void>>(async () => undefined);
const pagination = { hasNextPage: false, isFetchingNextPage: false, fetchNextPage: vi.fn() };
beforeEach(() => {
  setCSRFToken("token", "planning-list-binding");
  largeFixture = false;
  planningSearch.mockClear();
  pagination.hasNextPage = false;
  pagination.isFetchingNextPage = false;
  pagination.fetchNextPage.mockReset();
});

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, ...props }: { children: ReactNode }) => <a href="#" {...props}>{children}</a>,
  useRouterState: ({ select }: { select: (state: { location: { pathname: string } }) => string }) => select({ location: { pathname: "/work-orders/planning/session-2" } }),
  useNavigate: () => navigate,
}));

vi.mock("@/api/hooks", () => ({
  useWorkspaceCapabilities: () => ({ data: { project_root: "/tmp/project" } }),
  usePlanningSessions: (_limit: number, query: string) => (planningSearch(query), {
    ...pagination,
    data: {
      items: (largeFixture ? Array.from({ length: 10_000 }, (_, index) => ({
        planning_session_id: `large-${index}`, title: `Session ${index}`, title_state: "custom", conversation_state: "ready", current_draft_id: undefined,
      })) : [
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
      ]).filter((session) => !query || session.title.includes(query)),
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useDeletePlanningSession: () => ({ mutateAsync: deleteMutate, isPending: false, error: null }),
  newIdempotencyKey: (scope: string) => `${scope}-test`,
}));

describe("planning session list", () => {
  it("resolves a restored deletion's 204 outcome and removes its canonical cache after renewal", async () => {
    const user = userEvent.setup();
    deleteMutate.mockReset().mockRejectedValueOnce(new TypeError("Response lost"));
    navigate.mockClear();
    const original = render(<PlanningSessionList />);
    await user.click(screen.getByRole("button", { name: "Delete Session two" }));
    await user.click(screen.getByRole("button", { name: "Delete Session" }));
    await waitFor(() => expect(deleteMutate).toHaveBeenCalledOnce());
    const { key } = deleteMutate.mock.calls[0][0];
    original.unmount();
    setCSRFToken("renewed-token", "renewed-binding");
    const client = new QueryClient();
    client.setQueryData(plannerKeys.detail("session-2"), { planning_session_id: "session-2" });
    client.setQueryData(plannerKeys.messages("session-2"), { pages: [] });
    client.setQueryData(plannerKeys.plan("session-2"), { planning_draft_id: "draft-2" });
    client.setQueryData(plannerKeys.detail("session-1"), { planning_session_id: "session-1" });
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({ state: "completed", response_status: 204 }), { status: 200 }));
    try {
      render(<QueryClientProvider client={client}><PlanningSessionList /></QueryClientProvider>);
      await user.click(screen.getByRole("button", { name: "Resolve Deletion" }));
      await waitFor(() => expect(screen.queryByRole("button", { name: "Resolve Deletion" })).not.toBeInTheDocument());
      expect(fetch).toHaveBeenCalledOnce();
      expect(fetch.mock.calls[0][0]).toBe(`/api/v1/mutation-outcomes/${encodeURIComponent(key)}?operation=planning-session-delete`);
      expect(fetch.mock.calls[0][1]?.method ?? "GET").toBe("GET");
      expect(deleteMutate).toHaveBeenCalledOnce();
      expect(client.getQueryData(plannerKeys.detail("session-2"))).toBeUndefined();
      expect(client.getQueryData(plannerKeys.messages("session-2"))).toBeUndefined();
      expect(client.getQueryData(plannerKeys.plan("session-2"))).toBeUndefined();
      expect(client.getQueryData(plannerKeys.detail("session-1"))).toBeDefined();
      expect(navigate).toHaveBeenCalledWith({ to: "/work-orders/planning" });
    } finally { fetch.mockRestore(); }
  });
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
  it("requires explicit recovery of a restored deletion before choosing another session", async () => {
    const user = userEvent.setup();
    deleteMutate.mockReset();
    deleteMutate.mockRejectedValueOnce(new TypeError("Response lost")).mockResolvedValueOnce(undefined);
    const first = render(<PlanningSessionList />);
    await user.click(screen.getByRole("button", { name: "Delete Session one" }));
    await user.click(screen.getByRole("button", { name: "Delete Session" }));
    await waitFor(() => expect(deleteMutate).toHaveBeenCalledTimes(1));
    const original = deleteMutate.mock.calls[0][0];
    first.unmount();

    render(<PlanningSessionList />);
    expect(screen.getByRole("button", { name: "Delete Session two" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Delete Session two" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(deleteMutate).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Resolve Deletion" }));
    await waitFor(() => expect(deleteMutate).toHaveBeenCalledTimes(2));
    expect(deleteMutate.mock.calls[1][0]).toEqual(original);
    await waitFor(() => expect(screen.getByRole("button", { name: "Delete Session two" })).toBeEnabled());
  });
  it("keeps 10,000 loaded sessions bounded while preserving pagination and search identity", async () => {
    largeFixture = true;
    pagination.hasNextPage = true;
    const user = userEvent.setup();
    render(<PlanningSessionList />);
    await waitFor(() => expect(screen.getByText("Session 0")).toBeInTheDocument());
    expect(document.querySelectorAll('nav[aria-label="Planning sessions"] a').length).toBeLessThan(40);
    await user.click(screen.getByRole("button", { name: "Load More Sessions" }));
    expect(pagination.fetchNextPage).toHaveBeenCalledOnce();
    await user.type(screen.getByRole("searchbox", { name: "Search planning sessions" }), "Session 9999");
    await waitFor(() => expect(planningSearch).toHaveBeenLastCalledWith("Session 9999"));
    expect(await screen.findByText("Session 9999")).toBeInTheDocument();
    expect(screen.queryByText("Session 0")).not.toBeInTheDocument();
    expect(document.querySelectorAll('nav[aria-label="Planning sessions"] a').length).toBeLessThan(40);
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
    expect(navigate).toHaveBeenCalledWith({ to: "/work-orders/planning" });
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
