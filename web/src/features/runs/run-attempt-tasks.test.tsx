import type { PropsWithChildren } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch } from "@/api/client";
import type { RunExecutionTaskList } from "@/api/hooks";
import { RunAttemptTasks } from "./run-attempt-tasks";
import { appendLiveRunActivity } from "./run-live-activity";

vi.mock("@/api/client", () => ({ apiFetch: vi.fn() }));
const fetchMock = vi.mocked(apiFetch);

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const Wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return { client, wrapper: Wrapper };
}

const list = (overrides: Partial<RunExecutionTaskList> = {}): RunExecutionTaskList => ({
  state: "available",
  sequence: 4,
  recorded_at: "2026-10-20T10:00:00Z",
  tasks: [
    { content: "Add slugify", status: "completed" },
    { content: "Write slugify tests", status: "in_progress" },
    { content: "Add wrap", status: "pending" },
  ],
  ...overrides,
});

describe("pinned task list", () => {
  beforeEach(() => fetchMock.mockReset());

  it("summarizes progress and the current task, and expands to the full list", async () => {
    fetchMock.mockResolvedValueOnce(list());
    const { wrapper } = setup();
    render(<RunAttemptTasks runId="run-1" attemptId="attempt-1" status="running" />, { wrapper });
    const toggle = await screen.findByRole("button", { name: /Tasks 1 of 3/ });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(toggle).toHaveTextContent("Write slugify tests");
    expect(screen.queryByText("Add wrap")).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("/runs/run-1/executions/attempt-1/tasks");

    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("list", { name: "Reported tasks" })).toBeVisible();
    expect(screen.getByText("Add wrap")).toBeVisible();
  });

  it("marks a finished list complete without naming a current task", async () => {
    fetchMock.mockResolvedValueOnce(list({ tasks: [{ content: "Add slugify", status: "completed" }] }));
    const { wrapper } = setup();
    render(<RunAttemptTasks runId="run-1" attemptId="attempt-1" status="succeeded" />, { wrapper });
    expect(await screen.findByRole("button", { name: "Tasks 1 of 1 Completed" })).toBeVisible();
  });

  it("renders nothing without a reported list and says when the latest is unreadable", async () => {
    fetchMock.mockResolvedValueOnce({ state: "none", tasks: [] });
    const { wrapper } = setup();
    const view = render(<RunAttemptTasks runId="run-1" attemptId="attempt-1" />, { wrapper });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(view.container).toBeEmptyDOMElement();

    fetchMock.mockResolvedValueOnce(list({ state: "unreadable", tasks: [] }));
    view.rerender(<RunAttemptTasks runId="run-1" attemptId="attempt-2" />);
    expect(await screen.findByText("The latest task list could not be read.")).toBeVisible();
  });

  it("rereads once when live activity reports a newer task update", async () => {
    fetchMock.mockResolvedValue(list());
    const { client, wrapper } = setup();
    render(<RunAttemptTasks runId="run-1" attemptId="attempt-1" status="running" />, { wrapper });
    await screen.findByRole("button", { name: /Tasks 1 of 3/ });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    const update = (sequence: number, toolName = "todowrite") =>
      act(() =>
        appendLiveRunActivity(client, "run-1", "attempt-1", {
          id: `todo-${sequence}`, eventId: `event-${sequence}`, sequence, kind: "tool", toolName, label: "Updated tasks",
        }),
      );
    update(3);
    update(9, "read");
    expect(fetchMock).toHaveBeenCalledTimes(1);

    fetchMock.mockResolvedValueOnce(list({ sequence: 10, tasks: [{ content: "Add slugify", status: "completed" }] }));
    update(10);
    expect(await screen.findByRole("button", { name: "Tasks 1 of 1 Completed" })).toBeVisible();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});
