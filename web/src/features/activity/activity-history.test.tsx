import type { PropsWithChildren } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { VirtuosoMockContext } from "react-virtuoso";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch } from "@/api/client";
import { ActivityContentReader, ActivityHistory } from "./activity-history";
import { availableActivityContent } from "./activity-test-helpers";
import { mockActivityScrollGeometry } from "./activity-virtual-test-helpers";

vi.mock("@/api/client", () => ({ apiFetch: vi.fn() }));
const fetchMock = vi.mocked(apiFetch);
function wrapper() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
  };
}

describe("full provider content", () => {
  let restoreGeometry: () => void;
  beforeEach(() => {
    fetchMock.mockReset();
    restoreGeometry = mockActivityScrollGeometry();
  });
  afterEach(() => restoreGeometry());
  it("reads immutable content lazily in exact pages without splitting or trimming text", async () => {
    const user = userEvent.setup();
    const text = "\n界😀 exact result\n";
    const content = { ...availableActivityContent(text), preview: "\n界" };
    fetchMock.mockResolvedValueOnce({
      text: "\n界",
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
      next_cursor: "page-two",
    });
    render(<ActivityContentReader label="Output" content={content} />, {
      wrapper: wrapper(),
    });
    expect(fetchMock).not.toHaveBeenCalled();
    await user.click(screen.getByText("Output"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    fetchMock.mockResolvedValueOnce({
      text: "😀 exact result\n",
      offset: 4,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    await user.click(
      await screen.findByRole("button", { name: "Load More Content" }),
    );
    await waitFor(() =>
      expect(document.querySelector("pre")?.textContent).toBe(text),
    );
    expect(fetchMock.mock.calls[1][1]?.params?.cursor).toBe("page-two");
  });
  it("distinguishes absent and explicit empty content without fetching either", () => {
    render(
      <>
        <ActivityContentReader
          label="Absent"
          content={{ availability: "not_provided" }}
        />
        <ActivityContentReader
          label="Empty"
          content={availableActivityContent("")}
        />
      </>,
      { wrapper: wrapper() },
    );
    expect(
      screen.getByText("Not supplied by the provider."),
    ).toBeInTheDocument();
    expect(
      screen.getByText("The provider supplied empty content."),
    ).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
  it("shows unloaded gaps explicitly and requests their scoped cursor", async () => {
    const user = userEvent.setup();
    const load = vi.fn().mockResolvedValue(undefined);
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 260, itemHeight: 52 }}
      >
        <ActivityHistory
          scopeKey="operation-a"
          items={[]}
          gaps={[
            {
              after: 0,
              through: 90,
              count: 80,
              cursor: "older",
              direction: "earlier",
            },
          ]}
          isLoadingMore={false}
          loadError={null}
          loadMore={load}
        />
      </VirtuosoMockContext.Provider>,
    );
    expect(
      await screen.findByText("80 earlier events are not loaded."),
    ).toBeInTheDocument();
    expect(screen.queryByText("80 Events Not Loaded")).not.toBeInTheDocument();
    await user.click(
      await screen.findByRole("button", { name: "Load Events" }),
    );
    expect(load).toHaveBeenCalledWith("older");
  });
  it("shows observed provider messages on inspection without expanding technical metadata", async () => {
    const content = availableActivityContent(
      "## Recommendation\nProceed with the page.",
    );
    fetchMock.mockResolvedValue({
      text: content.preview,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 400, itemHeight: 100 }}
      >
        <ActivityHistory
          scopeKey="attempt-message"
          items={[
            {
              id: "p",
              eventId: "e",
              sequence: 1,
              kind: "message",
              label: "Provider Message",
              invocationId: "inv",
              providerPartId: "part",
              messageContent: content,
            },
          ]}
          gaps={[]}
          isLoadingMore={false}
          loadError={null}
          loadMore={vi.fn()}
        />
      </VirtuosoMockContext.Provider>,
      { wrapper: wrapper() },
    );
    await userEvent.click(await screen.findByText("Provider Message"));
    expect(
      await screen.findByRole("heading", { name: "Recommendation" }),
    ).toBeVisible();
    expect(screen.getByText("Proceed with the page.")).toBeVisible();
    expect(screen.getByText("Provider Message")).toBeVisible();
    expect(screen.queryByText("Provider Observation")).not.toBeInTheDocument();
    expect(screen.queryByText(/bytes loaded/)).not.toBeInTheDocument();
    expect(screen.queryByText("inv")).not.toBeInTheDocument();
    await userEvent.click(screen.getByText("Event Details"));
    expect(screen.getByText("Provider Observation")).toBeVisible();
    expect(screen.getByText(`${content.byte_length} Bytes`)).toBeVisible();
  });

  it("keeps incomplete message content and its continuation visible", async () => {
    const content = {
      ...availableActivityContent("First paragraph.\nSecond paragraph."),
      preview: "First paragraph.",
    };
    fetchMock.mockResolvedValue({
      text: content.preview,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
      next_cursor: "next",
    });
    render(
      <ActivityContentReader
        label="Message"
        presentation="message"
        content={content}
      />,
      { wrapper: wrapper() },
    );
    expect(
      await screen.findByRole("button", { name: "Load More Content" }),
    ).toBeVisible();
    expect(
      screen.getByText(`16 of ${content.byte_length} bytes loaded.`),
    ).toBeVisible();
  });

  it.each(["todowrite", "todoread"])("renders the actual %s task snapshot without inventing unknown task statuses", async (toolName) => {
    const content = availableActivityContent(JSON.stringify({ todos: [
      { content: "Inspect the existing route", status: "completed" },
      { content: "Implement the change", status: "in_progress" },
      { content: "Wait for review", status: "awaiting_review" },
    ] }));
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 400, itemHeight: 100 }}>
        <ActivityHistory scopeKey="tasks" items={[{
          id: "t", eventId: "e", sequence: 1, kind: "tool", toolName,
          label: "Tool Call", [toolName === "todowrite" ? "inputContent" : "outputContent"]: content,
        }]} gaps={[]} isLoadingMore={false} loadError={null} loadMore={vi.fn()} />
      </VirtuosoMockContext.Provider>,
      { wrapper: wrapper() },
    );
    await waitFor(() => expect(screen.getByText("Inspect the existing route")).toBeVisible());
    expect(screen.getByText("1 of 3 Completed")).toBeVisible();
    expect(screen.getByText("In Progress")).toBeVisible();
    expect(screen.getByText("awaiting_review")).toBeVisible();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("shows running tools quietly without inferring activity from an unspecified status", async () => {
    const activity = (status?: string) => (
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 400, itemHeight: 100 }}
      >
        <ActivityHistory
          scopeKey="active-tool"
          items={[
            {
              id: "t",
              eventId: "e",
              sequence: 1,
              kind: "tool",
              toolName: "bash",
              label: "Tool Call",
              toolStatus: status,
            },
          ]}
          gaps={[]}
          isLoadingMore={false}
          loadError={null}
          loadMore={vi.fn()}
        />
      </VirtuosoMockContext.Provider>
    );
    const view = render(activity("running"), { wrapper: wrapper() });
    await waitFor(() =>
      expect(screen.getByLabelText("Tool running")).toBeVisible(),
    );
    view.rerender(activity());
    expect(screen.queryByLabelText("Tool running")).not.toBeInTheDocument();
  });

  it("keeps raw commands out of the conversation until direct tool inspection", async () => {
    const command = "python3 - <<'PY'\nprint('exact command')\nPY";
    const content = availableActivityContent(JSON.stringify({ command }));
    fetchMock.mockResolvedValue({
      text: content.preview,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 400, itemHeight: 100 }}
      >
        <ActivityHistory
          scopeKey="tools"
          items={[
            {
              id: "t",
              eventId: "e",
              sequence: 1,
              kind: "tool",
              toolName: "bash",
              label: command,
              toolStatus: "completed",
              inputContent: content,
            },
          ]}
          gaps={[]}
          isLoadingMore={false}
          loadError={null}
          loadMore={vi.fn()}
        />
      </VirtuosoMockContext.Provider>,
      { wrapper: wrapper() },
    );
    const summary = await screen.findByText("Run Command");
    expect(summary.closest("summary")).not.toHaveTextContent("python3");
    expect(screen.queryByText("completed")).not.toBeInTheDocument();
    expect(document.querySelector("pre")).toBeNull();
    await userEvent.click(summary);
    await userEvent.click(screen.getByText("Input"));
    await waitFor(() =>
      expect(document.querySelector("pre")?.textContent).toBe(content.preview),
    );
    expect(screen.queryByText("Event ID")).not.toBeInTheDocument();
    await userEvent.click(screen.getByText("Event Details"));
    await waitFor(() => expect(screen.getByText("completed")).toBeVisible());
    expect(screen.getByText("Event ID")).toBeVisible();
  });

  it("shows provider reasoning as a readable excerpt and expands complete text in one step", async () => {
    const content = availableActivityContent(
      "I will check the route and then the exact failure.",
    );
    fetchMock.mockResolvedValue({
      text: content.preview,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 400, itemHeight: 100 }}
      >
        <ActivityHistory
          scopeKey="reasoning"
          items={[
            {
              id: "r",
              eventId: "e",
              sequence: 1,
              kind: "thinking",
              label: "Thinking",
              reasoningContent: content,
            },
          ]}
          gaps={[]}
          isLoadingMore={false}
          loadError={null}
          loadMore={vi.fn()}
        />
      </VirtuosoMockContext.Provider>,
      { wrapper: wrapper() },
    );
    await waitFor(() =>
      expect(screen.getByText(content.preview!)).toBeVisible(),
    );
    expect(fetchMock).not.toHaveBeenCalled();
    await userEvent.click(screen.getByText("Thinking"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
    expect(screen.getByText(content.preview!)).toBeVisible();
    expect(screen.queryByText("Provider Reasoning")).not.toBeInTheDocument();
  });

  it("keeps planner provider messages as optional inspection instead of a second automatic reply", async () => {
    const content = availableActivityContent("Canonical reply text");
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 400, itemHeight: 100 }}
      >
        <ActivityHistory
          scopeKey="planner-message"
          items={[
            {
              id: "p",
              eventId: "e",
              sequence: 1,
              kind: "message",
              label: "Provider Message",
              messageContent: content,
            },
          ]}
          gaps={[]}
          isLoadingMore={false}
          loadError={null}
          loadMore={vi.fn()}
        />
      </VirtuosoMockContext.Provider>,
      { wrapper: wrapper() },
    );
    expect(
      (await screen.findByText("Provider Message")).closest("details"),
    ).not.toHaveAttribute("open");
    expect(screen.queryByText("Canonical reply text")).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
