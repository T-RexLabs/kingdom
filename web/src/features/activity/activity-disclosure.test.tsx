import { useState, type PropsWithChildren } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { VirtuosoMockContext } from "react-virtuoso";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch } from "@/api/client";
import { ActivityDisclosure } from "./activity-disclosure";
import { ActivityChecklist } from "./activity-checklist";
import { ActivityContentReader } from "./activity-history";
import { availableActivityContent } from "./activity-test-helpers";
import { mockActivityScrollGeometry } from "./activity-virtual-test-helpers";

vi.mock("@/api/client", () => ({ apiFetch: vi.fn() }));
const fetchMock = vi.mocked(apiFetch);

function Wrapper({ children }: PropsWithChildren) {
  const [client] = useState(
    () => new QueryClient({ defaultOptions: { queries: { retry: false } } }),
  );
  return (
    <QueryClientProvider client={client}>
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 260, itemHeight: 52 }}
      >
        {children}
      </VirtuosoMockContext.Provider>
    </QueryClientProvider>
  );
}

describe("shared activity disclosure", () => {
  let restoreGeometry: () => void;
  beforeEach(() => {
    restoreGeometry = mockActivityScrollGeometry();
    fetchMock.mockReset();
  });
  afterEach(() => restoreGeometry());

  it("shows the real read and observed message without surrounding provider lifecycle rows", async () => {
    const labels = [
      "Launching Provider",
      "Opening Session",
      "Processing",
      "Read app.tsx",
      "Provider Step Completed",
      "Processing",
      "Provider Message",
      "Provider Step Completed",
      "Finalizing",
    ];
    const items = labels.map((label, index) => ({
      id: `part-${index}`,
      eventId: `event-${index}`,
      sequence: index + 1,
      label,
      kind:
        index === 3
          ? ("tool" as const)
          : index === 6
            ? ("message" as const)
            : ("progress" as const),
    }));
    render(
      <ActivityDisclosure
        scopeKey="one-read"
        items={items}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
      />,
      { wrapper: Wrapper },
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    await waitFor(() => expect(screen.getByText("Read app.tsx")).toBeVisible());
    expect(screen.getByText("Provider Message")).toBeVisible();
    for (const label of [
      "Launching Provider",
      "Opening Session",
      "Processing",
      "Provider Step Completed",
      "Finalizing",
    ]) {
      expect(screen.queryByText(label)).not.toBeInTheDocument();
    }
    expect(document.querySelectorAll("[data-activity-event]")).toHaveLength(2);
  });

  it("omits the activity disclosure when the complete loaded history only contains routine progress", () => {
    render(
      <ActivityDisclosure
        scopeKey="routine-only"
        items={[
          {
            id: "p",
            eventId: "e",
            sequence: 1,
            kind: "progress",
            label: "Processing",
          },
        ]}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
      />,
      { wrapper: Wrapper },
    );
    expect(
      screen.queryByRole("button", { name: "Show Activity" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText("No provider activity has been recorded."),
    ).not.toBeInTheDocument();
  });

  it("preserves exact gap paging when the loaded window only contains routine progress", async () => {
    const loadMore = vi.fn(async () => undefined);
    render(
      <ActivityDisclosure
        scopeKey="routine-window"
        items={[
          {
            id: "p",
            eventId: "e",
            sequence: 81,
            kind: "progress",
            label: "Processing",
          },
        ]}
        gaps={[
          {
            after: 0,
            through: 80,
            count: 80,
            cursor: "earlier-window",
            direction: "earlier",
          },
        ]}
        isLoadingMore={false}
        loadError={null}
        loadMore={loadMore}
      />,
      { wrapper: Wrapper },
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    expect(
      screen.getByText(
        "No thinking, tool calls, or messages in the loaded activity.",
      ),
    ).toBeVisible();
    await waitFor(() =>
      expect(
        screen.getByText("80 earlier events are not loaded."),
      ).toBeVisible(),
    );
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Load Events" }));
    expect(loadMore).toHaveBeenCalledWith("earlier-window");
  });

  it("does not claim that history is empty when its initial read failed", async () => {
    const retry = vi.fn(async () => undefined);
    render(
      <ActivityDisclosure
        scopeKey="failed-read"
        items={[]}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
        error={new Error("Read failed")}
        retry={retry}
      />,
      { wrapper: Wrapper },
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Activity could not be loaded.",
    );
    expect(
      screen.queryByText("No provider activity has been recorded."),
    ).not.toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", { name: "Retry Activity" }),
    );
    expect(retry).toHaveBeenCalledOnce();
  });

  it("lazily opens one activity panel and preserves inspected content across hide/show", async () => {
    const user = userEvent.setup();
    const content = availableActivityContent("Exact captured output");
    fetchMock.mockResolvedValue({
      text: content.preview,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    const props = {
      items: [
        {
          id: "tool",
          eventId: "event",
          sequence: 1,
          kind: "tool" as const,
          label: "Inspect File",
          outputContent: content,
        },
      ],
      gaps: [],
      isLoadingMore: false,
      loadError: null,
      loadMore: vi.fn(),
    };
    const view = render(
      <ActivityDisclosure scopeKey="attempt-a" {...props} />,
      { wrapper: Wrapper },
    );
    const show = screen.getByRole("button", { name: "Show Activity" });
    expect(
      document.getElementById(show.getAttribute("aria-controls")!),
    ).not.toBeVisible();
    expect(screen.queryByText("Inspect File")).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
    await user.click(show);
    await user.click(await screen.findByText("Inspect File"));
    await user.click(screen.getByText("Output"));
    await waitFor(() =>
      expect(screen.getByText("Exact captured output")).toBeVisible(),
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
    await user.click(screen.getByRole("button", { name: "Hide Activity" }));
    expect(screen.getByText("Exact captured output")).not.toBeVisible();
    await user.click(screen.getByRole("button", { name: "Show Activity" }));
    expect(screen.getByText("Exact captured output")).toBeVisible();
    expect(fetchMock).toHaveBeenCalledOnce();
    view.rerender(<ActivityDisclosure scopeKey="attempt-b" {...props} />);
    expect(
      screen.getByRole("button", { name: "Show Activity" }),
    ).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText("Exact captured output")).not.toBeInTheDocument();
  });

  it("keeps the 50,000-event history bounded when opened", async () => {
    const items = Array.from({ length: 50_000 }, (_, index) => ({
      id: `part-${index}`,
      eventId: `event-${index}`,
      sequence: index + 1,
      kind: "tool" as const,
      label: `Event ${index + 1}`,
    }));
    render(
      <ActivityDisclosure
        scopeKey="large"
        items={items}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
      />,
      { wrapper: Wrapper },
    );
    expect(document.querySelectorAll("[data-activity-event]")).toHaveLength(0);
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    await waitFor(() => expect(screen.getByText("Event 50000")).toBeVisible());
    expect(
      document.querySelectorAll("[data-activity-event]").length,
    ).toBeLessThan(30);
    const scroller = screen.getByTestId("virtuoso-scroller");
    act(() => scroller.scrollTo({ top: 20 * 52 }));
    await waitFor(() => expect(screen.getByText("Event 21")).toBeVisible());
    await userEvent.click(
      screen.getByRole("button", { name: "Hide Activity" }),
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    expect(screen.getByTestId("virtuoso-scroller")).toBe(scroller);
    expect(scroller.scrollTop).toBe(20 * 52);
    expect(screen.getByText("Event 21")).toBeVisible();
  });

  it("does not promote a partial JSON preview to a complete checklist", async () => {
    const text = JSON.stringify({
      todos: [{ content: "Inspect the exact input", status: "pending" }],
    });
    const content = { ...availableActivityContent(`${text}\n`), preview: text };
    fetchMock.mockResolvedValueOnce({
      text,
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
      next_cursor: "tail",
    });
    render(
      <ActivityContentReader label="Input" content={content} checklist />,
      { wrapper: Wrapper },
    );
    expect(
      screen.queryByRole("group", { name: "Reported checklist" }),
    ).not.toBeInTheDocument();
    await userEvent.click(screen.getByText("Input"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
    expect(
      screen.queryByRole("group", { name: "Reported checklist" }),
    ).not.toBeInTheDocument();
    fetchMock.mockResolvedValueOnce({
      text: "\n",
      offset: new TextEncoder().encode(text).length,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    await userEvent.click(
      await screen.findByRole("button", { name: "Load More Content" }),
    );
    expect(await screen.findByText("Inspect the exact input")).toBeVisible();
    expect(document.querySelector("pre")?.textContent).toBe(`${text}\n`);
    expect(fetchMock.mock.calls[1][1]?.params?.cursor).toBe("tail");
  });

  it("bounds large reported checklists and leaves malformed or unavailable data unprojected", async () => {
    const tasks = Array.from({ length: 10_000 }, (_, index) => ({
      content: `Task ${index}`,
      status: "pending",
    }));
    const view = render(
      <ActivityChecklist
        content={availableActivityContent(JSON.stringify({ todos: tasks }))}
      />,
      { wrapper: Wrapper },
    );
    await waitFor(() =>
      expect(screen.getAllByRole("listitem").length).toBeGreaterThan(0),
    );
    expect(screen.getAllByRole("listitem").length).toBeLessThan(30);
    view.rerender(
      <ActivityChecklist
        content={availableActivityContent(
          '{"todos":[{"content":"No reported status"}]}',
        )}
      />,
    );
    expect(
      screen.queryByRole("group", { name: "Reported checklist" }),
    ).not.toBeInTheDocument();
    view.rerender(
      <ActivityChecklist content={{ availability: "unavailable" }} />,
    );
    expect(
      screen.queryByRole("group", { name: "Reported checklist" }),
    ).not.toBeInTheDocument();
  });
});
