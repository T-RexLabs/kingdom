import { render as renderPlanner } from "./planning-test-render";
import type { ReactElement } from "react";
import { VirtuosoMockContext } from "react-virtuoso";
import { mockActivityScrollGeometry } from "@/features/activity/activity-virtual-test-helpers";
import {availableActivityContent} from "@/features/activity/activity-test-helpers";
const apiActivityDefaults={event_id:"fixture-event",sequence:1,recorded_at:"2026-06-12T10:00:00Z",input:{availability:"not_provided"},output:{availability:"not_provided"},reasoning:{availability:"not_provided"},message:{availability:"not_provided"}};
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerMessageCard } from "./planner-message";

const mockUseAcpRunActivity = vi.fn();
function render(ui: ReactElement) {
  return renderPlanner(
    <VirtuosoMockContext.Provider
      value={{ viewportHeight: 260, itemHeight: 52 }}
    >
      {ui}
    </VirtuosoMockContext.Provider>,
  );
}

vi.mock("@/api/hooks", async () => {
  const actual = await vi.importActual<typeof import("@/api/hooks")>("@/api/hooks");
  return {
    ...actual,
    useAcpRunActivity: (...args: unknown[]) => mockUseAcpRunActivity(...args),
  };
});

describe("PlannerMessageCard", () => {
  it("does not reserve timestamp metadata beneath a live assistant response", () => {
    const { container } = render(<PlannerMessageCard message={{ planning_message_id: "live", planning_session_id: "s", role: "assistant", content_markdown: "", created_at: "2026-06-12T10:00:00Z" }} isStreaming />);
    expect(container.querySelector(".text-\\[10px\\]")).not.toBeInTheDocument();
  });

  it("keeps completed reply actions and time in one compact metadata row", () => {
    const { container } = render(<PlannerMessageCard message={{ planning_message_id: "done", planning_session_id: "s", role: "assistant", content_markdown: "Done", created_at: "2026-06-12T10:00:00Z" }} onRegenerate={() => {}} />);
    const timestamp = container.querySelector("time");
    expect(timestamp).toHaveAttribute("datetime", "2026-06-12T10:00:00Z");
    expect(timestamp?.parentElement).toContainElement(screen.getByRole("button", { name: "Regenerate reply" }));
  });
  let restoreGeometry: () => void;
  beforeEach(() => {
    restoreGeometry = mockActivityScrollGeometry();
    mockUseAcpRunActivity.mockReset();
    mockUseAcpRunActivity.mockReturnValue({ data: { items: [] } });
  });

  afterEach(() => {
    restoreGeometry();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("right aligns short user messages inside the full-width transcript row", () => {
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "user-1",
          planning_session_id: "session-1",
          role: "user",
          content_markdown: "Hi",
          created_at: "2026-06-12T10:00:00Z",
        }}
      />,
    );
    const row = screen.getByText("Hi").closest(".group")?.firstElementChild;
    expect(row).toHaveClass("justify-end");
  });

  it("keeps one Show Activity disclosure above the canonical reply", async () => {
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          {
            ...apiActivityDefaults,
            id: "thought",
            event_id: "thought-event",
            sequence: 1,
            kind: "thinking",
            label: "Thinking",
            reasoning: availableActivityContent(
              "Compare the existing layout before editing.",
            ),
          },
          {
            ...apiActivityDefaults,
            id: "read",
            event_id: "read-event",
            sequence: 2,
            kind: "tool",
            tool_name: "read",
            label: "Read /project/app.tsx",
            input: availableActivityContent('{"filePath":"/project/app.tsx"}'),
          },
        ],
      },
    });
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Canonical reply",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      />,
    );
    const disclosure = screen.getByRole("button", { name: "Show Activity" });
    expect(disclosure).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText("Compare the existing layout before editing.")).not.toBeInTheDocument();
    expect(screen.queryByText("View Options")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    await userEvent.click(disclosure);
    const thinking = await screen.findByText(
      "Compare the existing layout before editing.",
    );
    const tool = await screen.findByText("Read app.tsx");
    const reply = screen.getByText("Canonical reply");
    expect(
      thinking.compareDocumentPosition(reply) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(
      tool.compareDocumentPosition(reply) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("shows the current provider lifecycle status while streaming without an empty history panel", () => {
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          created_at: "2026-06-12T10:00:00Z",
        }}
        isStreaming
        liveActivityItems={[
          {
            id: "phase",
            eventId: "phase-event",
            sequence: 1,
            kind: "progress",
            label: "Provider Session Started",
          },
        ]}
      />,
    );
    expect(
      screen.getByRole("status"),
    ).toHaveTextContent("Provider Session Started");
    expect(screen.queryByRole("button", { name: "Show Activity" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Hide Activity" })).not.toBeInTheDocument();
  });

  it("keeps routine events and provider message copies behind explicit inspection", async () => {
    const user = userEvent.setup();
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          {
            ...apiActivityDefaults,
            id: "phase",
            event_id: "phase-event",
            sequence: 1,
            kind: "progress",
            label: "Provider Session Started",
            detail: "raw provider lifecycle detail",
          },
          {
            ...apiActivityDefaults,
            id: "observed",
            event_id: "observed-event",
            sequence: 2,
            kind: "message",
            label: "Observed Provider Reply",
            message: {
              ...availableActivityContent("Provider observed reply"),
              content_url: undefined,
            },
          },
        ],
      },
    });
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          source_run_id: "run-1",
          content_markdown: "Canonical reply",
          created_at: "2026-06-12T10:00:00Z",
        }}
      />,
    );
    expect(mockUseAcpRunActivity).toHaveBeenCalledWith(
      "run-1",
      expect.objectContaining({ enabled: true }),
    );
    expect(
      screen.queryByText("Provider Session Started"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText("Observed Provider Reply"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText("Provider observed reply"),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Show Activity" }));
    expect(screen.queryByText("Provider Session Started")).not.toBeInTheDocument();
    expect(
      screen.queryByText("raw provider lifecycle detail"),
    ).not.toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByText("Observed Provider Reply")).toBeVisible(),
    );
    expect(
      screen.queryByText("Provider observed reply"),
    ).not.toBeInTheDocument();
    await user.click(screen.getByText("Observed Provider Reply"));
    expect(await screen.findByText("Provider observed reply")).toBeVisible();
    expect(screen.getAllByText("Canonical reply")).toHaveLength(1);
  });

  it("keeps unloaded history actionable above the canonical reply", async () => {
    const loadMore = vi.fn(async () => undefined);
    mockUseAcpRunActivity.mockReturnValue({
      data: { items: [] },
      gaps: [
        {
          after: 0,
          through: 80,
          count: 80,
          cursor: "earlier-scoped-history",
          direction: "earlier",
        },
      ],
      loadMore,
    });
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Canonical reply",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      />,
    );
    const activity = screen.getByRole("region", { name: "Planner activity" });
    await userEvent.click(within(activity).getByRole("button", { name: "Show Activity" }));
    await userEvent.click(
      await within(activity).findByRole("button", { name: "Load Events" }),
    );
    expect(loadMore).toHaveBeenCalledWith("earlier-scoped-history");
    expect(
      activity.compareDocumentPosition(screen.getByText("Canonical reply")) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("keeps a saved reply visible when activity loading fails and exposes a scoped retry", async () => {
    const refetch = vi.fn(async () => undefined);
    mockUseAcpRunActivity.mockReturnValue({
      data: { items: [] },
      isError: true,
      error: new Error("Activity unavailable"),
      refetch,
    });
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Canonical reply",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      />,
    );
    expect(screen.getByText("Canonical reply")).toBeVisible();
    await userEvent.click(
      screen.getByRole("button", { name: "Retry Activity" }),
    );
    expect(refetch).toHaveBeenCalledOnce();
  });

  it("lets a keyboard or assistive-technology user identify and edit their message", async () => {
    const onEdit = vi.fn();
    const user = userEvent.setup();
    render(<PlannerMessageCard message={{ planning_message_id: "user-1", planning_session_id: "session-1", role: "user", content_markdown: "My brief", created_at: "2026-06-12T10:00:00Z" }} onEdit={onEdit} />);
    expect(screen.getByRole("button", { name: "Edit message" })).toHaveTextContent("Edit");
    expect(screen.getByRole("button", { name: "Edit message" }).querySelector("svg")).toBeNull();
    await user.click(screen.getByRole("button", { name: "Edit message" }));
    expect(onEdit).toHaveBeenCalledTimes(1);
  });

  it("renders durable thought text while streaming after reconnect", async () => {
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          {
            ...apiActivityDefaults,
            id: "thought-1",
            kind: "thinking",
            label: "Thinking",
            reasoning: availableActivityContent("Recovered durable reasoning"),
            occurred_at: "2026-06-12T10:00:00Z",
          },
        ],
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        isStreaming
        activityRunId="run-1"
      />,
    );

    expect(
      await screen.findByText("Recovered durable reasoning"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Planner is thinking...")).not.toBeInTheDocument();
  });

  it("shows a reasoning preview once and renders a few sources inline as host links", async () => {
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          { ...apiActivityDefaults, id: "thought-1", event_id: "event-1", sequence: 1, kind: "thinking", label: "Thinking", reasoning: availableActivityContent("Determine the best next action"), occurred_at: "2026-06-12T10:00:00Z" },
          { ...apiActivityDefaults, id: "source-1", event_id: "event-2", sequence: 2, kind: "tool", tool_name: "webfetch", tool_status: "completed", label: "Fetched https://example.com", input:availableActivityContent('{"url":"https://example.com"}'), occurred_at: "2026-06-12T10:00:01Z" },
          { ...apiActivityDefaults, id: "tool-1", event_id: "event-3", sequence: 3, kind: "tool", label: "Read planner-message.tsx", input:availableActivityContent('{"filePath":"planner-message.tsx"}'), occurred_at: "2026-06-12T10:00:02Z" },
        ],
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Final answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Show Activity" }));
    expect(
      await screen.findByText("Determine the best next action"),
    ).toBeInTheDocument();
    expect(screen.getAllByText("Determine the best next action")).toHaveLength(
      1,
    );
    expect(
      screen.queryByRole("button", { name: /Thoughts/ }),
    ).not.toBeInTheDocument();

    // A few sources show inline (no toggle), as clickable host links; only
    // webfetches become sources — read/grep/etc. tools do not.
    expect(screen.queryByRole("button", { name: /Sources/ })).not.toBeInTheDocument();
    const sourceLink = screen.getByRole("link", { name: /example\.com/ });
    expect(sourceLink).toHaveAttribute("href", "https://example.com");
    expect(screen.queryByRole("link", { name: /planner-message\.tsx/ })).not.toBeInTheDocument();
  });

  it("collapses 5+ sources behind a Sources disclosure", async () => {
    const user = userEvent.setup();
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: Array.from({ length: 5 }, (_, i) => ({
          ...apiActivityDefaults, event_id:`event-${i}`,sequence:i+1,
          id: `source-${i}`,
          kind: "tool" as const,
          label: `Fetched https://site${i}.example.com`,
          input:availableActivityContent(`{"url":"https://site${i}.example.com"}`),
          occurred_at: `2026-06-12T10:00:0${i}Z`,
        })),
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Final answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      />,
    );

    // Collapsed until expanded once the burst gets large.
    expect(screen.getByRole("button", { name: /Sources/ })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /site0\.example\.com/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Sources/ }));
    expect(screen.getByRole("link", { name: /site0\.example\.com/ })).toHaveAttribute("href", "https://site0.example.com");
  });

  it("preserves separate observed thinking records during streaming", async () => {
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        isStreaming
        liveActivityItems={[
          { id: "thought-1", eventId: "event-4", sequence: 4, kind: "thinking", label: "Thinking", reasoningContent: availableActivityContent("First reasoning step") },
          { id: "thought-2", eventId: "event-5", sequence: 5, kind: "thinking", label: "Thinking", reasoningContent: { availability: "unavailable" } },
        ]}
      />,
    );

    expect(await screen.findByText(/First reasoning step/)).toBeInTheDocument();
    expect(
      await screen.findByText(
        /The provider did not expose this reasoning\./,
      ),
    ).toBeInTheDocument();
  });

  it("places the sources below the question and its selectables", () => {
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          {
            ...apiActivityDefaults,
            id: "source-1",
            kind: "tool",
            label: "Fetched https://example.com",
            input:availableActivityContent('{"url":"https://example.com"}'),
            occurred_at: "2026-06-12T10:00:01Z",
          },
        ],
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Here is the answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        activityRunId="run-1"
      >
        <div data-testid="question-choices">Which option do you prefer?</div>
      </PlannerMessageCard>,
    );

    const question = screen.getByTestId("question-choices");
    const sources = screen.getByRole("link", { name: /example\.com/ });
    // Sources must come after the question/selectables in document order.
    expect(question.compareDocumentPosition(sources) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("shows real live tools without routine progress inside the activity disclosure", async () => {
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        isStreaming
        liveActivityItems={[
          { id: "progress-1", eventId: "event-6", sequence: 6, kind: "progress", label: "Processing" },
          { id: "tool-1", eventId: "event-7", sequence: 7, kind: "tool", label: "Searching ACP activity handlers" },
          { id: "tool-2", eventId: "event-8", sequence: 8, kind: "tool", toolName: "webfetch", toolStatus: "running", label: "Fetching https://example.com", input: '{"url":"https://example.com"}' },
        ]}
      />,
    );

    await waitFor(() => expect(screen.getByText("Fetching https://example.com")).toBeVisible());
    expect(screen.getByRole("link", { name: /example\.com/ })).toHaveAttribute("href", "https://example.com");
    expect(screen.getByRole("status")).toHaveTextContent("Fetching https://example.com");
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
    expect(screen.queryByText("Searching ACP activity handlers")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Show Activity" }));
    expect(
      await screen.findByText("Searching ACP activity handlers"),
    ).toBeInTheDocument();
  });

  it("retains each real webfetch record without fabricating an aggregate event", async () => {
    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
        }}
        isStreaming
        liveActivityItems={[
          { id: "tool-1", eventId: "event-9", sequence: 9, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched https://popstroke.com/", input: '{"url":"https://popstroke.com/"}' },
          { id: "tool-2", eventId: "event-10", sequence: 10, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched https://www.driveshack.com/", input: '{"url":"https://www.driveshack.com/"}' },
          { id: "tool-3", eventId: "event-11", sequence: 11, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched https://bigshotsgolf.com/", input: '{"url":"https://bigshotsgolf.com/"}' },
          { id: "progress-1", eventId: "event-12", sequence: 12, kind: "progress", label: "Processing" },
        ]}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Fetched https://bigshotsgolf.com/");
    expect(screen.getAllByText(/^Fetched https:/)).toHaveLength(1);
    await userEvent.click(screen.getByRole("button", { name: "Show Activity" }));
    const history = screen.getByRole("region", { name: "Provider activity history" });
    await waitFor(() => expect(within(history).getAllByText(/^Fetched https:/)).toHaveLength(3));
    expect(screen.queryByText("Fetched 3 web sources")).not.toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
  });
});
