import { render } from "./planning-test-render";
import {availableActivityContent} from "@/features/activity/activity-test-helpers";
const apiActivityDefaults={event_id:"fixture-event",sequence:1,recorded_at:"2026-06-12T10:00:00Z",input:{availability:"not_provided"},output:{availability:"not_provided"},reasoning:{availability:"not_provided"},message:{availability:"not_provided"}};
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerMessageCard } from "./planner-message";

const mockUseAcpRunActivity = vi.fn();

vi.mock("@/api/hooks", async () => {
  const actual = await vi.importActual<typeof import("@/api/hooks")>("@/api/hooks");
  return {
    ...actual,
    useAcpRunActivity: (...args: unknown[]) => mockUseAcpRunActivity(...args),
  };
});

describe("PlannerMessageCard", () => {
  beforeEach(() => {
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {});
    mockUseAcpRunActivity.mockReset();
    mockUseAcpRunActivity.mockReturnValue({ data: { items: [] } });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("lets a keyboard or assistive-technology user identify and edit their message", async () => {
    const onEdit = vi.fn();
    const user = userEvent.setup();
    render(<PlannerMessageCard message={{ planning_message_id: "user-1", planning_session_id: "session-1", role: "user", content_markdown: "My brief", created_at: "2026-06-12T10:00:00Z" }} onEdit={onEdit} />);
    await user.click(screen.getByRole("button", { name: "Edit message" }));
    expect(onEdit).toHaveBeenCalledTimes(1);
  });

  it("renders durable thought text while streaming after reconnect", () => {
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

    expect(screen.getByText("Thinking")).toBeInTheDocument();
    expect(screen.getByText("Recovered durable reasoning")).toBeInTheDocument();
    expect(screen.queryByText("Planner is thinking...")).not.toBeInTheDocument();
  });

  it("keeps thoughts collapsed and renders a few sources inline as host links", async () => {
    const user = userEvent.setup();
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

    // Thoughts stays a collapsible disclosure.
    expect(screen.getByRole("button", { name: /Thoughts/ })).toBeInTheDocument();
    expect(screen.queryByText("Determine the best next action")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Thoughts/ }));
    expect(screen.getByText("Determine the best next action")).toBeInTheDocument();

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

  it("renders cumulative streaming thoughts instead of replacing earlier reasoning", () => {
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
          { id: "thought-1", eventId: "event-4", sequence: 4, kind: "thinking", label: "Thinking", detail: "First reasoning step" },
          { id: "thought-2", eventId: "event-5", sequence: 5, kind: "thinking", label: "Thinking", detail: "The provider emitted a reasoning step here, but only encrypted reasoning metadata is available." },
        ]}
      />,
    );

    expect(screen.getByText(/First reasoning step/)).toBeInTheDocument();
    expect(screen.getByText(/The provider emitted a reasoning step here, but only encrypted reasoning metadata is available\./)).toBeInTheDocument();
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

  it("renders specific live activity rows instead of collapsing everything to Processing", () => {
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

    expect(screen.getByText("Fetching https://example.com")).toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
    expect(screen.queryByText("Searching ACP activity handlers")).not.toBeInTheDocument();
  });

  it("compacts webfetch bursts into one current event line", () => {
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

    expect(screen.getByText("Fetched 3 web sources")).toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
  });
});
