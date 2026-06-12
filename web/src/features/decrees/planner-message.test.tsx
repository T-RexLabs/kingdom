import { render, screen } from "@testing-library/react";
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

  it("renders durable thought text while streaming after reconnect", () => {
    mockUseAcpRunActivity.mockReturnValue({
      data: {
        items: [
          {
            id: "thought-1",
            kind: "thinking",
            label: "Thinking",
            detail: "Recovered durable reasoning",
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
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
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
          { id: "thought-1", kind: "thinking", label: "Thinking", detail: "Determine the best next action", occurred_at: "2026-06-12T10:00:00Z" },
          { id: "source-1", kind: "tool", label: "Fetched https://example.com", input: '{"url":"https://example.com"}', occurred_at: "2026-06-12T10:00:01Z" },
          { id: "tool-1", kind: "tool", label: "Read planner-message.tsx", input: '{"filePath":"planner-message.tsx"}', occurred_at: "2026-06-12T10:00:02Z" },
        ],
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "Final answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
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
          id: `source-${i}`,
          kind: "tool" as const,
          label: `Fetched https://site${i}.example.com`,
          input: `{"url":"https://site${i}.example.com"}`,
          occurred_at: `2026-06-12T10:00:0${i}Z`,
        })),
      },
    });

    render(
      <PlannerMessageCard
        message={{
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "Final answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
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
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
        }}
        isStreaming
        liveActivityItems={[
          { id: "thought-1", kind: "thinking", label: "Thinking", detail: "First reasoning step" },
          { id: "thought-2", kind: "thinking", label: "Thinking", detail: "The provider emitted a reasoning step here, but only encrypted reasoning metadata is available." },
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
            id: "source-1",
            kind: "tool",
            label: "Fetched https://example.com",
            input: '{"url":"https://example.com"}',
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
          turn_index: 1,
          role: "assistant",
          content_markdown: "Here is the answer",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
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
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
        }}
        isStreaming
        liveActivityItems={[
          { id: "progress-1", kind: "progress", label: "Processing" },
          { id: "tool-1", kind: "tool", label: "Searching ACP activity handlers" },
          { id: "tool-2", kind: "tool", label: "Fetching https://example.com", input: '{"url":"https://example.com"}' },
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
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          source_run_id: "run-1",
          created_at: "2026-06-12T10:00:00Z",
          variant_index: 0,
          active: true,
        }}
        isStreaming
        liveActivityItems={[
          { id: "tool-1", kind: "tool", label: "Fetched https://popstroke.com/", input: '{"url":"https://popstroke.com/"}' },
          { id: "tool-2", kind: "tool", label: "Fetched https://www.driveshack.com/", input: '{"url":"https://www.driveshack.com/"}' },
          { id: "tool-3", kind: "tool", label: "Fetched https://bigshotsgolf.com/", input: '{"url":"https://bigshotsgolf.com/"}' },
          { id: "progress-1", kind: "progress", label: "Processing" },
        ]}
      />,
    );

    expect(screen.getByText("Fetched 3 web sources")).toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
  });
});
