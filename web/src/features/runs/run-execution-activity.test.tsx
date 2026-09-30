import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { components } from "@/api/schema";
import { RunExecutionActivity } from "./run-execution-activity";

const state = vi.hoisted(() => ({
  items: [] as components["schemas"]["AcpRunActivityItem"][],
  pending: false,
  gaps: [] as Array<{
    after: number;
    through: number;
    count: number;
    cursor: string;
    direction: "earlier" | "later";
  }>,
  historyProps: vi.fn(),
}));
vi.mock("@/api/hooks-runs", () => ({
  useRunExecutionActivity: () => ({
    data: { items: state.items, snapshotThrough: 1 },
    gaps: state.gaps,
    isLoading: state.pending,
    loadMore: vi.fn(),
  }),
}));
vi.mock("./run-live-activity", () => ({
  useLiveRunActivity: () => ({}),
  selectLiveRunActivity: () => [],
}));
vi.mock("@/features/activity/activity-history", () => ({
  ActivityHistory: (props: { items: Array<{ label: string }> }) => {
    state.historyProps(props);
    return (
      <div>
        {props.items.map((item, i) => (
          <p key={i}>{item.label}</p>
        ))}
      </div>
    );
  },
}));

describe("focused provider activity", () => {
  beforeEach(() => {
    state.items = [];
    state.gaps = [];
    state.pending = false;
    state.historyProps.mockClear();
  });
  it("keeps the canonical response inline without routine lifecycle rows or view options", async () => {
    const absent = { availability: "not_provided" as const };
    state.items = [
      {
        id: "p",
        event_id: "p",
        sequence: 1,
        kind: "progress",
        label: "Processing",
        occurred_at: "2026-09-30T10:00:00Z",
        recorded_at: "2026-09-30T10:00:00Z",
        input: absent,
        output: absent,
        reasoning: absent,
        message: absent,
      },
    ];
    render(
      <RunExecutionActivity
        runId="run"
        attemptId="a"
        status="succeeded"
        detail={
          {
            attempt_id: "a",
            output_status: "settled",
            settlement_status: "succeeded",
            output_text: "## Final answer",
          } as import("@/api/hooks").RunExecutionDetail
        }
      />,
    );
    expect(screen.getByRole("heading", { name: "Final answer" })).toBeVisible();
    expect(screen.queryByText("View Options")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(state.historyProps).not.toHaveBeenCalled();
    expect(
      screen.queryByRole("button", { name: "Show Activity" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Final answer" })).toBeVisible();
  });
  it("shows completed-attempt activity with complete canonical descriptors", async () => {
    const absent = { availability: "not_provided" as const };
    state.items = [
      {
        id: "part",
        event_id: "event",
        sequence: 1,
        kind: "thinking",
        label: "Thinking",
        occurred_at: "2026-09-29T10:00:00Z",
        recorded_at: "2026-09-29T10:00:01Z",
        input: absent,
        output: absent,
        message: absent,
        reasoning: {
          availability: "available",
          preview: "Exact thoughts",
          byte_length: 14,
          sha256: "hash",
          content_url: "/acp/runs/r/activity/e/content/reasoning",
        },
      },
    ];
    render(
      <RunExecutionActivity runId="run" attemptId="a" status="succeeded" />,
    );
    expect(
      screen.getByRole("heading", { name: "Provider Activity" }),
    ).toBeInTheDocument();
    expect(state.historyProps).toHaveBeenLastCalledWith(
      expect.objectContaining({
        items: [
          expect.objectContaining({
            reasoningContent: state.items[0].reasoning,
          }),
        ],
      }),
    );
  });
  it("keeps real thinking and tools visible as the canonical final reply arrives", () => {
    const absent = { availability: "not_provided" as const };
    const base = {
      occurred_at: "2026-09-30T10:00:00Z",
      recorded_at: "2026-09-30T10:00:00Z",
      input: absent,
      output: absent,
      reasoning: absent,
      message: absent,
    };
    state.items = [
      {
        ...base,
        id: "thought",
        event_id: "thought",
        sequence: 1,
        kind: "thinking",
        label: "Inspecting the existing tests",
      },
      {
        ...base,
        id: "read",
        event_id: "read",
        sequence: 2,
        kind: "tool",
        label: "Read tests.ts",
      },
    ];
    const view = render(
      <RunExecutionActivity runId="run" attemptId="a" status="running" />,
    );
    const thought = screen.getByText("Inspecting the existing tests");
    expect(thought).toBeVisible();
    expect(screen.getByText("Read tests.ts")).toBeVisible();
    view.rerender(
      <RunExecutionActivity
        runId="run"
        attemptId="a"
        status="succeeded"
        detail={
          {
            attempt_id: "a",
            output_status: "settled",
            settlement_status: "succeeded",
            output_text: "## Complete result",
          } as import("@/api/hooks").RunExecutionDetail
        }
      />,
    );
    expect(
      screen.getByRole("heading", { name: "Complete result" }),
    ).toBeVisible();
    expect(screen.getByText("Inspecting the existing tests")).toBe(thought);
    expect(thought).toBeVisible();
    expect(screen.getByText("Read tests.ts")).toBeVisible();
  });

  it("shows actual observed source links below the canonical response and clears them on attempt change", () => {
    const absent = { availability: "not_provided" as const };
    state.items = [
      {
        id: "fetch",
        event_id: "fetch-event",
        sequence: 1,
        kind: "tool",
        tool_name: "webfetch",
        tool_status: "completed",
        label: "Fetched https://docs.example.test/guide",
        input: {
          availability: "available",
          preview: '{"url":"https://docs.example.test/guide"}',
        },
        output: absent,
        reasoning: absent,
        message: absent,
        occurred_at: "2026-09-30T10:00:00Z",
        recorded_at: "2026-09-30T10:00:00Z",
      },
    ];
    const view = render(
      <RunExecutionActivity
        runId="run"
        attemptId="a"
        status="succeeded"
        detail={
          {
            attempt_id: "a",
            output_status: "settled",
            settlement_status: "succeeded",
            output_text: "## Verified result",
          } as import("@/api/hooks").RunExecutionDetail
        }
      />,
    );
    const response = screen.getByRole("region", { name: "Final response" });
    const source = screen.getByRole("link", { name: "docs.example.test" });
    expect(source).toHaveAttribute("href", "https://docs.example.test/guide");
    expect(source).toHaveAttribute("target", "_blank");
    expect(
      response.compareDocumentPosition(source) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    state.items = [];
    view.rerender(
      <RunExecutionActivity runId="run" attemptId="b" status="running" />,
    );
    expect(
      screen.queryByRole("link", { name: "docs.example.test" }),
    ).not.toBeInTheDocument();
  });

  it("distinguishes no recorded activity from a still running attempt", () => {
    state.items = [];
    const { rerender } = render(
      <RunExecutionActivity runId="run" attemptId="a" status="running" />,
    );
    expect(
      screen.getByText("Waiting for provider activity."),
    ).toBeInTheDocument();
    rerender(
      <RunExecutionActivity runId="run" attemptId="a" status="succeeded" />,
    );
    expect(
      screen.getByText(
        "No provider messages, thoughts, or tool activity were recorded for this attempt.",
      ),
    ).toBeInTheDocument();
  });
  it("does not imply a complete empty conversation while earlier history remains unloaded", () => {
    state.items = [];
    state.gaps = [
      {
        after: 0,
        through: 80,
        count: 80,
        cursor: "earlier",
        direction: "earlier",
      },
    ];
    render(
      <RunExecutionActivity runId="run" attemptId="a" status="succeeded" />,
    );
    expect(
      screen.getByText(
        "No messages, thoughts, or tool calls appear in this loaded window. Open activity to load earlier history.",
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(
        "No provider messages, thoughts, or tool activity were recorded for this attempt.",
      ),
    ).not.toBeInTheDocument();
    state.gaps = [];
  });

  it("does not describe a queued or paused attempt as a finished empty conversation", () => {
    const { rerender } = render(
      <RunExecutionActivity runId="run" attemptId="a" status="queued" />,
    );
    expect(
      screen.getByText(
        "This attempt is queued. Provider activity will appear when it starts.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByText(/were recorded for this attempt/),
    ).not.toBeInTheDocument();
    rerender(
      <RunExecutionActivity runId="run" attemptId="a" status="paused" />,
    );
    expect(
      screen.getByText("No provider activity has been recorded yet."),
    ).toBeVisible();
  });
});
