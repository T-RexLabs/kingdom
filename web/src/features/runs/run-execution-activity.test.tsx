import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { components } from "@/api/schema";
import { RunExecutionActivity } from "./run-execution-activity";

const state = vi.hoisted(() => ({
  items: [] as components["schemas"]["AcpRunActivityItem"][],
  pending: false,
  error: false,
  retry: vi.fn(),
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
    isError: state.error,
    refetch: state.retry,
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
      <div role="region" aria-label="Provider activity history">
        {props.items.map((item, i) => (
          <p key={i}>{item.label}</p>
        ))}
      </div>
    );
  },
}));

function observedThoughtAndTool(): components["schemas"]["AcpRunActivityItem"][] {
  const absent = { availability: "not_provided" as const };
  const base = {
    occurred_at: "2026-09-30T10:00:00Z",
    recorded_at: "2026-09-30T10:00:00Z",
    input: absent,
    output: absent,
    reasoning: absent,
    message: absent,
  };
  const thought = "Inspecting the existing tests";
  const input = '{"path":"/workspace/src/tests.ts"}';
  return [
    {
      ...base,
      id: "thought",
      event_id: "thought",
      sequence: 1,
      kind: "thinking",
      label: "Thinking",
      reasoning: {
        availability: "available",
        preview: thought,
        byte_length: new TextEncoder().encode(thought).length,
      },
    },
    {
      ...base,
      id: "read",
      event_id: "read",
      sequence: 2,
      kind: "tool",
      label: "Read /workspace/src/tests.ts",
      tool_name: "read",
      tool_status: "completed",
      input: {
        availability: "available",
        preview: input,
        byte_length: new TextEncoder().encode(input).length,
      },
    },
  ];
}

describe("focused provider activity", () => {
  beforeEach(() => {
    state.items = [];
    state.gaps = [];
    state.pending = false;
    state.error = false;
    state.retry.mockClear();
    state.historyProps.mockClear();
  });
  it("explains a finished attempt with only hidden message observations and no settled reply", () => {
    state.items = [{ ...observedThoughtAndTool()[0], kind: "message", label: "Provider Message" }];
    render(<RunExecutionActivity runId="run" attemptId="a" status="cancelled" />);
    expect(screen.getByText("No thinking, tool activity, or settled response is available for this attempt.")).toBeVisible();
    expect(screen.queryByText("Provider Message")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Show Activity" })).not.toBeInTheDocument();
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
  it("shows completed-attempt reasoning in history immediately", () => {
    state.items = observedThoughtAndTool();
    render(<RunExecutionActivity runId="run" attemptId="a" status="succeeded" />);
    expect(screen.getByRole("region", { name: "Provider activity history" })).toBeVisible();
    expect(state.historyProps).toHaveBeenLastCalledWith(expect.objectContaining({
      items: [expect.objectContaining({ reasoningContent: state.items[0].reasoning }), expect.objectContaining({ label: "Read /workspace/src/tests.ts" })],
    }));
    expect(screen.queryByRole("button", { name: "Show Activity" })).not.toBeInTheDocument();
  });
  it("keeps the scrollable activity before the response after settlement and resets its scope on attempt change", () => {
    state.items = observedThoughtAndTool();
    const view = render(<RunExecutionActivity runId="run" attemptId="a" status="running" />);
    const history = screen.getByRole("region", { name: "Provider activity history" });
    view.rerender(<RunExecutionActivity runId="run" attemptId="a" status="succeeded" detail={{
      attempt_id: "a", output_status: "settled", settlement_status: "succeeded", output_text: "## Complete result",
    } as import("@/api/hooks").RunExecutionDetail} />);
    const response = screen.getByRole("region", { name: "Final response" });
    expect(screen.getByRole("region", { name: "Provider activity history" })).toBe(history);
    expect(history.compareDocumentPosition(response) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(state.historyProps).toHaveBeenLastCalledWith(expect.objectContaining({ scopeKey: JSON.stringify(["run", "a"]) }));
    view.rerender(<RunExecutionActivity runId="run" attemptId="b" status="running" />);
    expect(history).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Provider activity history" })).toBeVisible();
    expect(state.historyProps).toHaveBeenLastCalledWith(expect.objectContaining({ scopeKey: JSON.stringify(["run", "b"]) }));
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
        "No thinking, tool activity, or settled response is available for this attempt.",
      ),
    ).toBeInTheDocument();
  });
  it("shows read failures with a retry and does not imply activity is still pending", async () => {
    state.error = true;
    render(<RunExecutionActivity runId="run" attemptId="a" status="running" />);
    expect(screen.getByRole("alert")).toHaveTextContent("Activity could not be loaded.");
    expect(screen.queryByText("Waiting for provider activity.")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Retry Activity" }));
    expect(state.retry).toHaveBeenCalledOnce();
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
        "No thinking or tool calls appear in this loaded window. Load earlier events below.",
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(
        "No thinking, tool activity, or settled response is available for this attempt.",
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
  it("shows queued workspace preparation from observed activity and the durable marker after reopening", () => {
    state.items = [{
      ...observedThoughtAndTool()[0],
      kind: "progress",
      label: "Preparing workspace",
    }];
    const view = render(<RunExecutionActivity runId="run" attemptId="a" status="queued" />);
    expect(screen.getByRole("status")).toHaveTextContent("Preparing workspace");
    expect(screen.queryByText(/This attempt is queued/)).not.toBeInTheDocument();
    state.items = [];
    view.rerender(<RunExecutionActivity runId="run" attemptId="a" status="queued" detail={{
      attempt_id: "a", progress_reason_code: "workspace_preparing", progress_event_type: "attempt.progress",
    } as import("@/api/hooks").RunExecutionDetail} />);
    expect(screen.getByRole("status")).toHaveTextContent("Preparing Workspace");
    expect(screen.queryByText(/This attempt is queued/)).not.toBeInTheDocument();
  });
});
