import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActivityDisclosure } from "./activity-disclosure";
import { availableActivityContent } from "./activity-test-helpers";
import type { ActivityItem } from "./activity-view";

vi.mock("./activity-history", () => ({
  ActivityHistory: ({ items }: { items: ActivityItem[] }) => (
    <div role="region" aria-label="Provider activity history">
      {items.map((item) => (
        <p key={item.id}>{item.label}</p>
      ))}
    </div>
  ),
}));

const props = {
  scopeKey: "operation-a",
  isLive: true,
  hasFinalResponse: false,
  completionBehavior: "collapse" as const,
  gaps: [],
  isLoadingMore: false,
  loadError: null,
  loadMore: vi.fn(),
};
const thought: ActivityItem = {
  id: "thought",
  eventId: "thought-event",
  sequence: 1,
  kind: "thinking",
  label: "Thinking",
  reasoningContent: availableActivityContent("Inspect the existing route."),
};
const tool: ActivityItem = {
  id: "search",
  eventId: "search-event",
  sequence: 2,
  kind: "tool",
  toolName: "grep",
  toolStatus: "running",
  label: "Searching the route handler",
};

afterEach(() => vi.unstubAllGlobals());

describe("compact activity preview", () => {
  it("shows plain reasoning and one useful action without automatically mounting history", () => {
    render(
      <ActivityDisclosure
        {...props}
        items={[
          thought,
          tool,
          {
            id: "progress",
            eventId: "p",
            sequence: 3,
            kind: "progress",
            label: "Processing",
          },
        ]}
      />,
    );
    const preview = screen.getByRole("region", {
      name: "Provider activity preview",
    });
    expect(
      within(preview).getByText("Inspect the existing route."),
    ).toBeVisible();
    expect(within(preview).getByRole("status")).toHaveTextContent(
      "Searching the route handler",
    );
    expect(screen.queryByText("Processing")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("region", { name: "Provider activity history" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Hide Activity" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Show Activity" }),
    ).toHaveAttribute("aria-expanded", "false");
  });

  it("reveals newly received reasoning while reconnect text appears immediately", () => {
    const frames: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      frames.push(callback);
      return frames.length;
    });
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    const view = render(<ActivityDisclosure {...props} items={[]} />);
    view.rerender(<ActivityDisclosure {...props} items={[thought]} />);
    expect(
      screen.queryByText("Inspect the existing route."),
    ).not.toBeInTheDocument();
    act(() => frames.splice(0).forEach((callback) => callback(31)));
    expect(screen.getByText("Ins")).toBeVisible();
    view.rerender(
      <ActivityDisclosure
        {...props}
        scopeKey="reconnected-operation"
        items={[thought]}
      />,
    );
    expect(screen.getByText("Inspect the existing route.")).toBeVisible();
  });

  it("preserves a stopped preview and collapses it only for a canonical planner response", () => {
    const view = render(<ActivityDisclosure {...props} items={[thought]} />);
    view.rerender(
      <ActivityDisclosure {...props} isLive={false} items={[thought]} />,
    );
    expect(screen.getByText("Inspect the existing route.")).toBeVisible();
    view.rerender(
      <ActivityDisclosure
        {...props}
        isLive={false}
        hasFinalResponse
        items={[thought]}
      />,
    );
    expect(
      screen.queryByRole("region", { name: "Provider activity preview" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show Activity" })).toBeVisible();
  });

  it("keeps the run preview and manually opened history after completion, then resets history on scope change", async () => {
    const view = render(
      <ActivityDisclosure
        {...props}
        completionBehavior="keep-open"
        items={[thought, tool]}
      />,
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Show Activity" }),
    );
    const history = screen.getByRole("region", {
      name: "Provider activity history",
    });
    view.rerender(
      <ActivityDisclosure
        {...props}
        completionBehavior="keep-open"
        isLive={false}
        hasFinalResponse
        items={[thought, tool]}
      />,
    );
    expect(screen.getByText("Inspect the existing route.")).toBeVisible();
    expect(
      screen.getByRole("region", { name: "Provider activity history" }),
    ).toBe(history);
    const preview = screen.getByRole("region", {
      name: "Provider activity preview",
    });
    expect(within(preview).queryByRole("status")).not.toBeInTheDocument();
    expect(
      within(preview)
        .getByText(/Last Activity:/)
        .closest("p"),
    ).toHaveTextContent("Searching the route handler");
    view.rerender(
      <ActivityDisclosure
        {...props}
        scopeKey="operation-b"
        completionBehavior="keep-open"
        isLive={false}
        hasFinalResponse
        items={[thought]}
      />,
    );
    expect(
      screen.queryByRole("region", { name: "Provider activity history" }),
    ).not.toBeInTheDocument();
  });

  it("shows only the newest checklist and never substitutes an older complete snapshot", async () => {
    const checklist = (
      id: string,
      text: string,
      sequence: number,
    ): ActivityItem => ({
      id,
      eventId: id,
      sequence,
      kind: "tool",
      toolName: "todowrite",
      label: "Update Tasks",
      inputContent: availableActivityContent(
        JSON.stringify({ todos: [{ content: text, status: "completed" }] }),
      ),
    });
    const older = checklist("older", "Older task", 1);
    const newer = checklist("newer", "Current task", 2);
    const view = render(
      <ActivityDisclosure {...props} items={[older, newer]} />,
    );
    expect(screen.getAllByText("Checklist")).toHaveLength(1);
    expect(screen.queryByText("Current task")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Checklist/ }));
    expect(screen.getByText("Current task")).toBeVisible();
    expect(screen.queryByText("Older task")).not.toBeInTheDocument();
    view.rerender(
      <ActivityDisclosure
        {...props}
        items={[
          older,
          {
            ...newer,
            inputContent: { ...newer.inputContent!, preview: '{"todos":[' },
          },
        ]}
      />,
    );
    expect(screen.queryByText("Checklist")).not.toBeInTheDocument();
    expect(screen.queryByText("Older task")).not.toBeInTheDocument();
  });

  it("bounds long reasoning in the primary flow and retains exact history through Read More", async () => {
    render(
      <ActivityDisclosure
        {...props}
        items={[
          {
            ...thought,
            reasoningContent: availableActivityContent(
              "Long thought ".repeat(2_000),
            ),
          },
        ]}
      />,
    );
    const preview = screen.getByRole("region", {
      name: "Provider activity preview",
    });
    expect(preview.textContent!.length).toBeLessThan(2_700);
    await userEvent.click(
      within(preview).getByRole("button", { name: "Read More" }),
    );
    expect(
      screen.getByRole("region", { name: "Provider activity history" }),
    ).toBeVisible();
  });
});
