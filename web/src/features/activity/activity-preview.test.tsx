import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActivityDisclosure } from "./activity-disclosure";
import { availableActivityContent } from "./activity-test-helpers";
import type { ActivityItem } from "./activity-view";
import { MarkdownBlock } from "@/components/ui/markdown-block";

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

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("compact activity preview", () => {
  it("omits completed tool labels while retaining failure status", () => {
    const items: ActivityItem[] = [
      {
        id: "completed",
        eventId: "completed-event",
        sequence: 1,
        kind: "tool",
        toolName: "read",
        toolStatus: "completed",
        label: "Read README.md",
      },
      {
        id: "failed",
        eventId: "failed-event",
        sequence: 2,
        kind: "tool",
        toolName: "bash",
        toolStatus: "failed",
        label: "Run formatter",
      },
    ];
    render(<ActivityDisclosure {...props} items={items} isLive={false} />);

    const list = screen.getByRole("list", { name: "Recent tool calls" });
    expect(list).toHaveTextContent("Read");
    expect(list).toHaveTextContent("Run Command");
    expect(within(list).queryByText("Done")).not.toBeInTheDocument();
    expect(within(list).getByText("Failed")).toBeVisible();
  });

  it("keeps recent real tool calls visible when routine phases follow, including after settlement", () => {
    const tools: ActivityItem[] = ["glob", "read", "bash", "grep"].map((name, index) => ({
      id: name, eventId: name, sequence: index + 1, kind: "tool", toolName: name,
      toolStatus: name === "glob" ? "failed" : "completed", label: `Observed ${name}`,
    }));
    const view = render(<ActivityDisclosure {...props} items={[tools[0], { id: "phase", eventId: "phase", sequence: 5, kind: "progress", label: "Provider Step Completed" }]} />);
    expect(screen.getByRole("list", { name: "Recent tool calls" })).toHaveTextContent("Observed glob");
    expect(screen.getByText("Failed")).toBeVisible();
    expect(screen.queryByText("Provider Step Completed")).not.toBeInTheDocument();
    view.rerender(<ActivityDisclosure {...props} completionBehavior="keep-open" isLive={false} hasFinalResponse items={[...tools, { id: "phase", eventId: "phase", sequence: 5, kind: "progress", label: "Provider Step Completed" }]} />);
    const list = screen.getByRole("list", { name: "Recent tool calls" });
    expect(within(list).getAllByRole("listitem")).toHaveLength(3);
    expect(list).not.toHaveTextContent("Observed glob");
    expect(list).toHaveTextContent("Observed grep");
    expect(screen.queryByText("Provider Step Completed")).not.toBeInTheDocument();
    expect(screen.getByRole("button", {name: "Show Activity"})).toBeVisible();
  });

  it("keeps clipped reasoning readable without hidden keyboard links, while exact Markdown stays interactive", async () => {
    const view = render(
      <ActivityDisclosure
        {...props}
        items={[
          {
            ...thought,
            reasoningContent: availableActivityContent(
              "[Earlier source](https://example.com)\n\nNewest thought.",
            ),
          },
        ]}
      />,
    );
    const viewport = screen.getByRole("group", { name: "Reasoning preview" });
    expect(within(viewport).getByText("Earlier source")).toBeInTheDocument();
    expect(within(viewport).queryByRole("link")).not.toBeInTheDocument();
    expect(viewport).not.toHaveAttribute("inert");
    await userEvent.tab();
    expect(screen.getByRole("button", { name: "Show Activity" })).toHaveFocus();
    view.unmount();
    render(<MarkdownBlock content="[Exact source](https://example.com)" />);
    expect(screen.getByRole("link", { name: "Exact source" })).toHaveAttribute(
      "href",
      "https://example.com",
    );
  });

  it("keeps the newest reasoning tail and renders its Markdown instead of stale leading text", () => {
    render(
      <ActivityDisclosure
        {...props}
        items={[
          {
            ...thought,
            reasoningContent: availableActivityContent(
              `Old leading thought ${"earlier reasoning ".repeat(300)}\n\n## Latest Direction\n\n**Inspect the newest route.**`,
            ),
          },
        ]}
      />,
    );
    expect(
      screen.getByRole("heading", { name: "Latest Direction" }),
    ).toBeVisible();
    expect(screen.getByText("Inspect the newest route.").tagName).toBe(
      "STRONG",
    );
    expect(screen.queryByText(/Old leading thought/)).not.toBeInTheDocument();
    expect(screen.queryByText(/\*\*Inspect/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Read More" })).toBeVisible();
  });

  it("bottom-aligns a bounded reasoning area and fades only overflowing text, including after resize", () => {
    let contentHeight = 240;
    let resize: ResizeObserverCallback | undefined;
    vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockImplementation(
      function (this: HTMLElement) {
        return this.hasAttribute("data-reasoning-content") ? contentHeight : 0;
      },
    );
    vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockImplementation(
      function (this: HTMLElement) {
        return this.getAttribute("aria-label") === "Reasoning preview"
          ? 120
          : 0;
      },
    );
    const disconnect = vi.fn();
    vi.stubGlobal(
      "ResizeObserver",
      class {
        constructor(callback: ResizeObserverCallback) {
          resize = callback;
        }
        observe = vi.fn();
        disconnect = disconnect;
      },
    );
    const view = render(<ActivityDisclosure {...props} items={[thought]} />);
    const viewport = screen.getByRole("group", { name: "Reasoning preview" });
    expect(viewport).toHaveClass(
      "max-h-[120px]",
      "overflow-hidden",
      "justify-end",
    );
    expect(viewport.style.maskImage).toContain("linear-gradient");
    expect(viewport).not.toHaveAttribute("tabindex");
    contentHeight = 60;
    act(() => resize?.([], {} as ResizeObserver));
    expect(viewport.style.maskImage).toBe("");
    contentHeight = 180;
    act(() => resize?.([], {} as ResizeObserver));
    expect(viewport.style.maskImage).toContain("linear-gradient");
    view.unmount();
    expect(disconnect).toHaveBeenCalledOnce();
  });

  it("renders updated reasoning immediately with reduced motion without adding a scrolling interaction", () => {
    vi.stubGlobal(
      "matchMedia",
      vi.fn(() => ({
        matches: true,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      })),
    );
    const requestFrame = vi.fn();
    vi.stubGlobal("requestAnimationFrame", requestFrame);
    const view = render(<ActivityDisclosure {...props} items={[]} />);
    view.rerender(
      <ActivityDisclosure
        {...props}
        items={[
          {
            ...thought,
            reasoningContent: availableActivityContent(
              "**Updated reasoning.**",
            ),
          },
        ]}
      />,
    );
    expect(screen.getByText("Updated reasoning.").tagName).toBe("STRONG");
    expect(
      screen.getByRole("group", { name: "Reasoning preview" }),
    ).toHaveClass("overflow-hidden");
    expect(requestFrame).not.toHaveBeenCalled();
  });

  it("immediately follows equal-length rolling tails as long reasoning grows", () => {
    const frames = vi.fn();
    vi.stubGlobal("requestAnimationFrame", frames);
    const first = `${"Earlier reasoning. ".repeat(200)}First latest sentence.`;
    const item = (text: string): ActivityItem => ({
      ...thought,
      reasoningContent: availableActivityContent(text),
    });
    const view = render(
      <ActivityDisclosure {...props} items={[item(first)]} />,
    );
    const viewport = screen.getByRole("group", { name: "Reasoning preview" });
    const firstLength = viewport.textContent!.length;
    expect(viewport.textContent).toMatch(/First latest sentence\.$/);
    view.rerender(
      <ActivityDisclosure
        {...props}
        items={[item(`${first} Second latest sentence.`)]}
      />,
    );
    expect(viewport.textContent!.length).toBe(firstLength);
    expect(viewport.textContent).toMatch(/Second latest sentence\.$/);
    expect(frames).not.toHaveBeenCalled();
  });

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
    expect(within(preview).getByRole("list", { name: "Recent tool calls" })).toHaveTextContent("Searching the route handler");
    expect(within(preview).getByText("Last Reported: Running")).toBeVisible();
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
