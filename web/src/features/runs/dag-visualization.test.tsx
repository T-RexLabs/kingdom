import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { RunGraphResponse, RunNodeExecution } from "@/api/hooks";
import { DAGVisualization } from "./dag-visualization";
import { layoutRunGraph } from "./run-graph-model";

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return {
    ...actual,
    useTheme: () => ({ theme: actual.builtInThemes[3] }),
    useRoleIdentity: (role: string) =>
      actual.roleIdentity(actual.builtInThemes[3], role),
    ThemeIcon: () => <span aria-hidden="true" />,
  };
});

const graph: RunGraphResponse = {
  run_id: "run",
  nodes: [
    {
      node_id: "work_intake",
      required_role: "governance",
      execution_ownership: "runtime_owned",
      state: "succeeded",
    },
    {
      node_id: "implementation",
      required_role: "builder",
      execution_ownership: "provider_backed",
      state: "running",
    },
  ],
  edges: [{ from: "work_intake", to: "implementation", on: "success" }],
};

const wideGraph: RunGraphResponse = {
  ...graph,
  nodes: [
    graph.nodes[0],
    ...Array.from({ length: 5 }, (_, index) => ({
      ...graph.nodes[1],
      node_id: `parallel_${index}`,
    })),
  ],
  edges: Array.from({ length: 5 }, (_, index) => ({
    from: "work_intake",
    to: `parallel_${index}`,
  })),
};

function mockViewportResize(initialWidth: number, initialHeight: number) {
  const resizeObservers: Array<(width: number, height: number) => void> = [];

  class MockResizeObserver {
    private readonly callback: ResizeObserverCallback;
    private target: Element | null = null;

    constructor(callback: ResizeObserverCallback) {
      this.callback = callback;
      resizeObservers.push((width, height) => this.resize(width, height));
    }

    observe(target: Element) {
      this.target = target;
      this.resize(initialWidth, initialHeight);
    }

    unobserve() {}

    disconnect() {
      this.target = null;
    }

    private resize(width: number, height: number) {
      if (!this.target) return;
      this.callback(
        [
          {
            target: this.target,
            contentRect: { width, height },
          } as unknown as ResizeObserverEntry,
        ],
        this as unknown as ResizeObserver,
      );
    }
  }

  vi.stubGlobal("ResizeObserver", MockResizeObserver);
  return {
    resize(width: number, height: number) {
      for (const resize of resizeObservers) resize(width, height);
    },
  };
}

function readGraphTransform(svg: Element) {
  const transform = svg.querySelector("g")?.getAttribute("transform") ?? "";
  const match = transform.match(/^translate\(([^,]+),([^)]+)\) scale\(([^)]+)\)$/);
  if (!match) throw new Error(`Unexpected graph transform: ${transform}`);
  return { x: Number(match[1]), y: Number(match[2]), scale: Number(match[3]) };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const running: RunNodeExecution = {
  node_id: "implementation",
  required_role: "builder",
  execution_ownership: "provider_backed",
  status: "running",
  attempt_count: 1,
  current_execution: {
    attempt_id: "a",
    node_id: "implementation",
    node_run_id: "nr",
    attempt: 1,
    agent_id: "builder",
    required_role: "builder",
    execution_mode: "provider_backed",
    status: "running",
    queued_at: "2026-09-29T10:00:00Z",
    activity_at: "2026-09-29T10:00:01Z",
    started_at: "2026-09-29T10:00:01Z",
    updated_at: "2026-09-29T10:00:01Z",
    stop_retry_available: false,
  },
};

describe("workflow DAG", () => {
  it("starts fitted and keeps the fit control available", () => {
    render(<DAGVisualization graph={graph} nodes={[]} />);
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    expect(svg).toHaveAttribute("data-view", "overview");
    fireEvent.click(screen.getByRole("button", { name: "Fit graph" }));
    expect(svg).toHaveAttribute("data-view", "overview");
  });
  it("fits wide layers to the measured canvas width and recenters after resize", () => {
    const viewport = mockViewportResize(700, 610);
    render(<DAGVisualization graph={wideGraph} nodes={[]} />);
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    expect(svg).toHaveAttribute("viewBox", expect.stringMatching(/^0 0 700 /));

    const lastNode = layoutRunGraph(wideGraph).nodes.reduce((rightmost, node) =>
      Math.max(rightmost, node.x + node.width),
    0);
    let frame = readGraphTransform(svg);
    expect(frame.scale).toBeLessThan(1);
    expect(frame.x + lastNode * frame.scale).toBeLessThanOrEqual(700);

    act(() => viewport.resize(1100, 610));
    expect(svg).toHaveAttribute("viewBox", expect.stringMatching(/^0 0 1100 /));
    frame = readGraphTransform(svg);
    expect(frame.scale).toBe(1);
    expect(frame.x).toBeGreaterThan(0);
    expect(frame.x + lastNode * frame.scale).toBeLessThanOrEqual(1100);

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    fireEvent.click(screen.getByRole("button", { name: "Fit graph" }));
    expect(readGraphTransform(svg)).toEqual(frame);
  });
  it("preserves a manually adjusted view when node status updates", () => {
    mockViewportResize(1100, 610);
    const { rerender } = render(
      <DAGVisualization graph={wideGraph} nodes={[]} />,
    );
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    const adjustedTransform = svg.querySelector("g")?.getAttribute("transform");

    const updatedGraph: RunGraphResponse = {
      ...wideGraph,
      nodes: wideGraph.nodes.map((node, index) =>
        index === 1 ? { ...node, state: "succeeded" } : node,
      ),
    };
    rerender(<DAGVisualization graph={updatedGraph} nodes={[]} />);

    expect(svg.querySelector("g")).toHaveAttribute(
      "transform",
      adjustedTransform,
    );
  });
  it("leaves normal wheel scrolling alone and zooms only with Ctrl or Meta", () => {
    render(<DAGVisualization graph={graph} nodes={[]} />);
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    const initial = svg.querySelector("g")?.getAttribute("transform");
    fireEvent.wheel(svg, { deltaY: 100, clientX: 50, clientY: 50 });
    expect(svg.querySelector("g")).toHaveAttribute("transform", initial!);
    fireEvent.wheel(svg, {
      deltaY: -100,
      ctrlKey: true,
      clientX: 50,
      clientY: 50,
    });
    expect(svg.querySelector("g")?.getAttribute("transform")).not.toBe(initial);
    fireEvent.click(screen.getByRole("button", { name: "Fit graph" }));
    fireEvent.wheel(svg, {
      deltaY: -100,
      metaKey: true,
      clientX: 50,
      clientY: 50,
    });
    expect(svg.querySelector("g")?.getAttribute("transform")).not.toBe(initial);
  });
  it("preserves stored edges, exposes runtime ownership and selects a node without opening a permanent inspector", () => {
    const onViewActivity = vi.fn();
    render(
      <DAGVisualization
        graph={graph}
        nodes={[running]}
        onViewActivity={onViewActivity}
      />,
    );
    expect(
      screen.getByRole("button", { name: /Work Intake.*Runtime step/ }),
    ).toBeInTheDocument();
    fireEvent.click(
      screen.getByRole("button", { name: /Implementation.*View activity/ }),
    );
    expect(onViewActivity).toHaveBeenCalledWith("implementation");
    expect(document.querySelector("path title")?.textContent).toMatch(
      /Work.*Intake → Implementation · success/,
    );
  });
  it("glows only observed running provider nodes and their incoming edges while connected", () => {
    const { container, rerender } = render(
      <DAGVisualization graph={graph} nodes={[running]} connected />,
    );
    expect(container.querySelectorAll('[data-active="true"]')).toHaveLength(2);
    rerender(
      <DAGVisualization graph={graph} nodes={[running]} connected={false} />,
    );
    expect(container.querySelectorAll('[data-active="true"]')).toHaveLength(0);
  });
  it("supports keyboard zoom, pan, reset and visible zoom controls", () => {
    render(<DAGVisualization graph={graph} nodes={[]} />);
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    const navigation = screen.getByLabelText("Workflow graph navigation");
    expect(svg).not.toHaveAttribute("tabindex");
    fireEvent.keyDown(navigation, { key: "+" });
    expect(svg.querySelector("g")).toHaveAttribute(
      "transform",
      expect.stringContaining("scale(1.3)"),
    );
    fireEvent.keyDown(navigation, { key: "ArrowRight" });
    fireEvent.click(screen.getByRole("button", { name: "Fit graph" }));
    expect(svg.querySelector("g")).toHaveAttribute(
      "transform",
      "translate(0,0) scale(1)",
    );
  });
  it("retains the graph and offers retry when execution status fails", () => {
    const retry = vi.fn();
    render(
      <DAGVisualization
        graph={graph}
        nodes={[]}
        executionsErrorMessage="Read failed"
        onRetryExecutions={retry}
      />,
    );
    expect(
      screen.getByRole("group", { name: "Run execution DAG" }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Retry Step List" }));
    expect(retry).toHaveBeenCalledOnce();
  });
});
