import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { RunGraphResponse, RunNodeExecution } from "@/api/hooks";
import { DAGVisualization } from "./dag-visualization";

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
  it("starts at readable dimensions and fits the whole graph only on request", () => {
    render(<DAGVisualization graph={graph} nodes={[]} />);
    const svg = screen.getByRole("group", { name: "Run execution DAG" });
    expect(svg).toHaveAttribute("data-view", "readable");
    fireEvent.click(screen.getByRole("button", { name: "Fit graph" }));
    expect(svg).toHaveAttribute("data-view", "overview");
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
    fireEvent.keyDown(svg, { key: "+" });
    expect(svg.querySelector("g")).toHaveAttribute(
      "transform",
      expect.stringContaining("scale(1.3)"),
    );
    fireEvent.keyDown(svg, { key: "ArrowRight" });
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
