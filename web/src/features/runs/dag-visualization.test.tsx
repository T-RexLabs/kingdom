import { beforeAll, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { DAGVisualization } from "./dag-visualization";
import type { RunGraphNode, RunNodeExecution } from "@/api/hooks";

type NodeState = RunGraphNode["state"];

function providerNode(node_id: string, required_role: string, state: NodeState) {
  return { node_id, required_role, execution_ownership: "provider_backed" as const, state };
}

function runtimeNode(node_id: string, required_role: string, state: NodeState) {
  return { node_id, required_role, execution_ownership: "runtime_owned" as const, state };
}

function logicalNode(node_id: string, required_role: string, status: RunNodeExecution["status"]): RunNodeExecution {
  return {
    node_id,
    required_role,
    execution_ownership: "provider_backed",
    status,
    attempt_count: 1,
  };
}

beforeAll(() => {
  Object.defineProperty(SVGElement.prototype, "getBBox", {
    configurable: true,
    value: () => ({ x: 0, y: 0, width: 120, height: 40 }),
  });
  Object.defineProperty(SVGElement.prototype, "getComputedTextLength", {
    configurable: true,
    value: () => 80,
  });
  Object.defineProperty(SVGElement.prototype, "getScreenCTM", {
    configurable: true,
    value: () => ({ inverse: () => ({}) }),
  });
  Object.defineProperty(SVGSVGElement.prototype, "createSVGPoint", {
    configurable: true,
    value: () => ({ x: 0, y: 0, matrixTransform: vi.fn(() => ({ x: 0, y: 0 })) }),
  });
});

describe("dag visualization", () => {
  it("uses the shared logical-node cards as the mobile fallback and opens the selected node in Activity", () => {
    const onViewActivity = vi.fn();
    render(
      <DAGVisualization
        graph={{
          run_id: "run-1",
          nodes: [providerNode("execution_kickoff", "council-member", "ready")],
          edges: [],
        }}
        nodes={[logicalNode("execution_kickoff", "council-member", "ready")]}
        onViewActivity={onViewActivity}
      />,
    );

    const action = screen.getByRole("button", { name: "View Execution Kickoff in Activity" });

    expect(action).toHaveClass("min-h-11");
    expect(screen.getByTestId("dag-visualization-root").querySelector(".lg\\:hidden")).toBeInTheDocument();
    expect(screen.getByTestId("dag-visualization-root").querySelector(".hidden.lg\\:block")).toBeInTheDocument();

    fireEvent.click(action);

    expect(onViewActivity).toHaveBeenCalledWith("execution_kickoff");
  });

	it("reports unavailable execution steps in the mobile fallback without claiming the graph is empty", () => {
		const onRetryExecutions = vi.fn();
		render(
			<DAGVisualization
				graph={{
					run_id: "run-1",
					nodes: [providerNode("execution_kickoff", "council-member", "ready")],
					edges: [],
				}}
				nodes={[]}
				executionsErrorMessage="The execution steps could not be loaded right now."
				onRetryExecutions={onRetryExecutions}
			/>,
		);

		expect(screen.getByRole("alert")).toHaveTextContent("Execution Step List Unavailable");
		expect(screen.getByRole("alert")).toHaveTextContent("The execution steps could not be loaded right now.");
		expect(screen.queryByText("No execution steps have been recorded yet.")).not.toBeInTheDocument();
		expect(screen.getByRole("img", { name: "Run execution DAG" })).toBeInTheDocument();

		fireEvent.click(screen.getByRole("button", { name: "Retry Step List" }));
		expect(onRetryExecutions).toHaveBeenCalledTimes(1);
	});

  it("labels the graph and supports keyboard node selection", () => {
    render(
      <DAGVisualization
        graph={{
          run_id: "run-1",
          nodes: [
            providerNode("kickoff", "chancellor", "running"),
            providerNode("build", "builder-berserker", "pending"),
          ],
          edges: [{ from: "kickoff", to: "build", on: "success" }],
        }}
      />,
    );

    const dag = screen.getByRole("img", { name: /run execution dag/i });

    expect(dag).toBeInTheDocument();
    expect(dag.querySelector("title")).toBeNull();

    const kickoffNode = screen.getByRole("button", { name: /Kickoff.*Execution mode Agent-executed.*Role Chancellor/i });
    fireEvent.keyDown(kickoffNode, { key: "Enter" });

    expect(screen.getByText(/Selected:/i)).toHaveTextContent("kickoff");
  });

  it("shows crown-handled nodes with role-forward labels and split tooltip semantics", () => {
    const { container } = render(
      <DAGVisualization
        graph={{
          run_id: "run-1",
          nodes: [runtimeNode("execution_kickoff", "council-member", "pending")],
          edges: [],
        }}
      />,
    );

    const kickoffNode = screen.getByRole("button", { name: /Execution Kickoff.*Execution mode Crown-handled.*Role Council Member/i });

    expect(container.querySelector('[data-node="execution_kickoff"] text[data-part="role-label"]')).toHaveTextContent("Council Member");
    expect(kickoffNode.getAttribute("aria-label")).not.toContain("Authority");

    fireEvent.mouseEnter(kickoffNode);

    expect(screen.getByText("Execution mode: Crown-handled")).toBeInTheDocument();
    expect(screen.getByText("Role: Council Member")).toBeInTheDocument();
    expect(screen.queryByText(/Runtime-owned/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/Authority:/i)).not.toBeInTheDocument();
  });

  it("uses the full panel height and anchors the readable legend to the panel corner", () => {
    render(
      <DAGVisualization
        graph={{
          run_id: "run-1",
          nodes: [
            providerNode("kickoff", "chancellor", "running"),
            providerNode("build", "builder-berserker", "pending"),
          ],
          edges: [{ from: "kickoff", to: "build", on: "success" }],
        }}
      />,
    );

    expect(screen.getByTestId("dag-visualization-root")).toHaveClass("h-full");
    expect(screen.getByTestId("dag-visualization-root")).toHaveClass("min-h-0");

    const legend = screen.getByTestId("dag-legend");

    expect(legend).toHaveClass("absolute");
		expect(legend).toHaveClass("bottom-3");
		expect(legend).toHaveClass("right-3");
		expect(legend).toHaveClass("sm:bottom-4");
		expect(legend).toHaveClass("sm:right-4");
		expect(legend).toHaveTextContent("Active");
		expect(legend).toHaveTextContent("Pending");
		expect(legend).not.toHaveTextContent("Ready");
		expect(legend).not.toHaveTextContent("Blocked");
		expect(legend).not.toHaveTextContent("Skipped");
	});

	it("shows blocked and ready when those states are present in the graph", () => {
		render(
			<DAGVisualization
				graph={{
					run_id: "run-1",
					nodes: [
						providerNode("launch", "chancellor", "starting"),
						providerNode("recover", "builder-berserker", "blocked"),
						runtimeNode("handoff", "council-member", "ready"),
					],
					edges: [],
				}}
			/>,
		);

		const legend = screen.getByTestId("dag-legend");

		expect(legend).toHaveTextContent("Launching");
		expect(legend).toHaveTextContent("Blocked");
		expect(legend).toHaveTextContent("Ready");
	});

  it("preserves the selected node across graph refreshes when the node still exists", () => {
    const initialGraph = {
      run_id: "run-1",
      nodes: [
        providerNode("kickoff", "chancellor", "running"),
        providerNode("build", "builder-berserker", "pending"),
      ],
      edges: [{ from: "kickoff", to: "build", on: "success" }],
    };

    const { rerender } = render(<DAGVisualization graph={initialGraph} />);

    fireEvent.click(screen.getByRole("button", { name: /Kickoff.*Execution mode Agent-executed.*Role Chancellor/i }));
    expect(screen.getByText(/Selected:/i)).toHaveTextContent("kickoff");

    rerender(
      <DAGVisualization
        graph={{
          ...initialGraph,
          nodes: [
            providerNode("kickoff", "chancellor", "succeeded"),
            providerNode("build", "builder-berserker", "running"),
          ],
        }}
      />,
    );

    expect(screen.getByText(/Selected:/i)).toHaveTextContent("kickoff");
  });

  it("preserves viewport transform across graph refreshes", () => {
    const initialGraph = {
      run_id: "run-1",
      nodes: [
        providerNode("kickoff", "chancellor", "running"),
        providerNode("build", "builder-berserker", "pending"),
      ],
      edges: [{ from: "kickoff", to: "build", on: "success" }],
    };

    const { container, rerender } = render(<DAGVisualization graph={initialGraph} />);
    const svg = screen.getByRole("img", { name: /run execution dag/i });

    fireEvent.wheel(svg, { deltaY: -120, clientX: 300, clientY: 200 });

    const transformBefore = container
      .querySelector('svg[aria-label="Run execution DAG"] > g')
      ?.getAttribute("transform");

    expect(transformBefore).toBeTruthy();

    rerender(
      <DAGVisualization
        graph={{
          ...initialGraph,
          nodes: [
            providerNode("kickoff", "chancellor", "succeeded"),
            providerNode("build", "builder-berserker", "running"),
          ],
        }}
      />,
    );

    const transformAfter = container
      .querySelector('svg[aria-label="Run execution DAG"] > g')
      ?.getAttribute("transform");

    expect(transformAfter).toBe(transformBefore);
  });

  it("reuses existing node and edge elements for state-only graph refreshes", () => {
    const initialGraph = {
      run_id: "run-1",
      nodes: [
        providerNode("kickoff", "chancellor", "running"),
        providerNode("build", "builder-berserker", "pending"),
      ],
      edges: [{ from: "kickoff", to: "build", on: "success" }],
    };

    const { container, rerender } = render(<DAGVisualization graph={initialGraph} />);

    const kickoffNodeBefore = container.querySelector('[data-node="kickoff"]');
    const kickoffChildBefore = kickoffNodeBefore?.firstElementChild;
    const edgeBefore = container.querySelector('[data-edge="kickoff->build"]');

    expect(kickoffNodeBefore).toBeTruthy();
    expect(kickoffChildBefore).toBeTruthy();
    expect(edgeBefore).toBeTruthy();

    rerender(
      <DAGVisualization
        graph={{
          ...initialGraph,
          nodes: [
            providerNode("kickoff", "chancellor", "succeeded"),
            providerNode("build", "builder-berserker", "running"),
          ],
        }}
      />,
    );

    const kickoffNodeAfter = container.querySelector('[data-node="kickoff"]');
    const kickoffChildAfter = kickoffNodeAfter?.firstElementChild;
    const edgeAfter = container.querySelector('[data-edge="kickoff->build"]');

    expect(kickoffNodeAfter).toBe(kickoffNodeBefore);
    expect(kickoffChildAfter).toBe(kickoffChildBefore);
    expect(edgeAfter).toBe(edgeBefore);
    expect(kickoffNodeAfter).toHaveAttribute("aria-label", expect.stringContaining("Status Succeeded"));
  });

  it("does not rebuild node contents when selection changes", () => {
    const graph = {
      run_id: "run-1",
      nodes: [
        providerNode("kickoff", "chancellor", "running"),
        providerNode("build", "builder-berserker", "pending"),
      ],
      edges: [{ from: "kickoff", to: "build", on: "success" }],
    };

    const { container } = render(<DAGVisualization graph={graph} />);

    const kickoffNode = container.querySelector('[data-node="kickoff"]');
    const kickoffChildBefore = kickoffNode?.firstElementChild;

    expect(kickoffNode).toBeTruthy();
    expect(kickoffChildBefore).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /Kickoff.*Execution mode Agent-executed.*Role Chancellor/i }));

    const kickoffChildAfter = container.querySelector('[data-node="kickoff"]')?.firstElementChild;
    expect(kickoffChildAfter).toBe(kickoffChildBefore);
    expect(screen.getByText(/Selected:/i)).toHaveTextContent("kickoff");
  });

  it("updates the rendered DAG when topology changes", () => {
    const initialGraph = {
      run_id: "run-1",
      nodes: [
        providerNode("kickoff", "chancellor", "running"),
        providerNode("build", "builder-berserker", "pending"),
      ],
      edges: [{ from: "kickoff", to: "build", on: "success" }],
    };

    const { container, rerender } = render(<DAGVisualization graph={initialGraph} />);

    rerender(
      <DAGVisualization
        graph={{
          ...initialGraph,
          nodes: [
            providerNode("kickoff", "chancellor", "succeeded"),
            providerNode("review", "reviewer-shieldmaiden", "running"),
            providerNode("build", "builder-berserker", "pending"),
          ],
          edges: [
            { from: "kickoff", to: "review", on: "success" },
            { from: "review", to: "build", on: "success" },
          ],
        }}
      />,
    );

    expect(container.querySelectorAll("[data-node]")).toHaveLength(3);
    expect(container.querySelector('[data-node="review"]')).toBeTruthy();
    expect(container.querySelectorAll("[data-edge]")).toHaveLength(2);
    expect(container.querySelector('[data-edge="kickoff->build"]')).toBeNull();
    expect(screen.getByRole("button", { name: /Review.*Execution mode Agent-executed.*Role Reviewer Shieldmaiden/i })).toBeInTheDocument();
  });
  it("draws a rework request as its own state rather than a failure", () => {
    const { container } = render(
      <DAGVisualization
        graph={{
          run_id: "run-rework",
          nodes: [
            providerNode("execution_review", "reviewer-shieldmaiden", "rework"),
            providerNode("build", "builder-berserker", "failed"),
          ],
          edges: [{ from: "execution_review", to: "build", on: "rework" }],
        }}
      />,
    );

    // The pulsing red border is the graph's "this step broke" signal. A
    // reviewer that asked for another pass did not break, and saying so is the
    // whole point of the state.
    const reworkNode = container.querySelector('[data-node="execution_review"] rect[data-part="bg"]');
    const failedNode = container.querySelector('[data-node="build"] rect[data-part="bg"]');
    expect(reworkNode).toBeTruthy();
    expect(reworkNode).not.toHaveClass("dag-node-failed-border");
    expect(failedNode).toHaveClass("dag-node-failed-border");
    expect(reworkNode?.getAttribute("stroke")).not.toBe(failedNode?.getAttribute("stroke"));

    expect(screen.getByRole("button", { name: /Execution Review\. Status Rework\./i })).toBeInTheDocument();
    expect(screen.getByTestId("dag-legend")).toHaveTextContent("Rework");
  });
});
