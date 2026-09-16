import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { RunExecution, RunNodeExecution } from "@/api/hooks";
import { RunActivityLanes } from "./run-activity-lanes";
import { liveRunActivityQueryKey, type LiveRunActivityBuffer } from "./run-live-activity";

function execution(overrides: Partial<RunExecution> & Pick<RunExecution, "node_id" | "attempt_id">): RunExecution {
  return {
    node_run_id: `${overrides.node_id}-run`,
    attempt: 1,
    agent_id: "agent",
    required_role: "clergy-delegate",
    execution_mode: "provider_backed",
    status: "running",
    ...overrides,
  } as RunExecution;
}

function node(execution: RunExecution): RunNodeExecution {
  return {
    node_id: execution.node_id,
    required_role: execution.required_role,
    execution_ownership: execution.execution_mode,
    status: "running",
    attempt_count: 1,
    current_execution: execution,
  };
}

function renderLanes(args: {
  executions: RunExecution[];
  buffer?: LiveRunActivityBuffer;
  selectedNodeId?: string | null;
  onSelectStep?: (nodeId: string) => void;
}) {
  const qc = new QueryClient();
  if (args.buffer) {
    qc.setQueryData(liveRunActivityQueryKey("run-1"), args.buffer);
  }
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
  return render(
    <RunActivityLanes
      runId="run-1"
      nodes={args.executions.map(node)}
      selectedNodeId={args.selectedNodeId ?? null}
      onSelectStep={args.onSelectStep ?? (() => {})}
    />,
    { wrapper },
  );
}

describe("RunActivityLanes", () => {
  it("renders one lane per active provider-backed node with its current action", () => {
    renderLanes({
      executions: [
        execution({ node_id: "advisor_consultation_noble", attempt_id: "att-noble", required_role: "noble-delegate" }),
        execution({ node_id: "advisor_consultation_clergy", attempt_id: "att-clergy", required_role: "clergy-delegate" }),
      ],
      buffer: {
        "att-clergy": [{ id: "c1", kind: "tool", label: "Reading guidelines.md" }],
        // att-noble has no activity yet -> falls back to "Working…"
      },
    });

    expect(screen.getByText("Reading guidelines.md")).toBeInTheDocument();
    expect(screen.getByText("Working…")).toBeInTheDocument();
    expect(screen.getByText("Live now · 2 running")).toBeInTheDocument();
    expect(screen.getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "Noble DelegateWorking…",
      "Clergy DelegateReading guidelines.md",
    ]);
  });

  it("renders for a single active provider-backed node (always-on card)", () => {
    renderLanes({
      executions: [
        execution({ node_id: "plan_drafting", attempt_id: "att-plan", required_role: "builder-berserker" }),
        execution({ node_id: "advisor_consultation_noble", attempt_id: "att-noble", status: "succeeded" }),
      ],
    });
    expect(screen.getByText("Live now · 1 running")).toBeInTheDocument();
    // Lanes name the agent (role), not the node.
    expect(screen.getByText("Builder Berserker")).toBeInTheDocument();
  });

  it("falls back to the node label instead of 'Unknown Role' before the role resolves", () => {
    renderLanes({
      executions: [
        execution({ node_id: "advisor_consultation_clergy", attempt_id: "att-clergy", required_role: "" }),
      ],
    });
    expect(screen.getByText("Advisor Consultation Clergy")).toBeInTheDocument();
    expect(screen.queryByText("Unknown Role")).not.toBeInTheDocument();
  });

  it("renders nothing when no node is active", () => {
    const { container } = renderLanes({
      executions: [
        execution({ node_id: "advisor_consultation_clergy", attempt_id: "att-clergy", status: "succeeded" }),
        execution({ node_id: "advisor_consultation_noble", attempt_id: "att-noble", status: "failed" }),
      ],
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("excludes runtime-owned nodes from the lanes", () => {
    renderLanes({
      executions: [
        execution({ node_id: "advisor_consultation_clergy", attempt_id: "att-clergy" }),
        execution({ node_id: "execution_kickoff", attempt_id: "att-kickoff", execution_mode: "runtime_owned" }),
      ],
    });
    expect(screen.getByText("Live now · 1 running")).toBeInTheDocument();
    expect(screen.getByText("Clergy Delegate")).toBeInTheDocument();
  });

  it("focuses a node when its lane is clicked and highlights the selected lane", async () => {
    const user = userEvent.setup();
    const onSelectStep = vi.fn();
    renderLanes({
      executions: [
        execution({ node_id: "advisor_consultation_clergy", attempt_id: "att-clergy", required_role: "clergy-delegate" }),
        execution({ node_id: "advisor_consultation_noble", attempt_id: "att-noble", required_role: "noble-delegate" }),
      ],
      selectedNodeId: "advisor_consultation_noble",
      onSelectStep,
    });

    const nobleLane = screen.getByText("Noble Delegate").closest("button");
    const clergyLane = screen.getByText("Clergy Delegate").closest("button");
    expect(nobleLane).toHaveAttribute("aria-pressed", "true");
    expect(clergyLane).toHaveAttribute("aria-pressed", "false");

    await user.click(clergyLane!);
    expect(onSelectStep).toHaveBeenCalledWith("advisor_consultation_clergy");
  });
});
