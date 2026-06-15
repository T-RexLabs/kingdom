import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import type { RunExecution } from "@/api/hooks";
import { RunLiveStatus } from "./run-live-status";

function execution(args: {
  node: string;
  attemptId: string;
  requiredRole: string;
  activityAt?: string;
  status?: RunExecution["status"];
  executionMode?: RunExecution["execution_mode"];
}): RunExecution {
  return {
    node_id: args.node,
    attempt_id: args.attemptId,
    node_run_id: `${args.node}-run`,
    attempt: 1,
    agent_id: "agent",
    required_role: args.requiredRole,
    execution_mode: args.executionMode ?? "provider_backed",
    status: args.status ?? "running",
    activity_at: args.activityAt ?? "2026-06-15T10:00:00Z",
  } as RunExecution;
}

function renderStatus(executions: RunExecution[]) {
  const qc = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
  return render(<RunLiveStatus runId="run-1" executions={executions} />, { wrapper });
}

describe("RunLiveStatus", () => {
  it("names the two most-recently-active agents by role and rolls the rest into +N more", () => {
    renderStatus([
      execution({ node: "advisor_consultation_burgher", attemptId: "att-burgher", requiredRole: "burgher-delegate", activityAt: "2026-06-15T10:00:01Z" }),
      execution({ node: "advisor_consultation_clergy", attemptId: "att-clergy", requiredRole: "clergy-delegate", activityAt: "2026-06-15T10:00:04Z" }),
      execution({ node: "advisor_consultation_noble", attemptId: "att-noble", requiredRole: "noble-delegate", activityAt: "2026-06-15T10:00:03Z" }),
      execution({ node: "advisor_consultation_peasant", attemptId: "att-peasant", requiredRole: "peasant-delegate", activityAt: "2026-06-15T10:00:02Z" }),
    ]);

    // Two freshest by activity_at are Clergy (…04) and Noble (…03); names are agent roles.
    expect(screen.getByText("Clergy Delegate")).toBeInTheDocument();
    expect(screen.getByText("Noble Delegate")).toBeInTheDocument();
    expect(screen.queryByText("Burgher Delegate")).not.toBeInTheDocument();
    expect(screen.queryByText("Peasant Delegate")).not.toBeInTheDocument();
    expect(screen.getByText("+2 more")).toBeInTheDocument();
  });

  it("shows no overflow badge at or below the cap", () => {
    renderStatus([
      execution({ node: "advisor_consultation_burgher", attemptId: "att-burgher", requiredRole: "burgher-delegate" }),
      execution({ node: "advisor_consultation_clergy", attemptId: "att-clergy", requiredRole: "clergy-delegate" }),
    ]);
    expect(screen.queryByText(/more$/)).not.toBeInTheDocument();
  });

  it("falls back to the node label instead of 'Unknown Role' before the role resolves", () => {
    renderStatus([
      execution({ node: "advisor_consultation_clergy", attemptId: "att-clergy", requiredRole: "" }),
    ]);
    expect(screen.getByText("Advisor Consultation Clergy")).toBeInTheDocument();
    expect(screen.queryByText("Unknown Role")).not.toBeInTheDocument();
  });

  it("renders nothing when no node is active", () => {
    const { container } = renderStatus([
      execution({ node: "plan_drafting", attemptId: "att-plan", requiredRole: "builder-berserker", status: "succeeded" }),
    ]);
    expect(container).toBeEmptyDOMElement();
  });

  it("excludes runtime-owned executions from the headline", () => {
    renderStatus([
      execution({ node: "execution_kickoff", attemptId: "att-kickoff", requiredRole: "king", executionMode: "runtime_owned" }),
      execution({ node: "advisor_consultation_clergy", attemptId: "att-clergy", requiredRole: "clergy-delegate" }),
    ]);

    expect(screen.getByText("Clergy Delegate")).toBeInTheDocument();
    expect(screen.queryByTestId("run-live-status")).toHaveTextContent("Clergy Delegate");
    expect(screen.queryByText("King")).not.toBeInTheDocument();
  });
});
