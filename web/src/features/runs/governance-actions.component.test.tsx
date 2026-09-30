import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { GovernanceActions } from "./governance-actions";
import type { RunGraphNode, RunSummary } from "@/api/hooks";

const finalApprovalState = {
  mutateAsync: vi.fn().mockResolvedValue({ status: "succeeded" }),
  isPending: false,
  isSuccess: false,
  error: null,
  data: null as null | { status: string },
};
const retryState = { mutateAsync: vi.fn(), isPending: false, error: null as Error | null };
const rerunState = { mutateAsync: vi.fn(), isPending: false, error: null as Error | null };

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: () => "idem-final-approval-test",
  useCreateFinalApproval: () => finalApprovalState,
  useRetryNode: () => retryState,
  useRerunFromNode: () => rerunState,
}));

describe("governance actions visibility", () => {
  const failedNode: RunGraphNode = {
    node_id: "execution_review",
    state: "failed",
    required_role: "reviewer",
    execution_ownership: "provider_backed",
    failure_reason: "Reviewer rejected missing evidence.",
  };

  const runningRun: RunSummary = {
    run_id: "run-1",
    work_order_id: "work order-1",
    work_order_title: "Title",
    workflow_id: "work-order-v1",
    autonomous_approvals: false,
    status: "running",
    display_state: "running",
    plan_state: "approved",
    created_at: "2026-03-06T10:00:00.000Z",
    updated_at: "2026-03-06T10:00:00.000Z",
    last_activity_at: "2026-03-06T10:00:00.000Z",
    start_state: "not_started",
  };

  const failedRun: RunSummary = {
    ...runningRun,
    status: "failed",
    display_state: "failed",
  };

  const cancelledRun: RunSummary = {
    ...runningRun,
    status: "cancelled",
    display_state: "cancelled",
  };

  const awaitingFinalApprovalRun: RunSummary = {
    ...runningRun,
    status: "awaiting_human_confirmation",
    display_state: "awaiting_final_decision",
  };

  beforeEach(() => {
    finalApprovalState.mutateAsync.mockClear();
    retryState.mutateAsync.mockReset();
    retryState.error = null;
    rerunState.mutateAsync.mockReset();
    rerunState.error = null;
  });

  it("hides retry and rerun controls when run is not failed", () => {
    render(
      <GovernanceActions
        run={runningRun}
        failedNodes={[failedNode]}
      />,
    );

    expect(screen.queryByText(/Nodes to Recover/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rerun" })).not.toBeInTheDocument();
  });

  it("shows retry and rerun controls when run is failed", () => {
    render(
      <GovernanceActions
        run={failedRun}
        failedNodes={[failedNode]}
      />,
    );

    expect(screen.getByText(/Nodes to Recover/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Rerun" })).toBeInTheDocument();
    expect(screen.getByText("Agent-Executed")).toBeInTheDocument();
    expect(screen.getByText("Reviewer")).toBeInTheDocument();
    expect(screen.queryByText(/authority/i)).not.toBeInTheDocument();
    expect(screen.getByText("Reviewer rejected missing evidence.")).toHaveClass("text-wrap-anywhere");
  });

  it("shows every supplied workflow node when a run is cancelled", async () => {
    render(
      <GovernanceActions
        run={cancelledRun}
        cancelledNodes={[{ ...failedNode, state: "cancelled", failure_reason: undefined }]}
      />,
    );

    expect(screen.getByText("Cancelled Run Nodes (1)")).toBeInTheDocument();
    await userEvent.click(screen.getByText("Cancelled Run Nodes (1)"));
    expect(screen.getByText("execution_review")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Rerun" })).toBeInTheDocument();
    expect(screen.queryByText("Failure reason unavailable.")).not.toBeInTheDocument();
  });

  it("starts fourteen cancelled nodes collapsed and exposes every recovery action on demand", async () => {
    const nodes = Array.from({ length: 14 }, (_, index) => ({ ...failedNode, node_id: `node-${index}`, state: "cancelled" as const, failure_reason: undefined }));
    render(<GovernanceActions run={cancelledRun} cancelledNodes={nodes} />);
    const summary = screen.getByText("Cancelled Run Nodes (14)");
    expect(summary.closest("details")).not.toHaveAttribute("open");
    // Native details retains its content in the DOM while hiding it.
    for (const action of screen.getAllByRole("button", { name: /^(Retry|Rerun)$/ })) {
      expect(action).not.toBeVisible();
    }
    await userEvent.click(summary);
    expect(screen.getAllByRole("button", { name: "Retry" })).toHaveLength(14);
    expect(screen.getAllByRole("button", { name: "Rerun" })).toHaveLength(14);
    for (const node of nodes) expect(screen.getByText(node.node_id)).toBeVisible();
  });

  it.each(["retry", "rerun"] as const)("preserves an uncertain %s request and its visible error while the recovery list is collapsed", async (action) => {
    const state = action === "retry" ? retryState : rerunState;
    state.mutateAsync.mockImplementationOnce(async () => {
      const error = new TypeError("Response lost; original outcome remains unresolved.");
      state.error = error;
      throw error;
    }).mockImplementationOnce(async () => { state.error = null; });
    const nodes = [failedNode, { ...failedNode, node_id: "second-node" }];
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} failedNodes={nodes} />);
    const summary = screen.getByText("Nodes to Recover (2)");
    await user.click(summary);
    await user.click(screen.getAllByRole("button", { name: action === "retry" ? "Retry" : "Rerun" })[0]);
    await waitFor(() => expect(state.mutateAsync).toHaveBeenCalledOnce());
    const original = state.mutateAsync.mock.calls[0][0];
    await user.click(summary);
    expect(screen.getByText("Response lost; original outcome remains unresolved.")).toBeVisible();
    expect(summary.closest("details")).not.toHaveAttribute("open");
    expect(screen.getByRole("button", { name: action === "retry" ? "Retry Again" : "Retry Rerun" })).not.toBeVisible();
    await user.click(summary);
    expect(screen.getByRole("button", { name: action === "retry" ? "Retry" : "Rerun" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: action === "retry" ? "Retry Again" : "Retry Rerun" }));
    await waitFor(() => expect(state.mutateAsync).toHaveBeenCalledTimes(2));
    expect(state.mutateAsync.mock.calls[1][0]).toEqual(original);
  });

  it("does not leave final approval stuck in success state when the run is actionable again", () => {
    finalApprovalState.isSuccess = true;
    finalApprovalState.data = { status: "succeeded" };

    render(
      <GovernanceActions
        run={awaitingFinalApprovalRun}
        failedNodes={[]}
      />,
    );

    expect(screen.queryByText(/Decision recorded/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Confirm Completion/i })).toBeInTheDocument();

    finalApprovalState.isSuccess = false;
    finalApprovalState.data = null;
  });

  it("hides final approval controls for autonomous runs in awaiting_human_confirmation", () => {
    render(
      <GovernanceActions
        run={{
          ...awaitingFinalApprovalRun,
          autonomous_approvals: true,
        }}
        failedNodes={[]}
      />,
    );

    expect(screen.queryByRole("button", { name: /Confirm Completion/i })).not.toBeInTheDocument();
    expect(screen.queryByText(/Awaiting Your Final Approval/i)).not.toBeInTheDocument();
  });

  it("allows confirming final approval without notes", async () => {
    const user = userEvent.setup();
    render(
      <GovernanceActions
        run={awaitingFinalApprovalRun}
        failedNodes={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Confirm Completion/i }));

    expect(finalApprovalState.mutateAsync).toHaveBeenCalledWith({
      body: { action: "confirm" },
      key: expect.stringMatching(/^kd1\./),
    });
  });

  it("requires notes when requesting final approval rework", async () => {
    const user = userEvent.setup();
    render(
      <GovernanceActions
        run={awaitingFinalApprovalRun}
        failedNodes={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: /^Request Rework$/i }));

    expect(screen.getByText("Add rework guidance before requesting rework.")).toBeInTheDocument();
    expect(finalApprovalState.mutateAsync).not.toHaveBeenCalled();
  });

  it("shows a stable fallback when a failed node has no failure reason", () => {
    render(
      <GovernanceActions
        run={failedRun}
        failedNodes={[
          {
            ...failedNode,
            failure_reason: undefined,
          },
        ]}
      />,
    );

    expect(screen.getByText("Failure reason unavailable.")).toBeInTheDocument();
  });

	it("wraps long failed node identifiers safely", () => {
		render(
			<GovernanceActions
				run={failedRun}
				failedNodes={[
					{
						...failedNode,
						node_id: "implementation_with_an_extremely_long_failure_identifier_that_should_wrap_cleanly",
					},
				]}
			/>,
		);

		expect(screen.getByText(/implementation_with_an_extremely_long_failure_identifier_that_should_wrap_cleanly/i)).toHaveClass("text-wrap-anywhere");
	});
});
