import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import { ApiError } from "@/api/client";
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

  it("shows just one recovery pair for fourteen cancelled nodes", async () => {
    const nodes = Array.from({ length: 14 }, (_, index) => ({ ...failedNode, node_id: `node_${index}`, state: "cancelled" as const }));
    render(<GovernanceActions run={cancelledRun} cancelledNodes={nodes} />);
    expect(screen.getAllByRole("button", { name: "Retry" })).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Rerun" })).toHaveLength(1);
    expect(screen.queryByText(/Cancelled Run Nodes/)).not.toBeInTheDocument();
    expect(screen.queryByText("node_0")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    const dialog = within(screen.getByRole("dialog", { name: "Retry Step" }));
    expect(dialog.getByRole("button", { name: "Retry Step" })).toBeDisabled();
    await userEvent.click(dialog.getByRole("button", { name: "Step" }));
    expect(screen.getAllByRole("option")).toHaveLength(14);
    await userEvent.click(screen.getByRole("option", { name: "Node 7 · Cancelled" }));
    await userEvent.click(dialog.getByRole("button", { name: "Retry Step" }));
    expect(retryState.mutateAsync).toHaveBeenCalledWith({ value: "node_7", key: expect.stringMatching(/^kd1\./) });
  });

  it("explains completion rerun promotion before submission", async () => {
    render(<GovernanceActions run={cancelledRun} cancelledNodes={[{ ...failedNode, node_id: "completion", state: "cancelled" }]} />);
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    expect(screen.getByText(/Completion restarts at plan drafting/)).toBeVisible();
    expect(rerunState.mutateAsync).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledWith({ value: "completion", key: expect.stringMatching(/^kd1\./) });
  });

  it.each(["retry", "rerun"] as const)("keeps an uncertain %s locked to the same target and key after closing the dialog", async (action) => {
    const state = action === "retry" ? retryState : rerunState;
    state.mutateAsync.mockImplementationOnce(async () => {
      const error = new TypeError("Response lost; original outcome remains unresolved.");
      state.error = error;
      throw error;
    }).mockImplementationOnce(async () => { state.error = null; });
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} failedNodes={[failedNode, { ...failedNode, node_id: "second_node" }]} />);
    const label = action === "retry" ? "Retry" : "Rerun";
    await user.click(screen.getByRole("button", { name: label }));
    await user.click(screen.getByRole("button", { name: "Step" }));
    await user.click(screen.getByRole("option", { name: "Execution Review · Failed" }));
    await user.click(screen.getByRole("button", { name: action === "retry" ? "Retry Step" : "Rerun from Step" }));
    await waitFor(() => expect(state.mutateAsync).toHaveBeenCalledOnce());
    const original = state.mutateAsync.mock.calls[0][0];
    expect(screen.getByRole("button", { name: "Step" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Response lost");
    expect(screen.getByRole("button", { name: action === "retry" ? "Rerun" : "Retry" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: action === "retry" ? "Retry Again" : "Retry Rerun" }));
    expect(screen.getByRole("button", { name: "Step" })).toBeDisabled();
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: action === "retry" ? "Retry Again" : "Retry Rerun" }));
    await waitFor(() => expect(state.mutateAsync).toHaveBeenCalledTimes(2));
    expect(state.mutateAsync.mock.calls[1][0]).toEqual(original);
  });

  it("does not duplicate pending submissions and closes after success", async () => {
    let resolve!: () => void;
    retryState.mutateAsync.mockReturnValue(new Promise<void>((done) => { resolve = done; }));
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} failedNodes={[failedNode]} />);
    await user.click(screen.getByRole("button", { name: "Retry" }));
    await user.dblClick(screen.getByRole("button", { name: "Retry Step" }));
    expect(retryState.mutateAsync).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "Step" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeDisabled();
    resolve();
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("leaves definitive cleanup conflicts available for a new explicit request", async () => {
    retryState.mutateAsync.mockRejectedValueOnce(new ApiError(409, {
      message: "Worktree cleanup is still running.", code: "dependency_blocked",
      retryable: false, category: "core", details: {}, correlationId: "api.retry_node",
    })).mockResolvedValueOnce({});
    const user = userEvent.setup();
    render(<GovernanceActions run={cancelledRun} cancelledNodes={[{ ...failedNode, node_id: "custom_step" }]} />);
    await user.click(screen.getByRole("button", { name: "Retry" }));
    await user.click(screen.getByRole("button", { name: "Retry Step" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Worktree cleanup is still running.");
    expect(screen.getByRole("button", { name: "Step" })).toBeEnabled();
    expect(retryState.mutateAsync).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "Retry Step" }));
    expect(retryState.mutateAsync).toHaveBeenCalledTimes(2);
    const [first, second] = retryState.mutateAsync.mock.calls;
    expect(second[0].value).toBe("custom_step");
    expect(second[0].key).not.toBe(first[0].key);
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

  it("shows failure context only when choosing recovery", async () => {
    render(<GovernanceActions run={failedRun} failedNodes={[failedNode]} />);
    expect(screen.queryByText(failedNode.failure_reason!)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(screen.getByText(failedNode.failure_reason!)).toHaveClass("text-wrap-anywhere");
  });
});
