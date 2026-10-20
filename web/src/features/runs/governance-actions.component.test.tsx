import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import { ApiError } from "@/api/client";
import userEvent from "@testing-library/user-event";
import { GovernanceActions } from "./governance-actions";
import type { RunGraphNode, RunResumePlan, RunSummary } from "@/api/hooks";

const finalApprovalState = {
  mutateAsync: vi.fn().mockResolvedValue({ status: "succeeded" }),
  isPending: false,
  isSuccess: false,
  error: null,
  data: null as null | { status: string },
};
const resumeState = { mutateAsync: vi.fn(), isPending: false, error: null as Error | null };
const availablePlan: RunResumePlan = {
  run_id: "run-1",
  available: true,
  steps: [{ node_id: "execution_review", source_attempt_id: "run-1:execution_review:1", checkpoint_captured_at: "2026-03-06T10:05:00.000Z" }],
};
const resumePlanState = { data: availablePlan as RunResumePlan | undefined, isPending: false, error: null as Error | null, refetch: vi.fn() };
const rerunState = { mutateAsync: vi.fn(), isPending: false, error: null as Error | null };

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: () => "idem-final-approval-test",
  useCreateFinalApproval: () => finalApprovalState,
  useResumeRun: () => resumeState,
  useRunResumePlan: () => resumePlanState,
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
    resumeState.mutateAsync.mockReset();
    resumeState.error = null;
    resumePlanState.data = availablePlan;
    resumePlanState.refetch.mockReset();
    rerunState.mutateAsync.mockReset();
    rerunState.error = null;
  });

  it("hides resume and rerun controls when run is not failed", () => {
    render(
      <GovernanceActions
        run={runningRun}
        recovery={{ nodes: [failedNode] }}
      />,
    );

    expect(screen.queryByText(/Nodes to Recover/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resume" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rerun" })).not.toBeInTheDocument();
  });

  it("offers recovery when a failed run has only cancelled steps", async () => {
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [{ ...failedNode, state: "cancelled" }] }} />);
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    await userEvent.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledWith({ value: "execution_review", key: expect.stringMatching(/^kd1\./) });
  });

  it("preselects the interrupted frontier instead of downstream cancelled steps", async () => {
    const nodes = [{ ...failedNode, node_id: "advisory_synthesis", state: "cancelled" as const }, { ...failedNode, node_id: "plan_drafting", state: "cancelled" as const }];
    render(<GovernanceActions run={failedRun} recovery={{ nodes, defaultNodeId: "advisory_synthesis" }} />);
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    expect(screen.getByRole("button", { name: "Step" })).toHaveTextContent("Advisory Synthesis");
    await userEvent.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledWith({ value: "advisory_synthesis", key: expect.stringMatching(/^kd1\./) });
  });

  it("shows just one recovery pair for fourteen cancelled nodes", async () => {
    const nodes = Array.from({ length: 14 }, (_, index) => ({ ...failedNode, node_id: `node_${index}`, state: "cancelled" as const }));
    render(<GovernanceActions run={cancelledRun} recovery={{ nodes }} />);
    expect(screen.getAllByRole("button", { name: "Resume" })).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Rerun" })).toHaveLength(1);
    expect(screen.queryByText(/Cancelled Run Nodes/)).not.toBeInTheDocument();
    expect(screen.queryByText("node_0")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    const dialog = within(screen.getByRole("dialog", { name: "Rerun from Step" }));
    expect(dialog.getByRole("button", { name: "Rerun from Step" })).toBeDisabled();
    await userEvent.click(dialog.getByRole("button", { name: "Step" }));
    expect(screen.getAllByRole("option")).toHaveLength(14);
    await userEvent.click(screen.getByRole("option", { name: "Node 7 · Cancelled" }));
    await userEvent.click(dialog.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledWith({ value: "node_7", key: expect.stringMatching(/^kd1\./) });
  });

  it("resumes exactly the server-confirmed steps and shows their checkpoint", async () => {
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode] }} />);
    await userEvent.click(screen.getByRole("button", { name: "Resume" }));
    const dialog = within(screen.getByRole("dialog", { name: "Resume Interrupted Work" }));
    expect(resumePlanState.refetch).toHaveBeenCalled();
    expect(dialog.getByRole("list", { name: "Interrupted steps" })).toHaveTextContent("Execution Review");
    expect(dialog.getByText(/Checkpoint saved/)).toBeVisible();
    await userEvent.click(dialog.getByRole("button", { name: "Resume" }));
    expect(resumeState.mutateAsync).toHaveBeenCalledWith({
      body: { reason: "manual resume from run detail", steps: [{ node_id: "execution_review", source_attempt_id: "run-1:execution_review:1" }] },
      key: expect.stringMatching(/^kd1\./),
    });
  });

  it("explains unavailable resume and offers Rerun without changing the action", async () => {
    resumePlanState.data = { run_id: "run-1", available: false, steps: [{ node_id: "execution_review", unavailable_reason: "No checkpoint was saved when this step stopped." }] };
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode] }} />);
    await userEvent.click(screen.getByRole("button", { name: "Resume" }));
    const dialog = within(screen.getByRole("dialog", { name: "Resume Interrupted Work" }));
    expect(dialog.getByText("No checkpoint was saved when this step stopped.")).toBeVisible();
    expect(dialog.getByText(/Rerun repeats a step from its trusted baseline/)).toBeVisible();
    expect(dialog.getByRole("button", { name: "Resume" })).toBeDisabled();
    await userEvent.click(dialog.getByRole("button", { name: "Rerun Instead" }));
    expect(screen.getByRole("dialog", { name: "Rerun from Step" })).toBeVisible();
    expect(resumeState.mutateAsync).not.toHaveBeenCalled();
    expect(rerunState.mutateAsync).not.toHaveBeenCalled();
  });

  it("explains completion rerun promotion before submission", async () => {
    render(<GovernanceActions run={cancelledRun} recovery={{ nodes: [{ ...failedNode, node_id: "completion", state: "cancelled" }] }} />);
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    expect(screen.getByText(/Completion restarts at plan drafting/)).toBeVisible();
    expect(rerunState.mutateAsync).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledWith({ value: "completion", key: expect.stringMatching(/^kd1\./) });
  });

  it("keeps an uncertain rerun locked to the same target and key after closing the dialog", async () => {
    rerunState.mutateAsync.mockImplementationOnce(async () => {
      const error = new TypeError("Response lost; original outcome remains unresolved.");
      rerunState.error = error;
      throw error;
    }).mockImplementationOnce(async () => { rerunState.error = null; });
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode, { ...failedNode, node_id: "second_node" }] }} />);
    await user.click(screen.getByRole("button", { name: "Rerun" }));
    await user.click(screen.getByRole("button", { name: "Step" }));
    await user.click(screen.getByRole("option", { name: "Execution Review · Failed" }));
    await user.click(screen.getByRole("button", { name: "Rerun from Step" }));
    await waitFor(() => expect(rerunState.mutateAsync).toHaveBeenCalledOnce());
    const original = rerunState.mutateAsync.mock.calls[0][0];
    expect(screen.getByRole("button", { name: "Step" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Response lost");
    expect(screen.getByRole("button", { name: "Resume" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry Rerun" }));
    expect(screen.getByRole("button", { name: "Step" })).toBeDisabled();
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Retry Rerun" }));
    await waitFor(() => expect(rerunState.mutateAsync).toHaveBeenCalledTimes(2));
    expect(rerunState.mutateAsync.mock.calls[1][0]).toEqual(original);
  });

  it("keeps an uncertain resume on its confirmed steps and key even if the plan changes", async () => {
    resumeState.mutateAsync.mockImplementationOnce(async () => {
      throw new TypeError("Response lost; original outcome remains unresolved.");
    }).mockResolvedValueOnce({});
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode] }} />);
    await user.click(screen.getByRole("button", { name: "Resume" }));
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Resume" }));
    await waitFor(() => expect(resumeState.mutateAsync).toHaveBeenCalledOnce());
    const original = resumeState.mutateAsync.mock.calls[0][0];
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    resumePlanState.data = { run_id: "run-1", available: false, unavailable_reason: "Only a failed or cancelled run can be resumed.", steps: [] };
    expect(screen.getByRole("button", { name: "Rerun" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry Resume" }));
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Retry Resume" }));
    await waitFor(() => expect(resumeState.mutateAsync).toHaveBeenCalledTimes(2));
    expect(resumeState.mutateAsync.mock.calls[1][0]).toEqual(original);
  });

  it("does not duplicate pending submissions and closes after success", async () => {
    let resolve!: () => void;
    resumeState.mutateAsync.mockReturnValue(new Promise<void>((done) => { resolve = done; }));
    const user = userEvent.setup();
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode] }} />);
    await user.click(screen.getByRole("button", { name: "Resume" }));
    await user.dblClick(within(screen.getByRole("dialog")).getByRole("button", { name: "Resume" }));
    expect(resumeState.mutateAsync).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeDisabled();
    resolve();
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("leaves definitive cleanup conflicts available for a new explicit request", async () => {
    rerunState.mutateAsync.mockRejectedValueOnce(new ApiError(409, {
      message: "Worktree cleanup is still running.", code: "dependency_blocked",
      retryable: false, category: "core", details: {}, correlationId: "api.rerun_from_node",
    })).mockResolvedValueOnce({});
    const user = userEvent.setup();
    render(<GovernanceActions run={cancelledRun} recovery={{ nodes: [{ ...failedNode, node_id: "custom_step" }] }} />);
    await user.click(screen.getByRole("button", { name: "Rerun" }));
    await user.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Worktree cleanup is still running.");
    expect(screen.getByRole("button", { name: "Step" })).toBeEnabled();
    expect(rerunState.mutateAsync).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "Rerun from Step" }));
    expect(rerunState.mutateAsync).toHaveBeenCalledTimes(2);
    const [first, second] = rerunState.mutateAsync.mock.calls;
    expect(second[0].value).toBe("custom_step");
    expect(second[0].key).not.toBe(first[0].key);
  });

  it("does not leave final approval stuck in success state when the run is actionable again", () => {
    finalApprovalState.isSuccess = true;
    finalApprovalState.data = { status: "succeeded" };

    render(
      <GovernanceActions
        run={awaitingFinalApprovalRun}
        recovery={{ nodes: [] }}
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
        recovery={{ nodes: [] }}
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
        recovery={{ nodes: [] }}
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
        recovery={{ nodes: [] }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /^Request Rework$/i }));

    expect(screen.getByText("Add rework guidance before requesting rework.")).toBeInTheDocument();
    expect(finalApprovalState.mutateAsync).not.toHaveBeenCalled();
  });

  it("shows failure context only when choosing recovery", async () => {
    render(<GovernanceActions run={failedRun} recovery={{ nodes: [failedNode] }} />);
    expect(screen.queryByText(failedNode.failure_reason!)).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Rerun" }));
    expect(screen.getByText(failedNode.failure_reason!)).toHaveClass("text-wrap-anywhere");
  });
});
