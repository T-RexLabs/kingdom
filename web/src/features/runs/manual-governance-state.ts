import type { RunSummary } from "@/api/hooks";
import { capitalize } from "@/lib/utils";

export interface ManualGovernanceState {
  isAwaitingPlanApproval: boolean;
  isAwaitingFinalApproval: boolean;
  noticeTitle: string | null;
  noticeMessage: string | null;
}

const EMPTY_GOVERNANCE_STATE: ManualGovernanceState = {
  isAwaitingPlanApproval: false,
  isAwaitingFinalApproval: false,
  noticeTitle: null,
  noticeMessage: null,
};

const PLAN_STATE_LABELS: Record<string, string> = {
  draft: "Draft",
  in_plan_review: "In Plan Review",
  awaiting_plan_decision: "Awaiting Your Plan Approval",
  approved: "Approved",
  needs_rework: "Rework Requested",
};

type ManualGovernanceRun = Pick<RunSummary, "display_state" | "plan_state" | "autonomous_approvals">;

export function describeManualGovernanceState(
  run: ManualGovernanceRun,
): ManualGovernanceState {
  if (run.autonomous_approvals !== false) {
    return EMPTY_GOVERNANCE_STATE;
  }

  const isAwaitingPlanApproval = run.display_state === "awaiting_plan_decision";
  if (isAwaitingPlanApproval) {
    return {
      isAwaitingPlanApproval: true,
      isAwaitingFinalApproval: false,
      noticeTitle: "Awaiting Your Plan Approval",
      noticeMessage:
        "Execution is paused until you confirm this plan or request rework.",
    };
  }

  const isAwaitingFinalApproval = run.display_state === "awaiting_final_decision";
  if (isAwaitingFinalApproval) {
    return {
      isAwaitingPlanApproval: false,
      isAwaitingFinalApproval: true,
      noticeTitle: "Awaiting Your Final Approval",
      noticeMessage:
        "This run is paused until you confirm completion or request rework.",
    };
  }

  return EMPTY_GOVERNANCE_STATE;
}

export function runPlanStateLabel(run: ManualGovernanceRun): string {
  if (run.plan_state === "awaiting_plan_decision" && run.autonomous_approvals !== false) {
    return "Awaiting Plan Decision";
  }
  return PLAN_STATE_LABELS[run.plan_state] ?? capitalize(run.plan_state.replace(/_/g, " "));
}
