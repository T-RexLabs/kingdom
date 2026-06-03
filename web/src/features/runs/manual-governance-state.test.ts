import { describe, expect, it } from "vitest";
import { describeManualGovernanceState, runPlanStateLabel } from "./manual-governance-state";

describe("manual governance state", () => {
  it("requires autonomous_approvals to be false before exposing manual actions", () => {
    const state = describeManualGovernanceState({
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_king_decision",
      autonomous_approvals: true,
    });

    expect(state.isAwaitingPlanApproval).toBe(false);
    expect(state.isAwaitingFinalApproval).toBe(false);
    expect(state.noticeTitle).toBeNull();
  });

  it("keeps autonomous plan-state copy neutral for inconsistent rows", () => {
    expect(runPlanStateLabel({
      display_state: "awaiting_plan_decision",
      plan_state: "awaiting_king_decision",
      autonomous_approvals: true,
    })).toBe("Awaiting Plan Decision");
  });

  it("preserves manual approval labels for explicit manual-governance runs", () => {
    const state = describeManualGovernanceState({
      display_state: "awaiting_final_decision",
      plan_state: "approved",
      autonomous_approvals: false,
    });

    expect(state.isAwaitingFinalApproval).toBe(true);
    expect(state.noticeTitle).toBe("Awaiting your final approval");
  });
});
