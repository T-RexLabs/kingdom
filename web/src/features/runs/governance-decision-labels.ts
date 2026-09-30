import type { GovernanceDecision } from "@/api/hooks";

export function governanceDecisionLabel(item: GovernanceDecision): string {
	if (item.stage === "plan") {
		return item.decision === "needs_rework" ? "Requested Plan Rework" : "Approved Plan";
	}
	return item.decision === "needs_rework" ? "Requested Final Rework" : "Confirmed Completion";
}

export function governanceDecisionStageLabel(item: GovernanceDecision): string {
	return item.stage === "plan" ? "Plan" : "Final";
}
