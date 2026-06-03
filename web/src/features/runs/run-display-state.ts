import type { RunSummary } from "@/api/hooks";
import { capitalize } from "@/lib/utils";

type RunDisplayStateLike = { status: string; display_state?: RunSummary["display_state"] };

const DISPLAY_STATE_LABELS: Record<RunSummary["display_state"], string> = {
  pending: "Pending",
  running: "Running",
  awaiting_plan_decision: "Awaiting Plan Decision",
  awaiting_final_decision: "Awaiting Final Decision",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
};

export function runDisplayStateLabel(run: RunDisplayStateLike): string {
  if (run.display_state) {
    return DISPLAY_STATE_LABELS[run.display_state] ?? capitalize(run.status.replace(/_/g, " "));
  }
  return capitalize(run.status.replace(/_/g, " "));
}

export function runDisplayBadgeStatus(run: RunDisplayStateLike): string {
  switch (run.display_state) {
    case "awaiting_plan_decision":
    case "awaiting_final_decision":
      return run.display_state;
    default:
      return run.status;
  }
}
