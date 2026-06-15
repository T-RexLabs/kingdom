import type { RunSummary } from "@/api/hooks";
import { statusLabel } from "@/lib/status-system";

type RunDisplayStateLike = { status: string; display_state?: RunSummary["display_state"] };

export function runDisplayStateLabel(run: RunDisplayStateLike): string {
  return statusLabel(run.display_state ?? run.status);
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
