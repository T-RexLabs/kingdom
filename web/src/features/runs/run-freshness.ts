import type { RunStreamConnectionState } from "./use-run-event-stream";

export const RUN_SURFACE_DEGRADED_REFRESH_MS = 3_000;

const TERMINAL_RUN_STATUSES = new Set(["succeeded", "failed", "cancelled"]);

export function isDegradedRunStreamState(state: RunStreamConnectionState): boolean {
  return state === "retrying" || state === "failed";
}

export function isTerminalRunStatus(status: string): boolean {
  return TERMINAL_RUN_STATUSES.has(status);
}
