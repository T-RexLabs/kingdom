export const RUNS_STATE_FILTERS = [
  "pending",
  "running",
  "awaiting_human_decision",
  "succeeded",
  "failed",
  "cancelled",
] as const;

export type RunsStateFilter = (typeof RUNS_STATE_FILTERS)[number];

export function isRunsStateFilter(value: unknown): value is RunsStateFilter {
  return typeof value === "string" && (RUNS_STATE_FILTERS as readonly string[]).includes(value);
}

export const RUN_QUICK_FILTERS = [
  { value: "all", label: "All" },
  { value: "running", label: "Running", stateFilter: "running" },
  { value: "awaiting_human_decision", label: "Awaiting Human Decision", stateFilter: "awaiting_human_decision" },
  { value: "succeeded", label: "Succeeded", stateFilter: "succeeded" },
  { value: "failed", label: "Failed", stateFilter: "failed" },
  { value: "pending", label: "Pending", stateFilter: "pending" },
  { value: "cancelled", label: "Cancelled", stateFilter: "cancelled" },
] as const satisfies ReadonlyArray<{
  value: "all" | RunsStateFilter;
  label: string;
  stateFilter?: RunsStateFilter;
}>;

export type RunsQuickFilterValue = (typeof RUN_QUICK_FILTERS)[number]["value"];

export function activeRunsFilterConfig(value: RunsQuickFilterValue) {
  return RUN_QUICK_FILTERS.find((filter) => filter.value === value) ?? RUN_QUICK_FILTERS[0];
}
