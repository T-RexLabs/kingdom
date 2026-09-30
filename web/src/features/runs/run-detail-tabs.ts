export const RUN_DETAIL_TABS = ["workflow", "execution"] as const;
export type RunDetailTab = (typeof RUN_DETAIL_TABS)[number];
export type RunDetailPanel = "files" | "plan" | "details";
export type RunDetailSection = "context" | "mailbox" | "audit";
export type RunDetailSearch = {
  tab?: RunDetailTab;
  panel?: RunDetailPanel;
  section?: RunDetailSection;
};
export function isRunDetailTab(value: unknown): value is RunDetailTab {
  return value === "workflow" || value === "execution";
}
export function parseRunDetailSearch(
  search: Record<string, unknown>,
): RunDetailSearch {
  const result: RunDetailSearch = {};
  if (isRunDetailTab(search.tab)) result.tab = search.tab;
  if (
    search.panel === "files" ||
    search.panel === "plan" ||
    search.panel === "details"
  )
    result.panel = search.panel;
  if (
    result.panel === "details" &&
    (search.section === "context" ||
      search.section === "mailbox" ||
      search.section === "audit")
  )
    result.section = search.section;
  return result;
}
