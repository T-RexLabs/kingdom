export const RUN_DETAIL_TABS = ["graph", "execution", "agents", "mailbox"] as const;

export type RunDetailTab = (typeof RUN_DETAIL_TABS)[number];

const RUN_DETAIL_TAB_SET = new Set<string>(RUN_DETAIL_TABS);

export function isRunDetailTab(value: unknown): value is RunDetailTab {
  return typeof value === "string" && RUN_DETAIL_TAB_SET.has(value);
}
