import { activityToolFileName } from "./activity-tool-summary";
import type { ActivityItem } from "./activity-view";

const fileVerbs: Record<string, string> = { read: "Reading", write: "Writing", edit: "Editing" };

const toolStatus: Record<string, (count: number) => string> = {
  webfetch: (count) => (count > 1 ? `Reading ${count} web pages…` : "Reading a web page…"),
  websearch: () => "Searching the web…",
  grep: () => "Searching the code…",
  glob: () => "Finding files…",
  bash: (count) => (count > 1 ? `Running ${count} commands…` : "Running a command…"),
  shell: (count) => (count > 1 ? `Running ${count} commands…` : "Running a command…"),
  apply_patch: () => "Applying changes…",
  todowrite: () => "Updating the task list…",
  todoread: () => "Reviewing the task list…",
  task: () => "Delegating a task…",
};

/** One line describing what a live turn is doing now, from its newest work. */
export function activityStatusLine(items: ActivityItem[]): string {
  const work = items.filter((item) => item.kind !== "progress");
  const newest = work.at(-1);
  if (!newest || newest.kind === "thinking") return "Thinking…";
  if (newest.kind === "message") return "Writing the reply…";
  const name = (newest.toolName ?? "").toLowerCase();
  let count = 0;
  for (let index = work.length - 1; index >= 0; index--) {
    const item = work[index];
    if (item.kind !== "tool" || (item.toolName ?? "").toLowerCase() !== name) break;
    count++;
  }
  const verb = fileVerbs[name];
  if (verb) {
    const fileName = count === 1 ? activityToolFileName(newest) : undefined;
    if (fileName) return `${verb} ${fileName}…`;
    return count > 1 ? `${verb} ${count} files…` : `${verb} a file…`;
  }
  return toolStatus[name]?.(count) ?? `Using ${newest.toolName ?? "a tool"}…`;
}
