import type { ActivityItem } from "./activity-view";

const actions: Record<string, string> = {
  read: "Read",
  write: "Write",
  edit: "Edit",
  glob: "Find Files",
  grep: "Search",
  bash: "Run Command",
  shell: "Run Command",
  webfetch: "Read Page",
  websearch: "Search the Web",
  apply_patch: "Apply Changes",
  todowrite: "Update Tasks",
  todoread: "Read Tasks",
  task: "Delegate Task",
};

/** Display only a short action/filename. Full provider input remains in details. */
export function activityToolSummary(item: ActivityItem): string {
  if (!item.toolName) return item.label.split(/\r?\n/)[0];
  const name = item.toolName.toLowerCase();
  const action = actions[name] ?? item.toolName;
  if (["bash", "shell"].includes(name) && item.toolTitle && item.toolTitle.length <= 120 && !/[\r\n]/.test(item.toolTitle)) return item.toolTitle;
  if (["webfetch", "websearch", "glob", "grep"].includes(name)) return item.label.split(/\r?\n/)[0];
  if (!["read", "write", "edit"].includes(name)) return action;
  try {
    const input: unknown = JSON.parse(
      item.inputContent?.preview ?? item.input ?? "",
    );
    if (!input || typeof input !== "object") return action;
    const fields = input as Record<string, unknown>;
    const path = fields.filePath ?? fields.file_path ?? fields.path;
    if (typeof path !== "string") return action;
    const basename = path.split(/[\\/]/).filter(Boolean).at(-1);
    return basename ? `${action} ${basename}` : action;
  } catch {
    return action;
  }
}
