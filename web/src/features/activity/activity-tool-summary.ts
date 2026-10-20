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
  if (name === "glob") {
    try {
      const input: unknown = JSON.parse(item.inputContent?.preview ?? item.input ?? "");
      if (input && typeof input === "object" && "pattern" in input && typeof input.pattern === "string") {
        return `${action} ${input.pattern.split(/\r?\n/)[0]}`;
      }
    } catch {
      // A bounded or absent input preview still has the provider's summary.
    }
  }
  if (name === "todowrite" || name === "todoread") {
    const counts = reportedTaskCounts(name === "todowrite" ? item.inputContent?.preview : item.outputContent?.preview);
    return counts ? `${action} · ${counts.completed} of ${counts.total}` : action;
  }
  if (["webfetch", "websearch", "glob", "grep"].includes(name)) return item.label.split(/\r?\n/)[0];
  if (!["read", "write", "edit"].includes(name)) return action;
  const fileName = activityToolFileName(item);
  return fileName ? `${action} ${fileName}` : action;
}

/** The base name of the file a read, write or edit call names, when its input is readable. */
export function activityToolFileName(item: ActivityItem): string | undefined {
  try {
    const input: unknown = JSON.parse(item.inputContent?.preview ?? item.input ?? "");
    if (!input || typeof input !== "object") return undefined;
    const fields = input as Record<string, unknown>;
    const path = fields.filePath ?? fields.file_path ?? fields.path;
    if (typeof path !== "string") return undefined;
    return path.split(/[\\/]/).filter(Boolean).at(-1);
  } catch {
    return undefined;
  }
}

/** Completed and total counts from a complete task list preview, when it parses. */
function reportedTaskCounts(preview: string | undefined): { completed: number; total: number } | undefined {
  try {
    const value: unknown = JSON.parse(preview ?? "");
    const tasks = Array.isArray(value) ? value : (value as { todos?: unknown } | null)?.todos;
    if (!Array.isArray(tasks) || tasks.length === 0) return undefined;
    const completed = tasks.filter((task) => (task as { status?: unknown } | null)?.status === "completed").length;
    return { completed, total: tasks.length };
  } catch {
    return undefined;
  }
}
