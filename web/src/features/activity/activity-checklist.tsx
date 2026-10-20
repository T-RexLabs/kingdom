import { useMemo } from "react";
import {
  CheckCircle2,
  Circle,
  CircleDot,
  CircleSlash,
} from "lucide-react";
import { Virtuoso } from "react-virtuoso";
import type { ActivityContent } from "./activity-view";

export type ReportedTask = { content: string; status: string };

/** A readable projection of one complete provider snapshot, never inferred task state. */
export function ActivityChecklist({ content }: { content: ActivityContent }) {
  const tasks = useMemo(() => readReportedTasks(content), [content]);
  if (!tasks) return null;
  const completed = tasks.filter((task) => task.status === "completed").length;
  return (
    <div
      className="space-y-2 py-2"
      role="group"
      aria-label="Reported checklist"
    >
      <p className="text-xs font-medium text-foreground-muted">
        {completed} of {tasks.length} Completed
      </p>
      {tasks.length > 8 ? (
        <div role="list" aria-label="Reported tasks">
          <Virtuoso
            style={{ height: 256 }}
            aria-label="Scrollable reported tasks"
            data={tasks}
            defaultItemHeight={52}
            itemContent={(_, task) => <ReportedTaskRow task={task} />}
          />
        </div>
      ) : (
        <div role="list" aria-label="Reported tasks" className="space-y-2">
          {tasks.map((task, index) => (
            <ReportedTaskRow key={index} task={task} />
          ))}
        </div>
      )}
    </div>
  );
}

export function ReportedTaskRow({ task }: { task: ReportedTask }) {
  const Icon =
    task.status === "completed"
      ? CheckCircle2
      : task.status === "in_progress"
        ? CircleDot
        : task.status === "cancelled"
          ? CircleSlash
          : Circle;
  const label =
    (
      {
        completed: "Completed",
        in_progress: "In Progress",
        pending: "Pending",
        cancelled: "Cancelled",
      } as Record<string, string>
    )[task.status] ?? task.status;
  return (
    <div
      role="listitem"
      className="flex items-start gap-2 py-1 text-xs leading-5"
    >
      <Icon
        aria-hidden="true"
        className={`mt-0.5 h-3.5 w-3.5 shrink-0 ${task.status === "in_progress" ? "text-accent" : "text-foreground-muted"}`}
      />
      <span
        className={`min-w-0 flex-1 whitespace-pre-wrap break-words ${["completed", "cancelled"].includes(task.status) ? "text-foreground-muted line-through" : "text-foreground"}`}
      >
        {task.content || "(Empty task)"}
      </span>
      <span className="max-w-[35%] break-words text-[10px] text-foreground-muted">
        {label || "Status Not Supplied"}
      </span>
    </div>
  );
}

function readReportedTasks(content: ActivityContent): ReportedTask[] | null {
  if (
    content.availability !== "available" ||
    content.preview === undefined ||
    new TextEncoder().encode(content.preview).length !== content.byte_length
  )
    return null;
  try {
    const value: unknown = JSON.parse(content.preview);
    const tasks = Array.isArray(value)
      ? value
      : value && typeof value === "object"
        ? (value as { todos?: unknown }).todos
        : null;
    if (!Array.isArray(tasks) || tasks.length === 0) return null;
    if (
      !tasks.every(
        (task) =>
          task &&
          typeof task === "object" &&
          typeof task.content === "string" &&
          typeof task.status === "string",
      )
    )
      return null;
    return tasks.map((task) => ({
      content: task.content,
      status: task.status,
    }));
  } catch {
    return null;
  }
}
