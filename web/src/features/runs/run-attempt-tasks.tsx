import { useEffect, useId, useRef, useState } from "react";
import { ChevronRight } from "lucide-react";
import { useRunExecutionTasks } from "@/api/hooks-runs";
import type { RunExecution } from "@/api/hooks";
import { ReportedTaskRow, type ReportedTask } from "@/features/activity/activity-checklist";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";
import { isActiveExecutionStatus } from "./run-status-semantics";

const TASK_TOOLS = new Set(["todowrite", "todoread"]);

/** The agent's latest reported task list, pinned under its identity. */
export function RunAttemptTasks({
  runId,
  attemptId,
  status,
}: {
  runId: string;
  attemptId: string | null;
  status?: RunExecution["status"];
}) {
  const running = isActiveExecutionStatus(status);
  const tasks = useRunExecutionTasks(runId, attemptId, {
    // Live activity drives refreshes; polling only backs up a dropped stream.
    refetchIntervalMs: running ? 10_000 : false,
  });
  const liveItems = selectLiveRunActivity(useLiveRunActivity(runId), attemptId);
  const liveTaskSequence = liveItems.reduce(
    (latest, item) =>
      item.kind === "tool" && TASK_TOOLS.has(item.toolName?.toLowerCase() ?? "")
        ? Math.max(latest, item.sequence)
        : latest,
    0,
  );
  const knownSequence = tasks.data?.sequence ?? 0;
  const requestedSequence = useRef(0);
  const { refetch } = tasks;
  useEffect(() => {
    // One read per newer live update; polling covers a not-yet-durable snapshot.
    if (liveTaskSequence <= Math.max(knownSequence, requestedSequence.current)) return;
    requestedSequence.current = liveTaskSequence;
    void refetch();
  }, [liveTaskSequence, knownSequence, refetch]);

  const list = tasks.data;
  if (!list || list.state === "none") return null;
  if (list.state === "unreadable")
    return (
      <p className="py-1 text-xs text-foreground-muted">
        The latest task list could not be read.
      </p>
    );
  if (!list.tasks.length) return null;
  return <PinnedTaskList key={attemptId} tasks={list.tasks} />;
}

function PinnedTaskList({ tasks }: { tasks: ReportedTask[] }) {
  const [open, setOpen] = useState(false);
  const panelId = useId();
  const completed = tasks.filter((task) => task.status === "completed").length;
  const current =
    tasks.find((task) => task.status === "in_progress") ??
    tasks.find((task) => task.status === "pending");
  return (
    <div className="min-w-0">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        onClick={() => setOpen((value) => !value)}
        className="flex min-h-9 w-full min-w-0 items-center gap-2 text-left text-xs text-foreground-muted hover:text-foreground"
      >
        <ChevronRight
          aria-hidden="true"
          className={`h-3.5 w-3.5 shrink-0 transition-transform ${open ? "rotate-90" : ""}`}
        />
        <span className="shrink-0 font-medium text-foreground">Tasks</span>
        <span className="shrink-0">
          {completed} of {tasks.length}
          {completed === tasks.length ? " Completed" : ""}
        </span>
        {current && completed < tasks.length && (
          <span className="min-w-0 truncate">· {current.content}</span>
        )}
      </button>
      <div
        id={panelId}
        hidden={!open}
        role="list"
        aria-label="Reported tasks"
        className="max-h-56 overflow-y-auto pl-5"
      >
        {open && tasks.map((task, index) => <ReportedTaskRow key={index} task={task} />)}
      </div>
    </div>
  );
}
