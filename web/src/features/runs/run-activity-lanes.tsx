import { useMemo } from "react";
import type { RunNodeExecution } from "@/api/hooks";
import { agentLabelForExecution, cn, getRoleColor } from "@/lib/utils";
import { buildActivityState } from "@/features/activity/activity-view";
import { isActiveProviderBackedExecution } from "./run-status-semantics";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";

// RunActivityLanes is the concurrency overview: while provider-backed nodes run
// (e.g. the advisor fan-out), it shows a compact lane per node with its current
// action, and doubles as a switcher — clicking a lane focuses that node's full
// activity in the detail surface below. It renders nothing once no node is running.
export function RunActivityLanes({
  runId,
  nodes,
  selectedNodeId,
  onSelectStep,
}: {
  runId: string;
  nodes: RunNodeExecution[];
  selectedNodeId: string | null;
  onSelectStep: (nodeId: string) => void;
}) {
  const liveBuffer = useLiveRunActivity(runId);

  const lanes = useMemo(() => {
    return nodes
      .flatMap((node) => {
        const execution = node.current_execution;
        return execution && isActiveProviderBackedExecution(execution) ? [{ node, execution }] : [];
      })
      .map(({ node, execution }) => {
        const state = buildActivityState({ liveItems: selectLiveRunActivity(liveBuffer, execution.attempt_id) });
        return {
          nodeId: node.node_id,
          label: agentLabelForExecution(node.required_role, node.node_id),
          requiredRole: node.required_role,
          action: state.currentEvent?.label ?? "Working…",
        };
      });
  }, [liveBuffer, nodes]);

  if (lanes.length === 0) {
    return null;
  }

  return (
    <section className="rounded-xl border border-border bg-background/70 p-4">
      <p className="text-xs uppercase tracking-[0.16em] text-foreground-muted">Live now · {lanes.length} running</p>
      <ul className="mt-3 space-y-1.5">
        {lanes.map((lane) => {
          const selected = lane.nodeId === selectedNodeId;
          return (
            <li key={lane.nodeId}>
              <button
                type="button"
                onClick={() => onSelectStep(lane.nodeId)}
                aria-pressed={selected}
                className={cn(
                  "flex w-full items-center gap-2.5 rounded-lg border px-3 py-2 text-left text-sm transition-colors",
                  selected
                    ? "border-gold/40 bg-surface-raised/55"
                    : "border-border/50 bg-surface-raised/25 hover:border-gold/30 hover:bg-surface-raised/40",
                )}
              >
                <span className={cn("h-2 w-2 shrink-0 rounded-full bg-current", getRoleColor(lane.requiredRole))} aria-hidden />
                <span className="shrink-0 font-medium text-foreground">{lane.label}</span>
                <span className="min-w-0 flex-1 truncate text-xs text-foreground-subtle">{lane.action}</span>
              </button>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
