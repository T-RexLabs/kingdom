import { Feather } from "lucide-react";
import type { RunNodeExecution } from "@/api/hooks";
import { agentLabelForExecution } from "@/lib/utils";
import { buildCurrentEvent } from "@/features/activity/activity-view";
import { isActiveProviderBackedExecution } from "./run-status-semantics";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";

// Cap on how many running agents the headline names before collapsing the rest into
// a "+N more" badge. The headline is a cross-tab heartbeat ("who's in the field"); the
// per-agent actions live in the Activity tab's lanes card, so this stays one line at any
// scale.
const MAX_NAMED_AGENTS = 2;

// RunLiveStatus is the always-visible "what's cooking now" headline. It names up to a
// couple of currently executing nodes with their current action and rolls the remainder
// into "+N more" — reassuring the operator that work is happening without opening the
// Activity tab, and staying single-line however many agents run. Renders nothing when no
// node is actively running.
export function RunLiveStatus({ runId, nodes }: { runId: string; nodes: RunNodeExecution[] }) {
  const liveBuffer = useLiveRunActivity(runId);
  const active = nodes
    .flatMap((node) => {
      const execution = node.current_execution;
      return execution && isActiveProviderBackedExecution(execution) ? [{ node, execution }] : [];
    })
    .map(({ node, execution }) => {
      const items = selectLiveRunActivity(liveBuffer, execution.attempt_id);
      const current = items.length > 0 ? buildCurrentEvent(items, "") : null;
      return {
        id: execution.attempt_id,
        label: agentLabelForExecution(node.required_role, node.node_id),
        verb: current?.label?.toLowerCase(),
        activityAt: execution.activity_at ?? "",
      };
    })
    // Favor the freshest work: the named agents are the most-recently-active ones.
    .sort((left, right) => right.activityAt.localeCompare(left.activityAt));

  if (active.length === 0) {
    return null;
  }

  const shown = active.slice(0, MAX_NAMED_AGENTS);
  const overflow = active.length - shown.length;

  return (
    <div data-testid="run-live-status" className="border-b border-border bg-surface-raised/20 px-page py-2">
      {/* items-start keeps the quill aligned to the first line when entries wrap. */}
      <div className="flex min-w-0 items-start gap-2 text-xs text-foreground-subtle">
        <Feather className="mt-0.5 h-3.5 w-3.5 shrink-0 animate-quill-write text-gold/75" />
        <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
          {shown.map((entry, index) => (
            <span key={entry.id} className="inline-flex items-center gap-2">
              {index > 0 ? <span aria-hidden className="text-foreground-subtle/40">·</span> : null}
              <span className="inline-flex min-w-0 items-baseline gap-1">
                <span className="font-medium text-foreground/90">{entry.label}</span>
                {entry.verb ? <span className="max-w-[20rem] truncate text-foreground-subtle">{entry.verb}</span> : null}
              </span>
            </span>
          ))}
          {overflow > 0 ? <span className="text-foreground-subtle/70">+{overflow} more</span> : null}
        </div>
      </div>
    </div>
  );
}
