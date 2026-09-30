import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/api/client";
import type { RunNodeExecution } from "@/api/hooks";
import type { components } from "@/api/schema";
import { ThemeIcon, useRoleIdentity } from "@/theme";
import { RelativeTime } from "@/components/ui/relative-time";
import { activityItemFromApi } from "@/features/activity/activity-view";
import { isActiveProviderBackedExecution } from "./run-status-semantics";
import { observedProviderRunning } from "./run-graph-model";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";

export function RunLiveStatus({
  runId,
  nodes,
  connected = false,
  onSelect,
}: {
  runId: string;
  nodes: RunNodeExecution[];
  connected?: boolean;
  onSelect?: (nodeId: string) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const active = nodes.filter((node) =>
    isActiveProviderBackedExecution(node.current_execution),
  );
  if (!active.length) return null;
  const shown = expanded ? active : active.slice(0, 3);
  return (
    <div
      data-testid="run-live-status"
      className="flex flex-wrap items-center gap-2 rounded-xl border border-border bg-surface-raised/40 p-2"
    >
      <span className="px-2 text-[10px] font-medium uppercase tracking-widest text-foreground-muted">
        {connected ? "Live" : "Reconnecting"}
      </span>
      {shown.map((node) => (
        <LiveAgent
          key={node.current_execution!.attempt_id}
          runId={runId}
          node={node}
          connected={connected}
          onSelect={onSelect}
        />
      ))}
      {active.length > 3 && (
        <button
          type="button"
          aria-expanded={expanded}
          className="min-h-9 px-2 text-xs text-accent"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? "Show Less" : `+${active.length - 3} More`}
        </button>
      )}
    </div>
  );
}

function LiveAgent({
  runId,
  node,
  connected,
  onSelect,
}: {
  runId: string;
  node: RunNodeExecution;
  connected: boolean;
  onSelect?: (nodeId: string) => void;
}) {
  const execution = node.current_execution!;
  const identity = useRoleIdentity(node.required_role);
  const buffer = useLiveRunActivity(runId);
  const live = selectLiveRunActivity(buffer, execution.attempt_id).at(-1);
  // A single bounded durable observation makes reopening an active run useful
  // before its next SSE event; arrival time never replaces the provider clock.
  const latest = useQuery({
    queryKey: [
      "runs",
      runId,
      "executions",
      execution.attempt_id,
      "latest-activity",
    ],
    queryFn: ({ signal }) =>
      apiFetch<components["schemas"]["AcpRunActivityResponse"]>(
        `/runs/${runId}/executions/${execution.attempt_id}/activity`,
        { signal, params: { limit: 1 } },
      ),
    refetchInterval: connected ? 5_000 : false,
  });
  const durable = latest.data?.items.at(-1);
  const event =
    live && (!durable || live.sequence > durable.sequence)
      ? live
      : durable
        ? activityItemFromApi(durable)
        : live;
  const observed = observedProviderRunning(execution, connected);
  return (
    <button
      type="button"
      onClick={() => onSelect?.(node.node_id)}
      title={`${identity.canonicalName} · Attempt ${execution.attempt}`}
      className="flex min-h-9 min-w-0 max-w-full items-center gap-2 rounded-lg border border-border/70 bg-background/70 px-2.5 py-1.5 text-left text-xs transition-colors hover:border-accent/40"
    >
      <ThemeIcon
        slot={`role.${node.required_role}`}
        size={15}
        className={
          observed
            ? "text-status-running motion-safe:animate-pulse"
            : "text-foreground-muted"
        }
      />
      <span className="font-medium">{identity.name}</span>
      <span className="max-w-48 truncate text-foreground-muted">
        {event?.label ??
          (execution.status === "starting"
            ? "Starting"
            : execution.status === "verifying"
              ? "Verifying"
              : "Waiting for provider activity")}
      </span>
      {event?.occurredAt && (
        <span className="shrink-0 text-[10px] text-foreground-subtle">
          <RelativeTime iso={event.occurredAt} />
        </span>
      )}
    </button>
  );
}
