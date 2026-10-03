import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/api/client";
import type { RunNodeExecution } from "@/api/hooks";
import type { components } from "@/api/schema";
import { useRoleIdentity } from "@/theme";
import { timestampDescription } from "@/lib/date-time";
import { activityItemFromApi } from "@/features/activity/activity-view";
import { activityToolSummary } from "@/features/activity/activity-tool-summary";
import { isActiveProviderBackedExecution, liveExecutionLabel } from "./run-status-semantics";
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
  const shown = expanded ? active : active.slice(0, 2);
  return (
    <div
      data-testid="run-live-status"
      className={`flex items-center gap-x-4 gap-y-1 ${expanded ? "flex-wrap" : "flex-nowrap"}`}
    >
      {!connected && (
        <span className="shrink-0 text-xs text-foreground-muted">
          Reconnecting
        </span>
      )}
      {shown.map((node) => (
        <LiveAgent
          key={node.current_execution!.attempt_id}
          runId={runId}
          node={node}
          connected={connected}
          onSelect={onSelect}
        />
      ))}
      {active.length > 2 && (
        <button
          type="button"
          aria-expanded={expanded}
          className="min-h-8 shrink-0 text-xs text-accent"
          onClick={() => setExpanded(!expanded)}
        >
          {expanded ? "Show Less" : `+${active.length - 2} More`}
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
  const label =
    event?.kind === "tool"
      ? activityToolSummary(event)
      : (event?.label ??
        (liveExecutionLabel(execution.progress_reason_code, execution.progress_event_type) ?? (execution.status === "starting"
          ? "Starting"
          : execution.status === "verifying"
            ? "Verifying"
            : "Waiting for provider activity")));
  const eventTime = event?.occurredAt
    ? `Occurred: ${timestampDescription(event.occurredAt)} (${event.occurredAt})`
    : event?.recordedAt
      ? `Recorded: ${timestampDescription(event.recordedAt)} (${event.recordedAt})`
      : null;
  return (
    <button
      type="button"
      onClick={() => onSelect?.(node.node_id)}
      title={[identity.canonicalName, `Attempt ${execution.attempt}`, eventTime]
        .filter(Boolean)
        .join(" · ")}
      className="flex min-h-8 min-w-0 max-w-80 items-center gap-2 text-left text-xs hover:text-accent focus-visible:outline-accent"
    >
      <span
        aria-hidden="true"
        className={`h-1.5 w-1.5 shrink-0 rounded-full ${
          observed
            ? "bg-status-running motion-safe:animate-pulse"
            : "bg-foreground-muted"
        }`}
      />
      <span className="shrink-0 font-medium">{identity.name}</span>
      <span className="truncate text-foreground-muted">{label}</span>
    </button>
  );
}
