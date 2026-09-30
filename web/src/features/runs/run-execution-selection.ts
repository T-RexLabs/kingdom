import type { RunNodeExecution } from "@/api/hooks";
import { isActiveProviderBackedExecution } from "./run-status-semantics";

/** Keep operator selection authoritative; otherwise keep real work in view. */
export function selectRunExecutionNode(
  nodes: RunNodeExecution[],
  selectedNodeId?: string | null,
): RunNodeExecution | null {
  const selected = nodes.find((node) => node.node_id === selectedNodeId);
  if (selected) return selected;
  const providers = nodes.filter(
    (node) => node.execution_ownership === "provider_backed",
  );
  const active = providers.find((node) =>
    isActiveProviderBackedExecution(node.current_execution),
  );
  if (active) return active;
  // Queued future work has no conversation yet. A completed or interrupted
  // attempt remains useful while the workflow waits for its next decision.
  const recorded = providers.filter(
    (node) =>
      node.current_execution && node.current_execution.status !== "queued",
  );
  recorded.sort((left, right) => {
    const byTime = observedTime(right) - observedTime(left);
    return (
      byTime ||
      (left.node_id < right.node_id ? -1 : left.node_id > right.node_id ? 1 : 0)
    );
  });
  return (
    recorded[0] ??
    providers.find((node) => !!node.current_execution) ??
    providers[0] ??
    nodes[0] ??
    null
  );
}

function observedTime(node: RunNodeExecution): number {
  const execution = node.current_execution;
  if (!execution) return 0;
  // Prefer the canonical activity clock, not response arrival or ingestion time.
  for (const value of [
    execution.activity_at,
    execution.started_at,
    execution.queued_at,
  ]) {
    const parsed = value ? Date.parse(value) : NaN;
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}
