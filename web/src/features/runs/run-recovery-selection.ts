import type { RunGraphNode, RunGraphResponse } from "@/api/hooks";

export interface RunRecoverySelection {
  nodes: RunGraphNode[];
  defaultNodeId?: string;
}

// Run failure can propagate from a cancelled provider attempt without any failed
// node. Recover the interrupted frontier, not an arbitrary cancelled descendant.
export function selectRunRecovery(
  status: string,
  nodes: RunGraphNode[],
  edges: RunGraphResponse["edges"],
): RunRecoverySelection {
  if (status !== "failed" && status !== "cancelled") return { nodes: [] };
  const interrupted = nodes.filter((node) => node.state === "failed" || node.state === "cancelled");
  const interruptedIds = new Set(interrupted.map((node) => node.node_id));
  const parents = new Map<string, string[]>();
  for (const edge of edges) {
    if (edge.on?.toLowerCase().includes("rework")) continue;
    const incoming = parents.get(edge.to) ?? [];
    incoming.push(edge.from);
    parents.set(edge.to, incoming);
  }
  const frontier = interrupted.filter((node) => {
    const pending = [...(parents.get(node.node_id) ?? [])];
    const visited = new Set([node.node_id]);
    while (pending.length > 0) {
      const ancestor = pending.pop()!;
      if (visited.has(ancestor)) continue;
      if (interruptedIds.has(ancestor)) return false;
      visited.add(ancestor);
      pending.push(...(parents.get(ancestor) ?? []));
    }
    return true;
  });
  return { nodes: interrupted, ...(frontier.length === 1 ? { defaultNodeId: frontier[0].node_id } : {}) };
}
