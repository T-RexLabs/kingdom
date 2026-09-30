import type { RunGraphResponse } from "@/api/hooks";

/** Animation is evidence of observed provider work, never a queued-state cue. */
export function observedProviderRunning(
  execution:
    | {
        execution_mode?: string;
        status?: string;
        started_at?: string;
        terminal_observed_at?: string;
        handoff_state?: string;
      }
    | null
    | undefined,
  connected: boolean,
): boolean {
  return (
    connected &&
    execution?.execution_mode === "provider_backed" &&
    execution.status === "running" &&
    !!execution.started_at &&
    !execution.terminal_observed_at &&
    !execution.handoff_state
  );
}

const NODE_WIDTH = 150;
const GAP = 14;
const MARGIN = 36;

/** Place the stored graph in centered layers. Conditional feedback edges are
 * preserved but do not constrain forward ranks. No workflow is reconstructed. */
export function layoutRunGraph(graph: RunGraphResponse) {
  const pending = new Set(graph.nodes.map((node) => node.node_id));
  const layers: (typeof graph.nodes)[] = [];
  const incoming = new Map(
    graph.nodes.map((node) => [node.node_id, new Set<string>()]),
  );
  for (const edge of graph.edges) {
    if (!edge.on?.toLowerCase().includes("rework") && pending.has(edge.from))
      incoming.get(edge.to)?.add(edge.from);
  }
  while (pending.size) {
    let layer = graph.nodes.filter(
      (node) =>
        pending.has(node.node_id) &&
        [...(incoming.get(node.node_id) ?? [])].every(
          (parent) => !pending.has(parent),
        ),
    );
    // A custom cyclic graph still remains inspectable. Advancing one node
    // deterministically allows feedback edges to route around its later ranks.
    if (!layer.length)
      layer = [graph.nodes.find((node) => pending.has(node.node_id))!];
    layers.push(layer);
    for (const node of layer) pending.delete(node.node_id);
  }
  const width = Math.max(
    360,
    Math.max(0, ...layers.map((layer) => layer.length)) * (NODE_WIDTH + GAP) -
      GAP +
      MARGIN * 2,
  );
  let y = MARGIN;
  const nodes = layers.flatMap((layer, rank) => {
    const rowHeight = layer.every(
      (node) => node.execution_ownership === "runtime_owned",
    )
      ? 34
      : 58;
    const start = (width - (layer.length * (NODE_WIDTH + GAP) - GAP)) / 2;
    const row = layer.map((node, index) => ({
      ...node,
      x: start + index * (NODE_WIDTH + GAP),
      y,
      width: NODE_WIDTH,
      height: rowHeight,
      rank,
    }));
    y += rowHeight + 25;
    return row;
  });
  const lookup = new Map(nodes.map((node) => [node.node_id, node]));
  const edges = graph.edges.flatMap((edge, index) => {
    const from = lookup.get(edge.from),
      to = lookup.get(edge.to);
    if (!from || !to) return [];
    const backward = to.rank <= from.rank;
    const x1 = from.x + from.width / 2,
      y1 = from.y + from.height;
    const x2 = to.x + to.width / 2,
      y2 = to.y;
    const midpoint = (y1 + y2) / 2;
    const lane = MARGIN / 2 + (index % 3) * 6;
    const path = backward
      ? `M ${from.x} ${from.y + from.height / 2} H ${lane} V ${to.y + to.height / 2} H ${to.x}`
      : `M ${x1} ${y1} C ${x1} ${midpoint} ${x2} ${midpoint} ${x2} ${y2}`;
    return [
      { ...edge, key: `${index}:${edge.from}:${edge.to}`, backward, path },
    ];
  });
  return { nodes, edges, width, height: Math.max(140, y + MARGIN - 25) };
}
