import { useEffect, useRef, useState, useCallback } from "react";
import * as d3 from "d3";
import type { RunGraphResponse, RunGraphNode } from "@/api/hooks";
import { nodeExecutionSemantics, nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { RUN_DETAIL_NEUTRAL_HEX, RUN_ROLE_ACCENT_HEX, RUN_STATUS_HEX } from "./run-detail-tones";

type NodeState = RunGraphNode["state"];
type ViewTransform = { x: number; y: number; k: number };

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NODE_WIDTH = 180;
const NODE_HEIGHT = 48;
const HORIZONTAL_GAP = 60;
const VERTICAL_GAP = 80;
const ROLE_STRIPE_WIDTH = 3;

/** Padding around the computed layout inside the SVG viewBox. */
const VIEW_PADDING = 60;

/** Zoom scale limits for the D3 zoom behavior. */
const ZOOM_MIN = 0.2;
const ZOOM_MAX = 4;

/** Step multiplier per zoom-button click (e.g. 1.4× in / out). */
const ZOOM_STEP = 1.4;

const STATUS_LEGEND_ORDER: NodeState[] = [
	"pending",
	"ready",
	"starting",
	"running",
	"blocked",
	"succeeded",
	"failed",
	"cancelled",
	"skipped",
];

/** Human-readable labels for the status legend. */
function nodeStateLabel(state: NodeState): string {
  return statusLabel(state);
}

function nodeAriaLabel(node: NodeLayout): string {
  const semantics = nodeExecutionSemantics(node.ownership, node.role);
  const parts = [
    `${nodeLabel(node.id)}.`,
    `Status ${nodeStateLabel(node.state)}.`,
    `Execution mode ${semantics.executionModeLabel}.`,
    `Role ${semantics.roleLabel}.`,
  ];
  return parts.join(" ");
}

// ---------------------------------------------------------------------------
// Role icon SVG data (Lucide 24×24 viewBox paths)
//
// Each entry is an array of [element, attrs] tuples matching Lucide's iconNode
// format.  We render these via D3 inside a scaled <g> so they appear as small
// inline icons on each DAG node.
// ---------------------------------------------------------------------------

type IconElement = [string, Record<string, string>];

const ROLE_ICON_PATHS: Record<string, IconElement[]> = {
  king: [
    ["path", { d: "M11.562 3.266a.5.5 0 0 1 .876 0L15.39 8.87a1 1 0 0 0 1.516.294L21.183 5.5a.5.5 0 0 1 .798.519l-2.834 10.246a1 1 0 0 1-.956.734H5.81a1 1 0 0 1-.957-.734L2.02 6.02a.5.5 0 0 1 .798-.519l4.276 3.664a1 1 0 0 0 1.516-.294z" }],
    ["path", { d: "M5 21h14" }],
  ],
  chancellor: [
    ["path", { d: "M15 12h-5" }],
    ["path", { d: "M15 8h-5" }],
    ["path", { d: "M19 17V5a2 2 0 0 0-2-2H4" }],
    ["path", { d: "M8 21h12a2 2 0 0 0 2-2v-1a1 1 0 0 0-1-1H11a1 1 0 0 0-1 1v1a2 2 0 1 1-4 0V5a2 2 0 1 0-4 0v2a1 1 0 0 0 1 1h3" }],
  ],
  "council-member": [
    ["rect", { width: "8", height: "4", x: "8", y: "2", rx: "1", ry: "1" }],
    ["path", { d: "M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2" }],
    ["path", { d: "M12 11h4" }],
    ["path", { d: "M12 16h4" }],
    ["path", { d: "M8 11h.01" }],
    ["path", { d: "M8 16h.01" }],
  ],
  "noble-delegate": [
    ["path", { d: "M10 18v-7" }],
    ["path", { d: "M11.12 2.198a2 2 0 0 1 1.76.006l7.866 3.847c.476.233.31.949-.22.949H3.474c-.53 0-.695-.716-.22-.949z" }],
    ["path", { d: "M14 18v-7" }],
    ["path", { d: "M18 18v-7" }],
    ["path", { d: "M3 22h18" }],
    ["path", { d: "M6 18v-7" }],
  ],
  "clergy-delegate": [
    ["path", { d: "M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z" }],
    ["path", { d: "m9 12 2 2 4-4" }],
  ],
  "burgher-delegate": [
    ["path", { d: "M15 21v-5a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v5" }],
    ["path", { d: "M17.774 10.31a1.12 1.12 0 0 0-1.549 0 2.5 2.5 0 0 1-3.451 0 1.12 1.12 0 0 0-1.548 0 2.5 2.5 0 0 1-3.452 0 1.12 1.12 0 0 0-1.549 0 2.5 2.5 0 0 1-3.77-3.248l2.889-4.184A2 2 0 0 1 7 2h10a2 2 0 0 1 1.653.873l2.895 4.192a2.5 2.5 0 0 1-3.774 3.244" }],
    ["path", { d: "M4 10.95V19a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8.05" }],
  ],
  "peasant-delegate": [
    ["path", { d: "M2 22 16 8" }],
    ["path", { d: "M3.47 12.53 5 11l1.53 1.53a3.5 3.5 0 0 1 0 4.94L5 19l-1.53-1.53a3.5 3.5 0 0 1 0-4.94Z" }],
    ["path", { d: "M7.47 8.53 9 7l1.53 1.53a3.5 3.5 0 0 1 0 4.94L9 15l-1.53-1.53a3.5 3.5 0 0 1 0-4.94Z" }],
    ["path", { d: "M11.47 4.53 13 3l1.53 1.53a3.5 3.5 0 0 1 0 4.94L13 11l-1.53-1.53a3.5 3.5 0 0 1 0-4.94Z" }],
    ["path", { d: "M20 2h2v2a4 4 0 0 1-4 4h-2V6a4 4 0 0 1 4-4Z" }],
    ["path", { d: "M11.47 17.47 13 19l-1.53 1.53a3.5 3.5 0 0 1-4.94 0L5 19l1.53-1.53a3.5 3.5 0 0 1 4.94 0Z" }],
    ["path", { d: "M15.47 13.47 17 15l-1.53 1.53a3.5 3.5 0 0 1-4.94 0L9 15l1.53-1.53a3.5 3.5 0 0 1 4.94 0Z" }],
    ["path", { d: "M19.47 9.47 21 11l-1.53 1.53a3.5 3.5 0 0 1-4.94 0L13 11l1.53-1.53a3.5 3.5 0 0 1 4.94 0Z" }],
  ],
  "builder-berserker": [
    ["path", { d: "m14 12-8.381 8.38a1 1 0 0 1-3.001-3L11 9" }],
    ["path", { d: "M15 15.5a.5.5 0 0 0 .5.5A6.5 6.5 0 0 0 22 9.5a.5.5 0 0 0-.5-.5h-1.672a2 2 0 0 1-1.414-.586l-5.062-5.062a1.205 1.205 0 0 0-1.704 0L9.352 5.648a1.205 1.205 0 0 0 0 1.704l5.062 5.062A2 2 0 0 1 15 13.828z" }],
  ],
  "reviewer-shieldmaiden": [
    ["path", { d: "M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z" }],
  ],
  "intelligence-qa-scout": [
    ["path", { d: "m21 21-4.34-4.34" }],
    ["circle", { cx: "11", cy: "11", r: "8" }],
  ],
  "intelligence-nightraven": [
    ["path", { d: "M16 7h.01" }],
    ["path", { d: "M3.4 18H12a8 8 0 0 0 8-8V7a4 4 0 0 0-7.28-2.3L2 20" }],
    ["path", { d: "m20 7 2 .5-2 .5" }],
    ["path", { d: "M10 18v3" }],
    ["path", { d: "M14 17.75V21" }],
    ["path", { d: "M7 18a6 6 0 0 0 3.84-10.61" }],
  ],
  "intelligence-loki-chaos": [
    ["path", { d: "M12 3q1 4 4 6.5t3 5.5a1 1 0 0 1-14 0 5 5 0 0 1 1-3 1 1 0 0 0 5 0c0-2-1.5-3-1.5-5q0-2 2.5-4" }],
  ],
};

/** Fallback icon (crown) for roles not in the map. */
const FALLBACK_ICON: IconElement[] = ROLE_ICON_PATHS.king;

/** Icon render size in the 24-unit viewBox coordinate space → target pixel size. */
const ICON_SIZE = 12;
const ICON_SCALE = ICON_SIZE / 24;

// ---------------------------------------------------------------------------
// Layout types & computation
// ---------------------------------------------------------------------------

interface NodeLayout {
  id: string;
  x: number;
  y: number;
  layer: number;
  role: string;
  ownership: RunGraphNode["execution_ownership"];
  state: NodeState;
}

/**
 * Compute a layered DAG layout via Kahn's algorithm (topological sort).
 *
 * Handles:
 * - Empty graphs (returns zero-size layout).
 * - Disconnected nodes (placed in layer 0 if no in-edges).
 * - Single-node graphs.
 */
function computeLayout(graph: RunGraphResponse): {
  nodes: NodeLayout[];
  width: number;
  height: number;
} {
  const { nodes, edges } = graph;

  if (nodes.length === 0) {
    return { nodes: [], width: 0, height: 0 };
  }

  // Build adjacency & in-degree maps.
  const adjacency = new Map<string, string[]>();
  const inDegree = new Map<string, number>();

  for (const n of nodes) {
    adjacency.set(n.node_id, []);
    inDegree.set(n.node_id, 0);
  }

  for (const e of edges) {
    adjacency.get(e.from)?.push(e.to);
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1);
  }

  // Kahn's topological sort into layers.
  const layers: string[][] = [];
  let queue = nodes
    .filter((n) => (inDegree.get(n.node_id) ?? 0) === 0)
    .map((n) => n.node_id);
  const visited = new Set<string>();

  while (queue.length > 0) {
    layers.push([...queue]);
    const nextQueue: string[] = [];

    for (const id of queue) {
      visited.add(id);
      for (const neighbor of adjacency.get(id) ?? []) {
        const newDeg = (inDegree.get(neighbor) ?? 1) - 1;
        inDegree.set(neighbor, newDeg);
        if (newDeg === 0 && !visited.has(neighbor)) {
          nextQueue.push(neighbor);
        }
      }
    }

    queue = nextQueue;
  }

  // Catch any nodes missed by the topological sort (e.g. in a cycle — shouldn't
  // happen in a well-formed DAG, but let's be defensive).
  for (const n of nodes) {
    if (!visited.has(n.node_id)) {
      if (layers.length === 0) layers.push([]);
      layers[layers.length - 1].push(n.node_id);
    }
  }

  // Build a lookup from node_id → layer index for edge routing later.
  const nodeLayerMap = new Map<string, number>();
  for (let l = 0; l < layers.length; l++) {
    for (const id of layers[l]) {
      nodeLayerMap.set(id, l);
    }
  }

  // Position nodes centered per layer.
  const nodeMap = new Map(nodes.map((n) => [n.node_id, n]));
  const layoutNodes: NodeLayout[] = [];
  let maxLayerWidth = 0;

  for (let layerIdx = 0; layerIdx < layers.length; layerIdx++) {
    const layer = layers[layerIdx];
    maxLayerWidth = Math.max(maxLayerWidth, layer.length);

    for (let i = 0; i < layer.length; i++) {
      const nodeId = layer[i];
      const nodeData = nodeMap.get(nodeId)!;
      const totalWidth =
        layer.length * (NODE_WIDTH + HORIZONTAL_GAP) - HORIZONTAL_GAP;
      const startX = -totalWidth / 2;

      layoutNodes.push({
        id: nodeId,
        x: startX + i * (NODE_WIDTH + HORIZONTAL_GAP),
        y: layerIdx * (NODE_HEIGHT + VERTICAL_GAP),
        layer: layerIdx,
        role: nodeData.required_role,
        ownership: nodeData.execution_ownership,
        state: nodeData.state,
      });
    }
  }

  const totalHeight = layers.length * (NODE_HEIGHT + VERTICAL_GAP);
  const totalWidth = maxLayerWidth * (NODE_WIDTH + HORIZONTAL_GAP);

  return { nodes: layoutNodes, width: totalWidth, height: totalHeight };
}

// ---------------------------------------------------------------------------
// Edge path generation
// ---------------------------------------------------------------------------

/**
 * Build a smooth cubic Bézier `d` attribute for an edge.
 *
 * For edges that span >1 layer gap, we insert intermediate control points so
 * the curve stays visually smooth instead of becoming a steep diagonal.
 */
function edgePath(
  from: NodeLayout,
  to: NodeLayout,
): string {
  const x1 = from.x + NODE_WIDTH / 2;
  const y1 = from.y + NODE_HEIGHT;
  const x2 = to.x + NODE_WIDTH / 2;
  const y2 = to.y - 6;

  const layerSpan = to.layer - from.layer;

  if (layerSpan <= 1) {
    // Simple single-span cubic Bézier.
    const cy1 = y1 + (y2 - y1) * 0.4;
    const cy2 = y1 + (y2 - y1) * 0.6;
    return `M ${x1} ${y1} C ${x1} ${cy1}, ${x2} ${cy2}, ${x2} ${y2}`;
  }

  // Multi-layer span: build a smooth poly-Bézier through intermediate points.
  // We create guide points at each intermediate layer boundary, then fit
  // smooth cubic segments through them.
  const points: [number, number][] = [[x1, y1]];

  for (let l = 1; l < layerSpan; l++) {
    const t = l / layerSpan;
    // Linearly interpolate X, use layer-based Y.
    const ix = x1 + (x2 - x1) * t;
    const iy = y1 + (y2 - y1) * t;
    points.push([ix, iy]);
  }

  points.push([x2, y2]);

  // Build a catmull-rom–like smooth path using D3's curveBasis.
  const lineGen = d3
    .line<[number, number]>()
    .x((d) => d[0])
    .y((d) => d[1])
    .curve(d3.curveBasis);

  return lineGen(points) ?? `M ${x1} ${y1} L ${x2} ${y2}`;
}

// ---------------------------------------------------------------------------
// Connectivity helpers (for selection dimming)
// ---------------------------------------------------------------------------

/** Returns sets of connected node ids and edge keys for a given node. */
function connectedSet(
  nodeId: string,
  edges: RunGraphResponse["edges"],
): { nodeIds: Set<string>; edgeKeys: Set<string> } {
  const nodeIds = new Set<string>([nodeId]);
  const edgeKeys = new Set<string>();

  for (const e of edges) {
    if (e.from === nodeId || e.to === nodeId) {
      nodeIds.add(e.from);
      nodeIds.add(e.to);
      edgeKeys.add(`${e.from}->${e.to}`);
    }
  }

  return { nodeIds, edgeKeys };
}

function toD3Transform(transform: ViewTransform): d3.ZoomTransform {
  return d3.zoomIdentity.translate(transform.x, transform.y).scale(transform.k);
}

function zoomExtentForSvg(svg: SVGSVGElement): [[number, number], [number, number]] {
  const rect = svg.getBoundingClientRect();
  const width = Math.max(rect.width || svg.clientWidth || 1, 1);
  const height = Math.max(rect.height || svg.clientHeight || 1, 1);
  return [[0, 0], [width, height]];
}

type EdgeLayout = {
  key: string;
  fromNode: NodeLayout;
  toNode: NodeLayout;
};

type DagStructure = {
  root: d3.Selection<SVGGElement, unknown, null, undefined>;
  background: d3.Selection<SVGRectElement, unknown, null, undefined>;
  edges: d3.Selection<SVGGElement, unknown, null, undefined>;
  nodes: d3.Selection<SVGGElement, unknown, null, undefined>;
  tooltip: d3.Selection<SVGGElement, unknown, null, undefined>;
  tooltipBg: d3.Selection<SVGRectElement, unknown, null, undefined>;
  tooltipText: d3.Selection<SVGTextElement, unknown, null, undefined>;
  emptyState: d3.Selection<SVGTextElement, unknown, null, undefined>;
};

function edgeKey(edge: Pick<RunGraphResponse["edges"][number], "from" | "to">): string {
  return `${edge.from}->${edge.to}`;
}

function edgeStrokeColor(state: NodeState): string {
  const color = RUN_STATUS_HEX[state as keyof typeof RUN_STATUS_HEX];
  return color
    ? d3.color(color)!.copy({ opacity: 0.45 }).formatRgb()
    : RUN_DETAIL_NEUTRAL_HEX.border;
}

function edgeMarkerId(state: NodeState): string {
  return RUN_STATUS_HEX[state as keyof typeof RUN_STATUS_HEX]
    ? `arrow-${state}`
    : "arrow-neutral";
}

function createDagStructure(
  svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
): DagStructure {
  svg.selectAll("*").remove();
  svg
    .append("desc")
    .text("Interactive workflow graph showing run nodes, their current states, and dependencies.");

  const defs = svg.append("defs");
  const gridSize = 20;
  defs
    .append("pattern")
    .attr("id", "dag-grid")
    .attr("width", gridSize)
    .attr("height", gridSize)
    .attr("patternUnits", "userSpaceOnUse")
    .append("path")
    .attr("d", `M ${gridSize} 0 L 0 0 0 ${gridSize}`)
    .attr("fill", "none")
    .attr("stroke", RUN_DETAIL_NEUTRAL_HEX.border)
    .attr("stroke-width", 0.5)
    .attr("stroke-opacity", 0.4);

  for (const [state, color] of Object.entries(RUN_STATUS_HEX)) {
    defs
      .append("marker")
      .attr("id", `arrow-${state}`)
      .attr("viewBox", "0 0 10 10")
      .attr("refX", 5)
      .attr("refY", 5)
      .attr("markerWidth", 7)
      .attr("markerHeight", 7)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M 0 0 L 10 5 L 0 10 z")
      .attr("fill", d3.color(color)!.copy({ opacity: 0.6 }).formatRgb());
  }

  defs
    .append("marker")
    .attr("id", "arrow-neutral")
    .attr("viewBox", "0 0 10 10")
    .attr("refX", 5)
    .attr("refY", 5)
    .attr("markerWidth", 7)
    .attr("markerHeight", 7)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M 0 0 L 10 5 L 0 10 z")
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.border);

  defs.append("style").text(`
      @keyframes dag-pulse-fail {
        0%, 100% { stroke-opacity: 1; }
        50% { stroke-opacity: 0.3; }
      }
      .dag-node-failed-border {
        animation: dag-pulse-fail 1.6s ease-in-out infinite;
      }
      .dag-text {
        text-rendering: optimizeLegibility;
        -webkit-font-smoothing: antialiased;
      }
    `);

  const emptyState = svg
    .append("text")
    .attr("x", 200)
    .attr("y", 100)
    .attr("text-anchor", "middle")
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.subtle)
    .attr("font-size", "14px")
    .attr("font-family", "'Inter', system-ui, sans-serif")
    .style("display", "none")
    .text("No nodes in this graph.");

  const root = svg.append("g");
  const background = root.append("rect").attr("fill", "url(#dag-grid)");
  const edges = root.append("g").attr("class", "dag-edges");
  const nodes = root.append("g").attr("class", "dag-nodes");
  const tooltip = root.append("g").attr("class", "dag-tooltip").style("display", "none");
  const tooltipBg = tooltip
    .append("rect")
    .attr("rx", 6)
    .attr("ry", 6)
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.surface)
    .attr("stroke", RUN_DETAIL_NEUTRAL_HEX.border)
    .attr("stroke-width", 1);
  const tooltipText = tooltip
    .append("text")
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.text)
    .attr("font-size", "11px")
    .attr("font-family", "'Inter', system-ui, sans-serif")
    .attr("class", "dag-text");

  return {
    root,
    background,
    edges,
    nodes,
    tooltip,
    tooltipBg,
    tooltipText,
    emptyState,
  };
}

function renderDagNode(
  nodeG: d3.Selection<SVGGElement, NodeLayout, null, undefined>,
  node: NodeLayout,
  showTooltip: (node: NodeLayout) => void,
  hideTooltip: () => void,
  toggleNodeSelection: (nodeId: string) => void,
) {
  nodeG
    .attr("data-node", node.id)
    .attr("tabindex", 0)
    .attr("role", "button")
    .attr("aria-label", nodeAriaLabel(node))
    .attr("aria-pressed", "false")
    .style("cursor", "pointer");

	const statusColor = RUN_STATUS_HEX[node.state as keyof typeof RUN_STATUS_HEX] ?? RUN_DETAIL_NEUTRAL_HEX.subtle;
	const roleColor = RUN_ROLE_ACCENT_HEX[node.role] ?? RUN_DETAIL_NEUTRAL_HEX.subtle;
	const semantics = nodeExecutionSemantics(node.ownership, node.role);
	const isActive = node.state === "starting" || node.state === "running";
	const isFailed = node.state === "failed";
	const isSkipped = node.state === "skipped";

  const bgRect = nodeG
    .selectAll<SVGRectElement, NodeLayout>('rect[data-part="bg"]')
    .data([node])
    .join("rect")
    .attr("data-part", "bg")
    .attr("x", node.x)
    .attr("y", node.y)
    .attr("width", NODE_WIDTH)
		.attr("height", NODE_HEIGHT)
		.attr("rx", 6)
		.attr("ry", 6)
		.attr("fill", RUN_DETAIL_NEUTRAL_HEX.surface)
		.attr("stroke", statusColor)
		.attr("stroke-width", isFailed ? 2 : isActive ? 2 : 1)
		.attr("opacity", isSkipped ? 0.5 : 1)
		.classed("dag-node-failed-border", isFailed)
		.classed("animate-glow-pulse", isActive);

	if (isActive) {
		bgRect.style("--glow-color", roleColor);
	}

  nodeG
    .selectAll<SVGRectElement, NodeLayout>('rect[data-part="role-stripe"]')
    .data([node])
    .join("rect")
    .attr("data-part", "role-stripe")
    .attr("x", node.x)
    .attr("y", node.y + 4)
    .attr("width", ROLE_STRIPE_WIDTH)
    .attr("height", NODE_HEIGHT - 8)
    .attr("rx", 1.5)
    .attr("ry", 1.5)
    .attr("fill", roleColor)
    .attr("opacity", isSkipped ? 0.5 : 0.9);

  const iconData = ROLE_ICON_PATHS[node.role] ?? FALLBACK_ICON;
  const iconCx = node.x + 14;
  const iconCy = node.y + NODE_HEIGHT / 2;
  const iconG = nodeG
    .selectAll<SVGGElement, NodeLayout>('g[data-part="icon"]')
    .data([node])
    .join("g")
    .attr("data-part", "icon")
    .attr(
      "transform",
      `translate(${iconCx - ICON_SIZE / 2}, ${iconCy - ICON_SIZE / 2}) scale(${ICON_SCALE})`,
    );

  iconG
    .selectAll<SVGElement, IconElement>("*")
    .data(iconData)
    .join(
      (enter) => enter.append((d) => document.createElementNS("http://www.w3.org/2000/svg", d[0])),
      (update) => update,
      (exit) => exit.remove(),
    )
    .each(function ([, attrs]) {
      const element = d3.select(this);
      for (const [key, value] of Object.entries(attrs)) {
        if (key !== "key") {
          element.attr(key, value);
        }
      }
    })
    .attr("fill", "none")
    .attr("stroke", roleColor)
    .attr("stroke-width", 2)
    .attr("stroke-linecap", "round")
    .attr("stroke-linejoin", "round")
    .attr("opacity", isSkipped ? 0.5 : 1);

	if (isActive) {
		iconG.classed("animate-glow-pulse", true).style("--glow-color", roleColor);
	} else {
    iconG.classed("animate-glow-pulse", false).style("--glow-color", null);
  }

  const textX = node.x + 26;
  nodeG
    .selectAll<SVGTextElement, NodeLayout>('text[data-part="role-label"]')
    .data([node])
    .join("text")
    .attr("data-part", "role-label")
    .attr("x", textX)
    .attr("y", node.y + 19)
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.text)
    .attr("font-size", "10px")
    .attr("font-weight", "600")
    .attr("dominant-baseline", "middle")
    .attr("opacity", isSkipped ? 0.5 : 1)
    .text(semantics.roleLabel)
    .each(function () {
      applyTextLengthConstraint(this as SVGTextElement, NODE_WIDTH - 30);
    });

  nodeG
    .selectAll<SVGTextElement, NodeLayout>('text[data-part="node-label"]')
    .data([node])
    .join("text")
    .attr("data-part", "node-label")
    .attr("x", textX)
    .attr("y", node.y + 33)
    .attr("fill", RUN_DETAIL_NEUTRAL_HEX.muted)
    .attr("font-size", "9px")
    .attr("font-weight", "400")
    .attr("dominant-baseline", "middle")
    .attr("opacity", isSkipped ? 0.5 : 1)
    .text(nodeLabel(node.id))
    .each(function () {
      applyTextLengthConstraint(this as SVGTextElement, NODE_WIDTH - 30);
    });

  nodeG.on("mouseenter", () => {
    showTooltip(node);
  });

  nodeG.on("mouseleave", () => {
    hideTooltip();
  });

  nodeG.on("focus", () => {
    showTooltip(node);
  });

  nodeG.on("blur", () => {
    hideTooltip();
  });

  nodeG.on("click", (event: MouseEvent) => {
    event.stopPropagation();
    toggleNodeSelection(node.id);
  });

  nodeG.on("keydown", (event: KeyboardEvent) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    toggleNodeSelection(node.id);
  });
}

function applyTextLengthConstraint(el: SVGTextElement, maxWidth: number) {
  el.removeAttribute("textLength");
  el.removeAttribute("lengthAdjust");
  if (el.getComputedTextLength() > maxWidth) {
    el.setAttribute("textLength", String(maxWidth));
    el.setAttribute("lengthAdjust", "spacing");
  }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function DAGVisualization({ graph }: { graph: RunGraphResponse }) {
  const svgRef = useRef<SVGSVGElement>(null);
  const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const gRef = useRef<d3.Selection<SVGGElement, unknown, null, undefined> | null>(null);
  const structureRef = useRef<DagStructure | null>(null);
  const transformRef = useRef<ViewTransform>({ x: 0, y: 0, k: 1 });

  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedNode) {
      return;
    }
    if (graph.nodes.some((node) => node.node_id === selectedNode)) {
      return;
    }
    setSelectedNode(null);
  }, [graph.nodes, selectedNode]);

  // -----------------------------------------------------------------------
  // Zoom control callbacks
  // -----------------------------------------------------------------------

  const zoomIn = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    const svg = d3.select(svgRef.current);
    zoomRef.current.scaleBy(svg.transition().duration(250), ZOOM_STEP);
  }, []);

  const zoomOut = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    const svg = d3.select(svgRef.current);
    zoomRef.current.scaleBy(svg.transition().duration(250), 1 / ZOOM_STEP);
  }, []);

  const zoomReset = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    const svg = d3.select(svgRef.current);
    zoomRef.current.transform(
      svg.transition().duration(350),
      d3.zoomIdentity,
    );
  }, []);

  useEffect(() => {
    if (!svgRef.current) {
      return;
    }

    const svg = d3.select(svgRef.current);
    const structure = createDagStructure(svg);
    structureRef.current = structure;
    gRef.current = structure.root;

    const savedTransform = toD3Transform(transformRef.current);
    structure.root.attr("transform", savedTransform.toString());

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .extent(() => zoomExtentForSvg(svgRef.current!))
      .scaleExtent([ZOOM_MIN, ZOOM_MAX])
      .on("zoom", (event) => {
        transformRef.current = { x: event.transform.x, y: event.transform.y, k: event.transform.k };
        structure.root.attr("transform", event.transform);
      });

    zoomRef.current = zoom;
    svg.call(zoom);
    svg.call(zoom.transform, savedTransform);
    svg.on("click.deselect", () => {
      setSelectedNode(null);
    });

    return () => {
      svg.on(".zoom", null);
      svg.on("click.deselect", null);
      zoomRef.current = null;
      structureRef.current = null;
      gRef.current = null;
    };
  }, []);

  // -----------------------------------------------------------------------
  // Main D3 render
  // -----------------------------------------------------------------------

  useEffect(() => {
    if (!svgRef.current || !structureRef.current) {
      return;
    }

    const svg = d3.select(svgRef.current);
    const structure = structureRef.current;

    const hideTooltip = () => {
      structure.tooltip.style("display", "none");
    };

    if (!graph.nodes.length) {
      transformRef.current = { x: 0, y: 0, k: 1 };
      if (zoomRef.current) {
        svg.call(zoomRef.current.transform, d3.zoomIdentity);
      }
      svg.attr("viewBox", "0 0 400 200");
      structure.root.style("display", "none");
      structure.emptyState.style("display", null);
      structure.edges.selectAll("*").remove();
      structure.nodes.selectAll("*").remove();
      hideTooltip();
      return;
    }

    structure.root.style("display", null);
    structure.emptyState.style("display", "none");

    const layout = computeLayout(graph);
    const posMap = new Map(layout.nodes.map((node) => [node.id, node]));

    const vbX = -layout.width / 2 - VIEW_PADDING;
    const vbY = -VIEW_PADDING;
    const vbW = layout.width + VIEW_PADDING * 2;
    const vbH = layout.height + VIEW_PADDING * 2;
    svg.attr("viewBox", `${vbX} ${vbY} ${vbW} ${vbH}`);

    structure.background
      .attr("x", vbX - 2000)
      .attr("y", vbY - 2000)
      .attr("width", vbW + 4000)
      .attr("height", vbH + 4000);

    const showTooltip = (node: NodeLayout) => {
      structure.tooltipText.selectAll("tspan").remove();
      const semantics = nodeExecutionSemantics(node.ownership, node.role);
      const lines = [
        nodeLabel(node.id),
        `Status: ${nodeStateLabel(node.state)}`,
        `Execution mode: ${semantics.executionModeLabel}`,
        `Role: ${semantics.roleLabel}`,
      ];
      const lineHeight = 15;
      const textPadding = 10;

      for (let index = 0; index < lines.length; index++) {
        structure.tooltipText
          .append("tspan")
          .attr("x", textPadding)
          .attr("dy", index === 0 ? 15 : lineHeight)
          .text(lines[index]);
      }

      const bbox = (structure.tooltipText.node() as SVGTextElement).getBBox();
      const bgW = bbox.width + textPadding * 2;
      const bgH = bbox.height + textPadding + 4;

      structure.tooltipBg.attr("width", bgW).attr("height", bgH);
      structure.tooltip.attr(
        "transform",
        `translate(${node.x + NODE_WIDTH / 2 - bgW / 2}, ${node.y - bgH - 8})`,
      );
      structure.tooltip.style("display", null);
    };

    const toggleNodeSelection = (nodeId: string) => {
      setSelectedNode((prev) => (prev === nodeId ? null : nodeId));
    };

    const layoutEdges: EdgeLayout[] = graph.edges.flatMap((edge) => {
      const fromNode = posMap.get(edge.from);
      const toNode = posMap.get(edge.to);
      if (!fromNode || !toNode) {
        return [];
      }
      return [{ key: edgeKey(edge), fromNode, toNode }];
    });

    const edges = structure.edges
      .selectAll<SVGPathElement, EdgeLayout>("path[data-edge]")
      .data(layoutEdges, (edge) => edge.key);

    edges.exit().remove();

    edges
      .enter()
      .append("path")
      .attr("fill", "none")
      .attr("stroke-width", 1.5)
      .merge(edges)
      .attr("data-edge", (edge) => edge.key)
      .attr("d", (edge) => edgePath(edge.fromNode, edge.toNode))
      .attr("stroke", (edge) => edgeStrokeColor(edge.toNode.state))
      .attr("marker-end", (edge) => `url(#${edgeMarkerId(edge.toNode.state)})`);

    const nodes = structure.nodes
      .selectAll<SVGGElement, NodeLayout>("g[data-node]")
      .data(layout.nodes, (node) => node.id);

    nodes.exit().remove();

    nodes
      .enter()
      .append("g")
      .merge(nodes)
      .each(function (node) {
        renderDagNode(
          d3.select(this),
          node,
          showTooltip,
          hideTooltip,
          toggleNodeSelection,
        );
      });

    hideTooltip();
  }, [graph]);

  // -----------------------------------------------------------------------
  // Selection highlight effect (runs when selectedNode changes)
  // -----------------------------------------------------------------------

  useEffect(() => {
    if (!svgRef.current || !gRef.current) return;

    const g = gRef.current;

    if (!selectedNode) {
      // Reset all opacities.
      g.selectAll("[data-node]")
        .attr("opacity", 1)
        .attr("aria-pressed", "false");
      g.selectAll("[data-edge]").attr("opacity", 1);
      return;
    }

    const { nodeIds, edgeKeys } = connectedSet(selectedNode, graph.edges);

    // Dim unconnected nodes.
    g.selectAll<SVGGElement, unknown>("[data-node]").attr("opacity", function () {
      const nid = d3.select(this).attr("data-node");
      return nodeIds.has(nid) ? 1 : 0.15;
    });
    g.selectAll<SVGGElement, unknown>("[data-node]").attr("aria-pressed", function () {
      const nid = d3.select(this).attr("data-node");
      return nid === selectedNode ? "true" : "false";
    });

    // Dim unconnected edges.
    g.selectAll<SVGPathElement, unknown>("[data-edge]").attr("opacity", function () {
      const ekey = d3.select(this).attr("data-edge");
      return edgeKeys.has(ekey) ? 1 : 0.1;
    });
  }, [selectedNode, graph.nodes, graph.edges]);

  // -----------------------------------------------------------------------
  // Render
  // -----------------------------------------------------------------------

  return (
    <div data-testid="dag-visualization-root" className="flex h-full min-h-0 w-full flex-col">
      {/* SVG container */}
      <div className="relative min-h-0 flex-1 overflow-hidden rounded-lg border border-border bg-[#0a0e17]">
        <DAGLegend statuses={legendStatusesForNodes(graph.nodes)} />
        <svg
          ref={svgRef}
          className="h-full w-full"
          role="img"
          aria-label="Run execution DAG"
        />
      </div>

      {/* Zoom controls */}
      <div className="mt-2 flex shrink-0 items-center gap-2">
        <button
          type="button"
          onClick={zoomIn}
          className="rounded border border-border bg-surface px-3 py-1 text-xs text-muted transition-colors hover:border-[#c9a84c] hover:text-[#e8e6e1]"
        >
          Zoom In
        </button>
        <button
          type="button"
          onClick={zoomOut}
          className="rounded border border-border bg-surface px-3 py-1 text-xs text-muted transition-colors hover:border-[#c9a84c] hover:text-[#e8e6e1]"
        >
          Zoom Out
        </button>
        <button
          type="button"
          onClick={zoomReset}
          className="rounded border border-border bg-surface px-3 py-1 text-xs text-muted transition-colors hover:border-[#c9a84c] hover:text-[#e8e6e1]"
        >
          Reset
        </button>
        {selectedNode && (
          <span className="ml-auto text-xs text-muted">
            Selected: <span className="text-[#c9a84c]">{selectedNode}</span>
          </span>
        )}
      </div>
    </div>
  );
}

function legendStatusesForNodes(nodes: RunGraphNode[]): NodeState[] {
	const present = new Set(nodes.map((node) => node.state));
	return STATUS_LEGEND_ORDER.filter((status) => present.has(status));
}

function DAGLegend({ statuses }: { statuses: NodeState[] }) {
  return (
    <div
      data-testid="dag-legend"
      className="pointer-events-none absolute bottom-3 right-3 z-10 rounded-md border border-border bg-surface/95 px-2.5 py-1.5 shadow-sm backdrop-blur-sm sm:bottom-4 sm:right-4 sm:rounded-xl sm:px-4 sm:py-3 sm:shadow-lg"
      aria-label="DAG status legend"
    >
      <div className="space-y-1 sm:space-y-2">
				{statuses.map((status) => (
          <div key={status} className="flex items-center gap-2 text-[11px] font-medium leading-none text-foreground sm:gap-3 sm:text-sm">
            <span
              className="h-1.5 w-1.5 shrink-0 rounded-full sm:h-2.5 sm:w-2.5"
              style={{ backgroundColor: RUN_STATUS_HEX[status] }}
              aria-hidden
            />
            <span>{statusLabel(status)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
