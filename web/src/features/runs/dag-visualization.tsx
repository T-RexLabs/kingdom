import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import * as d3 from "d3";
import { Maximize2, Minus, Plus } from "lucide-react";
import type { RunGraphResponse, RunNodeExecution } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { ThemeIcon, useTheme, roleIdentity, type IconSlot } from "@/theme";
import { cn, nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { layoutRunGraph, observedProviderRunning } from "./run-graph-model";

export function DAGVisualization({
  graph,
  nodes,
  onViewActivity,
  selectedNodeId,
  connected = false,
  executionsErrorMessage,
  onRetryExecutions,
}: {
  graph: RunGraphResponse;
  nodes: RunNodeExecution[];
  onViewActivity?: (nodeId: string) => void;
  selectedNodeId?: string | null;
  connected?: boolean;
  executionsErrorMessage?: string | null;
  onRetryExecutions?: () => void;
}) {
  const { theme } = useTheme();
  const layout = useMemo(() => layoutRunGraph(graph), [graph]);
  const svgRef = useRef<SVGSVGElement>(null);
  const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const viewportRef = useRef<HTMLDivElement>(null);
  const [fitOverview, setFitOverview] = useState(false);
  const [transform, setTransform] = useState(d3.zoomIdentity);
  const markerId = useId().replaceAll(":", "");
  const activeNodes = new Set(
    nodes
      .filter((node) =>
        observedProviderRunning(node.current_execution, connected),
      )
      .map((node) => node.node_id),
  );
  const selected = graph.nodes.find((node) => node.node_id === selectedNodeId);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .extent([
        [0, 0],
        [layout.width, layout.height],
      ])
      .scaleExtent([0.5, 3])
      .filter(
        (event: Event) =>
          !(
            event.target instanceof Element && event.target.closest("button")
          ) &&
          (event.type !== "wheel" ||
            (event as WheelEvent).ctrlKey ||
            (event as WheelEvent).metaKey),
      )
      .on("zoom", (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) =>
        setTransform(event.transform),
      );
    zoomRef.current = zoom;
    svg.call(zoom).on("dblclick.zoom", null);
    return () => {
      svg.on(".zoom", null);
      zoomRef.current = null;
    };
  }, [layout.width, layout.height]);

  const changeZoom = useCallback((factor?: number) => {
    if (!svgRef.current || !zoomRef.current) return;
    const svg = d3.select(svgRef.current);
    if (factor) svg.call(zoomRef.current.scaleBy, factor);
    else {
      setFitOverview(true);
      svg.call(zoomRef.current.transform, d3.zoomIdentity);
      if (viewportRef.current) {
        viewportRef.current.scrollTop = 0;
        viewportRef.current.scrollLeft = 0;
      }
    }
  }, []);

  return (
    <section
      data-testid="dag-visualization-root"
      className="ui-dag-canvas overflow-hidden rounded-2xl border border-border bg-surface/50"
      aria-label="Workflow graph"
    >
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border/70 px-5 py-3">
        <div>
          <h2 className="text-sm font-medium">Workflow</h2>
          <p className="mt-1 text-xs text-foreground-muted">
            Select a step to inspect its activity.
          </p>
        </div>
        <div className="flex items-center gap-1">
          <Button
            size="sm"
            variant="ghost"
            aria-label="Zoom out"
            onClick={() => changeZoom(1 / 1.3)}
          >
            <Minus size={15} />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label="Zoom in"
            onClick={() => changeZoom(1.3)}
          >
            <Plus size={15} />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label="Fit graph"
            onClick={() => changeZoom()}
          >
            <Maximize2 size={15} />
            <span>Fit</span>
          </Button>
        </div>
      </div>
      {executionsErrorMessage && (
        <div
          role="alert"
          className="flex flex-wrap items-center justify-between gap-2 px-5 py-3 text-xs text-status-blocked"
        >
          <span>Execution status unavailable. {executionsErrorMessage}</span>
          <button
            type="button"
            className="min-h-9 underline"
            onClick={onRetryExecutions}
          >
            Retry Step List
          </button>
        </div>
      )}
      {layout.nodes.length === 0 ? (
        <p className="p-8 text-sm text-foreground-muted">
          No workflow steps have been recorded yet.
        </p>
      ) : (
        <div
          ref={viewportRef}
          className="mx-auto max-h-[610px] max-w-[800px] overflow-auto px-2 py-1"
        >
          <svg
            ref={svgRef}
            viewBox={`0 0 ${layout.width} ${layout.height}`}
            className="mx-auto block touch-pan-y"
            data-view={fitOverview ? "overview" : "readable"}
            style={
              fitOverview
                ? { width: "100%", height: Math.min(610, layout.height) }
                : {
                    width: layout.width,
                    height: layout.height,
                    maxWidth: "none",
                  }
            }
            role="group"
            aria-label="Run execution DAG"
            aria-describedby={`${markerId}-help`}
            onKeyDown={(event) => {
              if (event.target !== event.currentTarget) return;
              const pan = {
                ArrowLeft: [30, 0],
                ArrowRight: [-30, 0],
                ArrowUp: [0, 30],
                ArrowDown: [0, -30],
              }[event.key];
              if (pan && zoomRef.current) {
                event.preventDefault();
                d3.select(event.currentTarget).call(
                  zoomRef.current.translateBy,
                  pan[0],
                  pan[1],
                );
              }
              if (event.key === "+" || event.key === "=") changeZoom(1.3);
              if (event.key === "-") changeZoom(1 / 1.3);
              if (event.key === "0") changeZoom();
            }}
            tabIndex={0}
          >
            <defs>
              <marker
                id={markerId}
                markerWidth="5"
                markerHeight="5"
                refX="4"
                refY="2.5"
                orient="auto"
              >
                <path d="M0,0 L5,2.5 L0,5" fill="context-stroke" />
              </marker>
            </defs>
            <g transform={transform.toString()}>
              {layout.edges.map((edge) => (
                <path
                  key={edge.key}
                  d={edge.path}
                  fill="none"
                  markerEnd={`url(#${markerId})`}
                  strokeWidth={activeNodes.has(edge.to) ? 2 : 1.2}
                  stroke={
                    activeNodes.has(edge.to)
                      ? "var(--color-status-running)"
                      : "var(--color-border)"
                  }
                  strokeDasharray={edge.backward ? "4 4" : undefined}
                  data-active={activeNodes.has(edge.to) || undefined}
                  className={
                    activeNodes.has(edge.to)
                      ? "motion-safe:animate-pulse"
                      : undefined
                  }
                  style={
                    activeNodes.has(edge.to)
                      ? {
                          filter:
                            "drop-shadow(0 0 4px var(--color-status-running))",
                        }
                      : undefined
                  }
                >
                  <title>{`${nodeLabel(edge.from)} → ${nodeLabel(edge.to)}${edge.on ? ` · ${edge.on}` : ""}`}</title>
                </path>
              ))}
              {layout.nodes.map((node) => {
                const runtime = node.execution_ownership === "runtime_owned";
                const identity = roleIdentity(theme, node.required_role);
                const label = runtime ? nodeLabel(node.node_id) : identity.name;
                const icon = runtime
                  ? (`node.${node.node_id.replaceAll("_", "-")}` as IconSlot)
                  : (`role.${node.required_role}` as const);
                const active = activeNodes.has(node.node_id);
                return (
                  <foreignObject
                    key={node.node_id}
                    x={node.x}
                    y={node.y}
                    width={node.width}
                    height={node.height}
                    className="overflow-visible"
                  >
                    <button
                      type="button"
                      onClick={() => onViewActivity?.(node.node_id)}
                      aria-pressed={selectedNodeId === node.node_id}
                      aria-label={`${nodeLabel(node.node_id)}. ${runtime ? "Runtime step" : identity.canonicalName}. Status ${statusLabel(node.state)}. View activity.`}
                      title={`${nodeLabel(node.node_id)} · ${runtime ? "Runtime" : identity.canonicalName} · ${statusLabel(node.state)}`}
                      data-node-id={node.node_id}
                      data-active={active || undefined}
                      className={cn(
                        "ui-dag-node flex h-full w-full items-center gap-2 rounded-lg border px-2.5 text-left transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent",
                        runtime
                          ? "bg-surface text-foreground-muted"
                          : "bg-surface-raised text-foreground",
                        selectedNodeId === node.node_id
                          ? "border-accent ring-1 ring-accent/25"
                          : active
                            ? "border-status-running/70"
                            : "border-border",
                        active && "motion-safe:animate-pulse",
                      )}
                      style={
                        active
                          ? {
                              boxShadow:
                                "var(--ui-dag-active-shadow, 0 0 16px color-mix(in srgb, var(--color-status-running) 16%, transparent))",
                            }
                          : undefined
                      }
                    >
                      <ThemeIcon
                        slot={icon}
                        size={runtime ? 12 : 18}
                        className={
                          active ? "text-status-running" : "text-accent/80"
                        }
                      />
                      <span className="min-w-0 flex-1">
                        <span
                          className={cn(
                            "block truncate font-medium",
                            "text-xs",
                          )}
                        >
                          {label}
                        </span>
                        {!runtime && (
                          <span className="mt-1 flex items-center gap-1 text-[10px] text-foreground-muted">
                            <span
                              className={cn(
                                "h-1 w-1 rounded-full",
                                active ? "bg-status-running" : "bg-current",
                              )}
                            />
                            {statusLabel(node.state)}
                          </span>
                        )}
                      </span>
                      {runtime && (
                        <span className="text-[10px]">
                          {statusLabel(node.state)}
                        </span>
                      )}
                    </button>
                  </foreignObject>
                );
              })}
            </g>
          </svg>
        </div>
      )}
      <p
        id={`${markerId}-help`}
        className="border-t border-border/70 px-5 py-2 text-[11px] text-foreground-muted"
      >
        Scroll to explore. Drag or use arrow keys to pan. Ctrl/⌘ + wheel or +/−
        to zoom; 0 to fit.
      </p>
      {selected && (
        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/70 px-5 py-2.5 text-xs">
          <span>
            {nodeLabel(selected.node_id)}{" "}
            <span className="text-foreground-muted">
              ·{" "}
              {selected.execution_ownership === "runtime_owned"
                ? "Runtime Step"
                : roleIdentity(theme, selected.required_role).canonicalName}
            </span>
          </span>
          <span>{statusLabel(selected.state)}</span>
        </div>
      )}
    </section>
  );
}
