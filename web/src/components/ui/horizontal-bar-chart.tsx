import { memo, useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";

export interface BarDatum {
  label: string;
  value: number;
  color?: string;
}

interface HorizontalBarChartProps {
  data: BarDatum[];
  ariaLabel?: string;
  /** Width in pixels */
  width?: number;
  /** Height per bar in pixels */
  barHeight?: number;
  /** Empty-state height in pixels */
  emptyHeight?: number;
  /** Format function for value labels */
  formatValue?: (v: number) => string;
}

let labelMeasureContext: CanvasRenderingContext2D | null | undefined;

function measureLabelWidth(label: string): number {
  if (typeof document === "undefined") {
    return label.length * 7;
  }

  if (typeof navigator !== "undefined" && /jsdom/i.test(navigator.userAgent)) {
    return label.length * 7;
  }

  if (labelMeasureContext === undefined) {
    try {
      labelMeasureContext = document.createElement("canvas").getContext("2d");
    } catch {
      labelMeasureContext = null;
    }
  }

  if (!labelMeasureContext) {
    return label.length * 7;
  }

  labelMeasureContext.font = "11px sans-serif";
  return labelMeasureContext.measureText(label).width;
}

/**
 * Horizontal bar chart for role-based breakdowns.
 * Dark theme with accent accent, JetBrains Mono for numbers.
 */
export const HorizontalBarChart = memo(function HorizontalBarChart({
  data,
  ariaLabel = "Horizontal bar chart",
  width,
  barHeight = 28,
  emptyHeight = 250,
  formatValue = (v) => v.toLocaleString(),
}: HorizontalBarChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoWidth, setAutoWidth] = useState(400);

  useEffect(() => {
    if (width !== undefined || !containerRef.current) return;
    const node = containerRef.current;
    const observer = new ResizeObserver((entries) => {
      const next = Math.max(220, Math.floor(entries[0]?.contentRect.width ?? 400));
      setAutoWidth((current) => (current === next ? current : next));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [width]);

  const resolvedWidth = width ?? autoWidth;
  const isEmpty = data.length === 0;

  const height = data.length * barHeight + 8;

  const { margin, x, y } = useMemo(() => {
    const maxLabelWidth = d3.max(data, (datum) => measureLabelWidth(datum.label)) ?? 0;
    const dynamicLeftMargin = Math.max(100, Math.min(240, Math.ceil(maxLabelWidth) + 14));
    const nextMargin = { top: 4, right: 60, bottom: 4, left: dynamicLeftMargin };
    const nextChartWidth = Math.max(40, resolvedWidth - nextMargin.left - nextMargin.right);
    const nextChartHeight = data.length * barHeight;

    return {
      margin: nextMargin,
      x: d3
        .scaleLinear()
        .domain([0, d3.max(data, (datum) => datum.value) || 1])
        .range([0, nextChartWidth]),
      y: d3
        .scaleBand<number>()
        .domain(d3.range(data.length))
        .range([0, nextChartHeight])
        .padding(0.25),
    };
  }, [barHeight, data, resolvedWidth]);

  if (isEmpty) {
    return (
      <div ref={containerRef} className="h-full w-full overflow-hidden">
        <div
          style={{ height: emptyHeight }}
          className="flex h-full w-full items-center justify-center text-xs text-foreground-subtle"
        >
          No data
        </div>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="w-full overflow-hidden">
      <svg width={resolvedWidth} height={height} role="img" aria-label={ariaLabel}>
        <g transform={`translate(${margin.left},${margin.top})`}>
          {data.map((datum, index) => (
            <rect
              key={`${datum.label}-${index}`}
              className="bar"
              x={0}
              y={y(index) ?? 0}
              width={datum.value > 0 ? Math.max(x(datum.value), 2) : 0}
              height={y.bandwidth()}
              rx={3}
              fill={datum.color ?? "var(--color-accent)"}
              opacity={0.8}
            />
          ))}
          {data.map((datum, index) => (
            <text
              key={`label-${datum.label}-${index}`}
              className="label"
              x={-8}
              y={(y(index) ?? 0) + y.bandwidth() / 2}
              dy="0.35em"
              textAnchor="end"
              fill="var(--color-foreground-muted)"
              fontSize="11px"
              fontFamily="var(--font-sans)"
            >
              {datum.label}
            </text>
          ))}
          {data.map((datum, index) => (
            <text
              key={`value-${datum.label}-${index}`}
              className="value"
              x={x(datum.value) + 6}
              y={(y(index) ?? 0) + y.bandwidth() / 2}
              dy="0.35em"
              fill="var(--color-foreground-subtle)"
              fontSize="11px"
              fontFamily="var(--font-mono)"
            >
              {formatValue(datum.value)}
            </text>
          ))}
        </g>
      </svg>
    </div>
  );
});

HorizontalBarChart.displayName = "HorizontalBarChart";
