import { memo, useMemo } from "react";
import * as d3 from "d3";

interface SparklineProps {
  /** Data points to render */
  data: number[];
  ariaLabel?: string;
  /** Width in pixels */
  width?: number;
  /** Height in pixels */
  height?: number;
  /** Stroke color (CSS value) */
  color?: string;
  /** Whether to show a fill area under the line */
  fill?: boolean;
}

/**
 * Minimal sparkline chart rendered with D3.
 * Shows a tiny line/area chart suitable for inline metric display.
 */
export const Sparkline = memo(function Sparkline({
  data,
  ariaLabel = "Sparkline chart",
  width = 120,
  height = 32,
  color = "var(--color-gold)",
  fill = true,
}: SparklineProps) {
  const hasEnoughData = data.length >= 2;

  const { areaPath, linePath, margin, x, y } = useMemo(() => {
    const nextMargin = { top: 2, right: 2, bottom: 2, left: 2 };
    const w = width - nextMargin.left - nextMargin.right;
    const h = height - nextMargin.top - nextMargin.bottom;

    if (!hasEnoughData) {
      const nextX = d3.scaleLinear().domain([0, 1]).range([0, w]);
      const nextY = d3.scaleLinear().domain([0, 1]).range([h, 0]);
      return {
        areaPath: null,
        linePath: "",
        margin: nextMargin,
        x: nextX,
        y: nextY,
      };
    }

    const nextX = d3
      .scaleLinear()
      .domain([0, data.length - 1])
      .range([0, w]);

    const yMin = Math.min(...data);
    const yMax = Math.max(...data);
    const yPadding = (yMax - yMin) * 0.1 || 1;

    const nextY = d3
      .scaleLinear()
      .domain([yMin - yPadding, yMax + yPadding])
      .range([h, 0]);

    const line = d3
      .line<number>()
      .x((_, i) => nextX(i))
      .y((d) => nextY(d))
      .curve(d3.curveMonotoneX);

    const area = d3
      .area<number>()
      .x((_, i) => nextX(i))
      .y0(h)
      .y1((d) => nextY(d))
      .curve(d3.curveMonotoneX);

    return {
      areaPath: fill ? area(data) ?? "" : null,
      linePath: line(data) ?? "",
      margin: nextMargin,
      x: nextX,
      y: nextY,
    };
  }, [data, fill, hasEnoughData, height, width]);

  if (!hasEnoughData) {
    return (
      <div
        style={{ width, height }}
        className="flex items-center justify-center text-[10px] text-foreground-subtle"
      >
        --
      </div>
    );
  }

  const lastIndex = data.length - 1;
  const lastValue = data[lastIndex];

  return (
    <svg width={width} height={height} role="img" aria-label={ariaLabel}>
      <g transform={`translate(${margin.left},${margin.top})`}>
        {fill && areaPath ? (
          <path
            className="sparkline-area"
            d={areaPath}
            fill={color}
            opacity={0.1}
          />
        ) : null}
        <path
          className="sparkline-line"
          d={linePath}
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          strokeLinecap="round"
        />
        <circle
          className="sparkline-dot"
          cx={x(lastIndex)}
          cy={y(lastValue)}
          r={2}
          fill={color}
        />
      </g>
    </svg>
  );
});

Sparkline.displayName = "Sparkline";
