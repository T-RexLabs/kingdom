import { memo, useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";
import { computeLegendLayout } from "./stacked-bar-chart-layout";

export interface StackedBarDatum {
  label: string;
  values: { key: string; value: number; color: string }[];
}

interface StackedBarChartProps {
  data: StackedBarDatum[];
  ariaLabel?: string;
  width?: number;
  height?: number;
  integerTicks?: boolean;
}

let legendMeasureContext: CanvasRenderingContext2D | null | undefined;

function measureLegendLabelWidth(label: string): number {
  if (typeof document === "undefined") {
    return label.length * 6;
  }

  if (typeof navigator !== "undefined" && /jsdom/i.test(navigator.userAgent)) {
    return label.length * 6;
  }

  if (legendMeasureContext === undefined) {
    try {
      legendMeasureContext = document.createElement("canvas").getContext("2d");
    } catch {
      legendMeasureContext = null;
    }
  }

  if (!legendMeasureContext) {
    return label.length * 6;
  }

  legendMeasureContext.font = "10px sans-serif";
  return legendMeasureContext.measureText(label).width;
}

function toLegendLabel(value: string): string {
  return value
    .split(/[_\s-]+/)
    .map((token) => (token.length > 0 ? token[0].toUpperCase() + token.slice(1) : token))
    .join(" ");
}

function buildIntegerYAxis(maxValue: number, desiredTickCount: number): { domainMax: number; ticks: number[] } {
	const safeMax = Math.max(0, Math.ceil(maxValue));
	const intervalCount = Math.max(1, desiredTickCount - 1);
	const step = Math.max(1, Math.ceil(Math.max(1, safeMax) / intervalCount));
	const domainMax = Math.max(step, step * Math.ceil(Math.max(1, safeMax) / step));
	const ticks: number[] = [];
	for (let tick = 0; tick <= domainMax; tick += step) {
		ticks.push(tick);
	}
	return { domainMax, ticks };
}

/**
 * Stacked bar chart for time series with categories
 * (e.g. run outcomes succeeded/failed/cancelled over time).
 * Dark theme, status colors.
 */
export const StackedBarChart = memo(function StackedBarChart({
  data,
  ariaLabel = "Stacked bar chart",
  width,
  height,
  integerTicks = false,
}: StackedBarChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoWidth, setAutoWidth] = useState(500);
  const [autoHeight, setAutoHeight] = useState(220);

  useEffect(() => {
    if ((width !== undefined && height !== undefined) || !containerRef.current) return;
    const node = containerRef.current;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      if (width === undefined) {
        const nextWidth = Math.max(240, Math.floor(rect.width));
        setAutoWidth((current) => (current === nextWidth ? current : nextWidth));
      }
      if (height === undefined) {
        const nextHeight = Math.max(180, Math.floor(rect.height));
        setAutoHeight((current) => (current === nextHeight ? current : nextHeight));
      }
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [height, width]);

  const resolvedWidth = width ?? autoWidth;
  const resolvedHeight = height ?? autoHeight;
  const isEmpty = data.length === 0;

  const {
    chartHeight,
    chartWidth,
    colorMap,
    legendEntries,
    legendLayout,
    margin,
    segments,
    x,
    xTickValues,
    y,
    yTicks,
  } = useMemo(() => {
    const nextMargin = { top: 26, right: 12, bottom: 30, left: 34 };
    const nextChartWidth = Math.max(0, resolvedWidth - nextMargin.left - nextMargin.right);
    const nextChartHeight = Math.max(0, resolvedHeight - nextMargin.top - nextMargin.bottom);

    const allKeys = Array.from(new Set(data.flatMap((datum) => datum.values.map((value) => value.key))));
    const nextColorMap = new Map<string, string>();
    for (const datum of data) {
      for (const value of datum.values) {
        nextColorMap.set(value.key, value.color);
      }
    }

    const nextX = d3
      .scaleBand<string>()
      .domain(data.map((datum) => datum.label))
      .range([0, nextChartWidth])
      .padding(0.2);

    const maxTotal = d3.max(data, (datum) => datum.values.reduce((sum, value) => sum + value.value, 0)) || 1;
    const integerYAxis = integerTicks ? buildIntegerYAxis(maxTotal, 4) : null;

    const nextY = d3
      .scaleLinear()
      .domain([0, integerYAxis?.domainMax ?? maxTotal * 1.1])
      .range([nextChartHeight, 0]);

    const nextYTicks = integerYAxis?.ticks ?? nextY.ticks(4);
    const maxTicks = Math.max(1, Math.floor(nextChartWidth / 50));
    const tickStep = Math.max(1, Math.ceil(data.length / maxTicks));
    const nextXTickValues = data
      .map((datum) => datum.label)
      .filter((_, index) => index % tickStep === 0);

    const nextSegments = data.flatMap((datum, datumIndex) => {
      let yOffset = 0;
      return allKeys.flatMap((key) => {
        const match = datum.values.find((value) => value.key === key);
        const value = match?.value ?? 0;
        const currentOffset = yOffset;
        yOffset += value;
        if (value <= 0) {
          return [];
        }

        return [{
          key: `${datum.label}-${datumIndex}-${key}`,
          label: datum.label,
          seriesKey: key,
          x: nextX(datum.label) ?? 0,
          y: nextY(currentOffset + value),
          width: nextX.bandwidth(),
          height: nextY(currentOffset) - nextY(currentOffset + value),
        }];
      });
    });

    const nextLegendEntries = allKeys.map((key) => ({ key, label: toLegendLabel(key) }));
    const nextLegendLayout = computeLegendLayout({
      keys: nextLegendEntries.map((entry) => entry.label),
      chartLeft: nextMargin.left,
      chartWidth: nextChartWidth,
      markerWidth: 8,
      markerToTextGap: 8,
      itemGap: 18,
      measureLabel: measureLegendLabelWidth,
    });

    return {
      chartHeight: nextChartHeight,
      chartWidth: nextChartWidth,
      colorMap: nextColorMap,
      legendEntries: nextLegendEntries,
      legendLayout: nextLegendLayout,
      margin: nextMargin,
      segments: nextSegments,
      x: nextX,
      xTickValues: nextXTickValues,
      y: nextY,
      yTicks: nextYTicks,
    };
  }, [data, integerTicks, resolvedHeight, resolvedWidth]);

  if (isEmpty) {
    return (
      <div ref={containerRef} className="h-full w-full overflow-hidden">
        <div
          style={{ height: resolvedHeight }}
          className="flex h-full w-full items-center justify-center text-xs text-foreground-subtle"
        >
          No data
        </div>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="h-full w-full overflow-hidden">
      <svg width={resolvedWidth} height={resolvedHeight} role="img" aria-label={ariaLabel}>
        <g className="legend" transform="translate(0,10)">
          {legendLayout.items.map((item, index) => {
            const entry = legendEntries[index];
            return (
              <g key={entry.key} className="legend-item" transform={`translate(${item.x},0)`}>
                <rect
                  width={8}
                  height={8}
                  rx={2}
                  fill={colorMap.get(entry.key) ?? "var(--color-gold)"}
                  opacity={0.85}
                />
                <text
                  className="legend-label"
                  x={12}
                  y={8}
                  fill="var(--color-foreground-subtle)"
                  fontSize="10px"
                  fontFamily="var(--font-sans)"
                >
                  {item.key}
                </text>
              </g>
            );
          })}
        </g>
        <g className="chart-root" transform={`translate(${margin.left},${margin.top})`}>
          <g className="grid">
            {yTicks.map((tick) => (
              <line
                key={`grid-${tick}`}
                className="grid-line"
                x1={0}
                x2={chartWidth}
                y1={y(tick)}
                y2={y(tick)}
                stroke="var(--color-border)"
                strokeOpacity={0.5}
              />
            ))}
          </g>
          <g className="bars">
            {segments.map((segment) => (
              <rect
                key={segment.key}
                className="stack-segment"
                x={segment.x}
                y={segment.y}
                width={segment.width}
                height={segment.height}
                rx={2}
                fill={colorMap.get(segment.seriesKey) ?? "var(--color-gold)"}
                opacity={0.85}
              />
            ))}
          </g>
          <g className="x-axis" transform={`translate(0,${chartHeight})`}>
            {xTickValues.map((tick) => (
              <text
                key={`x-${tick}`}
                className="x-axis-tick"
                x={(x(tick) ?? 0) + x.bandwidth() / 2}
                y={8}
                dy="0.71em"
                textAnchor="middle"
                fill="var(--color-foreground-subtle)"
                fontSize="10px"
                fontFamily="var(--font-mono)"
              >
                {tick}
              </text>
            ))}
          </g>
          <g className="y-axis">
            {yTicks.map((tick) => (
              <text
                key={`y-${tick}`}
                className="y-axis-tick"
                x={-6}
                y={y(tick)}
                dy="0.32em"
                textAnchor="end"
                fill="var(--color-foreground-subtle)"
                fontSize="10px"
                fontFamily="var(--font-mono)"
              >
                {tick}
              </text>
            ))}
          </g>
        </g>
      </svg>
    </div>
  );
});

StackedBarChart.displayName = "StackedBarChart";
