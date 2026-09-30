import { memo, useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";

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
 */
export const StackedBarChart = memo(function StackedBarChart({
  data,
  ariaLabel = "Stacked bar chart",
  width,
  height,
  integerTicks = false,
}: StackedBarChartProps) {
  const plotRef = useRef<HTMLDivElement>(null);
  const [plotSize, setPlotSize] = useState<{
    width: number;
    height: number;
  } | null>(null);
  const isEmpty = data.length === 0;

  useEffect(() => {
    if (!plotRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      const next = {
        width: Math.max(0, Math.floor(rect.width)),
        height: Math.max(0, Math.floor(rect.height)),
      };
      setPlotSize((current) =>
        current?.width === next.width && current.height === next.height
          ? current
          : next,
      );
    });
    observer.observe(plotRef.current);
    return () => observer.disconnect();
  }, [isEmpty]);

  const resolvedWidth = plotSize?.width ?? width ?? 500;
  const resolvedHeight = plotSize?.height ?? Math.max(0, (height ?? 220) - 26);

  const {
    chartHeight,
    chartWidth,
    colorMap,
    legendEntries,
    margin,
    segments,
    x,
    xTickValues,
    y,
    yTicks,
  } = useMemo(() => {
    const nextMargin = { top: 4, right: 12, bottom: 30, left: 34 };
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

    return {
      chartHeight: nextChartHeight,
      chartWidth: nextChartWidth,
      colorMap: nextColorMap,
      legendEntries: nextLegendEntries,
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
      <div
        style={{ width, height }}
        className="flex h-full w-full max-w-full items-center justify-center text-xs text-foreground-subtle"
      >
        No data
      </div>
    );
  }

  return (
    <div
      style={{ width, height }}
      className="flex h-full w-full min-w-0 max-w-full flex-col gap-3"
    >
      <div className="legend flex shrink-0 flex-wrap justify-center gap-x-4 gap-y-1 text-[10px] text-foreground-subtle">
        {legendEntries.map((entry) => (
          <span
            key={entry.key}
            className="legend-item flex min-w-0 max-w-full items-center gap-2"
          >
            <span
              aria-hidden="true"
              className="size-2 shrink-0 rounded-sm opacity-85"
              style={{
                backgroundColor: colorMap.get(entry.key) ?? "var(--color-accent)",
              }}
            />
            <span className="legend-label min-w-0 break-words">
              {entry.label}
            </span>
          </span>
        ))}
      </div>
      <div ref={plotRef} className="min-h-0 min-w-0 flex-1 overflow-hidden">
        <svg
          className="block max-w-full"
          width={resolvedWidth}
          height={resolvedHeight}
          role="img"
          aria-label={ariaLabel}
        >
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
                  fill={colorMap.get(segment.seriesKey) ?? "var(--color-accent)"}
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
    </div>
  );
});

StackedBarChart.displayName = "StackedBarChart";
