import { memo } from "react";

export interface BarDatum {
  label: string;
  value: number;
  color?: string;
}

interface HorizontalBarChartProps {
  data: BarDatum[];
  ariaLabel?: string;
  /** Width in pixels; otherwise fills the available space. */
  width?: number;
  /** Minimum height per bar in pixels. Labels can wrap to additional lines. */
  barHeight?: number;
  /** Empty-state height in pixels */
  emptyHeight?: number;
  /** Format function for value labels */
  formatValue?: (v: number) => string;
}

/** Native text layout keeps role labels and values readable in every theme font. */
export const HorizontalBarChart = memo(function HorizontalBarChart({
  data,
  ariaLabel = "Horizontal bar chart",
  width,
  barHeight = 40,
  emptyHeight = 250,
  formatValue = (v) => v.toLocaleString(),
}: HorizontalBarChartProps) {
  if (data.length === 0) {
    return (
      <div
        style={{ height: emptyHeight, width }}
        className="flex w-full max-w-full items-center justify-center text-xs text-foreground-subtle"
      >
        No data
      </div>
    );
  }

  const maxValue = data.reduce((max, datum) => Math.max(max, datum.value), 0) || 1;

  return (
    <div
      role="img"
      aria-label={ariaLabel}
      aria-description={data
        .map((datum) => `${datum.label}: ${formatValue(datum.value)}`)
        .join("; ")}
      style={{ width }}
      className="w-full min-w-0 max-w-full"
    >
      {data.map((datum, index) => (
        <div
          key={`${datum.label}-${index}`}
          style={{ minHeight: barHeight }}
          className="horizontal-bar-row flex min-w-0 flex-col justify-center gap-1.5 py-1"
        >
          <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_fit-content(35%)] items-start gap-x-3">
            <span className="label min-w-0 break-words text-xs leading-snug text-foreground-muted">
              {datum.label}
            </span>
            <span className="value min-w-0 break-all text-right font-mono text-[11px] leading-snug text-foreground-subtle">
              {formatValue(datum.value)}
            </span>
          </div>
          <div className="bar-track w-full min-w-0 overflow-hidden rounded-sm bg-border/30">
            <div
              className="bar h-2 rounded-sm opacity-80"
              style={{
                width: `${(Math.max(0, datum.value) / maxValue) * 100}%`,
                backgroundColor: datum.color ?? "var(--color-accent)",
              }}
            />
          </div>
        </div>
      ))}
    </div>
  );
});

HorizontalBarChart.displayName = "HorizontalBarChart";
