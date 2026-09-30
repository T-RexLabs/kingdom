import { Fragment, memo } from "react";

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
  barHeight = 28,
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

  const maxValue = data.reduce((max, datum) => Math.max(max, datum.value), 1);

  return (
    <div
      role="img"
      aria-label={ariaLabel}
      aria-description={data
        .map((datum) => `${datum.label}: ${formatValue(datum.value)}`)
        .join("; ")}
      style={{ width, gridAutoRows: `minmax(${barHeight}px, auto)` }}
      className="grid w-full min-w-0 max-w-full grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)_fit-content(25%)] items-center gap-x-2 py-1"
    >
      {data.map((datum, index) => (
        <Fragment key={`${datum.label}-${index}`}>
          <span className="label min-w-0 break-words py-1 text-right text-[11px] leading-snug text-foreground-muted">
            {datum.label}
          </span>
          <div className="min-w-0">
            <div
              className="bar h-3 rounded-sm opacity-80"
              style={{
                width: `${(Math.max(0, datum.value) / maxValue) * 100}%`,
                backgroundColor: datum.color ?? "var(--color-accent)",
              }}
            />
          </div>
          <span className="value min-w-0 break-all py-1 font-mono text-[11px] leading-snug text-foreground-subtle">
            {formatValue(datum.value)}
          </span>
        </Fragment>
      ))}
    </div>
  );
});

HorizontalBarChart.displayName = "HorizontalBarChart";
