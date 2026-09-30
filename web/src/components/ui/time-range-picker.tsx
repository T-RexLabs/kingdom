import { useId, useState } from "react";
import { useInteractions } from "@floating-ui/react";
import { CalendarDays, ChevronDown, Clock } from "lucide-react";
import type { AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { cn } from "@/lib/utils";
import { usePopoverFloating } from "@/lib/use-popover-floating";
import { Button } from "./button";
import { fieldClass, fieldLabelClass } from "./field-styles";
import { PopoverSurface } from "./popover-surface";

const MINUTES_PER_UNIT: Record<string, number> = { m: 1, h: 60, d: 1440 };

const PRESETS = [
  [30, "30m", "Last 30 Minutes"],
  [60, "1h", "Last Hour"],
  [180, "3h", "Last 3 Hours"],
  [360, "6h", "Last 6 Hours"],
  [720, "12h", "Last 12 Hours"],
  [1440, "24h", "Last 24 Hours"],
  [2880, "2d", "Last 2 Days"],
  [10080, "7d", "Last 7 Days"],
  [43200, "30d", "Last 30 Days"],
] as const;

function durationLabel(minutes: number): string {
  if (minutes % 1440 === 0) return `${minutes / 1440}d`;
  if (minutes % 60 === 0) return `${minutes / 60}h`;
  return `${minutes}m`;
}

export function TimeRangePicker({
  value,
  onChange,
  className,
}: {
  value: AnalyticsTimeRange;
  onChange: (range: AnalyticsTimeRange) => void;
  className?: string;
}) {
  const id = useId();
  const [open, setOpen] = useState(false);
  const relativeMinutes = value.kind === "relative" ? value.minutes : null;
  const preset = PRESETS.some(([minutes]) => minutes === relativeMinutes);
  const selectionKey = value.kind === "relative" ? `relative:${value.minutes}` : `${value.startAt}:${value.endAt}`;
  const {
    context,
    setReference,
    setPanel,
    modalOwned,
    panelStyle,
    baseInteractions,
  } = usePopoverFloating({
    open,
    onOpenChange: setOpen,
    role: "dialog",
    placement: "bottom-end",
    widthMode: "css",
    maxHeight: 480,
  });
  const { getReferenceProps, getFloatingProps } = useInteractions(baseInteractions);

  return (
    <div className={cn("flex min-w-0 max-w-full flex-col gap-2", className)}>
      <span id={`${id}-label`} className="text-xs text-foreground-muted">
        Time Window
      </span>
      <div className="flex max-w-full flex-wrap items-center gap-2">
        <div
          role="group"
          aria-labelledby={`${id}-label`}
          className="flex max-w-full flex-wrap items-center gap-0.5 rounded-md border border-border bg-background p-1"
        >
          {PRESETS.map(([minutes, label, accessibleLabel]) => (
            <Button
              key={minutes}
              type="button"
              variant="ghost"
              size="sm"
              aria-label={accessibleLabel}
              aria-pressed={relativeMinutes === minutes}
              onClick={() => {
                onChange({ kind: "relative", minutes });
                setOpen(false);
              }}
              className={cn(
                "min-w-8 px-2 font-mono",
                relativeMinutes === minutes && "bg-accent/15 text-accent hover:bg-accent/20 hover:text-accent",
              )}
            >
              {label}
            </Button>
          ))}
        </div>
        <Button
          ref={setReference}
          {...getReferenceProps({
            type: "button",
            "aria-label": "Custom time window",
            "aria-expanded": open,
          })}
          variant="outline"
          size="sm"
          className={cn("min-h-10 max-w-full", !preset && "border-accent bg-accent/10 text-accent")}
        >
          <CalendarDays className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
          <span className="whitespace-normal text-left">{value.kind === "absolute" ? calendarRangeLabel(value) : preset ? "Custom" : durationLabel(value.minutes)}</span>
          <ChevronDown className={cn("h-3 w-3 transition-transform", open && "rotate-180")} aria-hidden="true" />
        </Button>
      </div>
      {open && (
        <PopoverSurface
          context={context}
          panelRef={setPanel}
          style={panelStyle}
          modalOwned={modalOwned}
          floatingProps={getFloatingProps({ "aria-labelledby": `${id}-title` })}
          initialFocus={2}
          className="w-[24rem] max-w-[calc(100vw-1rem)]"
        >
          <CustomWindow
            key={selectionKey}
            id={id}
            initial={value}
            onApply={(range) => {
              onChange(range);
              setOpen(false);
            }}
          />
        </PopoverSurface>
      )}
    </div>
  );
}

function localDateValue(date: Date): string {
  return `${date.getFullYear().toString().padStart(4, "0")}-${(date.getMonth() + 1).toString().padStart(2, "0")}-${date.getDate().toString().padStart(2, "0")}`;
}

function parseLocalDate(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const date = new Date(`${value}T00:00:00`);
  return Number.isFinite(date.getTime()) && localDateValue(date) === value ? date : null;
}

function calendarRangeLabel(range: Extract<AnalyticsTimeRange, { kind: "absolute" }>): string {
  const formatter = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });
  return formatter.formatRange(new Date(range.startAt), new Date(new Date(range.endAt).getTime() - 1));
}

function CustomWindow({ id, initial, onApply }: {
  id: string;
  initial: AnalyticsTimeRange;
  onApply: (range: AnalyticsTimeRange) => void;
}) {
  const [mode, setMode] = useState(initial.kind);
  const [draft, setDraft] = useState(`now-${durationLabel(initial.kind === "relative" ? initial.minutes : 1440)}`);
  const [fromDate, setFromDate] = useState(() => localDateValue(initial.kind === "absolute"
    ? new Date(initial.startAt) : new Date(Date.now() - initial.minutes * 60_000)));
  const [toDate, setToDate] = useState(() => localDateValue(initial.kind === "absolute"
    ? new Date(new Date(initial.endAt).getTime() - 1) : new Date()));
  const [error, setError] = useState("");
  const descriptionId = `${id}-help${error ? ` ${id}-error` : ""}`;
  return (
    <form
      noValidate
      className="min-h-0 overflow-y-auto p-5"
      onSubmit={(event) => {
        event.preventDefault();
        if (mode === "absolute") {
          const start = parseLocalDate(fromDate);
          const end = parseLocalDate(toDate);
          if (!start || !end) {
            setError("Choose a valid start and end date.");
            return;
          }
          if (end < start) {
            setError("The end date must be on or after the start date.");
            return;
          }
          // Advance the local calendar day, not 24 hours: selected days may span daylight-saving changes.
          end.setDate(end.getDate() + 1);
          end.setHours(0, 0, 0, 0);
          if (end.getFullYear() > 9999) {
            setError("Choose an end date before December 31, 9999.");
            return;
          }
          onApply({ kind: "absolute", startAt: start.toISOString(), endAt: end.toISOString() });
          return;
        }
        const expression = /^now\s*-\s*(\d+(?:\.\d+)?)\s*(m|h|d)$/i.exec(draft.trim());
        const minutes = expression ? Number(expression[1]) * (MINUTES_PER_UNIT[expression[2].toLowerCase()] ?? 0) : 0;
        if (!Number.isSafeInteger(minutes) || minutes < 1 || minutes > 525600) {
          setError("Enter a window such as now-90m, now-36h or now-14d, from 1 minute to 365 days in whole minutes.");
          return;
        }
        onApply({ kind: "relative", minutes });
      }}
    >
      <h2 id={`${id}-title`} className="text-sm font-semibold">Custom Time Window</h2>
      <div role="group" aria-label="Window mode" className="mt-4 grid grid-cols-2 gap-1 rounded-md border border-border bg-background p-1">
        {(["relative", "absolute"] as const).map((option) => (
          <Button key={option} type="button" variant="ghost" size="sm" aria-pressed={mode === option}
            onClick={() => { setMode(option); setError(""); }}
            className={cn(mode === option && "bg-accent/15 text-accent hover:bg-accent/20 hover:text-accent")}
          >
            {option === "absolute" ? <CalendarDays className="h-3.5 w-3.5" aria-hidden="true" /> : <Clock className="h-3.5 w-3.5" aria-hidden="true" />}
            {option === "absolute" ? "Date Range" : "Relative"}
          </Button>
        ))}
      </div>
      <p id={`${id}-help`} className="mt-3 text-xs leading-relaxed text-foreground-muted">
        {mode === "absolute" ? `Includes both dates in ${Intl.DateTimeFormat().resolvedOptions().timeZone}.` : "Show a rolling window of activity ending now."}
      </p>
      <div className="mt-5 space-y-4">
        <div>
          <label htmlFor={`${id}-from`} className={fieldLabelClass}>From</label>
          {mode === "absolute" ? (
            <input id={`${id}-from`} type="date" value={fromDate}
              onChange={(event) => { setFromDate(event.target.value); setError(""); }}
              aria-invalid={Boolean(error)} aria-describedby={descriptionId} className={cn(fieldClass, "min-w-0")}
            />
          ) : (
            <input id={`${id}-from`} type="text" value={draft} autoComplete="off" spellCheck={false}
              onChange={(event) => { setDraft(event.target.value); setError(""); }}
              aria-invalid={Boolean(error)} aria-describedby={descriptionId} className={cn(fieldClass, "font-mono")}
            />
          )}
          {mode === "relative" && <p className="mt-2 text-xs leading-relaxed text-foreground-muted">Use now-90m, now-36h or now-14d. Up to 365 days.</p>}
        </div>
        <div>
          <label htmlFor={`${id}-to`} className={fieldLabelClass}>To</label>
          {mode === "absolute" ? (
            <input id={`${id}-to`} type="date" value={toDate}
              onChange={(event) => { setToDate(event.target.value); setError(""); }}
              aria-invalid={Boolean(error)} aria-describedby={descriptionId} className={cn(fieldClass, "min-w-0")}
            />
          ) : (
            <input id={`${id}-to`} type="text" value="Now" readOnly tabIndex={-1}
              className={cn(fieldClass, "bg-surface-raised text-foreground-muted")}
            />
          )}
        </div>
      </div>
      {error && <p id={`${id}-error`} role="alert" className="mt-3 text-xs leading-relaxed text-status-failed">{error}</p>}
      <div className="mt-5 flex justify-end border-t border-border pt-4">
        <Button type="submit" size="sm">{mode === "absolute" ? "Apply Range" : "Apply Window"}</Button>
      </div>
    </form>
  );
}
