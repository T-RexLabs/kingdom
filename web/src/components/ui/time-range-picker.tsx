import { useId, useRef, useState } from "react";
import { useInteractions } from "@floating-ui/react";
import { CalendarDays, ChevronDown } from "lucide-react";
import { parseAnalyticsTimestamp, type AnalyticsTimeRange } from "@/lib/analytics-time-range";
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
          initialFocus={0}
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

function nextLocalDay(date: Date): Date {
  const next = new Date(date);
  // Calendar days can be 23 or 25 hours, or begin after a midnight DST jump.
  next.setDate(next.getDate() + 1);
  next.setHours(0, 0, 0, 0);
  return next;
}

function endpointDraft(timestamp: string, endpoint: "from" | "to"): string {
  const instant = new Date(timestamp);
  const day = localDateValue(new Date(instant.getTime() - (endpoint === "to" ? 1 : 0)));
  const midnight = parseLocalDate(day);
  const boundary = midnight && (endpoint === "to" ? nextLocalDay(midnight) : midnight);
  // Keep precise endpoints precise when reopening a mixed or timestamp range.
  return boundary?.getTime() === instant.getTime() ? day : timestamp;
}

function relativeMinutes(value: string): number | null {
  const expression = /^now\s*-\s*(\d+(?:\.\d+)?)\s*(m|h|d)$/i.exec(value.trim());
  const minutes = expression ? Number(expression[1]) * MINUTES_PER_UNIT[expression[2].toLowerCase()] : 0;
  return Number.isSafeInteger(minutes) && minutes >= 1 && minutes <= 525600 ? minutes : null;
}

function endpointDate(value: string, endpoint: "from" | "to", now: number): Date | null {
  const trimmed = value.trim();
  if (/^now$/i.test(trimmed)) return new Date(now);
  const minutes = relativeMinutes(trimmed);
  if (minutes !== null) return new Date(now - minutes * 60_000);
  const day = parseLocalDate(trimmed);
  if (day) return endpoint === "to" ? nextLocalDay(day) : day;
  const timestamp = parseAnalyticsTimestamp(trimmed);
  return timestamp === null ? null : new Date(timestamp);
}

function TimeEndpointField({ id, label, value, onChange, error, descriptionId }: {
  id: string;
  label: "From" | "To";
  value: string;
  onChange: (value: string) => void;
  error: boolean;
  descriptionId: string;
}) {
  const calendar = useRef<HTMLInputElement>(null);
  return (
    <div>
      <label htmlFor={id} className={fieldLabelClass}>{label}</label>
      <div className="flex items-stretch gap-2">
        <input id={id} type="text" value={value} autoComplete="off" spellCheck={false}
          onChange={(event) => onChange(event.target.value)}
          aria-invalid={error} aria-describedby={descriptionId} className={cn(fieldClass, "font-mono")}
        />
        <div className="relative flex shrink-0">
          <Button type="button" variant="outline" aria-label={`Choose ${label} date`}
            aria-controls={`${id}-calendar`} className="h-auto min-h-10 w-10 px-0"
            onClick={() => calendar.current?.showPicker()}
          >
            <CalendarDays className="h-4 w-4" aria-hidden="true" />
          </Button>
          {/* Anchor the native calendar beside its trigger. Only preselect literal dates, so
              choosing today replaces “Now” instead of being swallowed as an unchanged value. */}
          <input ref={calendar} id={`${id}-calendar`} type="date" tabIndex={-1} aria-hidden="true"
            value={parseLocalDate(value.trim()) ? value.trim() : ""}
            onChange={(event) => onChange(event.target.value)}
            className="pointer-events-none absolute inset-0 h-full w-full opacity-0"
          />
        </div>
      </div>
    </div>
  );
}

function CustomWindow({ id, initial, onApply }: {
  id: string;
  initial: AnalyticsTimeRange;
  onApply: (range: AnalyticsTimeRange) => void;
}) {
  const [from, setFrom] = useState(() => initial.kind === "relative"
    ? `now-${durationLabel(initial.minutes)}` : endpointDraft(initial.startAt, "from"));
  const [to, setTo] = useState(() => initial.kind === "relative" ? "Now" : endpointDraft(initial.endAt, "to"));
  const [error, setError] = useState("");
  const descriptionId = `${id}-help${error ? ` ${id}-error` : ""}`;
  return (
    <form
      noValidate
      className="min-h-0 overflow-y-auto p-5"
      onSubmit={(event) => {
        event.preventDefault();
        const minutes = relativeMinutes(from);
        if (minutes !== null && /^now$/i.test(to.trim())) {
          onApply({ kind: "relative", minutes });
          return;
        }
        const now = Date.now();
        const start = endpointDate(from, "from", now);
        const end = endpointDate(to, "to", now);
        if (!start || !end) {
          setError("Enter a date (YYYY-MM-DD), a timestamp, Now, or a relative time such as now-90m. Relative times support 1 minute to 365 days in whole minutes.");
          return;
        }
        if (start.getUTCFullYear() < 0 || end.getUTCFullYear() > 9999) {
          setError("Choose dates within the years 0000 to 9999.");
          return;
        }
        if (end <= start) {
          setError("The end must be after the start. Calendar dates include the whole day.");
          return;
        }
        onApply({ kind: "absolute", startAt: start.toISOString(), endAt: end.toISOString() });
      }}
    >
      <h2 id={`${id}-title`} className="text-sm font-semibold">Custom Time Window</h2>
      <p id={`${id}-help`} className="mt-3 text-xs leading-relaxed text-foreground-muted">
        Type now-1d, Now, or a date. Calendar dates include the whole day in {Intl.DateTimeFormat().resolvedOptions().timeZone}.
      </p>
      <div className="mt-5 space-y-4">
        <TimeEndpointField id={`${id}-from`} label="From" value={from}
          onChange={(value) => { setFrom(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
        <TimeEndpointField id={`${id}-to`} label="To" value={to}
          onChange={(value) => { setTo(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
      </div>
      {error && <p id={`${id}-error`} role="alert" className="mt-3 text-xs leading-relaxed text-status-failed">{error}</p>}
      <div className="mt-5 flex justify-end border-t border-border pt-4">
        <Button type="submit" size="sm">Apply Window</Button>
      </div>
    </form>
  );
}
