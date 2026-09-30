import { useId, useRef, useState } from "react";
import { useInteractions } from "@floating-ui/react";
import { CalendarDays, ChevronDown, RotateCcw } from "lucide-react";
import { DEFAULT_ANALYTICS_TIME_RANGE, parseAnalyticsTimestamp, type AnalyticsTimeRange } from "@/lib/analytics-time-range";
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
  const timed = Boolean(endpointDraft(range.startAt, "from").time || endpointDraft(range.endAt, "to").time);
  const formatter = new Intl.DateTimeFormat(undefined, {
    month: "short", day: "numeric", year: "numeric",
    ...(timed ? { hour: "2-digit", minute: "2-digit", hourCycle: "h23" } as const : {}),
  });
  return formatter.formatRange(new Date(range.startAt), new Date(new Date(range.endAt).getTime() - (timed ? 0 : 1)));
}

function nextLocalDay(date: Date): Date {
  const next = new Date(date);
  // Calendar days can be 23 or 25 hours, or begin after a midnight DST jump.
  next.setDate(next.getDate() + 1);
  next.setHours(0, 0, 0, 0);
  return next;
}

interface EndpointDraft {
  value: string;
  time: string;
  // Preserve the offset in an existing DST fold until the user edits this endpoint.
  originalInstant?: string;
}

function localTimeValue(date: Date): string {
  const time = `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
  const seconds = date.getSeconds();
  const milliseconds = date.getMilliseconds();
  return time + (seconds || milliseconds ? `:${String(seconds).padStart(2, "0")}` : "")
    + (milliseconds ? `.${String(milliseconds).padStart(3, "0")}` : "");
}

function endpointDraft(timestamp: string, endpoint: "from" | "to", timed = false): EndpointDraft {
  const instant = new Date(timestamp);
  const day = localDateValue(new Date(instant.getTime() - (endpoint === "to" ? 1 : 0)));
  const midnight = parseLocalDate(day);
  const boundary = midnight && (endpoint === "to" ? nextLocalDay(midnight) : midnight);
  return !timed && boundary?.getTime() === instant.getTime()
    ? { value: day, time: "" }
    : { value: localDateValue(instant), time: localTimeValue(instant), originalInstant: timestamp };
}

function relativeMinutes(value: string): number | null {
  const expression = /^now\s*-\s*(\d+(?:\.\d+)?)\s*(m|h|d)$/i.exec(value.trim());
  const minutes = expression ? Number(expression[1]) * MINUTES_PER_UNIT[expression[2].toLowerCase()] : 0;
  return Number.isSafeInteger(minutes) && minutes >= 1 && minutes <= 525600 ? minutes : null;
}

function endpointDate(draft: EndpointDraft, endpoint: "from" | "to", now: number): Date | null {
  if (draft.originalInstant) return new Date(draft.originalInstant);
  const trimmed = draft.value.trim();
  const time = draft.time.trim();
  if (time) {
    if (!parseLocalDate(trimmed) || !/^([01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,3})?)?$/.test(time)) return null;
    const normalizedTime = time.length === 5 ? `${time}:00` : time;
    const date = new Date(`${trimmed}T${normalizedTime}`);
    // Date normalizes nonexistent local times forward. Reject that silent shift.
    const [hours, minutes, seconds = "0"] = time.split(":");
    return localDateValue(date) === trimmed && date.getHours() === Number(hours)
      && date.getMinutes() === Number(minutes) && date.getSeconds() === Math.floor(Number(seconds))
      ? date : null;
  }
  if (/^now$/i.test(trimmed)) return new Date(now);
  const minutes = relativeMinutes(trimmed);
  if (minutes !== null) return new Date(now - minutes * 60_000);
  const day = parseLocalDate(trimmed);
  if (day) return endpoint === "to" ? nextLocalDay(day) : day;
  const timestamp = parseAnalyticsTimestamp(trimmed);
  return timestamp === null ? null : new Date(timestamp);
}

function TimeEndpointField({ id, label, draft, onChange, onNow, error, descriptionId }: {
  id: string;
  label: "From" | "To";
  draft: EndpointDraft;
  onChange: (draft: EndpointDraft) => void;
  onNow?: () => void;
  error: boolean;
  descriptionId: string;
}) {
  const calendar = useRef<HTMLInputElement>(null);
  return (
    <div>
      <div className="mb-1.5 flex min-h-7 items-center justify-between gap-2">
        <label htmlFor={id} className={cn(fieldLabelClass, "mb-0")}>{label}</label>
        {onNow && <Button type="button" variant="outline" size="sm" onClick={onNow}>Now</Button>}
      </div>
      <div className="flex items-stretch gap-2 max-[360px]:flex-wrap">
        <input id={id} type="text" value={draft.value} autoComplete="off" spellCheck={false}
          onChange={(event) => onChange({ value: event.target.value, time: draft.time })}
          aria-invalid={error} aria-describedby={descriptionId} className={cn(fieldClass, "flex-1 font-mono")}
        />
        <input type="text" aria-label={`${label} time`} placeholder="HH:MM" value={draft.time}
          autoComplete="off" spellCheck={false} aria-invalid={error} aria-describedby={descriptionId}
          onChange={(event) => {
            const time = event.target.value;
            const resolved = time && !parseLocalDate(draft.value.trim()) ? endpointDate(draft, "from", Date.now()) : null;
            onChange({ value: resolved ? localDateValue(resolved) : draft.value, time });
          }}
          className={cn(fieldClass, "shrink-0 px-2 font-mono max-[360px]:order-1 max-[360px]:basis-full", draft.time.length > 5 ? "w-32" : "w-[5.5rem]")}
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
            value={parseLocalDate(draft.value.trim()) ? draft.value.trim() : ""}
            onChange={(event) => onChange({ value: event.target.value, time: draft.time })}
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
  const timed = initial.kind === "absolute" && Boolean(endpointDraft(initial.startAt, "from").time || endpointDraft(initial.endAt, "to").time);
  const [from, setFrom] = useState<EndpointDraft>(() => initial.kind === "relative"
    ? { value: `now-${durationLabel(initial.minutes)}`, time: "" } : endpointDraft(initial.startAt, "from", timed));
  const [to, setTo] = useState<EndpointDraft>(() => initial.kind === "relative"
    ? { value: "Now", time: "" } : endpointDraft(initial.endAt, "to", timed));
  const [error, setError] = useState("");
  const descriptionId = `${id}-help${error ? ` ${id}-error` : ""}`;
  return (
    <form
      noValidate
      className="min-h-0 overflow-y-auto p-5"
      onSubmit={(event) => {
        event.preventDefault();
        const minutes = relativeMinutes(from.value);
        if (minutes !== null && !from.time.trim() && !to.time.trim() && /^now$/i.test(to.value.trim())) {
          onApply({ kind: "relative", minutes });
          return;
        }
        const now = Date.now();
        const start = endpointDate(from, "from", now);
        const end = endpointDate(to, "to", now);
        if (!start || !end) {
          setError("Enter valid dates and local times (HH:MM), Now, or a relative time such as now-90m. Relative times support 1 minute to 365 days in whole minutes.");
          return;
        }
        if (start.getUTCFullYear() < 0 || end.getUTCFullYear() > 9999) {
          setError("Choose dates within the years 0000 to 9999.");
          return;
        }
        if (end <= start) {
          setError("The end must be after the start. Leave the time blank to include the whole day.");
          return;
        }
        onApply({ kind: "absolute", startAt: start.toISOString(), endAt: end.toISOString() });
      }}
    >
      <h2 id={`${id}-title`} className="text-sm font-semibold">Custom Time Window</h2>
      <p id={`${id}-help`} className="mt-3 text-xs leading-relaxed text-foreground-muted">
        Use dates or relative times such as now-1d. Times are 24-hour in {Intl.DateTimeFormat().resolvedOptions().timeZone}; leave them blank for whole days.
      </p>
      <div className="mt-5 space-y-4">
        <TimeEndpointField id={`${id}-from`} label="From" draft={from}
          onChange={(value) => { setFrom(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
        <TimeEndpointField id={`${id}-to`} label="To" draft={to}
          onNow={() => { setTo({ value: "Now", time: "" }); setError(""); }}
          onChange={(value) => { setTo(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
      </div>
      {error && <p id={`${id}-error`} role="alert" className="mt-3 text-xs leading-relaxed text-status-failed">{error}</p>}
      <div className="mt-5 flex items-center justify-between gap-3 border-t border-border pt-4">
        <Button type="button" variant="ghost" size="sm" onClick={() => {
          setFrom({ value: `now-${durationLabel(DEFAULT_ANALYTICS_TIME_RANGE.minutes)}`, time: "" });
          setTo({ value: "Now", time: "" });
          setError("");
        }}>
          <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
          Reset
        </Button>
        <Button type="submit" size="sm">Apply Window</Button>
      </div>
    </form>
  );
}
