import { useId, useState } from "react";
import { FloatingNode, FloatingTree, useFloatingNodeId, useInteractions } from "@floating-ui/react";
import { CalendarDays, ChevronDown, RotateCcw } from "lucide-react";
import { DEFAULT_ANALYTICS_TIME_RANGE, parseAnalyticsTimestamp, type AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { cn } from "@/lib/utils";
import { usePopoverFloating } from "@/lib/use-popover-floating";
import { Button } from "./button";
import { CalendarPicker } from "./calendar-picker";
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

interface TimeRangePickerProps {
  value: AnalyticsTimeRange;
  onChange: (range: AnalyticsTimeRange) => void;
  className?: string;
}

export function TimeRangePicker(props: TimeRangePickerProps) {
  return <FloatingTree><TimeRangePickerControl {...props} /></FloatingTree>;
}

function TimeRangePickerControl({ value, onChange, className }: TimeRangePickerProps) {
  const id = useId();
  const nodeId = useFloatingNodeId();
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
    nodeId,
    open,
    onOpenChange: setOpen,
    role: "dialog",
    placement: "bottom-end",
    widthMode: "css",
    maxHeight: 480,
  });
  const { getReferenceProps, getFloatingProps } = useInteractions(baseInteractions);

  return (
    <FloatingNode id={nodeId}>
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
    </FloatingNode>
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
  const start = new Date(range.startAt);
  const end = new Date(range.endAt);
  const wholeDays = [start, end].every((date) => parseLocalDate(localDateValue(date))?.getTime() === date.getTime());
  const formatter = new Intl.DateTimeFormat(undefined, {
    month: "short", day: "numeric", year: "numeric",
    ...(!wholeDays ? { hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" } as const : {}),
  });
  return formatter.formatRange(start, new Date(end.getTime() - (wholeDays ? 1 : 0)));
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
  // Preserve the offset in an existing DST fold until the user edits this endpoint.
  originalInstant?: string;
}

function localTimestampValue(date: Date): string {
  const time = [date.getHours(), date.getMinutes(), date.getSeconds()].map((part) => String(part).padStart(2, "0")).join(":");
  const fraction = date.getMilliseconds() ? `.${String(date.getMilliseconds()).padStart(3, "0")}` : "";
  return `${localDateValue(date)} ${time}${fraction}`;
}

function parseLocalTimestamp(value: string): Date | null {
  const parts = /^(\d{4}-\d{2}-\d{2})[ T]([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d)(\.\d{1,3})?)?$/.exec(value);
  if (!parts || !parseLocalDate(parts[1])) return null;
  const [, day, hours, minutes, seconds = "00", fraction = ""] = parts;
  const date = new Date(`${day}T${hours}:${minutes}:${seconds}${fraction}`);
  // Date normalizes nonexistent local times forward. Reject that silent shift.
  return localDateValue(date) === day && date.getHours() === Number(hours)
    && date.getMinutes() === Number(minutes) && date.getSeconds() === Number(seconds)
    ? date : null;
}

function endpointDraft(timestamp: string): EndpointDraft {
  return { value: localTimestampValue(new Date(timestamp)), originalInstant: timestamp };
}

function relativeMinutes(value: string): number | null {
  const expression = /^now\s*-\s*(\d+(?:\.\d+)?)\s*(m|h|d)$/i.exec(value.trim());
  const minutes = expression ? Number(expression[1]) * MINUTES_PER_UNIT[expression[2].toLowerCase()] : 0;
  return Number.isSafeInteger(minutes) && minutes >= 1 && minutes <= 525600 ? minutes : null;
}

function endpointDate(draft: EndpointDraft, endpoint: "from" | "to", now: number): Date | null {
  if (draft.originalInstant) return new Date(draft.originalInstant);
  const trimmed = draft.value.trim();
  if (/^now$/i.test(trimmed)) return new Date(now);
  const minutes = relativeMinutes(trimmed);
  if (minutes !== null) return new Date(now - minutes * 60_000);
  const day = parseLocalDate(trimmed);
  if (day) return endpoint === "to" ? nextLocalDay(day) : day;
  const local = parseLocalTimestamp(trimmed);
  if (local) return local;
  const timestamp = parseAnalyticsTimestamp(trimmed);
  return timestamp === null ? null : new Date(timestamp);
}

function TimeEndpointField({ id, label, draft, onChange, error, descriptionId }: {
  id: string;
  label: "From" | "To";
  draft: EndpointDraft;
  onChange: (draft: EndpointDraft) => void;
  error: boolean;
  descriptionId: string;
}) {
  const timestamp = parseAnalyticsTimestamp(draft.value.trim());
  const selected = parseLocalTimestamp(draft.value.trim()) ?? (timestamp === null ? null : new Date(timestamp));
  const selectedDay = selected ? localDateValue(selected) : parseLocalDate(draft.value.trim()) ? draft.value.trim() : "";
  return (
    <div>
      <label htmlFor={id} className={fieldLabelClass}>{label}</label>
      <div className={cn(fieldClass, "flex items-stretch overflow-hidden p-0 focus-within:border-accent focus-within:ring-1 focus-within:ring-accent")}>
        <input id={id} type="text" value={draft.value} placeholder="YYYY-MM-DD HH:mm:ss"
          autoComplete="off" spellCheck={false} onChange={(event) => onChange({ value: event.target.value })}
          aria-invalid={error} aria-describedby={descriptionId}
          className="min-w-0 flex-1 bg-transparent px-3 py-2.5 font-mono text-sm outline-none placeholder:text-foreground-subtle"
        />
        <CalendarPicker label={label} value={selectedDay}
          onChange={(day) => {
            if (day === selectedDay) return;
            onChange({ value: selected ? `${day} ${localTimestampValue(selected).slice(11)}` : day });
          }}
        />
      </div>
    </div>
  );
}

function CustomWindow({ id, initial, onApply }: {
  id: string;
  initial: AnalyticsTimeRange;
  onApply: (range: AnalyticsTimeRange) => void;
}) {
  const [from, setFrom] = useState<EndpointDraft>(() => initial.kind === "relative"
    ? { value: `now-${durationLabel(initial.minutes)}` } : endpointDraft(initial.startAt));
  const [to, setTo] = useState<EndpointDraft>(() => initial.kind === "relative"
    ? { value: "Now" } : endpointDraft(initial.endAt));
  const [error, setError] = useState("");
  const descriptionId = `${id}-help ${id}-zone${error ? ` ${id}-error` : ""}`;
  return (
    <form
      noValidate
      className="min-h-0 overflow-y-auto p-5"
      onSubmit={(event) => {
        event.preventDefault();
        const minutes = relativeMinutes(from.value);
        if (minutes !== null && /^now$/i.test(to.value.trim())) {
          onApply({ kind: "relative", minutes });
          return;
        }
        const now = Date.now();
        const start = endpointDate(from, "from", now);
        const end = endpointDate(to, "to", now);
        if (!start || !end) {
          setError("Enter a valid local date and time (YYYY-MM-DD HH:mm:ss), Now, or a relative time such as now-90m. Relative times support 1 minute to 365 days in whole minutes.");
          return;
        }
        if (start.getUTCFullYear() < 0 || end.getUTCFullYear() > 9999) {
          setError("Choose dates within the years 0000 to 9999.");
          return;
        }
        if (end <= start) {
          setError("The end must be after the start.");
          return;
        }
        onApply({ kind: "absolute", startAt: start.toISOString(), endAt: end.toISOString() });
      }}
    >
      <h2 id={`${id}-title`} className="text-sm font-semibold">Custom Time Window</h2>
      <p id={`${id}-help`} className="mt-3 text-xs leading-relaxed text-foreground-muted">
        Use YYYY-MM-DD HH:mm:ss, now-1d, or Now.
      </p>
      <div className="mt-5 space-y-4">
        <TimeEndpointField id={`${id}-from`} label="From" draft={from}
          onChange={(value) => { setFrom(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
        <TimeEndpointField id={`${id}-to`} label="To" draft={to}
          onChange={(value) => { setTo(value); setError(""); }} error={Boolean(error)} descriptionId={descriptionId}
        />
      </div>
      <dl id={`${id}-zone`} className="mt-4 flex flex-wrap items-baseline gap-x-2 gap-y-1 text-xs text-foreground-muted">
        <dt>Browser Time Zone</dt>
        <dd className="font-medium text-foreground">{Intl.DateTimeFormat().resolvedOptions().timeZone}</dd>
      </dl>
      {error && <p id={`${id}-error`} role="alert" className="mt-3 text-xs leading-relaxed text-status-failed">{error}</p>}
      <div className="mt-5 flex items-center justify-between gap-3 border-t border-border pt-4">
        <Button type="button" variant="ghost" size="sm" onClick={() => {
          setFrom({ value: `now-${durationLabel(DEFAULT_ANALYTICS_TIME_RANGE.minutes)}` });
          setTo({ value: "Now" });
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
