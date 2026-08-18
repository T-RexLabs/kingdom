import { useState, useRef, useEffect, useCallback, useId } from "react";
import { cn } from "@/lib/utils";
import { Clock, Calendar } from "lucide-react";
import { minutesToRelativeExpr, parseRelativeTime } from "./time-range";

export interface TimePreset {
  label: string;
  /** Duration in total minutes */
  minutes: number;
}

const PRESETS: TimePreset[] = [
  { label: "30m", minutes: 30 },
  { label: "1h", minutes: 60 },
  { label: "3h", minutes: 180 },
  { label: "6h", minutes: 360 },
  { label: "12h", minutes: 720 },
  { label: "24h", minutes: 1440 },
  { label: "2d", minutes: 2880 },
  { label: "7d", minutes: 10080 },
  { label: "30d", minutes: 43200 },
];

interface TimeRangePickerProps {
  /** Currently selected window in total minutes */
  value: number;
  /** Called when user picks a new window (total minutes) */
  onChange: (totalMinutes: number) => void;
  className?: string;
}

/**
 * Format a Date for the date input (YYYY-MM-DD).
 */
function toDateLocal(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

/**
 * Grafana-style time window selector with preset buttons and a custom
 * time range popover accessible via the clock icon.
 */
export function TimeRangePicker({ value, onChange, className }: TimeRangePickerProps) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(() => ({
    sourceValue: value,
    fromExpr: minutesToRelativeExpr(value),
    toExpr: "now",
    error: null as string | null,
  }));
  if (draft.sourceValue !== value) {
    setDraft({
      sourceValue: value,
      fromExpr: minutesToRelativeExpr(value),
      toExpr: "now",
      error: null,
    });
  }
  const { fromExpr, toExpr, error } = draft;
  const fromInputId = useId();
  const toInputId = useId();
  const popoverRef = useRef<HTMLDivElement>(null);
  const fromDateRef = useRef<HTMLInputElement>(null);
  const toDateRef = useRef<HTMLInputElement>(null);

  const updateDraft = useCallback((fields: Partial<Omit<typeof draft, "sourceValue">>) => {
    setDraft((existing) => ({
      ...(existing.sourceValue === value
        ? existing
        : { sourceValue: value, fromExpr: minutesToRelativeExpr(value), toExpr: "now", error: null }),
      ...fields,
    }));
  }, [value]);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [open]);

  const handleApply = useCallback(() => {
    const fromDate = parseRelativeTime(fromExpr);
    const toDate = parseRelativeTime(toExpr);

    if (!fromDate) {
      updateDraft({ error: "Invalid 'From' expression. Use: now, now-4h, now-2d, now-30m" });
      return;
    }
    if (!toDate) {
      updateDraft({ error: "Invalid 'To' expression. Use: now, now-4h, now-2d" });
      return;
    }

    const diffMs = toDate.getTime() - fromDate.getTime();
    if (diffMs <= 0) {
      updateDraft({ error: "'From' must be before 'To'" });
      return;
    }

    const totalMinutes = Math.max(1, Math.round(diffMs / (1000 * 60)));
    if (totalMinutes > 525600) {
      updateDraft({ error: "Maximum range is 1 year" });
      return;
    }

    updateDraft({ error: null });
    setOpen(false);
    onChange(totalMinutes);
  }, [fromExpr, toExpr, onChange, updateDraft]);

  const handleFromCalendar = useCallback(() => {
    fromDateRef.current?.showPicker();
  }, []);

  const handleToCalendar = useCallback(() => {
    toDateRef.current?.showPicker();
  }, []);

  const handleFromDateChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    // date input gives "YYYY-MM-DD" — parse as local midnight
    const parts = e.target.value.split("-");
    if (parts.length !== 3) return;
    const d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
    if (isNaN(d.getTime())) return;
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffH = Math.round(diffMs / (1000 * 60 * 60));
    if (diffH < 24) {
      updateDraft({ fromExpr: `now-${Math.max(1, diffH)}h` });
    } else {
      const diffD = Math.round(diffMs / (1000 * 60 * 60 * 24));
      updateDraft({ fromExpr: `now-${diffD}d` });
    }
  }, [updateDraft]);

  const handleToDateChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    // date input gives "YYYY-MM-DD" — parse as local midnight
    const parts = e.target.value.split("-");
    if (parts.length !== 3) return;
    const d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2]));
    if (isNaN(d.getTime())) return;
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffH = Math.round(diffMs / (1000 * 60 * 60));
    if (diffH <= 0) {
      updateDraft({ toExpr: "now" });
    } else if (diffH < 24) {
      updateDraft({ toExpr: `now-${diffH}h` });
    } else {
      const diffD = Math.round(diffMs / (1000 * 60 * 60 * 24));
      updateDraft({ toExpr: `now-${diffD}d` });
    }
  }, [updateDraft]);

  const isPreset = PRESETS.some((p) => p.minutes === value);

  return (
    <div className={cn("relative flex w-full flex-wrap items-center gap-1.5 sm:w-auto sm:flex-nowrap", className)} ref={popoverRef}>
      <button
        onClick={() => setOpen(!open)}
        className={cn(
          "flex items-center justify-center rounded p-1 transition-colors",
          open
            ? "text-gold"
            : "text-foreground-subtle hover:text-foreground"
        )}
        title="Custom time range"
        aria-label="Custom time range"
      >
        <Clock className="h-3.5 w-3.5" />
      </button>
      <div className="flex max-w-full flex-wrap items-center rounded-md border border-border bg-background p-0.5 sm:flex-nowrap">
        {PRESETS.map((preset) => (
          <button
            key={preset.minutes}
            onClick={() => {
              onChange(preset.minutes);
              setOpen(false);
            }}
            className={cn(
              "rounded px-2 py-1 text-xs font-mono font-medium transition-colors",
              value === preset.minutes
                ? "bg-gold/15 text-gold"
                : "text-foreground-subtle hover:text-foreground hover:bg-surface-raised"
            )}
          >
            {preset.label}
          </button>
        ))}
      </div>

      {/* Custom time range display when not a standard preset */}
      {!isPreset && (
        <span className="ml-1 rounded bg-gold/15 px-2 py-0.5 text-xs font-mono text-gold">
          {value < 60 ? `${value}m` : value < 1440 ? `${value / 60}h` : `${value / 1440}d`}
        </span>
      )}

      {/* Popover */}
      {open && (
        <div className="absolute right-0 top-full z-50 mt-2 w-72 rounded-lg border border-border bg-surface shadow-xl shadow-black/40">
          <div className="p-4 space-y-4">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-foreground-muted">
              Absolute time range
            </h3>

            {/* From */}
            <div className="space-y-1.5">
              <label htmlFor={fromInputId} className="text-xs text-foreground-muted">From</label>
              <div className="flex items-center gap-1">
                <input
                  id={fromInputId}
                  type="text"
                  value={fromExpr}
                  onChange={(e) => updateDraft({ fromExpr: e.target.value, error: null })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleApply(); }}
                  placeholder="now-24h"
                  className="flex-1 rounded border border-border bg-background px-3 py-1.5 font-mono text-sm text-foreground placeholder:text-foreground-subtle focus:border-gold/50 focus:outline-none focus:ring-1 focus:ring-gold/30"
                />
                <button
                  onClick={handleFromCalendar}
                  className="flex items-center justify-center rounded border border-border bg-background p-1.5 text-foreground-subtle transition-colors hover:border-gold/50 hover:text-gold"
                  title="Pick date"
                  aria-label="Pick from date"
                >
                  <Calendar className="h-3.5 w-3.5" />
                </button>
                <input
                  ref={fromDateRef}
                  type="date"
                  onChange={handleFromDateChange}
                  value={toDateLocal(parseRelativeTime(fromExpr) ?? new Date())}
                  className="sr-only color-scheme-dark"
                  tabIndex={-1}
                />
              </div>
            </div>

            {/* To */}
            <div className="space-y-1.5">
              <label htmlFor={toInputId} className="text-xs text-foreground-muted">To</label>
              <div className="flex items-center gap-1">
                <input
                  id={toInputId}
                  type="text"
                  value={toExpr}
                  onChange={(e) => updateDraft({ toExpr: e.target.value, error: null })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleApply(); }}
                  placeholder="now"
                  className="flex-1 rounded border border-border bg-background px-3 py-1.5 font-mono text-sm text-foreground placeholder:text-foreground-subtle focus:border-gold/50 focus:outline-none focus:ring-1 focus:ring-gold/30"
                />
                <button
                  onClick={handleToCalendar}
                  className="flex items-center justify-center rounded border border-border bg-background p-1.5 text-foreground-subtle transition-colors hover:border-gold/50 hover:text-gold"
                  title="Pick date"
                  aria-label="Pick to date"
                >
                  <Calendar className="h-3.5 w-3.5" />
                </button>
                <input
                  ref={toDateRef}
                  type="date"
                  onChange={handleToDateChange}
                  value={toDateLocal(parseRelativeTime(toExpr) ?? new Date())}
                  className="sr-only color-scheme-dark"
                  tabIndex={-1}
                />
              </div>
            </div>

            {/* Error message */}
            {error && (
              <p className="text-xs text-status-failed">{error}</p>
            )}

            {/* Apply button */}
            <button
              onClick={handleApply}
              className="w-full rounded-md bg-gold/15 px-4 py-2 text-sm font-medium text-gold transition-colors hover:bg-gold/25 focus:outline-none focus:ring-2 focus:ring-gold/30"
            >
              Apply time range
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
