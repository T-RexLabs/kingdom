import { useId, useState } from "react";
import { useInteractions } from "@floating-ui/react";
import { ChevronDown, Clock } from "lucide-react";
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

/** The API accepts a rolling duration ending now, not an absolute date range. */
export function TimeRangePicker({
  value,
  onChange,
  className,
}: {
  value: number;
  onChange: (minutes: number) => void;
  className?: string;
}) {
  const id = useId();
  const [open, setOpen] = useState(false);
  const preset = PRESETS.some(([minutes]) => minutes === value);
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
              aria-pressed={value === minutes}
              onClick={() => {
                onChange(minutes);
                setOpen(false);
              }}
              className={cn(
                "min-w-8 px-2 font-mono",
                value === minutes && "bg-accent/15 text-accent hover:bg-accent/20 hover:text-accent",
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
          className={cn("min-h-10", !preset && "border-accent bg-accent/10 text-accent")}
        >
          <Clock className="h-3.5 w-3.5" aria-hidden="true" />
          <span>{preset ? "Custom" : durationLabel(value)}</span>
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
          className="w-[22rem] max-w-[calc(100vw-1rem)]"
        >
          <CustomWindow
            key={value}
            id={id}
            initial={value}
            onApply={(minutes) => {
              onChange(minutes);
              setOpen(false);
            }}
          />
        </PopoverSurface>
      )}
    </div>
  );
}

function CustomWindow({
  id,
  initial,
  onApply,
}: {
  id: string;
  initial: number;
  onApply: (minutes: number) => void;
}) {
  const [draft, setDraft] = useState(`now-${durationLabel(initial)}`);
  const [error, setError] = useState("");
  return (
    <form
      className="min-h-0 overflow-y-auto p-5"
      onSubmit={(event) => {
        event.preventDefault();
        const expression = /^now\s*-\s*(\d+(?:\.\d+)?)\s*(m|h|d)$/i.exec(draft.trim());
        const minutes = expression
          ? Number(expression[1]) * (MINUTES_PER_UNIT[expression[2].toLowerCase()] ?? 0)
          : 0;
        if (!Number.isSafeInteger(minutes) || minutes < 1 || minutes > 525600) {
          setError("Enter a window such as now-90m, now-36h or now-14d, from 1 minute to 365 days in whole minutes.");
          return;
        }
        onApply(minutes);
      }}
    >
      <h2 id={`${id}-title`} className="text-sm font-semibold">Custom Time Window</h2>
      <p className="mt-2 text-xs leading-relaxed text-foreground-muted">
        Show a rolling window of activity ending now.
      </p>
      <div className="mt-5 space-y-4">
        <div>
          <label htmlFor={`${id}-from`} className={fieldLabelClass}>From</label>
          <input
            id={`${id}-from`}
            type="text"
            value={draft}
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => {
              setDraft(event.target.value);
              setError("");
            }}
            aria-invalid={Boolean(error)}
            aria-describedby={`${id}-help${error ? ` ${id}-error` : ""}`}
            className={cn(fieldClass, "font-mono")}
          />
          <p id={`${id}-help`} className="mt-2 text-xs leading-relaxed text-foreground-muted">
            Use now-90m, now-36h or now-14d. Up to 365 days.
          </p>
        </div>
        <div>
          <label htmlFor={`${id}-to`} className={fieldLabelClass}>To</label>
          <input
            id={`${id}-to`}
            type="text"
            value="Now"
            readOnly
            tabIndex={-1}
            className={cn(fieldClass, "bg-surface-raised text-foreground-muted")}
          />
        </div>
      </div>
      {error && (
        <p id={`${id}-error`} role="alert" className="mt-3 text-xs leading-relaxed text-status-failed">
          {error}
        </p>
      )}
      <div className="mt-5 flex justify-end border-t border-border pt-4">
        <Button type="submit" size="sm">Apply Window</Button>
      </div>
    </form>
  );
}
