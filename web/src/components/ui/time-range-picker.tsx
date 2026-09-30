import { useId, useState } from "react";
import { Modal } from "./modal-shell";
import { Button } from "./button";
import { cn } from "@/lib/utils";

const PRESETS = [
  [30, "Last 30 Minutes"],
  [60, "Last Hour"],
  [180, "Last 3 Hours"],
  [360, "Last 6 Hours"],
  [720, "Last 12 Hours"],
  [1440, "Last 24 Hours"],
  [2880, "Last 2 Days"],
  [10080, "Last 7 Days"],
  [43200, "Last 30 Days"],
] as const;
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
  const [custom, setCustom] = useState(false);
  const preset = PRESETS.some(([minutes]) => minutes === value);
  return (
    <div className={cn("space-y-2", className)}>
      <label htmlFor={id} className="block text-xs text-foreground-muted">
        Time Window
      </label>
      <select
        id={id}
        value={preset ? String(value) : "selected-custom"}
        onChange={(event) =>
          event.target.value === "custom"
            ? setCustom(true)
            : onChange(Number(event.target.value))
        }
        className="min-h-10 w-full min-w-44 rounded-md border border-border bg-surface px-3 text-sm"
      >
        {PRESETS.map(([minutes, label]) => (
          <option key={minutes} value={minutes}>
            {label}
          </option>
        ))}
        {!preset && (
          <option value="selected-custom">
            Last {value.toLocaleString()} Minutes
          </option>
        )}
        <option value="custom">Custom Window…</option>
      </select>
      <Modal
        open={custom}
        onClose={() => setCustom(false)}
        title="Custom Time Window"
        size="sm"
      >
        <CustomWindow
          key={value}
          initial={value}
          onApply={(minutes) => {
            onChange(minutes);
            setCustom(false);
          }}
        />
      </Modal>
    </div>
  );
}
function CustomWindow({
  initial,
  onApply,
}: {
  initial: number;
  onApply: (minutes: number) => void;
}) {
  const id = useId();
  const [draft, setDraft] = useState(String(initial));
  const [error, setError] = useState("");
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        const minutes = Number(draft);
        if (
          !/^\d+$/.test(draft) ||
          !Number.isSafeInteger(minutes) ||
          minutes < 1 ||
          minutes > 525600
        ) {
          setError("Enter a whole number from 1 to 525,600 minutes.");
          return;
        }
        onApply(minutes);
      }}
    >
      <p className="mb-5 text-sm text-foreground-muted">
        Show this many minutes of activity, ending now. Run-specific usage
        always covers that entire run.
      </p>
      <label htmlFor={id} className="mb-2 block text-sm">
        Minutes
      </label>
      <input
        id={id}
        type="text"
        inputMode="numeric"
        value={draft}
        onChange={(event) => {
          setDraft(event.target.value);
          setError("");
        }}
        aria-invalid={Boolean(error)}
        aria-describedby={error ? `${id}-error` : undefined}
        className="min-h-11 w-full rounded-md border border-border bg-background px-3"
      />
      {error && (
        <p
          id={`${id}-error`}
          role="alert"
          className="mt-2 text-sm text-status-failed"
        >
          {error}
        </p>
      )}
      <div className="mt-6 flex justify-end">
        <Button type="submit">Apply Window</Button>
      </div>
    </form>
  );
}
