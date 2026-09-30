import { useId, useState } from "react";
import { Modal } from "./modal-shell";
import { Button } from "./button";
import { cn } from "@/lib/utils";
import { SelectField } from "./select-field";

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
      <SelectField
        id={id}
        label="Time Window"
        labelClassName="text-xs text-foreground-muted"
        className="min-w-44"
        value={String(value)}
        onChange={(next) =>
          next === "custom"
            ? setCustom(true)
            : onChange(Number(next))
        }
        options={[
          ...PRESETS.map(([minutes, label]) => ({ value: String(minutes), label })),
          ...(!preset ? [{ value: String(value), label: `Last ${value.toLocaleString()} Minutes` }] : []),
          { value: "custom", label: "Custom Window…" },
        ]}
      />
      <Modal
        open={custom}
        onClose={() => setCustom(false)}
        getFinalFocus={() => document.getElementById(id)}
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
      <input data-ui-field=""
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
