import { InfoTooltip } from "@/components/ui/info-tooltip";
import { cn } from "@/lib/utils";
import { useId } from "react";

export function ToggleRow({
  label,
  checked,
  onChange,
  info,
  description,
  disabled = false,
  className,
}: {
  label: string;
  checked: boolean;
  onChange: (next: boolean) => void;
  info?: string;
  description?: string;
  disabled?: boolean;
  className?: string;
}) {
  const descriptionId = useId();
  return (
    <div className={cn("flex min-h-10 w-full items-center justify-between gap-3", disabled && "opacity-60", className)}>
      <div className="min-w-0">
        <span className="text-sm font-medium leading-snug text-foreground">{label}</span>
        {description && <p id={descriptionId} className="mt-1 text-xs leading-relaxed text-foreground-muted">{description}</p>}
      </div>
      <div className="flex shrink-0 items-center gap-2">
        {info && <InfoTooltip label={`${label} definition`} message={info} />}
        <button
          type="button"
          role="switch"
          aria-checked={checked}
          aria-disabled={disabled}
          aria-label={label}
          aria-describedby={description ? descriptionId : undefined}
          disabled={disabled}
          onClick={() => onChange(!checked)}
          className={cn(
            "relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:cursor-not-allowed",
            checked ? "border-accent/70 bg-accent/25" : "border-border bg-background",
          )}
        >
          <span
            className={cn(
              "inline-block h-4 w-4 transform rounded-full transition",
              checked ? "translate-x-6 bg-accent" : "translate-x-1 bg-foreground-subtle",
            )}
          />
        </button>
      </div>
    </div>
  );
}
