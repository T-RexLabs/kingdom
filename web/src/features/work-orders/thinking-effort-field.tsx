import type { ProviderModel } from "@/api/hooks";
import { SelectField } from "@/components/ui/select-field";

export function ThinkingEffortField({
  id,
  label = "",
  value,
  model,
  disabled = false,
  onChange,
}: {
  id: string;
  label?: string;
  value: string;
  model?: ProviderModel;
  disabled?: boolean;
  onChange: (next: string) => void;
}) {
  const options = model?.thinking_effort_options ?? [];

  if (options.length === 0) {
    return (
      <div>
        {label ? (
          <label htmlFor={id} className="mb-1.5 block text-sm font-medium">
            {label}
          </label>
        ) : null}
        <div
          id={id}
          className="flex min-h-10 items-center py-2 text-sm text-foreground-muted"
        >
          Not available for this model
        </div>
      </div>
    );
  }

  return (
    <SelectField
      id={id}
      label={label || undefined}
      value={value}
      options={options.map((o) => ({ label: o.label ?? o.value, value: o.value }))}
      placeholder="Select effort"
      disabled={disabled}
      onChange={onChange}
    />
  );
}
