import { cn } from "@/lib/utils";
import { fieldLabelClass, selectTriggerClass } from "./field-styles";
import { PopoverListbox, type PopoverListboxItem } from "./popover-listbox";
import { SelectTriggerContent } from "./select-trigger";

export type SelectOption = {
  label: string;
  value: string;
  disabled?: boolean;
};

/**
 * Plain dropdown select. Thin wrapper over `PopoverListbox` so all dropdowns
 * in the app share the same positioning, focus, and keyboard behaviour.
 */
export function SelectField({
  id,
  label,
  ariaLabel,
  value,
  options,
  placeholder = "Select...",
  disabled = false,
  className,
  labelClassName,
  triggerClassName,
  onChange,
}: {
  id: string;
  label?: string;
  ariaLabel?: string;
  value: string;
  options: SelectOption[];
  placeholder?: string;
  disabled?: boolean;
  className?: string;
  labelClassName?: string;
  triggerClassName?: string;
  onChange: (next: string) => void;
}) {
  const selected = options.find((option) => option.value === value);
  const items: PopoverListboxItem[] = options.map((option) => ({
    id: option.value,
    label: option.label,
    disabled: option.disabled,
  }));

  return (
    <div className={cn("relative", className)}>
      {label ? (
        <label htmlFor={id} className={cn(fieldLabelClass, labelClassName)}>
          {label}
        </label>
      ) : null}
      <PopoverListbox
        triggerId={id}
        triggerRole="button"
        triggerAriaLabel={ariaLabel || label || undefined}
        triggerDisabled={disabled}
        triggerClassName={cn(
          selectTriggerClass,
          selected ? "text-foreground" : "text-foreground-subtle",
          triggerClassName,
        )}
        triggerContent={<SelectTriggerContent>{selected?.label ?? placeholder}</SelectTriggerContent>}
        items={items}
        selectedId={selected?.value ?? null}
        onSelect={onChange}
        minWidth={160}
      />
    </div>
  );
}
