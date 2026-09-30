import { useMemo, useState } from "react";
import {
  PopoverListbox,
  type PopoverListboxItem,
} from "@/components/ui/popover-listbox";
import type { ProviderModel } from "@/api/hooks";
import { modelDisplayLabel, selectedProviderModelLabel } from "./provider-model-selection";

/**
 * Searchable, grouped model picker. Shares positioning + a11y with
 * `SelectField` via the same `PopoverListbox` primitive.
 */
export function ModelSelectField({
  id,
  label = "",
  value,
  options,
  loading,
  disabled = false,
  triggerPlaceholder = "Select model",
  emptyStateLabel = "No models found.",
  onChange,
}: {
  id: string;
  label?: string;
  value: string;
  options: ProviderModel[];
  loading: boolean;
  disabled?: boolean;
  triggerPlaceholder?: string;
  emptyStateLabel?: string;
  onChange: (next: string) => void;
}) {
  const [query, setQuery] = useState("");
  const normalizedQuery = query.trim().toLowerCase();

  const filtered = useMemo(
    () =>
      normalizedQuery === ""
        ? options
        : options.filter((item) =>
            `${item.provider_name} ${item.model_name}`.toLowerCase().includes(normalizedQuery),
          ),
    [options, normalizedQuery],
  );

  // Stable provider order so the listbox does not jump as items filter in/out.
  const items: PopoverListboxItem[] = useMemo(() => {
    const grouped = new Map<string, ProviderModel[]>();
    for (const item of filtered) {
      const provider = item.provider_name.trim() || "Other";
      const bucket = grouped.get(provider);
      if (bucket) bucket.push(item);
      else grouped.set(provider, [item]);
    }
    const providers = [...grouped.keys()].sort((left, right) => left.localeCompare(right));
    const flat: PopoverListboxItem[] = [];
    for (const provider of providers) {
      for (const item of grouped.get(provider) ?? []) {
        flat.push({
          id: item.id,
          label: modelDisplayLabel(item),
          group: provider,
        });
      }
    }
    return flat;
  }, [filtered]);

  const selectedLabel = selectedProviderModelLabel(options, value, triggerPlaceholder);
  const comboboxLabel = label === "" ? "Model" : /model/i.test(label) ? label : `${label} Model`;

  if (loading) {
    return (
      <div className="relative">
        {label !== "" ? (
          <label htmlFor={id} className="mb-1.5 block text-sm font-medium">
            {label}
          </label>
        ) : null}
        <button
          id={id}
          type="button"
          disabled
          className="w-full truncate rounded-md border border-border bg-background px-3 py-2 text-left text-sm text-foreground-subtle"
          aria-label={comboboxLabel}
        >
          {selectedProviderModelLabel(options, value, "Loading models...")}
        </button>
      </div>
    );
  }

  return (
    <div className="relative">
      {label !== "" ? (
        <label htmlFor={id} className="mb-1.5 block text-sm font-medium">
          {label}
        </label>
      ) : null}
      <PopoverListbox
        triggerId={id}
        triggerRole="combobox"
        triggerAriaLabel={comboboxLabel}
        triggerDisabled={disabled}
        triggerClassName={[
          "w-full truncate rounded-md border border-border bg-background px-3 py-2 text-left text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent",
          disabled ? "cursor-not-allowed opacity-50 text-foreground-subtle" : "text-foreground",
        ].join(" ")}
        triggerContent={selectedLabel}
        items={items}
        selectedId={value || null}
        onSelect={(next) => {
          setQuery("");
          onChange(next);
        }}
        search={{ value: query, onChange: setQuery, placeholder: "Search models" }}
        groupBy
        minWidth={280}
        maxHeight={360}
        emptyState={
          <p className="px-3 py-2 text-xs text-foreground-subtle">{emptyStateLabel}</p>
        }
      />
    </div>
  );
}
