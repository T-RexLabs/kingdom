import { useMemo, useState } from "react";
import { Check, ImageOff } from "lucide-react";
import { Button } from "@/components/ui/button";
import { fieldLabelClass, selectTriggerClass } from "@/components/ui/field-styles";
import { PopoverListbox, type PopoverListboxItem } from "@/components/ui/popover-listbox";
import { SelectTriggerContent } from "@/components/ui/select-trigger";
import { ManifestIcon } from "@/theme/theme-icon";
import { imageAssetURL, type ThemeAssetURLs } from "@/theme/assets";
import type { ResolvedThemeManifest } from "@/theme";
import iconCatalogText from "../../../../contracts/theme/lucide-icons.json?raw";

const symbolNames: string[] = JSON.parse(iconCatalogText);
const pageSize = 48;
function displayName(value: string) {
  return value.replaceAll("-", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
const symbolOptions = symbolNames.map((name) => ({ id: `lucide:${name}`, label: displayName(name), group: "Symbols" }));

function ImageThumbnail({ url }: { url: string }) {
  const [failed, setFailed] = useState(false);
  return failed ? <ImageOff size={20} aria-hidden="true" /> : <img src={url} alt="" className="h-6 w-6 object-contain" onError={() => setFailed(true)} />;
}

function ChoiceThumbnail({ reference, kind, theme, assetURLs }: {
  reference: string;
  kind: "icon" | "image";
  theme: ResolvedThemeManifest;
  assetURLs: ThemeAssetURLs;
}) {
  const url = imageAssetURL(theme, assetURLs, reference);
  return <span className="flex h-8 w-8 shrink-0 items-center justify-center" aria-hidden="true">
    {kind === "icon" && reference
      ? <ManifestIcon theme={{ ...theme, icons: { ...theme.icons, brand: reference } }} assetURLs={assetURLs} slot="brand" size={24} />
      : url ? <ImageThumbnail key={url} url={url} /> : <ImageOff size={20} />}
  </span>;
}

/** Search the full catalog; render glyphs only for a bounded, expandable batch. */
export function ThemeImagePicker({ id, label, help, value, inheritedValue = "", kind, theme, assetURLs, disabled, onChange }: {
  id: string;
  label: string;
  help?: string;
  value: string;
  inheritedValue?: string;
  kind: "icon" | "image";
  theme: ResolvedThemeManifest;
  assetURLs: ThemeAssetURLs;
  disabled: boolean;
  onChange: (reference: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(pageSize);
  const options = useMemo<PopoverListboxItem[]>(() => {
    const assets = Object.entries(theme.assets)
      .filter(([id, asset]) => assetURLs[id] && (kind === "icon" ? asset.mediaType === "image/svg+xml" : asset.mediaType.startsWith("image/")))
      .map(([id]) => ({ id: `asset:${id}`, label: `${displayName(id)} (Asset)`, group: "Local Assets" }));
    return kind === "image"
      ? [{ id: "", label: "None / Inherited", group: "Default" }, ...assets]
      : [...assets, ...symbolOptions];
  }, [theme, assetURLs, kind]);
  const selected = options.find((option) => option.id === value);
  const normalized = query.trim().toLowerCase();
  const matching = options.filter((option) => `${option.label} ${option.id}`.toLowerCase().includes(normalized));
  return <div className="relative min-w-0">
    <label htmlFor={id} className={fieldLabelClass}>{label}</label>
    <PopoverListbox
      triggerId={id}
      triggerRole="combobox"
      triggerAriaLabel={label}
      triggerDisabled={disabled}
      triggerClassName={selectTriggerClass}
      triggerContent={<SelectTriggerContent><span className="flex min-w-0 items-center gap-2"><ChoiceThumbnail reference={value || inheritedValue} kind={kind} theme={theme} assetURLs={assetURLs} /><span className="truncate">{selected?.label ?? (value || "Choose an image")}</span></span></SelectTriggerContent>}
      items={matching.slice(0, limit)}
      selectedId={selected?.id ?? null}
      onOpenChange={(open) => { if (!open) { setQuery(""); setLimit(pageSize); } }}
      onSelect={onChange}
      search={{ value: query, onChange: (next) => { setQuery(next); setLimit(pageSize); }, placeholder: `Search ${label}` }}
      groupBy
      minWidth={260}
      maxHeight={360}
      emptyState={<p className="px-3 py-3 text-sm text-foreground-muted">No matching images or symbols.</p>}
      footer={matching.length > limit ? <Button variant="ghost" size="sm" className="w-full" onClick={() => setLimit((current) => current + pageSize)}>Show More Choices</Button> : undefined}
      renderItem={(item, { selected }) => <>
        <ChoiceThumbnail reference={item.id || inheritedValue} kind={kind} theme={theme} assetURLs={assetURLs} />
        <span className="min-w-0 flex-1 text-wrap-anywhere">{item.label}</span>
        <Check size={16} className={selected ? "shrink-0" : "invisible shrink-0"} aria-hidden="true" />
      </>}
    />
    {help && <p className="appearance-help mt-2">{help}</p>}
  </div>;
}
