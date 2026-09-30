import { useEffect, useRef, useState } from "react";
import { Upload, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ThemeManifest } from "@/theme";
import { fileBase64, themeError, type ThemeDraft } from "./theme-api";
import { Field } from "./editor-fields";
import { objectValue, setDraftField, stringAt, referencesAsset } from "./editor-values";

const mimeTypes: Record<string, string> = { svg: "image/svg+xml", png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg", webp: "image/webp", avif: "image/avif", woff2: "font/woff2" };
export function EditorAssets({ manifest, assets, disabled, generation, currentGeneration, change }: { manifest: ThemeManifest; assets: Record<string, string>; disabled: boolean; generation: number; currentGeneration: () => number; change: (draft: ThemeDraft) => void }) {
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const input = useRef<HTMLInputElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [reading, setReading] = useState(false);
  const declarations = objectValue(manifest.assets);
  async function upload(file?: File) {
    if (!file) return;
    setReading(true); setError(null);
    try {
      const extension = file.name.split(".").pop()?.toLowerCase() ?? "";
      if (!mimeTypes[extension]) throw new Error("Choose SVG, PNG, JPEG, WebP, AVIF or WOFF2.");
      if (Object.keys(declarations).length >= 64) throw new Error("A theme supports at most 64 assets.");
      const base = file.name.replace(/\.[^.]+$/, "").toLowerCase().replace(/[^a-z0-9-]/g, "-").replace(/^-+/, "").slice(0, 48);
      const stem = /^[a-z]/.test(base) ? base : `asset-${base || "image"}`;
      let id = stem; let number = 2;
      while (declarations[id]) id = `${stem}-${number++}`;
      const encoded = await fileBase64(file);
      if (!mounted.current) return;
      if (generation !== currentGeneration()) throw new Error("The draft changed while reading this file. Add the asset again to keep those edits.");
      const path = `assets/${id}.${extension}`;
      const next = setDraftField(manifest, ["assets", id], { path, mediaType: mimeTypes[extension] });
      change({ source: JSON.stringify(next, null, 2), assets: { ...assets, [path]: encoded } });
    } catch (error) { setError(themeError(error)); }
    finally { setReading(false); }
  }
  function remove(id: string) {
    const next = setDraftField(manifest, ["assets", id], undefined);
    const files = { ...assets };
    delete files[stringAt(declarations[id], "path")];
    change({ source: JSON.stringify(next, null, 2), assets: files });
  }
  return <section className="appearance-form-section"><div className="flex items-center justify-between gap-3"><h3>Local Assets</h3><Button variant="outline" size="sm" disabled={disabled || reading} onClick={() => input.current?.click()}><Upload size={14} />{reading ? "Reading…" : "Add Asset"}</Button><input ref={input} type="file" hidden accept=".svg,.png,.jpg,.jpeg,.webp,.avif,.woff2" aria-label="Upload theme asset" onChange={(event) => { void upload(event.target.files?.[0]); event.target.value = ""; }} /></div><p className="appearance-help">Static images, SVG icons and licensed WOFF2 fonts. Up to 8 MiB per file and 32 MiB per package. Select uploaded assets in Identity &amp; Lore or Typography.</p>{error && <p role="alert" className="mt-3 text-sm text-status-failed">{error}</p>}
    {Object.entries(declarations).map(([id, asset]) => { const used = referencesAsset({ ...manifest, assets: undefined }, id); return <div key={id} className="appearance-asset-row"><div className="flex items-center justify-between gap-3"><div className="min-w-0"><p className="truncate text-sm">{id}</p><p className="mt-1 text-xs text-foreground-muted">{stringAt(asset, "mediaType")} · {assets[stringAt(asset, "path")] ? `${Math.round(assets[stringAt(asset, "path")].length * .75 / 1024)} KiB` : "File Missing"}</p></div><button type="button" className="appearance-icon-button" disabled={disabled || used} aria-label={`Remove asset ${id}`} title={used ? "Remove this asset's references first." : "Remove asset"} onClick={() => remove(id)}><X size={15} /></button></div>{stringAt(asset, "mediaType") === "font/woff2" && <div className="mt-3"><Field label={`${id} License or Ownership`}><input className="appearance-input" disabled={disabled} value={stringAt(asset, "license")} placeholder="For example, SIL Open Font License 1.1" onChange={(event) => change({ source: JSON.stringify(setDraftField(manifest, ["assets", id, "license"], event.target.value), null, 2), assets })} /></Field></div>}</div>; })}
  </section>;
}
