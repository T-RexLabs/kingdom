import { API_BASE } from "@/api/client";
import type { ResolvedThemeManifest, ThemePackage, ThemeReference } from "./contract";
import { validReference } from "./contract";

export type ThemeAssetURLs = Readonly<Record<string, string>>;
export function revisionAssetURL(reference: ThemeReference, assetID: string): string {
  if (!validReference(reference) || !/^[a-z][a-z0-9-]{0,63}$/.test(assetID)) throw new Error("Invalid theme asset identity.");
  return `${API_BASE}/themes/${encodeURIComponent(reference.id)}/revisions/${reference.resolvedDigest}/assets/${encodeURIComponent(assetID)}`;
}
export function revisionAssets(pkg: ThemePackage): ThemeAssetURLs {
  const ref = { id: pkg.manifest.id, resolvedDigest: pkg.resolvedDigest };
  return Object.fromEntries(Object.keys(pkg.manifest.assets).map((id) => [id, revisionAssetURL(ref, id)]));
}

// Preview bytes must be the exact draft submitted for server validation. They
// are rendered as isolated image/font resources, never injected into markup.
export function previewAssets(pkg: ThemePackage, files: Readonly<Record<string, string>>) {
  const urls: Record<string, string> = {};
  let total = 0;
  try {
    for (const [id, asset] of Object.entries(pkg.manifest.assets)) {
      const encoded = files[asset.path];
      if (typeof encoded !== "string" || encoded.length > Math.ceil((8 << 20) / 3) * 4) throw new Error("Preview asset is missing or exceeds its size limit.");
      const binary = atob(encoded);
      total += binary.length;
      if (!binary.length || binary.length > 8 << 20 || total > 32 << 20) throw new Error("Preview assets exceed their size limit.");
      const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
      urls[id] = URL.createObjectURL(new Blob([bytes], { type: asset.mediaType }));
    }
  } catch (error) {
    Object.values(urls).forEach((url) => URL.revokeObjectURL(url));
    throw error;
  }
  return { urls, dispose: () => Object.values(urls).forEach((url) => URL.revokeObjectURL(url)) };
}
export function imageAssetURL(manifest: ResolvedThemeManifest, urls: ThemeAssetURLs, reference?: string): string | undefined {
  if (!reference?.startsWith("asset:")) return undefined;
  const id = reference.slice(6);
  return manifest.assets[id]?.mediaType.startsWith("image/") ? urls[id] : undefined;
}
const fontStacks = {
  body: 'ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  display: 'ui-serif, Georgia, Cambria, "Times New Roman", serif',
  mono: 'ui-monospace, "SFMono-Regular", Consolas, "Liberation Mono", monospace',
};
function bundledFont(reference: string): string | undefined {
  if (reference === "bundled:dm-sans") return `"DM Sans", ${fontStacks.body}`;
  if (reference === "bundled:instrument-serif") return `"Instrument Serif", ${fontStacks.display}`;
  if (reference === "bundled:jetbrains-mono") return `"JetBrains Mono", ${fontStacks.mono}`;
  if (reference === "bundled:system-sans") return fontStacks.body;
  if (reference === "bundled:system-serif") return fontStacks.display;
  if (reference === "bundled:system-mono") return fontStacks.mono;
}
export function themeFonts(manifest: ResolvedThemeManifest, urls: ThemeAssetURLs, revision: string) {
  const loads: { family: string; url: string }[] = [];
  const families = Object.fromEntries((["body", "display", "mono"] as const).map((slot) => {
    const ref = manifest.typography[slot];
    const bundled = bundledFont(ref);
    if (bundled) return [slot, bundled];
    const id = ref.startsWith("asset:") ? ref.slice(6) : "";
    const asset = manifest.assets[id];
    if (asset?.mediaType !== "font/woff2" || !urls[id]) return [slot, fontStacks[slot]];
    const family = `kingdom-theme-${manifest.id}-${revision.slice(0, 16)}-${id}`;
    if (!loads.some((load) => load.family === family)) loads.push({ family, url: urls[id] });
    return [slot, `"${family}", ${fontStacks[slot]}`];
  })) as Record<keyof typeof fontStacks, string>;
  return { families, loads };
}
