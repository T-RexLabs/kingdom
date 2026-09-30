import { Circle } from "lucide-react";
import { DynamicIcon, iconNames } from "lucide-react/dynamic";
import { useState, type CSSProperties } from "react";
import { useTheme } from "./context";
import type { ThemeAssetURLs } from "./assets";
import type { IconSlot, ResolvedThemeManifest } from "./contract";

const lucideNames = new Set<string>(iconNames);
function IconFallback() { return <Circle size="100%" aria-hidden="true" />; }
function AssetIcon({ url, size, title, className }: { url: string; size: number | string; title?: string; className?: string }) {
  const [failed, setFailed] = useState(false);
  if (failed) return <span className={className} style={{ display: "inline-flex", width: size, height: size }} role={title ? "img" : undefined} aria-label={title} aria-hidden={title ? undefined : true}><IconFallback /></span>;
  // A mask uses the same foreground color as Lucide; SVG stays an isolated
  // resource. The hidden image detects decoding failures for a neutral fallback.
  return <span className={className} style={{ display: "inline-block", flexShrink: 0, width: size, height: size, backgroundColor: "currentColor", maskImage: `url("${url}")`, maskRepeat: "no-repeat", maskPosition: "center", maskSize: "contain" } as CSSProperties} role={title ? "img" : undefined} aria-label={title} aria-hidden={title ? undefined : true}><img src={url} alt="" hidden onError={() => setFailed(true)} /></span>;
}
export function ThemeIcon({ slot, size = 20, title, className }: { slot: IconSlot | `role.${string}`; size?: number | string; title?: string; className?: string }) {
  const { theme, assetURLs } = useTheme();
  return <ManifestIcon theme={theme} assetURLs={assetURLs} slot={slot} size={size} title={title} className={className} />;
}
/** Scoped package icon for previews; never changes the active appearance. */
export function ManifestIcon({ theme, assetURLs, slot, size = 20, title, className }: { theme: ResolvedThemeManifest; assetURLs: ThemeAssetURLs; slot: IconSlot | `role.${string}`; size?: number | string; title?: string; className?: string }) {
  const reference = theme.icons[slot as IconSlot];
  const assetID = reference?.startsWith("asset:") ? reference.slice(6) : "";
  if (assetID && theme.assets[assetID]?.mediaType === "image/svg+xml" && assetURLs[assetID]) {
    return <AssetIcon key={assetURLs[assetID]} url={assetURLs[assetID]} size={size} title={title} className={className} />;
  }
  const symbol = reference?.startsWith("lucide:") ? reference.slice(7) : "circle";
  if (!lucideNames.has(symbol)) return <Circle size={size} className={className} role={title ? "img" : undefined} aria-label={title} aria-hidden={title ? undefined : true} />;
  return <span className={className} style={{ display: "inline-flex", flexShrink: 0, width: size, height: size }} role={title ? "img" : undefined} aria-label={title} aria-hidden={title ? undefined : true}><DynamicIcon key={symbol} name={symbol as (typeof iconNames)[number]} size="100%" fallback={IconFallback} aria-hidden="true" /></span>;
}
