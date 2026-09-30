import { useLayoutEffect, useMemo, useRef } from "react";
import { builtinAssetURLs, revisionAssets } from "@/theme/assets";
import { applyPresentation } from "@/theme/presentation";
import { ManifestIcon } from "@/theme/theme-icon";
import type { ResolvedThemeManifest, ThemePackage } from "@/theme";

export function ThemePreviewCard({ manifest, pkg, files, compact = false }: { manifest: ResolvedThemeManifest; pkg?: ThemePackage; files?: Readonly<Record<string, string>>; compact?: boolean }) {
  const root = useRef<HTMLDivElement>(null);
  const urls = useMemo(() => {
    if (!pkg) return builtinAssetURLs(manifest);
    if (!files) return revisionAssets(pkg);
    const inherited = builtinAssetURLs(manifest);
    return Object.fromEntries(Object.entries(pkg.manifest.assets).flatMap(([id, asset]) => {
      const url = typeof files[asset.path] === "string"
        ? `data:${asset.mediaType};base64,${files[asset.path]}` : inherited[id];
      return url ? [[id, url]] : [];
    }));
  }, [pkg, files, manifest]);
  useLayoutEffect(() => root.current ? applyPresentation({ theme: manifest, activePackage: pkg ?? null, assetURLs: urls }, root.current) : undefined, [manifest, pkg, urls]);
  const colors = manifest.tokens;
  const classic = manifest.surfaces.chrome === "classic";
  return <div ref={root} className={`ui-window theme-overlay theme-study-preview ${compact ? "compact" : ""}`} style={{ background: colors.canvas, color: colors["text.primary"], borderColor: classic ? undefined : colors["border.default"], fontFamily: "var(--theme-font-body)" }} aria-label={`${manifest.name} presentation sample`}>
    <div className="theme-study-rail" style={{ borderColor: colors["border.default"], color: colors["accent.base"] }} aria-hidden="true"><ManifestIcon theme={manifest} assetURLs={urls} slot="brand" size={24} /><i /><i /><i /></div>
    <div className="min-w-0 flex-1">
      <div className="ui-window-titlebar mb-3 flex items-center justify-between gap-3 text-[10px]" style={classic ? undefined : { color: colors["text.secondary"] }}><span>Theme Sample</span><span>Typography &amp; Color</span></div>
      <div className="theme-ornament mb-3 h-1 w-10 rounded-full" style={{ background: colors["accent.base"] }} aria-hidden="true" />
      <p className="mb-2 text-2xl leading-tight" style={{ fontFamily: "var(--theme-font-display)", fontWeight: manifest.typography.headingWeight }}>An Idea Becomes Reality</p>
      {!compact && <p className="mb-5 text-xs" style={{ color: colors["text.secondary"] }}>A clear plan. A team with purpose. Every step in view.</p>}
      <div className="flex items-center gap-2" aria-hidden="true">{["Plan", "Build", "Review"].map((step, index) => <span key={step} className="ui-button ui-button--outline flex-1 rounded-md border px-2 py-3 text-center text-[10px]" style={classic ? undefined : { borderWidth: "var(--theme-border-width)", borderRadius: "var(--theme-radius)", borderColor: index === 1 ? colors["accent.base"] : colors["border.default"], background: index === 1 ? colors["dag.nodeActiveBackground"] : colors["surface.base"], color: index === 1 ? colors["accent.base"] : colors["text.secondary"] }}>{step}</span>)}</div>
      {!compact && <div className="mt-5 flex items-center justify-between gap-3 border-t pt-4 text-xs" style={{ borderColor: colors["border.default"] }}><span>{manifest.identities.builder.name}<span className="ml-2" style={{ color: colors["text.subtle"] }}>Builder</span></span><span className="ui-badge shrink-0 rounded px-2.5 py-1" style={{ color: colors["status.succeeded.foreground"], background: colors["status.succeeded.background"] }}>Complete</span></div>}
      {!compact && <div className="mt-5 space-y-3 border-t pt-4 text-xs" style={{ borderColor: colors["border.default"] }}>
        <p style={{ color: colors["text.secondary"] }}>Native Control Sample</p>
        <label className="flex items-center gap-2"><input type="checkbox" defaultChecked className="theme-native-checkbox" /> Checkbox</label>
        <label className="grid gap-2">Date<input type="date" defaultValue="1998-06-25" className="theme-native-sample" /></label>
      </div>}
    </div>
  </div>;
}
