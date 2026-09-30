import { useMemo } from "react";
import { builtinAssetURLs, type ThemeAssetURLs } from "@/theme/assets";
import type { ResolvedThemeManifest } from "@/theme";

/** These bytes and declarations must come from the same successful validation. */
export type ValidatedAssetPreview = {
  manifest: ResolvedThemeManifest;
  files: Readonly<Record<string, string>>;
};

export function useValidatedAssetURLs(theme: ResolvedThemeManifest, preview?: ValidatedAssetPreview): ThemeAssetURLs {
  return useMemo(() => {
    const inherited = builtinAssetURLs(theme);
    if (!preview) return inherited;
    return Object.fromEntries(Object.entries(preview.manifest.assets).flatMap(([id, asset]) => {
      const encoded = preview.files[asset.path];
      const url = typeof encoded === "string" ? `data:${asset.mediaType};base64,${encoded}` : inherited[id];
      return url ? [[id, url]] : [];
    }));
  }, [theme, preview]);
}
