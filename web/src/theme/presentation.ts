import { defaultTheme } from "./contract";
import { themeFonts } from "./assets";
import type { ThemeState } from "./controller";

export function tokenVariable(slot: string): string {
  return `--theme-${slot.replaceAll(".", "-").replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)}`;
}

// Only a schema-validated manifest reaches this function. Token values are
// hex colors; all other CSS values below come from fixed application rules.
export function applyPresentation(state: Pick<ThemeState, "theme" | "assetURLs" | "activePackage">, root: HTMLElement = document.documentElement): () => void {
  const { theme, assetURLs, activePackage } = state;
  const fonts = themeFonts(theme, assetURLs, activePackage?.resolvedDigest ?? "builtin");
  const properties: Record<string, string> = Object.fromEntries(Object.entries(theme.tokens).map(([slot, value]) => [tokenVariable(slot), value]));
  Object.assign(properties, {
    "--theme-font-body": fonts.families.body,
    "--theme-font-display": fonts.families.display,
    "--theme-font-mono": fonts.families.mono,
    "--theme-weight-body": String(theme.typography.bodyWeight),
    "--theme-weight-heading": String(theme.typography.headingWeight),
    "--theme-radius": `${theme.surfaces.radius}px`,
    "--theme-border-width": `${theme.surfaces.borderWidth}px`,
    "--theme-shadow": theme.surfaces.elevation === "soft" ? "0 12px 40px rgb(0 0 0 / 0.12)" : "none",
    "color-scheme": theme.colorScheme,
  });
  const previous = Object.fromEntries(Object.keys(properties).map((key) => [key, [root.style.getPropertyValue(key), root.style.getPropertyPriority(key)]]));
  const attributes = { "data-theme": theme.id, "data-color-scheme": theme.colorScheme, "data-ornamentation": theme.surfaces.ornamentation };
  const oldAttributes = Object.fromEntries(Object.keys(attributes).map((key) => [key, root.getAttribute(key)]));
  for (const [key, value] of Object.entries(properties)) root.style.setProperty(key, value);
  for (const [key, value] of Object.entries(attributes)) root.setAttribute(key, value);
  const chromeColor = root === document.documentElement ? document.querySelector<HTMLMetaElement>('meta[name="theme-color"]') : null;
  const previousChromeColor = chromeColor?.content;
  if (chromeColor) chromeColor.content = theme.tokens.canvas;
  const loaded: FontFace[] = [];
  let disposed = false;
  if (typeof FontFace !== "undefined" && document.fonts) {
    for (const { family, url } of fonts.loads) {
      const font = new FontFace(family, `url("${url}")`, { display: "swap", weight: "100 900" });
      void font.load().then((face) => {
        if (!disposed) { document.fonts.add(face); loaded.push(face); }
      }).catch(() => { /* The fixed fallback stack remains readable. */ });
    }
  }
  return () => {
    disposed = true;
    if (chromeColor && previousChromeColor !== undefined) chromeColor.content = previousChromeColor;
    for (const font of loaded) document.fonts.delete(font);
    for (const [key, [value, priority]] of Object.entries(previous)) {
      if (value) root.style.setProperty(key, value, priority);
      else root.style.removeProperty(key);
    }
    for (const [key, value] of Object.entries(oldAttributes)) {
      if (value === null) root.removeAttribute(key);
      else root.setAttribute(key, value);
    }
  };
}

// Call once before mounting authentication/loading surfaces. The actual pinned
// revision remains behind ThemeProvider's workspace-aware loading boundary.
export function initializeDefaultPresentation(): () => void {
  return applyPresentation({ theme: defaultTheme, assetURLs: {}, activePackage: null });
}
