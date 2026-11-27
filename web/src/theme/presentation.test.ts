import { describe, expect, it, vi } from "vitest";
import { builtInThemes, canonicalRoleName, defaultTheme, resolvedManifest, roleIdentity, themePackage } from "./contract";
import { applyPresentation, tokenVariable } from "./presentation";
import { imageAssetURL, previewAssets, revisionAssetURL, themeFonts } from "./assets";
import * as browserBrand from "./browser-brand";
import iconCatalog from "../../../contracts/theme/lucide-icons.json?raw";
import { iconNames } from "lucide-react/dynamic";

describe("canonical presentation contract", () => {
  it("ships all five complete themes from the shared contract", () => {
    expect(builtInThemes.map((theme) => theme.id)).toEqual(["obsidian-court", "nautilus", "astral-council", "software-lab", "t-rex-labs", "workstation-98"]);
    for (const theme of builtInThemes) {
      expect(Object.keys(resolvedManifest(theme).identities)).toHaveLength(14);
      expect(theme.identities.builder).not.toHaveProperty("icon");
      expect(theme.icons["role.builder"]).toBeTruthy();
    }
    expect(new Set(JSON.parse(iconCatalog))).toEqual(new Set(iconNames));
  });
  it("keeps canonical responsibility visible across themed and future roles", () => {
    expect(roleIdentity(defaultTheme, "builder").name).toBe("Berserker");
    expect(roleIdentity(defaultTheme, "builder").canonicalName).toBe("Builder");
    expect(canonicalRoleName("future-advisor")).toBe("Future Advisor");
    expect(roleIdentity(defaultTheme, "future-advisor").name).toBe("Future Advisor");
  });
  it.each(["minimal", "balanced", "high", "ludicrous"] as const)("accepts a %s revision and restores the previous ornamentation after preview", (ornamentation) => {
    const manifest = JSON.parse(JSON.stringify({ ...defaultTheme, surfaces: { ...defaultTheme.surfaces, ornamentation } }));
    const pkg = themePackage({ manifest, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
    expect(pkg.manifest.surfaces).toEqual({ ...defaultTheme.surfaces, ornamentation });
    const root = document.createElement("div");
    root.dataset.ornamentation = "balanced";
    const restore = applyPresentation({ theme: pkg.manifest, assetURLs: {}, activePackage: pkg }, root);
    expect(root.dataset.ornamentation).toBe(ornamentation);
    restore();
    expect(root.dataset.ornamentation).toBe("balanced");
  });
  it.each(["unbounded", "crazy"])("rejects unsupported ornamentation %s in a resolved revision", (ornamentation) => {
    const manifest = { ...defaultTheme, surfaces: { ...defaultTheme.surfaces, ornamentation } };
    expect(() => resolvedManifest(manifest)).toThrow("not a complete supported presentation");
  });
  it("applies all tokens and restores root appearance after a light preview", () => {
    const root = document.createElement("div");
    root.style.setProperty("--theme-canvas", "#112233");
    root.setAttribute("data-color-scheme", "dark");
    const restore = applyPresentation({ theme: builtInThemes[3], assetURLs: {}, activePackage: null }, root);
    expect(root.style.getPropertyValue("color-scheme")).toBe("light");
    expect(root.style.getPropertyValue(tokenVariable("dag.nodeActiveBackground"))).toBe(builtInThemes[3].tokens["dag.nodeActiveBackground"]);
    expect(root.style.getPropertyValue("--theme-font-display")).toContain("DM Sans");
    restore();
    expect(root.style.getPropertyValue("--theme-canvas")).toBe("#112233");
    expect(root.getAttribute("data-color-scheme")).toBe("dark");
    expect(root.style.getPropertyValue("--theme-font-display")).toBe("");
  });
  it("gives every boundary its own primary-action colors, falling back to the accent", () => {
    const storybook = builtInThemes.find((item) => item.id === "t-rex-labs")!;
    const app = document.createElement("div");
    const sample = document.createElement("div");
    app.append(sample);
    const restoreApp = applyPresentation({ theme: storybook, activePackage: null, assetURLs: {} }, app);
    const restoreSample = applyPresentation({ theme: defaultTheme, activePackage: null, assetURLs: {} }, sample);
    expect(app.dataset.chrome).toBe("storybook");
    expect(app.style.getPropertyValue("--theme-action-base")).toBe(storybook.tokens["action.base"]);
    expect(sample.style.getPropertyValue("--theme-action-base")).toBe(defaultTheme.tokens["accent.base"]);
    expect(sample.style.getPropertyValue("--theme-action-border")).toBe(defaultTheme.tokens["text.primary"]);
    restoreSample();
    restoreApp();
    expect(app.style.getPropertyValue("--theme-action-base")).toBe("");
  });
  it("applies classic chrome and browser branding without changing scoped samples", () => {
    const theme = builtInThemes.find((item) => item.id === "workstation-98")!;
    const restoreBrowserBrand = vi.fn();
    const renderBrand = vi.spyOn(browserBrand, "applyBrowserBrand").mockReturnValue(restoreBrowserBrand);
    document.documentElement.dataset.chrome = "flat";
    const assetID = theme.icons.brand.slice(6);
    const restore = applyPresentation({ theme, activePackage: null, assetURLs: { [assetID]: "/brand.svg" } });
    try {
      expect(document.documentElement.dataset.chrome).toBe("classic");
      expect(document.documentElement.style.getPropertyValue("--theme-font-body")).toContain("Tahoma");
      expect(renderBrand).toHaveBeenCalledWith("/brand.svg", theme.tokens.canvas);
      const sample = document.createElement("div");
      const restoreSample = applyPresentation({ theme: defaultTheme, activePackage: null, assetURLs: {} }, sample);
      expect(sample.dataset.chrome).toBe("flat");
      expect(renderBrand).toHaveBeenCalledTimes(1);
      restoreSample();
      expect(restoreBrowserBrand).not.toHaveBeenCalled();
    } finally { restore(); renderBrand.mockRestore(); }
    expect(document.documentElement.dataset.chrome).toBe("flat");
    expect(restoreBrowserBrand).toHaveBeenCalledOnce();
    delete document.documentElement.dataset.chrome;
  });
  it("previews inherited built-in icons without revoking static URLs", () => {
    const manifest = structuredClone(builtInThemes.find((theme) => theme.id === "workstation-98")!);
    manifest.id = "corporate-copy";
    const pkg = themePackage({ manifest, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
    const revoke = vi.fn();
    vi.stubGlobal("URL", class extends URL { static revokeObjectURL = revoke; });
    try {
      const preview = previewAssets(pkg, {});
      expect(Object.keys(preview.urls)).toHaveLength(Object.keys(manifest.assets).length);
      expect(Object.values(preview.urls).every((url) => url.length > 0)).toBe(true);
      preview.dispose();
      expect(revoke).not.toHaveBeenCalled();
    } finally { vi.unstubAllGlobals(); }
  });
  it("generates same-origin asset URLs from safe identity and ignores asset paths", () => {
    const url = revisionAssetURL({ id: "custom", resolvedDigest: "a".repeat(64) }, "portrait");
    expect(url).toBe(`/api/v1/themes/custom/revisions/${"a".repeat(64)}/assets/portrait`);
    expect(() => revisionAssetURL({ id: "../private", resolvedDigest: "a".repeat(64) }, "portrait")).toThrow();
    expect(imageAssetURL(defaultTheme, {}, "https://bad.invalid/image.svg")).toBeUndefined();
    expect(themeFonts(defaultTheme, {}, "test").families.display).toContain("Instrument Serif");
  });
  it("revokes private asset URLs on cancellation and rejected partial allocation", () => {
    const create = vi.fn(() => "blob:private-preview");
    const revoke = vi.fn();
    vi.stubGlobal("URL", class extends URL { static createObjectURL = create; static revokeObjectURL = revoke; });
    try {
      const manifest = structuredClone(defaultTheme);
      manifest.assets = { crest: { path: "assets/crest.svg", mediaType: "image/svg+xml" } };
      manifest.icons["role.builder"] = "asset:crest";
      const pkg = themePackage({ manifest, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
      const preview = previewAssets(pkg, { "assets/crest.svg": btoa('<svg viewBox="0 0 24 24"/>') });
      expect(preview.urls.crest).toBe("blob:private-preview");
      preview.dispose();
      expect(revoke).toHaveBeenCalledWith("blob:private-preview");
      expect(() => previewAssets(pkg, {})).toThrow("missing");
    } finally { vi.unstubAllGlobals(); }
  });
});

it("never treats stored preference payloads as a theme stylesheet", async () => {
  const { readPreference, preferenceKey } = await import("./preferences");
  const ref = { id: "obsidian-court", resolvedDigest: "a".repeat(64) };
  localStorage.setItem(preferenceKey("workspace"), JSON.stringify({ ...ref, tokens: { canvas: "url(https://bad.invalid)" } }));
  expect(readPreference("workspace")).toEqual(ref);
});

it("does not narrow the CSS weight range when loading an authored variable font", () => {
  const theme = structuredClone(defaultTheme);
  theme.assets["body-font"] = { path: "assets/body.woff2", mediaType: "font/woff2" };
  theme.typography.body = "asset:body-font";
  const Font = vi.fn(function(this: { load: () => Promise<never> }) { this.load = () => new Promise(() => {}); });
  vi.stubGlobal("FontFace", Font);
  const originalFonts = Object.getOwnPropertyDescriptor(document, "fonts");
  Object.defineProperty(document, "fonts", { configurable: true, value: { add: vi.fn(), delete: vi.fn() } });
  try {
    const dispose = applyPresentation({ theme, assetURLs: { "body-font": "blob:validated-font" }, activePackage: null }, document.createElement("div"));
    expect(Font).toHaveBeenCalledWith(expect.any(String), 'url("blob:validated-font")', { display: "swap", weight: "1 1000" });
    dispose();
  } finally {
    vi.unstubAllGlobals();
    if (originalFonts) Object.defineProperty(document, "fonts", originalFonts);
    else Reflect.deleteProperty(document, "fonts");
  }
});
