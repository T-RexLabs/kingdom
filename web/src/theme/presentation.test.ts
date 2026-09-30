import { describe, expect, it, vi } from "vitest";
import { builtInThemes, canonicalRoleName, defaultTheme, resolvedManifest, roleIdentity, themePackage } from "./contract";
import { applyPresentation, tokenVariable } from "./presentation";
import { imageAssetURL, previewAssets, revisionAssetURL, themeFonts } from "./assets";
import iconCatalog from "../../../contracts/theme/lucide-icons.json?raw";
import { iconNames } from "lucide-react/dynamic";

describe("canonical presentation contract", () => {
  it("ships all four complete themes from the shared contract", () => {
    expect(builtInThemes.map((theme) => theme.id)).toEqual(["obsidian-court", "nautilus", "astral-council", "software-lab"]);
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
