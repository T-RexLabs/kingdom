import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { builtInThemes, defaultTheme, themePackage, type ThemeCatalog, type ThemePackage, type ThemeReference } from "./contract";
import { ThemeController, type ThemeClient } from "./controller";
import { preferenceKey } from "./preferences";

function pkg(index: number): ThemePackage {
  return themePackage({ manifest: structuredClone(builtInThemes[index]), sourceDigest: "a".repeat(64), resolvedDigest: String(index + 1).repeat(64) });
}
const packages = [0, 1, 2, 3].map(pkg);
function ref(index: number): ThemeReference { return { id: packages[index].manifest.id, resolvedDigest: packages[index].resolvedDigest }; }
function catalog(workspaceID = "workspace-one"): ThemeCatalog {
  return { workspaceID, sources: [], items: packages.map((value) => ({ id: value.manifest.id, version: value.manifest.version, name: value.manifest.name, builtin: true, resolvedDigest: value.resolvedDigest })) };
}
function client(): ThemeClient {
  return { catalog: vi.fn(async () => catalog()), revision: vi.fn(async (reference) => packages.find((value) => value.manifest.id === reference.id)!) };
}
function pending<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}
beforeEach(() => localStorage.clear());
afterEach(() => vi.restoreAllMocks());

describe("personal theme selection", () => {
  it("loads a workspace's pinned revision before releasing its boundary", async () => {
    localStorage.setItem(preferenceKey("workspace-one"), JSON.stringify(ref(1)));
    const api = client();
    const response = pending<ThemePackage>();
    api.revision = vi.fn(() => response.promise);
    const controller = new ThemeController(api);
    const loading = controller.initialize();
    await Promise.resolve();
    expect(controller.getSnapshot().loading).toBe(true);
    expect(api.revision).toHaveBeenCalledWith(ref(1));
    response.resolve(packages[1]);
    await loading;
    expect(controller.getSnapshot().loading).toBe(false);
    expect(controller.getSnapshot().theme.id).toBe("nautilus");
  });
  it("isolates preferences by workspace and stores only a revision reference", async () => {
    localStorage.setItem(preferenceKey("another-workspace"), JSON.stringify(ref(3)));
    const controller = new ThemeController(client());
    await controller.initialize();
    expect(controller.getSnapshot().theme.id).toBe(defaultTheme.id);
    await controller.selectTheme(ref(2));
    expect(JSON.parse(localStorage.getItem(preferenceKey("workspace-one"))!)).toEqual(ref(2));
    expect(JSON.parse(localStorage.getItem(preferenceKey("another-workspace"))!)).toEqual(ref(3));
  });
  it("keeps the saved pin when unavailable and uses a valid bundled fallback", async () => {
    const saved = { id: "missing-package", resolvedDigest: "c".repeat(64) };
    localStorage.setItem(preferenceKey("workspace-one"), JSON.stringify(saved));
    const controller = new ThemeController(client());
    await controller.initialize();
    expect(controller.getSnapshot().theme.id).toBe(defaultTheme.id);
    expect(controller.getSnapshot().error).toContain("unavailable");
    expect(JSON.parse(localStorage.getItem(preferenceKey("workspace-one"))!)).toEqual(saved);
  });
  it("retries the pinned package after an initial revision read failure", async () => {
    const api = client();
    vi.mocked(api.revision).mockRejectedValueOnce(new Error("disconnected"));
    const controller = new ThemeController(api);
    await controller.initialize();
    expect(controller.getSnapshot().error).not.toBeNull();
    await controller.refreshCatalog();
    expect(controller.getSnapshot().selected).toEqual(ref(0));
    expect(controller.getSnapshot().error).toBeNull();
  });
  it("allows only the latest selection request to apply or persist", async () => {
    const api = client();
    const controller = new ThemeController(api);
    await controller.initialize();
    const first = pending<ThemePackage>();
    const second = pending<ThemePackage>();
    vi.mocked(api.revision).mockImplementationOnce(() => first.promise).mockImplementationOnce(() => second.promise);
    const older = controller.selectTheme(ref(1));
    const newer = controller.selectTheme(ref(3));
    second.resolve(packages[3]);
    expect(await newer).toBe(true);
    first.resolve(packages[1]);
    expect(await older).toBe(false);
    expect(controller.getSnapshot().theme.id).toBe("software-lab");
    expect(JSON.parse(localStorage.getItem(preferenceKey("workspace-one"))!)).toEqual(ref(3));
  });
  it("rejects a mismatched response without losing the current selection", async () => {
    const api = client();
    const controller = new ThemeController(api);
    await controller.initialize();
    vi.mocked(api.revision).mockResolvedValueOnce(packages[3]);
    await expect(controller.selectTheme(ref(1))).rejects.toThrow("does not match");
    expect(controller.getSnapshot().selected).toEqual(ref(0));
    expect(localStorage.getItem(preferenceKey("workspace-one"))).toBeNull();
  });
});

describe("private preview", () => {
  it("restores the prior theme and never changes the personal preference", async () => {
    const controller = new ThemeController(client());
    await controller.initialize();
    await controller.selectTheme(ref(1));
    controller.previewTheme(packages[3]);
    expect(controller.getSnapshot().theme.colorScheme).toBe("light");
    expect(controller.getSnapshot().isPreviewing).toBe(true);
    expect(controller.getSnapshot().selected).toEqual(ref(1));
    expect(JSON.parse(localStorage.getItem(preferenceKey("workspace-one"))!)).toEqual(ref(1));
    controller.cancelPreview();
    expect(controller.getSnapshot().theme.id).toBe("nautilus");
    expect(controller.getSnapshot().isPreviewing).toBe(false);
  });
  it("invalidates an earlier asynchronous selection when preview starts", async () => {
    const api = client();
    const controller = new ThemeController(api);
    await controller.initialize();
    const response = pending<ThemePackage>();
    vi.mocked(api.revision).mockImplementationOnce(() => response.promise);
    const selection = controller.selectTheme(ref(1));
    controller.previewTheme(packages[3]);
    response.resolve(packages[1]);
    expect(await selection).toBe(false);
    expect(controller.getSnapshot().theme.id).toBe("software-lab");
    controller.cancelPreview();
    expect(controller.getSnapshot().selected).toEqual(ref(0));
  });
  it("does not retain a caller's mutable manifest", async () => {
    const controller = new ThemeController(client());
    await controller.initialize();
    const editable = structuredClone(packages[3]);
    controller.previewTheme(editable);
    editable.manifest.tokens.canvas = "url(https://bad.invalid)";
    expect(controller.getSnapshot().theme.tokens.canvas).toMatch(/^#[0-9a-f]{6}$/);
  });
  it("does not install an invalid response as CSS", async () => {
    const controller = new ThemeController(client());
    await controller.initialize();
    const invalid = structuredClone(packages[3]);
    invalid.manifest.tokens.canvas = "red; background-image:url(https://bad.invalid)";
    expect(() => controller.previewTheme(invalid)).toThrow("not a complete");
    expect(controller.getSnapshot().theme.id).toBe(defaultTheme.id);
  });
  it("survives a Strict Mode initialize/dispose/initialize sequence", async () => {
    const controller = new ThemeController(client());
    await controller.initialize();
    controller.dispose();
    await controller.initialize();
    expect(controller.getSnapshot().loading).toBe(false);
    expect(controller.getSnapshot().selected).toEqual(ref(0));
  });
});
