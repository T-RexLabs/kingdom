import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { defaultTheme, themePackage, validReference, type ResolvedThemeManifest, type ThemeCatalog, type ThemePackage, type ThemeReference } from "./contract";
import { previewAssets, revisionAssets, type ThemeAssetURLs } from "./assets";
import { readPreference, writePreference } from "./preferences";

export type ThemeClient = {
  catalog: () => Promise<ThemeCatalog>;
  revision: (reference: ThemeReference) => Promise<components["schemas"]["ThemePackage"]>;
};
const client: ThemeClient = {
  catalog: () => apiFetch<ThemeCatalog>("/themes"),
  revision: (ref) => apiFetch(`/themes/${encodeURIComponent(ref.id)}/revisions/${encodeURIComponent(ref.resolvedDigest)}`),
};
export type ThemeState = {
  catalog: ThemeCatalog | null;
  selected: ThemeReference | null;
  activePackage: ThemePackage | null;
  theme: ResolvedThemeManifest;
  assetURLs: ThemeAssetURLs;
  isPreviewing: boolean;
  loading: boolean;
  error: string | null;
};
export class ThemeController {
  private state: ThemeState = { catalog: null, selected: null, activePackage: null, theme: defaultTheme, assetURLs: {}, isPreviewing: false, loading: true, error: null };
  private listeners = new Set<() => void>();
  private selectedPackage: ThemePackage | null = null;
  private selectionRequest = 0;
  private catalogRequest = 0;
  private releasePreview: (() => void) | undefined;
  private disposed = false;
  private readonly client: ThemeClient;
  constructor(themeClient: ThemeClient = client) { this.client = themeClient; }
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
  private update(patch: Partial<ThemeState>) {
    if (this.disposed) return;
    this.state = { ...this.state, ...patch };
    this.listeners.forEach((listener) => listener());
  }
  initialize = async () => {
    this.disposed = false;
    this.update({ loading: true });
    await this.refreshCatalog();
  };
  dispose = () => {
    this.disposed = true;
    this.selectionRequest++;
    this.catalogRequest++;
    this.releasePreview?.();
    this.releasePreview = undefined;
  };
  private activate(pkg: ThemePackage | null) {
    this.releasePreview?.();
    this.releasePreview = undefined;
    this.selectedPackage = pkg;
    this.update({ activePackage: pkg, selected: pkg ? { id: pkg.manifest.id, resolvedDigest: pkg.resolvedDigest } : null, theme: pkg?.manifest ?? defaultTheme, assetURLs: pkg ? revisionAssets(pkg) : {}, isPreviewing: false, loading: false });
  }
  refreshCatalog = async () => {
    const request = ++this.catalogRequest;
    let selection: number | undefined;
    try {
      const catalog = await this.client.catalog();
      if (request !== this.catalogRequest || this.disposed) return;
      if (!catalog.workspaceID || !Array.isArray(catalog.items)) throw new Error("Theme catalog is unavailable.");
      const changedWorkspace = catalog.workspaceID !== this.state.catalog?.workspaceID;
      this.update({ catalog, error: null, ...(changedWorkspace ? { loading: true } : {}) });
      if (!changedWorkspace && (this.state.selected || this.state.isPreviewing)) {
        if (this.state.selected && !catalog.items.some((item) => item.id === this.state.selected?.id && item.resolvedDigest === this.state.selected?.resolvedDigest)) {
          this.selectionRequest++;
          this.activate(null);
          this.update({ error: "Your selected theme is unavailable. Obsidian Court is active; your saved preference is preserved." });
        }
        this.update({ loading: false });
        return;
      }
      selection = ++this.selectionRequest;
      const saved = readPreference(catalog.workspaceID);
      const fallback = catalog.items.find((item) => item.id === defaultTheme.id && item.builtin);
      const ref = saved ?? fallback;
      if (!ref) throw new Error("The default theme revision is unavailable.");
      if (saved && !catalog.items.some((item) => item.id === saved.id && item.resolvedDigest === saved.resolvedDigest)) {
        this.activate(null);
        this.update({ error: "Your selected theme is unavailable. Obsidian Court is active; your saved preference is preserved." });
        return;
      }
      const pkg = themePackage(await this.client.revision(ref), ref);
      if (selection !== this.selectionRequest || request !== this.catalogRequest || this.disposed) return;
      this.activate(pkg);
    } catch {
      if (request !== this.catalogRequest || this.disposed || (selection !== undefined && selection !== this.selectionRequest)) return;
      if (this.state.loading) this.activate(null);
      this.update({ error: "Appearance could not be loaded. The current presentation remains usable; refresh to try again.", loading: false });
    }
  };
  selectTheme = async (reference: ThemeReference): Promise<boolean> => {
    if (!validReference(reference)) throw new Error("Choose a valid theme revision.");
    const workspaceID = this.state.catalog?.workspaceID;
    if (!workspaceID) throw new Error("Load the workspace theme catalog before choosing a theme.");
    const request = ++this.selectionRequest;
    try {
      const pkg = themePackage(await this.client.revision(reference), reference);
      if (request !== this.selectionRequest || this.disposed) return false;
      this.activate(pkg);
      const saved = writePreference(workspaceID, reference);
      this.update({ error: saved ? null : "This theme is active for this visit. Your browser could not save the preference." });
      return true;
    } catch (error) {
      if (request !== this.selectionRequest || this.disposed) return false;
      this.update({ error: "The selected theme could not be loaded. Your previous presentation is still active." });
      throw error;
    }
  };
  previewTheme = (value: components["schemas"]["ThemePackage"], assets: Readonly<Record<string, string>> = {}) => {
    const pkg = themePackage(value);
    const preview = previewAssets(pkg, assets);
    this.selectionRequest++;
    this.releasePreview?.();
    this.releasePreview = preview.dispose;
    this.update({ activePackage: pkg, theme: pkg.manifest, assetURLs: preview.urls, isPreviewing: true, error: null });
  };
  cancelPreview = () => {
    this.selectionRequest++;
    this.activate(this.selectedPackage);
  };
}
