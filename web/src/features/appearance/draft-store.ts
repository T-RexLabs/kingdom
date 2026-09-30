import { useSyncExternalStore } from "react";
import { builtInThemes, defaultTheme, type ThemeManifest } from "@/theme";
import type { ThemeDraft, ThemeSource } from "./theme-api";

export type DraftState = ThemeDraft & {
  origin: string;
  sourceID: string | null;
  expectedDigest: string;
  generation: number;
  dirty: boolean;
  storageWarning: boolean;
};
function key(workspace: string) { return `kingdom:theme-draft:v1:${workspace}`; }
export function parseDraft(source: string): ThemeManifest | null {
  try { const value: unknown = JSON.parse(source); return value && typeof value === "object" && !Array.isArray(value) ? value as ThemeManifest : null; } catch { return null; }
}
export function newDraft(base = defaultTheme.id, id = "my-theme"): ThemeDraft {
  const theme = builtInThemes.find((entry) => entry.id === base) ?? defaultTheme;
  return { source: JSON.stringify({ schemaVersion: 1, resolverVersion: 1, id, version: "1.0.0", name: "My Theme", base: { id: theme.id, version: theme.version } }, null, 2), assets: {} };
}
export class DraftStore {
  private state: DraftState;
  private listeners = new Set<() => void>();
  readonly workspace: string;
  constructor(workspace: string) {
    this.workspace = workspace;
    this.state = { ...newDraft(), origin: "new", sourceID: null, expectedDigest: "", generation: 1, dirty: false, storageWarning: false };
    try {
      const raw = sessionStorage.getItem(key(workspace));
      const saved = raw ? JSON.parse(raw) as Partial<DraftState> : null;
      if (saved && typeof saved.source === "string" && saved.source.length <= 256 << 10 && typeof saved.origin === "string" && typeof saved.expectedDigest === "string" && saved.assets && typeof saved.assets === "object" && !Array.isArray(saved.assets) && Object.values(saved.assets).every((value) => typeof value === "string")) {
        this.state = { source: saved.source, assets: saved.assets, origin: saved.origin, expectedDigest: saved.expectedDigest, sourceID: typeof saved.sourceID === "string" ? saved.sourceID : null, generation: Number.isSafeInteger(saved.generation) && saved.generation! > 0 ? saved.generation! : 1, dirty: saved.dirty === true, storageWarning: false };
      }
    } catch { this.state.storageWarning = true; }
  }
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; };
  private update(next: DraftState) {
    try { sessionStorage.setItem(key(this.workspace), JSON.stringify(next)); next.storageWarning = false; } catch { next.storageWarning = true; }
    this.state = next;
    this.listeners.forEach((listener) => listener());
  }
  edit = (draft: ThemeDraft) => this.update({ ...this.state, ...draft, generation: this.state.generation + 1, dirty: true });
  replace = (draft: ThemeDraft, origin: string, sourceID: string | null = null, expectedDigest = "", dirty = false) => this.update({ ...draft, origin, sourceID, expectedDigest, generation: this.state.generation + 1, dirty, storageWarning: false });
  saved = (source: ThemeSource, generation: number) => {
    if (this.state.generation !== generation) return false;
    const id = parseDraft(source.source)?.id ?? this.state.sourceID;
    this.update({ ...this.state, source: source.source, assets: source.assets, sourceID: id, expectedDigest: source.sourceDigest, generation: generation + 1, dirty: false });
    return true;
  };
}
const stores = new Map<string, DraftStore>();
export function getDraftStore(workspace: string): DraftStore {
  let store = stores.get(workspace);
  if (!store) { store = new DraftStore(workspace); stores.set(workspace, store); }
  return store;
}
export function useThemeDraft(workspace: string) {
  const store = getDraftStore(workspace);
  return [useSyncExternalStore(store.subscribe, store.getSnapshot), store] as const;
}
