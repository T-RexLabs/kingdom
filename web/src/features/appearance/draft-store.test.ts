import { beforeEach, expect, it, vi } from "vitest";
import { DraftStore, newDraft, parseDraft } from "./draft-store";
import { referencesAsset, setDraftField } from "./editor-values";

beforeEach(() => { sessionStorage.clear(); vi.restoreAllMocks(); });
it("keeps invalid in-progress JSON and assets private, isolated by workspace and recoverable", () => {
  const store = new DraftStore("one");
  store.edit({ source: '{"name":', assets: { "assets/mark.svg": "encoded" } });
  expect(new DraftStore("one").getSnapshot()).toMatchObject({ source: '{"name":', assets: { "assets/mark.svg": "encoded" }, dirty: true });
  expect(new DraftStore("two").getSnapshot().source).not.toBe('{"name":');
  expect(parseDraft(store.getSnapshot().source)).toBeNull();
});
it("does not let an older save response erase newer draft edits", () => {
  const store = new DraftStore("save-order");
  const before = store.getSnapshot();
  store.edit({ ...newDraft(), source: before.source.replace("My Theme", "Newest Edit") });
  expect(store.saved({ source: before.source, sourceDigest: "a".repeat(64), assets: {}, diagnostics: [] }, before.generation)).toBe(false);
  expect(store.getSnapshot().source).toContain("Newest Edit");
  expect(store.getSnapshot().expectedDigest).toBe("");
});
it("marks explicitly imported and copied drafts as unsaved, and degrades safely when storage is full", () => {
  const store = new DraftStore("import");
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("full"); });
  store.replace(newDraft(), "imported", null, "", true);
  expect(store.getSnapshot()).toMatchObject({ dirty: true, storageWarning: true, origin: "imported" });
});
it("does not trust recovered metadata types", () => {
  sessionStorage.setItem("kingdom:theme-draft:v1:hostile", JSON.stringify({ ...newDraft(), origin: "source:custom", expectedDigest: "", generation: "infinite", sourceID: {}, dirty: "false" }));
  expect(new DraftStore("hostile").getSnapshot()).toMatchObject({ sourceID: null, generation: 1, dirty: false });
});
it("keeps empty inputs while editing, deletes only explicit resets, and matches exact asset references", () => {
  const manifest = parseDraft(newDraft().source)!;
  const empty = setDraftField(manifest, ["identities", "builder", "name"], "");
  expect(empty.identities?.builder?.name).toBe("");
  expect(setDraftField(empty, ["identities", "builder", "name"], undefined).identities?.builder?.name).toBeUndefined();
  expect(manifest.identities).toBeUndefined();
  expect(referencesAsset({ icon: "asset:mark-long" }, "mark")).toBe(false);
  expect(referencesAsset({ nested: { icon: "asset:mark" } }, "mark")).toBe(true);
});
