import { builtinEnvironments, type Environment } from "vitest/environments";

/**
 * jsdom, with `localStorage` and `sessionStorage` intact.
 *
 * Node 26 defines both on `globalThis` as accessors that return `undefined`
 * unless `--localstorage-file` is passed. Vitest only copies a window key onto
 * the global when the key is absent there or appears in its own allow-list
 * (`getWindowKeys`), and neither storage is on that list — so on Node 26 the
 * placeholders win and jsdom's real Storage never lands.
 *
 * Deleting the placeholders first restores the genuine jsdom objects, which
 * matters beyond `localStorage` merely being defined: tests assert Kingdom
 * writes nothing to browser storage, and one does it by spying on
 * `Storage.prototype.setItem`. A hand-rolled stand-in would shadow that
 * prototype and quietly make the assertion unfalsifiable.
 */
const jsdomWithStorage = {
  name: "jsdom-storage",
  transformMode: "web",
  async setup(global, options) {
    delete (global as Record<string, unknown>).localStorage;
    delete (global as Record<string, unknown>).sessionStorage;
    return builtinEnvironments.jsdom.setup(global, options);
  },
} satisfies Environment;

export default jsdomWithStorage;
