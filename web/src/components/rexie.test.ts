import { describe, expect, it } from "vitest";
import { KONAMI_CODE, REXIE_QUIPS, ROAR_CLICKS, ROAR_WINDOW_MS, isTypingTarget, nextQuip, registerKey, registerPoke, skyAt } from "./rexie";

describe("Rexie", () => {
  it("follows the local hour from night to dusk", () => {
    expect([0, 4, 5, 7, 8, 17, 18, 20, 21, 23].map(skyAt)).toEqual(["night", "night", "dawn", "dawn", "day", "day", "dusk", "dusk", "night", "night"]);
  });

  it("never repeats a quip on consecutive pokes", () => {
    let index = -1;
    const seen = Array.from({ length: REXIE_QUIPS.length + 1 }, () => (index = nextQuip(index)));
    expect(seen.slice(0, -1)).toEqual(REXIE_QUIPS.map((_, i) => i));
    expect(seen.at(-1)).toBe(0);
  });

  it("roars only at enough quick pokes", () => {
    let clicks: number[] = [];
    for (let i = 0; i < ROAR_CLICKS - 1; i++) {
      const result = registerPoke(clicks, 1000 + i * 100);
      expect(result.roar).toBe(false);
      clicks = result.clicks;
    }
    expect(registerPoke(clicks, 1000 + ROAR_CLICKS * 100)).toEqual({ clicks: [], roar: true });
    expect(registerPoke(clicks, 1000 + ROAR_WINDOW_MS * 10).roar).toBe(false);
  });

  it("recognises the Konami code and a typed rawr", () => {
    let trail: string[] = [];
    let egg = null;
    for (const key of ["x", ...KONAMI_CODE.map((k) => (k.length === 1 ? k.toUpperCase() : k.replace("arrow", "Arrow")))]) {
      ({ trail, egg } = registerKey(trail, key));
    }
    expect(egg).toBe("parade");
    for (const key of "rawr") ({ trail, egg } = registerKey(trail, key));
    expect(egg).toBe("roar");
    expect(registerKey([], "r").egg).toBeNull();
  });

  it("leaves keys typed into fields alone", () => {
    expect(isTypingTarget(document.createElement("textarea"))).toBe(true);
    expect(isTypingTarget(document.createElement("input"))).toBe(true);
    expect(isTypingTarget(document.createElement("div"))).toBe(false);
    expect(isTypingTarget(null)).toBe(false);
  });
});
