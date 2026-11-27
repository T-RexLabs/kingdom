/** Rexie is the crowned T-Rex who lives in the ludicrous ornament band.
 * Everything here is decorative: it never changes a control, a status, or
 * what the crew is doing. */

export const REXIE_ROAR_EVENT = "kingdom:rexie-roar";

export const REXIE_QUIPS = [
  "Arms too short to scroll. Read the plan for me?",
  "I'm not small. The ship is big.",
  "The Quartermaster sent it back again. Rude. Correct, but rude.",
  "Every attempt is kept. Even the one with the meteor.",
  "Sixty-six million years and I still check the tests.",
  "Rawr means \"looks good to me\" in dinosaur.",
  "Steady course. Capable crew. Snacks below deck.",
  "I approved the plan. I think. My arms were involved.",
  "The Lookout says there's a kraken. The Lookout always says that.",
  "Psst. Type rawr. Go on.",
  "Up, up, down, down... I forget the rest. Arms.",
] as const;

export const ROAR_TEXT = "RAAAWR!";

/** Clicks inside this window count toward a roar. */
export const ROAR_WINDOW_MS = 1500;
export const ROAR_CLICKS = 5;

export const KONAMI_CODE = ["arrowup", "arrowup", "arrowdown", "arrowdown", "arrowleft", "arrowright", "arrowleft", "arrowright", "b", "a"] as const;
export const ROAR_WORD = "rawr";

export type Sky = "dawn" | "day" | "dusk" | "night";

export function skyAt(hour: number): Sky {
  if (hour >= 21 || hour < 5) return "night";
  if (hour < 8) return "dawn";
  if (hour < 18) return "day";
  return "dusk";
}

/** The next quip in a fixed rotation, so repeat pokes never show the same line twice in a row. */
export function nextQuip(previous: number): number {
  return (previous + 1) % REXIE_QUIPS.length;
}

/** Keeps only the recent clicks and reports whether they add up to a roar. */
export function registerPoke(clicks: readonly number[], now: number): { clicks: number[]; roar: boolean } {
  const recent = [...clicks.filter((time) => now - time < ROAR_WINDOW_MS), now];
  return recent.length >= ROAR_CLICKS ? { clicks: [], roar: true } : { clicks: recent, roar: false };
}

export type KeyTrail = readonly string[];

/** Feeds one key into the trail and reports which easter egg it completes, if any. */
export function registerKey(trail: KeyTrail, key: string): { trail: string[]; egg: "parade" | "roar" | null } {
  const next = [...trail, key.toLowerCase()].slice(-KONAMI_CODE.length);
  if (KONAMI_CODE.every((expected, index) => next[index] === expected)) return { trail: [], egg: "parade" };
  if (next.slice(-ROAR_WORD.length).join("") === ROAR_WORD) return { trail: [], egg: "roar" };
  return { trail: next, egg: null };
}

export function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName);
}
