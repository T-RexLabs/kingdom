import { useSyncExternalStore } from "react";

/** Opt-in ship sounds for Rexie's easter eggs. Off until the person turns
 * them on, remembered per browser, and only ever played from a gesture. */

export type RexieSound = "creak-short" | "foghorn" | "bell" | "gulls-flyby";

const STORAGE_KEY = "kingdom.rexie.sound";
const CHANGE_EVENT = "kingdom:rexie-sound";
const VOLUME = 0.5;
/** Long clips are cut to match the animation they accompany. */
const MAX_MS: Partial<Record<RexieSound, number>> = { foghorn: 2600 };

export function soundEnabled(): boolean {
  try {
    return window.localStorage.getItem(STORAGE_KEY) === "on";
  } catch {
    return false;
  }
}

export function setSoundEnabled(enabled: boolean) {
  try {
    if (enabled) window.localStorage.setItem(STORAGE_KEY, "on");
    else window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Storage can be unavailable; the sounds simply stay off.
  }
  window.dispatchEvent(new Event(CHANGE_EVENT));
}

function subscribe(onChange: () => void) {
  window.addEventListener(CHANGE_EVENT, onChange);
  window.addEventListener("storage", onChange);
  return () => {
    window.removeEventListener(CHANGE_EVENT, onChange);
    window.removeEventListener("storage", onChange);
  };
}

export function useSoundEnabled(): boolean {
  return useSyncExternalStore(subscribe, soundEnabled, () => false);
}

export function playSound(sound: RexieSound) {
  if (!soundEnabled()) return;
  const audio = new Audio(`/sounds/${sound}.m4a`);
  audio.volume = VOLUME;
  // A blocked or missing clip is never worth an error.
  void audio.play().catch(() => undefined);
  const limit = MAX_MS[sound];
  if (limit) window.setTimeout(() => audio.pause(), limit);
}
