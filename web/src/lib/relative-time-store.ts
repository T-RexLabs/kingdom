import { useSyncExternalStore } from "react";
import { modalIsOpen } from "./modal-state";

const listeners = new Set<() => void>();
let intervalID: number | null = null;
let snapshot = currentSecond();

function currentSecond(): number {
  return Math.floor(Date.now() / 1000);
}

function syncSnapshot() {
  const next = currentSecond();
  if (next !== snapshot) {
    snapshot = next;
  }
}

function emitChange() {
	if (modalIsOpen()) {
		return;
	}
  const previous = snapshot;
  syncSnapshot();
  if (snapshot === previous) {
    return;
  }
  for (const listener of listeners) {
    listener();
  }
}

function startClock() {
  if (intervalID !== null || typeof window === "undefined") {
    return;
  }
  intervalID = window.setInterval(emitChange, 1_000);
}

function stopClock() {
  if (intervalID === null || listeners.size > 0 || typeof window === "undefined") {
    return;
  }
  window.clearInterval(intervalID);
  intervalID = null;
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  startClock();
  return () => {
    listeners.delete(listener);
    stopClock();
  };
}

function getSnapshot() {
  syncSnapshot();
  return snapshot;
}

export function useRelativeTimeNow(): number {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot) * 1_000;
}
