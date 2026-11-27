import { afterEach, describe, expect, it, vi } from "vitest";
import { playSound, setSoundEnabled, soundEnabled } from "./rexie-sound";

describe("Rexie sounds", () => {
  afterEach(() => {
    setSoundEnabled(false);
    vi.unstubAllGlobals();
  });

  it("stay off until turned on, and stay quiet while off", () => {
    const play = vi.fn(() => Promise.resolve());
    vi.stubGlobal("Audio", vi.fn(() => ({ play, volume: 1 })));
    expect(soundEnabled()).toBe(false);
    playSound("bell");
    expect(play).not.toHaveBeenCalled();

    setSoundEnabled(true);
    expect(soundEnabled()).toBe(true);
    playSound("bell");
    expect(Audio).toHaveBeenCalledWith("/sounds/bell.m4a");
    expect(play).toHaveBeenCalledTimes(1);
  });

  it("swallows a blocked clip and cuts the long foghorn short", async () => {
    vi.useFakeTimers();
    const pause = vi.fn();
    vi.stubGlobal("Audio", vi.fn(() => ({ play: () => Promise.reject(new Error("blocked")), pause, volume: 1 })));
    setSoundEnabled(true);
    expect(() => playSound("foghorn")).not.toThrow();
    await Promise.resolve();
    vi.advanceTimersByTime(3000);
    expect(pause).toHaveBeenCalled();
    vi.useRealTimers();
  });
});
