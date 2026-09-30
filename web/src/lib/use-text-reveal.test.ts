import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useTextReveal, BASE_SPEED, THROTTLE_MS } from "./use-text-reveal";

/*
 * We mock requestAnimationFrame so tests run synchronously.
 * Each call to vi.advanceTimersByTime triggers queued rAF callbacks.
 */

let rafCallbacks: Array<(now: number) => void> = [];
let currentTime = 0;

function flushRAF() {
  const cbs = [...rafCallbacks];
  rafCallbacks = [];
  for (const cb of cbs) {
    cb(currentTime);
  }
}

function advanceAndFlush(ms: number, flushCount = 1) {
  currentTime += ms;
  for (let i = 0; i < flushCount; i++) {
    flushRAF();
  }
}

beforeEach(() => {
  rafCallbacks = [];
  currentTime = 0;

  vi.stubGlobal("requestAnimationFrame", (cb: (now: number) => void) => {
    rafCallbacks.push(cb);
    return rafCallbacks.length;
  });

  vi.stubGlobal("cancelAnimationFrame", (id: number) => {
    // Simple: just clear all (tests use a single hook at a time)
    rafCallbacks = rafCallbacks.filter((_, i) => i + 1 !== id);
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("useTextReveal", () => {
  it("shows every new text revision immediately under reduced motion without scheduling frames", () => {
    vi.stubGlobal("matchMedia", vi.fn(() => ({
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })));
    const { result, rerender } = renderHook(
      ({ text }) => useTextReveal(text, true, "zero"),
      { initialProps: { text: "" } },
    );
    rerender({ text: "Newly observed reasoning" });
    expect(result.current.displayText).toBe("Newly observed reasoning");
    expect(result.current.isStreamActive).toBe(false);
    expect(result.current.hasCaughtUpToTarget).toBe(true);
    expect(rafCallbacks).toHaveLength(0);
  });

  it("responds to a reduced-motion change during reveal and resumes only for later text", () => {
    let reduced = false;
    let notify: (() => void) | undefined;
    const remove = vi.fn();
    vi.stubGlobal("matchMedia", vi.fn(() => ({
      get matches() { return reduced; },
      addEventListener: (_event: string, listener: () => void) => { notify = listener; },
      removeEventListener: remove,
    })));
    const { result, rerender, unmount } = renderHook(
      ({ text }) => useTextReveal(text, true),
      { initialProps: { text: "" } },
    );
    rerender({ text: "Partially revealed reasoning" });
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText).not.toBe("Partially revealed reasoning");
    act(() => { reduced = true; notify?.(); });
    expect(result.current.displayText).toBe("Partially revealed reasoning");
    expect(result.current.isStreamActive).toBe(false);
    act(() => { reduced = false; notify?.(); });
    expect(result.current.displayText).toBe("Partially revealed reasoning");
    rerender({ text: "Partially revealed reasoning with more" });
    expect(result.current.displayText).toBe("Partially revealed reasoning");
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText.length).toBeGreaterThan("Partially revealed reasoning".length);
    unmount();
    expect(remove).toHaveBeenCalledWith("change", expect.any(Function));
  });

  it("returns targetText directly when not streaming", () => {
    const { result } = renderHook(() => useTextReveal("Hello world", false));
    expect(result.current.displayText).toBe("Hello world");
    expect(result.current.isStreamActive).toBe(false);
    expect(result.current.hasCaughtUpToTarget).toBe(true);
  });

  it("hydrates existing buffered text instantly on streaming start", () => {
    // Simulate page refresh mid-stream: text already exists when isStreaming becomes true
    const { result } = renderHook(() => useTextReveal("Already buffered text here", true));

    // The baseline text should be shown immediately — no animation
    expect(result.current.displayText).toBe("Already buffered text here");
    expect(result.current.isStreamActive).toBe(true);
    // Caught up because baseline equals target — no new text yet
    expect(result.current.hasCaughtUpToTarget).toBe(true);
  });

  it("reveals only newly arriving text after baseline", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    // Baseline is empty
    expect(result.current.displayText).toBe("");

    // New text arrives in a batch
    rerender({ text: "Hello, this is a streaming response.", streaming: true });

    // Before any rAF tick, displayText should still be at baseline
    expect(result.current.displayText).toBe("");

    // Advance time past throttle and flush rAF — should reveal BASE_SPEED chars
    act(() => advanceAndFlush(THROTTLE_MS + 1));

    expect(result.current.displayText.length).toBe(BASE_SPEED);
    expect(result.current.displayText).toBe("Hel");
    expect(result.current.isStreamActive).toBe(true);
    expect(result.current.hasCaughtUpToTarget).toBe(false);

    // Flush several more frames
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText.length).toBe(BASE_SPEED * 2);

    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText.length).toBe(BASE_SPEED * 3);
  });

  it("drains remaining text after streaming ends", () => {
    const fullText = "Short response that ends quickly.";

    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    // Text arrives
    rerender({ text: fullText, streaming: true });

    // Reveal a bit
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    const partialLength = result.current.displayText.length;
    expect(partialLength).toBeGreaterThan(0);
    expect(partialLength).toBeLessThan(fullText.length);

    // Streaming ends — should NOT snap, should start draining
    rerender({ text: fullText, streaming: false });

    // isStreamActive should still be true (draining)
    expect(result.current.isStreamActive).toBe(true);

    // Flush enough frames for the drain to complete
    for (let i = 0; i < 20; i++) {
      act(() => advanceAndFlush(THROTTLE_MS + 1));
      if (!result.current.isStreamActive) break;
    }

    // Should have drained to full text
    expect(result.current.displayText).toBe(fullText);
    expect(result.current.isStreamActive).toBe(false);
  });

  it("handles target text shrinking without getting stuck", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    // Text grows
    rerender({ text: "Some prose text before the question block appears", streaming: true });

    // Reveal most of it
    for (let i = 0; i < 15; i++) {
      act(() => advanceAndFlush(THROTTLE_MS + 1));
    }
    const revealedBefore = result.current.displayText.length;
    expect(revealedBefore).toBeGreaterThan(20);

    // Target SHRINKS — simulates parseStreamingQuestion stripping a partial block
    rerender({ text: "Some prose text", streaming: true });

    // The public return value must clamp immediately so no stale structured
    // block text can flash before the next animation frame.
    expect(result.current.displayText).toBe("Some prose text");

    // Flush — the hook should clamp to the new shorter target, not get stuck
    act(() => advanceAndFlush(THROTTLE_MS + 1));

    expect(result.current.displayText).toBe("Some prose text");
    expect(result.current.isStreamActive).toBe(true); // Still streaming, just caught up

    // Target grows again when the block is finalized
    rerender({ text: "Some prose text with more content now", streaming: true });

    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText.length).toBeGreaterThan("Some prose text".length);
  });

  it("handles target text shrinking during drain without getting stuck", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    // Text arrives and we reveal some
    rerender({ text: "Long text that will be partially revealed before drain", streaming: true });
    for (let i = 0; i < 10; i++) {
      act(() => advanceAndFlush(THROTTLE_MS + 1));
    }

    // End streaming — starts drain
    rerender({ text: "Long text that will be partially revealed before drain", streaming: false });
    expect(result.current.isStreamActive).toBe(true);

    // Target shrinks during drain
    rerender({ text: "Short", streaming: false });

    // Flush — should clamp and finish
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText).toBe("Short");
    expect(result.current.isStreamActive).toBe(false);
  });

  it("keeps draining when the target grows after streaming ends", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    const initialTarget = "A response that still has buffered text";
    rerender({ text: initialTarget, streaming: true });
    act(() => advanceAndFlush(THROTTLE_MS + 1));
    rerender({ text: initialTarget, streaming: false });

    const partialText = result.current.displayText;
    const extendedTarget = `${initialTarget} and a final suffix`;
    rerender({ text: extendedTarget, streaming: false });

    expect(result.current.displayText).toBe(partialText);
    expect(result.current.isStreamActive).toBe(true);

    act(() => advanceAndFlush(THROTTLE_MS + 1));
    expect(result.current.displayText.length).toBeGreaterThan(partialText.length);
    expect(result.current.displayText.length).toBeLessThan(extendedTarget.length);
  });

  it("snaps immediately when streaming ends with no unrevealed text", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "Fully revealed", streaming: true } },
    );

    // Baseline equals full text — nothing to reveal
    expect(result.current.displayText).toBe("Fully revealed");

    // Streaming ends
    rerender({ text: "Fully revealed", streaming: false });

    expect(result.current.displayText).toBe("Fully revealed");
    expect(result.current.isStreamActive).toBe(false);
  });

  it("is a no-op when isStreaming is always false", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "Static text", streaming: false } },
    );

    expect(result.current.displayText).toBe("Static text");
    expect(result.current.isStreamActive).toBe(false);

    // Text changes while not streaming — should reflect immediately
    rerender({ text: "Updated text", streaming: false });
    expect(result.current.displayText).toBe("Updated text");
    expect(result.current.isStreamActive).toBe(false);
  });

  it("reports hasCaughtUpToTarget correctly through the reveal lifecycle", () => {
    const { result, rerender } = renderHook(
      ({ text, streaming }) => useTextReveal(text, streaming),
      { initialProps: { text: "", streaming: true } },
    );

    // Baseline empty, target empty → caught up
    expect(result.current.hasCaughtUpToTarget).toBe(true);

    // Text arrives → behind
    rerender({ text: "Short text.", streaming: true });
    expect(result.current.hasCaughtUpToTarget).toBe(false);

    // Reveal all the way
    for (let i = 0; i < 20; i++) {
      act(() => advanceAndFlush(THROTTLE_MS + 1));
      if (result.current.hasCaughtUpToTarget) break;
    }
    expect(result.current.hasCaughtUpToTarget).toBe(true);
    expect(result.current.isStreamActive).toBe(true); // still streaming, just caught up

    // Streaming ends → still caught up
    rerender({ text: "Short text.", streaming: false });
    expect(result.current.hasCaughtUpToTarget).toBe(true);
    expect(result.current.isStreamActive).toBe(false);
  });
});
