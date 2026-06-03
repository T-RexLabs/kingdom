import { useEffect, useRef, useState } from "react";

export const BASE_SPEED = 3;
export const CATCHUP_THRESHOLD = 100;
export const CATCHUP_DIVISOR = 15;
export const DRAIN_DIVISOR = 5;
export const DRAIN_MIN_SPEED = 10;
export const THROTTLE_MS = 30;

/**
 * Progressively reveals streamed text at a natural typing pace.
 *
 * On mount (or when `isStreaming` becomes true), whatever text already exists
 * is treated as the baseline and shown immediately — only text arriving after
 * that point gets animated.  This prevents replayed / reconnected backlog from
 * being "re-typed" on page refresh.
 *
 * When `isStreaming` turns false the hook does NOT snap.  Instead it enters a
 * fast "drain" phase that finishes revealing any remaining text, then reports
 * `isStreamActive = false` so the caller can switch to markdown rendering.
 *
 * When `isStreaming` is false on mount the hook is a no-op.
 *
 * **Target shrink safety**: `targetText` can shrink mid-stream (e.g. when
 * `parseStreamingQuestion` strips a partial structured_question block from
 * visible content).  The reveal cursor is clamped to the current target length
 * so the hook never gets stuck.
 *
 * **Rendering tradeoff**: Callers render plain text during reveal to avoid
 * ReactMarkdown re-parsing at ~30 fps, then switch to full MarkdownBlock once
 * `isStreamActive` becomes false.
 */
export function useTextReveal(targetText: string, isStreaming: boolean) {
  const targetRef = useRef(targetText);
  const revealedRef = useRef(targetText.length);
  const rafRef = useRef(0);

  const [displayText, setDisplayText] = useState(targetText);
  const [isStreamActive, setIsRevealing] = useState(false);

  // Keep targetRef in sync — the rAF callback reads from it.
  useEffect(() => {
    targetRef.current = targetText;
  });

  useEffect(() => {
    if (!isStreaming) {
      // Streaming ended (or was never active).
      const target = targetRef.current;

      if (revealedRef.current >= target.length) {
        // Already caught up — snap.
        revealedRef.current = target.length;
        setDisplayText(target);
        setIsRevealing(false);
        return;
      }

      // Still unrevealed text — drain at an accelerated pace.
      let lastRender = 0;

      const drain = (now: number) => {
        const t = targetRef.current;
        const current = revealedRef.current;

        // Target shrank — clamp the cursor so we don't get stuck.
        if (current > t.length) {
          revealedRef.current = t.length;
          setDisplayText(t);
          setIsRevealing(false);
          return;
        }

        if (current >= t.length) {
          setDisplayText(t);
          setIsRevealing(false);
          return;
        }

        if (now - lastRender < THROTTLE_MS) {
          rafRef.current = requestAnimationFrame(drain);
          return;
        }
        lastRender = now;

        const behind = t.length - current;
        const speed = Math.max(DRAIN_MIN_SPEED, Math.ceil(behind / DRAIN_DIVISOR));
        revealedRef.current = Math.min(current + speed, t.length);
        setDisplayText(t.slice(0, revealedRef.current));

        rafRef.current = requestAnimationFrame(drain);
      };

      rafRef.current = requestAnimationFrame(drain);
      return () => cancelAnimationFrame(rafRef.current);
    }

    // Streaming started — capture baseline (show buffered text instantly).
    const target = targetRef.current;
    revealedRef.current = target.length;
    setDisplayText(target);
    setIsRevealing(true);

    let lastRender = 0;

    const tick = (now: number) => {
      const t = targetRef.current;
      let current = revealedRef.current;

      // Target shrank (e.g. structured_question block stripped mid-stream).
      // Clamp the cursor so we don't spin waiting for text that was removed.
      if (current > t.length) {
        revealedRef.current = t.length;
        current = t.length;
        setDisplayText(t);
      }

      if (current >= t.length) {
        // Caught up — wait for more text.
        rafRef.current = requestAnimationFrame(tick);
        return;
      }

      if (now - lastRender < THROTTLE_MS) {
        rafRef.current = requestAnimationFrame(tick);
        return;
      }
      lastRender = now;

      const behind = t.length - current;
      const speed =
        behind > CATCHUP_THRESHOLD
          ? Math.max(BASE_SPEED, Math.ceil(behind / CATCHUP_DIVISOR))
          : BASE_SPEED;

      revealedRef.current = Math.min(current + speed, t.length);
      setDisplayText(t.slice(0, revealedRef.current));

      rafRef.current = requestAnimationFrame(tick);
    };

    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [isStreaming]);

  return {
    displayText: isStreamActive ? displayText : targetText,
    isStreamActive,
    hasCaughtUpToTarget: !isStreamActive || displayText === targetText,
  };
}
