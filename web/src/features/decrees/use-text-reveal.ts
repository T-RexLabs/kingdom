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
  const streamingRef = useRef(isStreaming);

  const [displayText, setDisplayText] = useState(targetText);
  const [isStreamActive, setIsRevealing] = useState(false);

  // Keep targetRef in sync — the rAF callback reads from it.
  useEffect(() => {
    targetRef.current = targetText;
  });

  useEffect(() => {
    const wasStreaming = streamingRef.current;
    streamingRef.current = isStreaming;

    if (isStreaming && !wasStreaming) {
      // Streaming started — capture the already-buffered baseline instantly and
      // animate only text that arrives after this point.
      const target = targetRef.current;
      revealedRef.current = target.length;
      setDisplayText(target);
    }

    if (isStreaming) {
      setIsRevealing(true);
    }

    if (!isStreaming && revealedRef.current >= targetRef.current.length) {
      revealedRef.current = targetRef.current.length;
      setDisplayText(targetRef.current);
      setIsRevealing(false);
    }
  }, [isStreaming]);

  useEffect(() => {
    let lastRender = 0;

    const advance = (now: number) => {
      const target = targetRef.current;
      let current = revealedRef.current;

      if (current > target.length) {
        revealedRef.current = target.length;
        current = target.length;
        setDisplayText(target);
      }

      if (current >= target.length) {
        if (!streamingRef.current) {
          setDisplayText(target);
          setIsRevealing(false);
        }
        rafRef.current = 0;
        return;
      }

      if (now - lastRender < THROTTLE_MS) {
        rafRef.current = requestAnimationFrame(advance);
        return;
      }
      lastRender = now;

      const behind = target.length - current;
      const speed = streamingRef.current
        ? behind > CATCHUP_THRESHOLD
          ? Math.max(BASE_SPEED, Math.ceil(behind / CATCHUP_DIVISOR))
          : BASE_SPEED
        : Math.max(DRAIN_MIN_SPEED, Math.ceil(behind / DRAIN_DIVISOR));

      revealedRef.current = Math.min(current + speed, target.length);
      setDisplayText(target.slice(0, revealedRef.current));
      setIsRevealing(true);
      rafRef.current = requestAnimationFrame(advance);
    };

    const target = targetRef.current;
    if (revealedRef.current > target.length) {
      revealedRef.current = target.length;
      setDisplayText(target);
    }

    if (revealedRef.current >= target.length) {
      if (!isStreaming) {
        setDisplayText(target);
        setIsRevealing(false);
      }
      return;
    }

    if (rafRef.current === 0) {
      rafRef.current = requestAnimationFrame(advance);
    }

    return () => {
      if (rafRef.current !== 0) {
        cancelAnimationFrame(rafRef.current);
        rafRef.current = 0;
      }
    };
  }, [targetText, isStreaming]);

  const safeDisplayText = isStreamActive
    ? (targetText.startsWith(displayText) ? displayText : targetText)
    : targetText;

  return {
    displayText: safeDisplayText,
    isStreamActive,
    hasCaughtUpToTarget: !isStreamActive || safeDisplayText === targetText,
  };
}
