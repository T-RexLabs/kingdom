import { useEffect, useRef, useState } from "react";

export const BASE_SPEED = 3;
export const CATCHUP_THRESHOLD = 100;
export const CATCHUP_DIVISOR = 15;
export const DRAIN_DIVISOR = 5;
export const DRAIN_MIN_SPEED = 10;
export const THROTTLE_MS = 30;

type RevealMode = "baseline" | "zero";

type RevealState = {
  cursor: number;
  displayText: string;
  isStreamActive: boolean;
  isStreaming: boolean;
  mode: RevealMode;
  targetText: string;
};

function initialRevealState(targetText: string, isStreaming: boolean, mode: RevealMode): RevealState {
  const startsAtZero = mode === "zero" && isStreaming;
  return {
    cursor: startsAtZero ? 0 : targetText.length,
    displayText: startsAtZero ? "" : targetText,
    isStreamActive: isStreaming,
    isStreaming,
    mode,
    targetText,
  };
}

function reconcileRevealState(
  state: RevealState,
  targetText: string,
  isStreaming: boolean,
  mode: RevealMode,
): RevealState {
  if (state.targetText === targetText && state.isStreaming === isStreaming && state.mode === mode) {
    return state;
  }

  if (isStreaming && !state.isStreaming) {
    return initialRevealState(targetText, true, mode);
  }

  if (!isStreaming && !state.isStreaming && !state.isStreamActive) {
    return initialRevealState(targetText, false, mode);
  }

  const cursor = Math.min(state.cursor, targetText.length);
  const hasCaughtUp = cursor >= targetText.length;
  return {
    cursor,
    displayText: state.cursor > targetText.length ? targetText : state.displayText,
    isStreamActive: isStreaming || !hasCaughtUp,
    isStreaming,
    mode,
    targetText,
  };
}

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
export function useTextReveal(
  targetText: string,
  isStreaming: boolean,
  mode: RevealMode = "baseline",
) {
  const [revealState, setRevealState] = useState(() => initialRevealState(targetText, isStreaming, mode));
  const lastRenderRef = useRef(0);

  const reconciledState = reconcileRevealState(revealState, targetText, isStreaming, mode);
  if (reconciledState !== revealState) {
    setRevealState(reconciledState);
  }

  useEffect(() => {
    if (!reconciledState.isStreamActive || reconciledState.cursor >= targetText.length) {
      return;
    }

    let rafId = 0;

    const advance = (now: number) => {
      if (now - lastRenderRef.current < THROTTLE_MS) {
        rafId = requestAnimationFrame(advance);
        return;
      }
      lastRenderRef.current = now;

      setRevealState((current) => {
        if (
          current.targetText !== targetText ||
          current.isStreaming !== isStreaming ||
          current.mode !== mode
        ) {
          return current;
        }

        const behind = targetText.length - current.cursor;
        const speed = isStreaming
          ? behind > CATCHUP_THRESHOLD
            ? Math.max(BASE_SPEED, Math.ceil(behind / CATCHUP_DIVISOR))
            : BASE_SPEED
          : Math.max(DRAIN_MIN_SPEED, Math.ceil(behind / DRAIN_DIVISOR));
        const cursor = Math.min(current.cursor + speed, targetText.length);

        return {
          ...current,
          cursor,
          displayText: targetText.slice(0, cursor),
          isStreamActive: isStreaming || cursor < targetText.length,
        };
      });
    };

    rafId = requestAnimationFrame(advance);

    return () => {
      if (rafId !== 0) {
        cancelAnimationFrame(rafId);
      }
    };
  }, [isStreaming, mode, reconciledState.cursor, reconciledState.isStreamActive, targetText]);

  const safeDisplayText = reconciledState.isStreamActive
    ? (targetText.startsWith(reconciledState.displayText) ? reconciledState.displayText : targetText)
    : targetText;

  return {
    displayText: safeDisplayText,
    isStreamActive: reconciledState.isStreamActive,
    hasCaughtUpToTarget: !reconciledState.isStreamActive || safeDisplayText === targetText,
  };
}
