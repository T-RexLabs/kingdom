import type { PlanningSessionDetail } from "@/api/hooks";
import { useCallback, useEffect, useEffectEvent, useMemo, useReducer, useRef } from "react";
import { getCanonicalPlanningSessionTitle } from "./planning-session-title";

/**
 * The previous controller stored "did the user override the title?" in a
 * local ref that flipped to true on every `setTitleInput` call — including
 * keystrokes that happened to type out the canonical title verbatim. Once
 * flipped, auto-generation was permanently disabled for that session, even
 * across remounts. The fix is to source the override signal from the
 * authoritative place: the server-confirmed `session.title_state` field.
 *
 * Responsibilities now split cleanly: this controller owns *edit state*
 * (is the input live, what's buffered in the draft, has a reveal animation
 * fired, has auto-gen run for this session) and exposes two explicit
 * commit-side primitives — `exitEdit` and `abandonEdit` — that the page
 * uses to terminate the edit. The page owns the *commit*: it reads the
 * live `titleInput`, decides when to call its own `commitTitle`, and
 * decides when (if ever) to exit edit mode based on the commit outcome.
 * This split eliminates the closure-ordering hazard the previous
 * `stopEditing` had, where a discard handler that reset the draft and
 * exited the edit could still re-fire the abandoned commit because both
 * dispatches happened to land in the same React batch.
 *
 * State is a small reducer rather than a tangle of refs + effects so the
 * transitions are explicit and the keystroke regression cannot return.
 */

type Reveal = { title: string; expiresAt: number };

type TitleState = {
  /** Buffer for the in-progress edit — only meaningful while isEditing. */
  draft: string;
  isEditing: boolean;
  /** Last canonical title we already animated; gates one-shot reveal. */
  lastRevealedFor: string | null;
  /** Drives the per-character reveal animation; null when idle. */
  revealing: Reveal | null;
  /** Tracks the prior session id so reset transitions are clean. */
  sessionId: string | null;
};

type TitleAction =
  | { type: "session_changed"; sessionId: string; canonical: string }
  | { type: "edit_start"; canonical: string }
  | { type: "edit_change"; draft: string }
  | { type: "edit_stop" }
  | { type: "abandon_edit"; canonical: string }
  | { type: "draft_sync"; canonical: string }
  | { type: "reveal_start"; title: string; durationMs: number; now: number }
  | { type: "reveal_end" };

const initialState: TitleState = {
  draft: "",
  isEditing: false,
  lastRevealedFor: null,
  revealing: null,
  sessionId: null,
};

function reducer(state: TitleState, action: TitleAction): TitleState {
  switch (action.type) {
    case "session_changed":
      return {
        ...initialState,
        sessionId: action.sessionId,
        draft: action.canonical,
      };
    case "edit_start":
      return { ...state, isEditing: true, draft: action.canonical, revealing: null };
    case "edit_change":
      // Crucially: NO "user has overridden" flag is set here. The override
      // signal is derived from session.title_state on the server. Typing the
      // same text as the canonical title cannot lock out auto-gen.
      return { ...state, draft: action.draft };
    case "edit_stop":
      return { ...state, isEditing: false };
    case "abandon_edit":
      // Atomic exit + draft reset. The page's discard handler fans out to
      // this single dispatch so there is no window where the input has
      // closed but the draft still holds the abandoned value — and no
      // window where a follow-up callback can read a stale draft and
      // mistakenly fire a commit for it.
      return { ...state, isEditing: false, draft: action.canonical };
    case "draft_sync":
      // Mirror the canonical title into the draft when not editing — keeps
      // the input bound to whatever the server believes the title is.
      if (state.isEditing) return state;
      return { ...state, draft: action.canonical };
    case "reveal_start":
      return {
        ...state,
        revealing: { title: action.title, expiresAt: action.now + action.durationMs },
        lastRevealedFor: action.title,
      };
    case "reveal_end":
      return { ...state, revealing: null };
    default:
      return state;
  }
}

function titleRevealDurationMs(title: string): number {
  const wordCount = title.split(/\s+/).filter(Boolean).length;
  return wordCount * 100 + 250;
}

interface UsePlanningSessionTitleControllerArgs {
  sessionId: string;
  session: PlanningSessionDetail | undefined;
}

interface UsePlanningSessionTitleControllerResult {
  displayTitle: string;
  isEditing: boolean;
  titleInput: string;
  isRevealing: boolean;
  startEditing: () => void;
  /**
   * Exit edit mode without touching the draft. The page calls this when
   * a commit succeeds — the cache mirror effect then reseeds the draft
   * from the new canonical title on the next render. Splitting this
   * from `abandonEdit` matters: success and abandonment leave the input
   * value in different states, and conflating them was the root of the
   * old discard-fires-commit bug.
   */
  exitEdit: () => void;
  /**
   * Atomic exit + draft reset to canonical. The page's Discard button
   * uses this so the input collapses to the header AND the draft no
   * longer holds the abandoned value — no follow-up onCommitDraft, no
   * closure trap.
   */
  abandonEdit: (canonical: string) => void;
  setTitleInput: (next: string) => void;
}

export function usePlanningSessionTitleController({
  sessionId,
  session,
}: UsePlanningSessionTitleControllerArgs): UsePlanningSessionTitleControllerResult {
  const [state, dispatch] = useReducer(reducer, initialState);
  const canonicalTitle = useMemo(() => getCanonicalPlanningSessionTitle(session), [session]);
  const revealTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const resetForSession = useEffectEvent(() => {
    dispatch({ type: "session_changed", sessionId, canonical: canonicalTitle });
  });

  // Reset only when the session id changes. The effect event reads the latest
  // canonical title without making title updates restart the edit session.
  useEffect(() => {
    resetForSession();
  }, [sessionId]);

  // Mirror the canonical title into the draft when not editing — keeps the
  // input bound to whatever the server believes the title is.
  useEffect(() => {
    if (!session) return;
    dispatch({ type: "draft_sync", canonical: canonicalTitle });
  }, [session, canonicalTitle]);

  // Trigger the reveal animation on the first transition into a generated
  // title. Idempotent — `lastRevealedFor` is the guard.
  useEffect(() => {
    if (!session) return;
    if (session.title_state !== "generated" || canonicalTitle === "") return;
    if (state.lastRevealedFor === canonicalTitle) return;

    dispatch({
      type: "reveal_start",
      title: canonicalTitle,
      durationMs: titleRevealDurationMs(canonicalTitle),
      now: Date.now(),
    });
  }, [session, canonicalTitle, state.lastRevealedFor]);

  // Schedule the reveal end. Lives separately from the trigger so it
  // cleans up on unmount and on rapid successive reveals.
  useEffect(() => {
    if (!state.revealing) return;
    const remaining = Math.max(0, state.revealing.expiresAt - Date.now());
    revealTimeoutRef.current = setTimeout(() => {
      dispatch({ type: "reveal_end" });
      revealTimeoutRef.current = null;
    }, remaining);
    return () => {
      if (revealTimeoutRef.current !== null) {
        clearTimeout(revealTimeoutRef.current);
        revealTimeoutRef.current = null;
      }
    };
  }, [state.revealing]);

  const startEditing = useCallback(() => {
    dispatch({ type: "edit_start", canonical: canonicalTitle });
  }, [canonicalTitle]);

  // Pure edit-mode exit. The page calls this on commit success; the
  // draft_sync effect then re-mirrors the canonical title back into the
  // draft on the next render once `isEditing` is false.
  const exitEdit = useCallback(() => {
    dispatch({ type: "edit_stop" });
  }, []);

  // Atomic exit + reset. Used by Discard. One dispatch means there is no
  // intermediate render where the draft still carries the abandoned
  // value — closing the closure window the previous stopEditing left
  // open for a stray commit re-fire.
  const abandonEdit = useCallback((canonical: string) => {
    dispatch({ type: "abandon_edit", canonical });
  }, []);

  const setTitleInput = useCallback((next: string) => {
    dispatch({ type: "edit_change", draft: next });
  }, []);

  // Display priority: reveal animation > in-progress edit > server's
  // canonical title.
  //
  // The previous logic preferred state.draft for any title_state==="custom"
  // session whenever draft was non-empty. That created a staleness bug:
  // after autosave the server may have normalized the title (e.g. trimmed
  // whitespace), but the local draft still held the user's untrimmed
  // input, so the header rendered the stale draft until a refetch. The
  // server is the only authority for already-saved titles; the optimistic
  // cache write fired from `stopEditing` keeps the displayed value in
  // step with the user's intent without trusting local state.
  const displayTitle = state.revealing
    ? state.revealing.title
    : state.isEditing && state.draft.trim()
      ? state.draft
      : canonicalTitle;

  return {
    displayTitle,
    isEditing: state.isEditing,
    titleInput: state.draft,
    isRevealing: state.revealing !== null,
    startEditing,
    exitEdit,
    abandonEdit,
    setTitleInput,
  };
}
