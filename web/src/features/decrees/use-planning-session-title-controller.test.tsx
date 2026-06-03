import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PlanningSessionDetail } from "@/api/hooks";
import { usePlanningSessionTitleController } from "./use-planning-session-title-controller";

function buildSession(
  overrides: Partial<PlanningSessionDetail> = {}
): PlanningSessionDetail {
  return {
    planning_session_id: "planning-session-1",
    title: "Planner kickoff",
    title_state: "custom",
    lifecycle_state: "active",
    conversation_state: "ready",
    current_question_message_id: "assistant-1",
    current_operation_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "planning-context-1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
  };
}

describe("usePlanningSessionTitleController", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("auto-generates once when the second user message appears", async () => {
    const generateTitle = vi.fn(async () =>
      buildSession({ title: "Mock Generated Title", title_state: "generated" })
    );

    const { rerender } = renderHook(
      ({ sessionId, session, userMessageCount }) =>
        usePlanningSessionTitleController({
          sessionId,
          session,
          userMessageCount,
          generateTitle,
        }),
      {
        initialProps: {
          sessionId: "planning-session-1",
          session: buildSession({ title: "plan_1", title_state: "placeholder" }),
          userMessageCount: 1,
        },
      }
    );

    expect(generateTitle).not.toHaveBeenCalled();

    rerender({
      sessionId: "planning-session-1",
      session: buildSession({ title: "plan_1", title_state: "placeholder" }),
      userMessageCount: 2,
    });

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(1);
    });

    rerender({
      sessionId: "planning-session-1",
      session: buildSession({ title: "Mock Generated Title", title_state: "generated" }),
      userMessageCount: 2,
    });

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(1);
    });
  });

  it("reveals a generated title once and keeps it visible after completion", async () => {
    vi.useFakeTimers();
    const generateTitle = vi.fn(async () =>
      buildSession({ title: "Mock Generated Title", title_state: "generated" })
    );

    const { result, rerender } = renderHook(
      ({ session }) =>
        usePlanningSessionTitleController({
          sessionId: "planning-session-1",
          session,
          userMessageCount: 2,
          generateTitle,
        }),
      {
        initialProps: {
          session: buildSession({ title: "plan_1", title_state: "placeholder" }),
        },
      }
    );

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(1);
    });

    rerender({
      session: buildSession({ title: "Mock Generated Title", title_state: "generated" }),
    });

    expect(result.current.isRevealing).toBe(true);
    expect(result.current.displayTitle).toBe("Mock Generated Title");

    await act(async () => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.isRevealing).toBe(false);
    expect(result.current.displayTitle).toBe("Mock Generated Title");

    rerender({
      session: buildSession({ title: "Mock Generated Title", title_state: "generated" }),
    });

    expect(result.current.isRevealing).toBe(false);
  });

  it("resets generation state when switching sessions", async () => {
    const generateTitle = vi.fn(async () =>
      buildSession({ title: "Mock Generated Title", title_state: "generated" })
    );

    const { rerender } = renderHook(
      ({ sessionId, session }) =>
        usePlanningSessionTitleController({
          sessionId,
          session,
          userMessageCount: 2,
          generateTitle,
        }),
      {
        initialProps: {
          sessionId: "planning-session-1",
          session: buildSession({ planning_session_id: "planning-session-1", title: "plan_1", title_state: "placeholder" }),
        },
      }
    );

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(1);
    });

    rerender({
      sessionId: "planning-session-2",
      session: buildSession({ planning_session_id: "planning-session-2", title: "plan_2", title_state: "placeholder" }),
    });

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(2);
    });
  });

  it("manual edit during reveal stops animation without replaying it", async () => {
    vi.useFakeTimers();
    const generateTitle = vi.fn(async () =>
      buildSession({ title: "Mock Generated Title", title_state: "generated" })
    );

    const { result, rerender } = renderHook(
      ({ session }) =>
        usePlanningSessionTitleController({
          sessionId: "planning-session-1",
          session,
          userMessageCount: 2,
          generateTitle,
        }),
      {
        initialProps: {
          session: buildSession({ title: "plan_1", title_state: "placeholder" }),
        },
      }
    );

    await vi.waitFor(() => {
      expect(generateTitle).toHaveBeenCalledTimes(1);
    });

    rerender({
      session: buildSession({ title: "Mock Generated Title", title_state: "generated" }),
    });

    expect(result.current.isRevealing).toBe(true);

    act(() => {
      result.current.startEditing();
    });

    expect(result.current.isRevealing).toBe(false);
    expect(result.current.isEditing).toBe(true);
    expect(result.current.titleInput).toBe("Mock Generated Title");

    act(() => {
      result.current.exitEdit();
    });

    rerender({
      session: buildSession({ title: "Mock Generated Title", title_state: "generated" }),
    });

    await act(async () => {
      vi.advanceTimersByTime(1000);
    });

    expect(result.current.isRevealing).toBe(false);
    expect(result.current.displayTitle).toBe("Mock Generated Title");
  });

  it("abandonEdit resets the draft to canonical and exits edit mode in one render", () => {
    // Regression guard for the discard closure trap. The previous
    // `stopEditing` read state.draft from a closure formed at render
    // time, so a discard handler that called `setTitleInput(canonical)`
    // and then `stopEditing()` would still hand the *abandoned* draft
    // to onCommitDraft (because the dispatch from setTitleInput hadn't
    // landed yet). The atomic abandon_edit reducer action means there
    // is no intermediate state where the draft still carries the
    // abandoned value — and the page-owned commit funnel never sees it.
    const generateTitle = vi.fn(async () => buildSession());
    // Build the session ONCE outside the renderHook factory so the
    // effect-deps stay stable across re-renders. Creating a fresh
    // session inside the factory would change the reference every
    // render and force draft_sync to re-fire endlessly.
    const session = buildSession({ title: "Planner kickoff", title_state: "custom" });

    const { result } = renderHook(() =>
      usePlanningSessionTitleController({
        sessionId: "planning-session-1",
        session,
        userMessageCount: 1,
        generateTitle,
      }),
    );

    act(() => {
      result.current.startEditing();
    });
    expect(result.current.isEditing).toBe(true);
    expect(result.current.titleInput).toBe("Planner kickoff");

    act(() => {
      result.current.setTitleInput("garbage user typed by accident");
    });
    expect(result.current.titleInput).toBe("garbage user typed by accident");

    act(() => {
      result.current.abandonEdit("Planner kickoff");
    });

    // One dispatch, two effects: edit mode is off AND the draft is the
    // canonical title. No intermediate render where draft was still
    // "garbage" but isEditing was already false.
    expect(result.current.isEditing).toBe(false);
    expect(result.current.titleInput).toBe("Planner kickoff");
    // generateTitle is unrelated to abandon — assert as a witness that
    // abandonment did not trigger any side effects via callbacks.
    expect(generateTitle).not.toHaveBeenCalled();
  });
});
