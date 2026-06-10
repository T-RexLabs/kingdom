import { renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import type { PlanningSessionDetail } from "@/api/hooks";
import { usePlanningSessionAutoTitleCatchup } from "./use-planning-session-auto-title-catchup";

const mutateAsync = vi.fn(async () => undefined);

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: (scope: string) => `${scope}-test`,
  useGeneratePlanningSessionTitle: () => ({
    mutateAsync,
    isPending: false,
  }),
}));

function buildSession(overrides: Partial<PlanningSessionDetail> = {}): PlanningSessionDetail {
  return {
    planning_session_id: "planning-session-1",
    title: "plan_1",
    title_state: "placeholder",
    lifecycle_state: "active",
    conversation_state: "ready",
    auto_title_eligible: false,
    current_question_message_id: "assistant-1",
    current_operation_id: undefined,
    current_title_operation_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "planning-context-1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
  };
}

describe("usePlanningSessionAutoTitleCatchup", () => {
  beforeEach(() => {
    mutateAsync.mockClear();
  });

  it("fires once for an eligible placeholder session across rerenders", async () => {
    const { rerender } = renderHook(
      ({ sessionId, session }) => usePlanningSessionAutoTitleCatchup(sessionId, session),
      {
        initialProps: {
          sessionId: "planning-session-1",
          session: buildSession({ auto_title_eligible: true }),
        },
      },
    );

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledTimes(1);
    });

    rerender({
      sessionId: "planning-session-1",
      session: buildSession({ auto_title_eligible: true, updated_at: "2026-04-21T10:06:00Z" }),
    });

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledTimes(1);
    });
  });

  it("does not fire for ineligible or already-running title sessions", async () => {
    const { rerender } = renderHook(
      ({ sessionId, session }) => usePlanningSessionAutoTitleCatchup(sessionId, session),
      {
        initialProps: {
          sessionId: "planning-session-1",
          session: buildSession({ auto_title_eligible: false }),
        },
      },
    );

    rerender({
      sessionId: "planning-session-1",
      session: buildSession({ auto_title_eligible: true, current_title_operation_id: "op_title_1" }),
    });

    await waitFor(() => {
      expect(mutateAsync).not.toHaveBeenCalled();
    });
  });
});
