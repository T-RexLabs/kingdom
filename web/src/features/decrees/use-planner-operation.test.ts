import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import type { AcpRun, AcpRunEvent, PlanningSessionDetail } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { patchPlannerTerminalCaches } from "./use-planner-operation";

function buildSession(overrides: Partial<PlanningSessionDetail> = {}): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    acp_session_id: "8cfa85f2-f95a-53ec-ae58-2c46f21495dc",
    title: "Planner kickoff",
    title_state: "custom",
    auto_title_eligible: false,
    lifecycle_state: "active",
    conversation_state: "processing",
    current_question_message_id: "msg_old",
    current_run_id: "run_1",
    current_draft_id: undefined,
    context_snapshot_id: "snap_1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
  };
}

function buildRun(overrides: Partial<AcpRun> = {}): AcpRun {
  return {
    run_id: "run_1",
    session_id: "8cfa85f2-f95a-53ec-ae58-2c46f21495dc",
    planning_session_id: "ps_1",
    agent_name: "planner-reply",
    status: "completed",
    created_at: "2026-04-21T10:00:00Z",
    finished_at: "2026-04-21T10:05:00Z",
    ...overrides,
  };
}

function buildEvent(overrides: Partial<AcpRunEvent> = {}): AcpRunEvent {
  return {
    event_id: "run_1:9",
    run_id: "run_1",
    seq: 9,
    type: "run.completed",
    payload: {
      planning_message_id: "msg_new",
      result_ref: "msg_new",
      turn_index: 2,
    },
    created_at: "2026-04-21T10:05:00Z",
    ...overrides,
  };
}

describe("patchPlannerTerminalCaches", () => {
  it("unlocks the next selectable answer without waiting for a full refresh", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());
    qc.setQueryData(plannerKeys.messages("ps_1"), { pages: [], pageParams: [] });

    patchPlannerTerminalCaches(qc, buildRun(), buildEvent());

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    const messagesState = qc.getQueryState(plannerKeys.messages("ps_1"));
    expect(detail?.conversation_state).toBe("ready");
    expect(detail?.current_run_id).toBeUndefined();
    expect(detail?.current_question_message_id).toBe("msg_new");
    expect(messagesState?.isInvalidated).toBe(true);
  });
});
