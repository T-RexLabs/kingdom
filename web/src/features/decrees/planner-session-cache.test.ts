import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import type { PlanningSessionDetail, PlanningSessionListResponse } from "@/api/hooks";
import {
  applyGeneratedPlanningSessionTitleIfPlaceholder,
  applyPlanningSessionFields,
  compareAndSwapPlanningSessionTitle,
} from "./planner-session-cache";
import { plannerKeys, PLANNER_LIST_LIMIT } from "./planner-query-keys";

function buildDetail(
  overrides: Partial<PlanningSessionDetail> = {},
): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    title: "Planner kickoff",
    title_state: "custom",
    auto_title_eligible: false,
    lifecycle_state: "active",
    conversation_state: "ready",
    current_question_message_id: "msg_1",
    current_operation_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "snap_1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
  };
}

function buildListResponse(
  items: Partial<PlanningSessionListResponse["items"][number]>[],
): PlanningSessionListResponse {
  return {
    items: items.map((overrides) => ({
      planning_session_id: "ps_1",
      title: "Planner kickoff",
      title_state: "custom" as const,
      lifecycle_state: "active" as const,
      conversation_state: "ready" as const,
      current_question_message_id: "msg_1",
      current_operation_id: undefined,
      current_draft_id: undefined,
      context_snapshot_id: "snap_1",
      created_at: "2026-04-21T10:00:00Z",
      updated_at: "2026-04-21T10:05:00Z",
      ...overrides,
    })),
  };
}

describe("applyPlanningSessionFields", () => {
  it("patches only the supplied fields on the detail cache", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "stale", current_operation_id: "op_streaming" }),
    );

    applyPlanningSessionFields(qc, "ps_1", { title: "fresh" });

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.title).toBe("fresh");
    // Concurrent SSE state survives the field-scoped patch.
    expect(detail?.current_operation_id).toBe("op_streaming");
    // Detail-only fields untouched.
    expect(detail?.model_selection).toEqual({ model: "github-copilot/gpt-5.4" });
  });

  it("propagates explicit undefined to clear current_operation_id", () => {
    // Presence-based contract: passing { foo: undefined } explicitly
    // sets foo to undefined. Required for terminal-event
    // reconciliation that needs to clear current_operation_id when
    // an operation finalises.
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ current_operation_id: "op_streaming" }),
    );

    applyPlanningSessionFields(qc, "ps_1", { current_operation_id: undefined });

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.current_operation_id).toBeUndefined();
  });

  it("filters detail-only fields out of list-item summaries", () => {
    // model_selection lives on detail but NOT on list summaries.
    // The helper must whitelist list-mirrored fields so the summary
    // doesn't gain a phantom model_selection key.
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(),
      buildListResponse([{ planning_session_id: "ps_1", title: "stale" }]),
    );

    applyPlanningSessionFields(qc, "ps_1", {
      title: "fresh",
      model_selection: { model: "different" },
    });

    const item = qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0];
    expect(item?.title).toBe("fresh");
    // The summary type doesn't include model_selection, so we assert
    // it never got attached. Cast to any for the introspection.
    expect((item as Record<string, unknown> | undefined)?.model_selection).toBeUndefined();
  });

  it("fans out across every list-limit slice via the prefix", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(50),
      buildListResponse([{ planning_session_id: "ps_1", title: "stale" }]),
    );
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(PLANNER_LIST_LIMIT),
      buildListResponse([{ planning_session_id: "ps_1", title: "stale" }]),
    );

    applyPlanningSessionFields(qc, "ps_1", { title: "fresh" });

    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(50))?.items[0]?.title).toBe("fresh");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(PLANNER_LIST_LIMIT))?.items[0]?.title).toBe("fresh");
  });

  it("preserves cache reference when no field actually changes", () => {
    // No spurious re-renders on a no-op patch. React-query consumers
    // rely on reference equality to decide whether to re-render.
    const qc = new QueryClient();
    const before = buildDetail({ title: "stable" });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), before);

    applyPlanningSessionFields(qc, "ps_1", { title: "stable" });

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))).toBe(before);
  });

  it("no-ops on an empty fields object", () => {
    const qc = new QueryClient();
    const before = buildDetail();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), before);

    applyPlanningSessionFields(qc, "ps_1", {});

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))).toBe(before);
  });

  it("ignores list items belonging to other sessions", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(),
      buildListResponse([
        { planning_session_id: "ps_other", title: "stable" },
        { planning_session_id: "ps_1", title: "stale" },
      ]),
    );

    applyPlanningSessionFields(qc, "ps_1", { title: "fresh" });

    const items = qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items ?? [];
    expect(items.find((i) => i.planning_session_id === "ps_other")?.title).toBe("stable");
    expect(items.find((i) => i.planning_session_id === "ps_1")?.title).toBe("fresh");
  });
});

describe("compareAndSwapPlanningSessionTitle", () => {
  it("reverts both detail and list when the cache still holds the optimistic write", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "Doomed", title_state: "custom" }),
    );
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(),
      buildListResponse([{ planning_session_id: "ps_1", title: "Doomed", title_state: "custom" }]),
    );

    compareAndSwapPlanningSessionTitle(
      qc, "ps_1", "Doomed", "custom", "Planner kickoff", "custom",
    );

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("Planner kickoff");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0]?.title).toBe("Planner kickoff");
  });

  it("is a no-op when the detail title has moved on", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "v3", title_state: "custom" }),
    );

    compareAndSwapPlanningSessionTitle(
      qc, "ps_1", "v1", "custom", "Planner kickoff", "custom",
    );

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("v3");
  });

  it("is a no-op when the title_state has flipped", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "Doomed", title_state: "generated" }),
    );

    compareAndSwapPlanningSessionTitle(
      qc, "ps_1", "Doomed", "custom", "Planner kickoff", "custom",
    );

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title_state).toBe("generated");
    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("Doomed");
  });

  it("preserves non-title fields on the detail entry during a successful CAS", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({
        title: "Doomed",
        title_state: "custom",
        current_operation_id: "op_streaming",
        context_snapshot_id: "snap_99",
      }),
    );

    compareAndSwapPlanningSessionTitle(
      qc, "ps_1", "Doomed", "custom", "Planner kickoff", "custom",
    );

    const reverted = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(reverted?.title).toBe("Planner kickoff");
    expect(reverted?.current_operation_id).toBe("op_streaming");
    expect(reverted?.context_snapshot_id).toBe("snap_99");
  });

  it("reverts every cached list slice, not just the default-limit one", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(50),
      buildListResponse([{ planning_session_id: "ps_1", title: "Doomed", title_state: "custom" }]),
    );
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(PLANNER_LIST_LIMIT),
      buildListResponse([{ planning_session_id: "ps_1", title: "Doomed", title_state: "custom" }]),
    );

    compareAndSwapPlanningSessionTitle(
      qc, "ps_1", "Doomed", "custom", "Planner kickoff", "custom",
    );

    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(50))?.items[0]?.title).toBe("Planner kickoff");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(PLANNER_LIST_LIMIT))?.items[0]?.title).toBe("Planner kickoff");
  });
});

describe("applyGeneratedPlanningSessionTitleIfPlaceholder", () => {
  it("adopts the generated title only for placeholder cache entries", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "plan_1", title_state: "placeholder" }),
    );
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(),
      buildListResponse([{ planning_session_id: "ps_1", title: "plan_1", title_state: "placeholder" }]),
    );

    applyGeneratedPlanningSessionTitleIfPlaceholder(qc, "ps_1", "Generated title");

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("Generated title");
    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title_state).toBe("generated");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0]?.title).toBe("Generated title");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0]?.title_state).toBe("generated");
  });

  it("preserves newer custom titles", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildDetail({ title: "Operator title", title_state: "custom" }),
    );
    qc.setQueryData<PlanningSessionListResponse>(
      plannerKeys.list(),
      buildListResponse([{ planning_session_id: "ps_1", title: "Operator title", title_state: "custom" }]),
    );

    applyGeneratedPlanningSessionTitleIfPlaceholder(qc, "ps_1", "Generated title");

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title).toBe("Operator title");
    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))?.title_state).toBe("custom");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0]?.title).toBe("Operator title");
    expect(qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list())?.items[0]?.title_state).toBe("custom");
  });
});
