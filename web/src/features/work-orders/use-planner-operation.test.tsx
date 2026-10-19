import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { usePlanningSessionDraft, type AcpRun, type PlanningSessionDetail } from "@/api/hooks";
import { usePlannerOperation } from "./use-planner-operation";
import { plannerKeys } from "./planner-query-keys";

const subscribeSSEMock = vi.fn();
const apiFetchMock = vi.fn();
vi.mock("@/lib/sse-stream", () => ({
  subscribeSSE: (args: unknown) => subscribeSSEMock(args),
}));

vi.mock("@/api/client", () => ({
  API_BASE: "/api/v1",
  buildAuthHeaders: () => ({}),
  apiFetch: (...args: unknown[]) => apiFetchMock(...args),
  ApiError: class ApiError extends Error {
    public readonly status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

type StreamSubscription = {
  signal: AbortSignal;
  onEvent: (event: { id: string; type: string; data: string }) => boolean | Promise<boolean>;
  isTerminalEventType: (type: string) => boolean;
};

let queryClient: QueryClient;

function Wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

function buildOperation(overrides: Partial<AcpRun> = {}): AcpRun {
  return {
    run_id: "acp-run-1",
    session_id: "provider-session-1",
    planning_session_id: "planning-session-1",
    agent_name: "planner-reply",
    status: "in-progress",
    created_at: "2026-04-21T10:00:00Z",
    ...overrides,
  };
}

async function flush(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe("usePlannerOperation", () => {
  beforeEach(() => {
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    subscribeSSEMock.mockReset();
    apiFetchMock.mockReset();
    subscribeSSEMock.mockImplementation(() => new Promise<void>(() => undefined));
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("keeps one stream and streamed text when polling refreshes the same run", async () => {
    const subscriptions: StreamSubscription[] = [];
    subscribeSSEMock.mockImplementation((args: StreamSubscription) => {
      subscriptions.push(args);
      return new Promise<void>(() => undefined);
    });

    const hook = renderHook(({ operation }) => usePlannerOperation(operation), {
      initialProps: { operation: buildOperation() },
      wrapper: Wrapper,
    });
    await flush();

    expect(subscriptions).toHaveLength(1);
    await act(async () => {
      await subscriptions[0].onEvent({
        id: "acp-run-1:1",
        type: "message.part",
        data: JSON.stringify({ text: "streamed answer" }),
      });
    });
    expect(hook.result.current.operation?.replyText).toBe("streamed answer");

    hook.rerender({ operation: buildOperation({ output_text: "stale poll snapshot" }) });
    await flush();

    expect(subscriptions).toHaveLength(1);
    expect(hook.result.current.operation?.replyText).toBe("streamed answer");
  });

  it("clears a formalize preview on a durable retry before accumulating retry output", async () => {
    const subscriptions: StreamSubscription[] = [];
    subscribeSSEMock.mockImplementation((args: StreamSubscription) => {
      subscriptions.push(args);
      return new Promise<void>(() => undefined);
    });

    const hook = renderHook(({ operation }) => usePlannerOperation(operation), {
      initialProps: { operation: buildOperation({ agent_name: "planner-formalize" }) },
      wrapper: Wrapper,
    });
    await flush();

    await act(async () => {
      await subscriptions[0].onEvent({
        id: "acp-run-1:1",
        type: "message.part",
        data: JSON.stringify({ text: "First attempt preview" }),
      });
    });
    expect(hook.result.current.operation?.formalizeText).toBe("First attempt preview");

    const retryEvent = {
      id: "acp-run-1:2",
      type: "generic",
      data: JSON.stringify({
        kind: "kingdom.progress",
        phase: "retry_scheduled",
        retry_decision_id: "retry-decision-1",
        retry_index: 1,
      }),
    };
    await act(async () => {
      await subscriptions[0].onEvent(retryEvent);
    });

    expect(hook.result.current.operation?.formalizeText).toBe("");
    expect(hook.result.current.operation?.status).toBe("streaming");

    await act(async () => {
      await subscriptions[0].onEvent({
        id: "acp-run-1:3",
        type: "message.part",
        data: JSON.stringify({ text: "Retry output" }),
      });
    });
    expect(hook.result.current.operation?.formalizeText).toBe("Retry output");

    // Replaying the durable retry event must not erase output from its attempt.
    await act(async () => {
      await subscriptions[0].onEvent(retryEvent);
    });
    expect(hook.result.current.operation?.formalizeText).toBe("Retry output");
  });

  it("ends the stream on the completed event and streams activity until then", async () => {
    const subscriptions: StreamSubscription[] = [];
    subscribeSSEMock.mockImplementation((args: StreamSubscription) => {
      subscriptions.push(args);
      return new Promise<void>(() => undefined);
    });

    const hook = renderHook(({ operation }) => usePlannerOperation(operation), {
      initialProps: { operation: buildOperation() },
      wrapper: Wrapper,
    });
    await flush();

    hook.rerender({ operation: buildOperation({ status: "completed", output_text: "terminal answer" }) });
    await flush();

    expect(subscriptions).toHaveLength(1);
    expect(subscriptions[0].isTerminalEventType("run.completed")).toBe(true);
    expect(subscriptions[0].isTerminalEventType("run.failed")).toBe(true);
    expect(subscriptions[0].isTerminalEventType("run.cancelled")).toBe(true);
    expect(subscriptions[0].isTerminalEventType("generic")).toBe(false);
    expect(hook.result.current.operation?.replyText).toBe("terminal answer");
  });

  it("rejects stale events after ownership moves to another run", async () => {
    const subscriptions: StreamSubscription[] = [];
    subscribeSSEMock.mockImplementation((args: StreamSubscription) => {
      subscriptions.push(args);
      return new Promise<void>(() => undefined);
    });

    const hook = renderHook(({ operation }) => usePlannerOperation(operation), {
      initialProps: { operation: buildOperation() },
      wrapper: Wrapper,
    });
    await flush();

    hook.rerender({ operation: buildOperation({ run_id: "acp-run-2", output_text: undefined }) });
    await flush();

    expect(subscriptions).toHaveLength(2);
    expect(subscriptions[0].signal.aborted).toBe(true);
    expect(await subscriptions[0].onEvent({
      id: "acp-run-1:1",
      type: "message.part",
      data: JSON.stringify({ text: "stale answer" }),
    })).toBe(false);
    expect(hook.result.current.operation?.operationId).toBe("acp-run-2");
    expect(hook.result.current.operation?.replyText).toBe("");
  });
  it("shares one stream between page and background consumers and transfers ownership", async () => {
    const operation = buildOperation();
    const first = renderHook(() => usePlannerOperation(operation), { wrapper: Wrapper });
    const second = renderHook(() => usePlannerOperation(operation), { wrapper: Wrapper });
    await flush();
    expect(subscribeSSEMock).toHaveBeenCalledTimes(1);
    const initial = subscribeSSEMock.mock.calls[0][0] as StreamSubscription;
    await act(async () => { await initial.onEvent({ id: "acp-run-1:1", type: "message.part", data: JSON.stringify({ text: "Shared text" }) }); });
    expect(first.result.current.operation?.replyText).toBe("Shared text");
    expect(second.result.current.operation?.replyText).toBe("Shared text");
    first.unmount();
    await flush();
    expect(initial.signal.aborted).toBe(true);
    expect(subscribeSSEMock).toHaveBeenCalledTimes(2);
    const transferred = subscribeSSEMock.mock.calls[1][0] as StreamSubscription;
    await act(async () => { await transferred.onEvent({ id: "acp-run-1:1", type: "message.part", data: JSON.stringify({ text: "Shared text" }) }); });
    expect(second.result.current.operation?.replyText).toBe("Shared text");
    second.unmount();
    expect(transferred.signal.aborted).toBe(true);
  });

  describe("formalize plan refresh", () => {
    const sessionId = "planning-session-1";
    const runId = "formalize-run-1";
    const nextDraft = {
      planning_draft_id: "draft-new",
      planning_session_id: sessionId,
      work_order_title: "Updated plan",
      work_order_text: "The newly generated plan is visible.",
      created_at: "2026-04-21T10:10:00Z",
    };

    function seedFormalizeSession() {
      const session: PlanningSessionDetail = {
        planning_session_id: sessionId,
        acp_session_id: "provider-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        auto_title_eligible: false,
        lifecycle_state: "active",
        conversation_state: "processing",
        active_leaf_message_id: "message-1",
        current_run_id: runId,
        current_draft_id: "draft-old",
        context_snapshot_id: "snapshot-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4" },
        external_sources: [],
      };
      queryClient.setQueryData(plannerKeys.detail(sessionId), session);
    }

    function formalizeCompletionStream(): StreamSubscription {
      return subscribeSSEMock.mock.calls[0][0] as StreamSubscription;
    }

    async function completeFormalize() {
      await act(async () => {
        await formalizeCompletionStream().onEvent({
          id: `${runId}:4`,
          type: "run.completed",
          data: JSON.stringify({ planning_draft_id: "draft-new", result_ref: "draft-new" }),
        });
      });
    }

    it("refreshes an older cached plan while the Plan tab is open", async () => {
      seedFormalizeSession();
      queryClient.setQueryData(plannerKeys.plan(sessionId), {
        ...nextDraft,
        planning_draft_id: "draft-old",
        work_order_title: "Old plan",
        work_order_text: "Old plan text.",
      });
      apiFetchMock.mockResolvedValueOnce(nextDraft);

      const hook = renderHook(() => ({
        plan: usePlanningSessionDraft(sessionId),
        operation: usePlannerOperation(buildOperation({ run_id: runId, planning_session_id: sessionId, agent_name: "planner-formalize" })),
      }), { wrapper: Wrapper });
      await flush();
      expect(hook.result.current.plan.data?.work_order_text).toBe("Old plan text.");

      await completeFormalize();

      await waitFor(() => expect(hook.result.current.plan.data?.work_order_text).toBe(nextDraft.work_order_text));
      expect(queryClient.getQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId))?.current_draft_id).toBe("draft-new");
      expect(apiFetchMock).toHaveBeenCalledWith(`/planning-sessions/${sessionId}/plan`);
    });

    it("loads the generated plan when switching from Chat after an empty plan was cached", async () => {
      seedFormalizeSession();
      queryClient.setQueryData(plannerKeys.plan(sessionId), null);
      apiFetchMock.mockResolvedValueOnce(nextDraft);

      const hook = renderHook(({ showPlan }: { showPlan: boolean }) => ({
        plan: usePlanningSessionDraft(showPlan ? sessionId : undefined),
        operation: usePlannerOperation(buildOperation({ run_id: runId, planning_session_id: sessionId, agent_name: "planner-formalize" })),
      }), { initialProps: { showPlan: false }, wrapper: Wrapper });
      await flush();
      expect(hook.result.current.plan.data).toBeUndefined();

      await completeFormalize();
      hook.rerender({ showPlan: true });

      await waitFor(() => expect(hook.result.current.plan.data?.work_order_text).toBe(nextDraft.work_order_text));
      expect(queryClient.getQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId))?.current_draft_id).toBe("draft-new");
      expect(apiFetchMock).toHaveBeenCalledWith(`/planning-sessions/${sessionId}/plan`);
    });

    it("refetches an invalidated plan when returning after generation completed off-page", async () => {
      seedFormalizeSession();
      queryClient.setQueryData(plannerKeys.plan(sessionId), null);
      apiFetchMock.mockResolvedValueOnce(nextDraft);

      const tracker = renderHook(
        () => usePlannerOperation(buildOperation({ run_id: runId, planning_session_id: sessionId, agent_name: "planner-formalize" })),
        { wrapper: Wrapper },
      );
      await flush();
      await completeFormalize();
      expect(apiFetchMock).not.toHaveBeenCalled();
      tracker.unmount();

      const plan = renderHook(() => usePlanningSessionDraft(sessionId), { wrapper: Wrapper });

      await waitFor(() => expect(plan.result.current.data?.work_order_text).toBe(nextDraft.work_order_text));
      expect(apiFetchMock).toHaveBeenCalledWith(`/planning-sessions/${sessionId}/plan`);
    });
  });

});
