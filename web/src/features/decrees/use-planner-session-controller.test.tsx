import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PlanningSessionDetail } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { applyPlanningSessionFields } from "./planner-session-cache";
import { usePlannerSessionController } from "./use-planner-session-controller";

// Mock @/api/hooks so the controller can be exercised without the
// real network layer or a router. Each hook returns a stable shape;
// tests drive behaviour by invoking the controller's submitReply /
// regenerate / startFormalize functions directly.
const createMutateAsync = vi.fn();
const activateMutateAsync = vi.fn();

vi.mock("@/api/hooks", async () => {
  const actual = await vi.importActual<typeof import("@/api/hooks")>("@/api/hooks");
  return {
    ...actual,
    newIdempotencyKey: (scope: string) => `${scope}-test`,
    useCreatePlanningOperation: () => ({
      mutateAsync: createMutateAsync,
      error: null,
    }),
    useActivateVariant: () => ({
      mutateAsync: activateMutateAsync,
    }),
    usePlanningOperation: () => ({ data: null }),
  };
});

// Controllable mock so individual tests can drive streamed operation
// state and connection through phases without remounting. The default
// shape (idle, no operation) keeps the snapshot-rollback tests below
// behaving the same as when the mock was a static factory.
const usePlannerOperationMock = vi.fn();
vi.mock("./use-planner-operation", () => ({
  usePlannerOperation: () => usePlannerOperationMock(),
}));

function buildSession(
  overrides: Partial<PlanningSessionDetail> = {},
): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    title: "Planner kickoff",
    title_state: "custom",
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

let qc: QueryClient;

function makeWrapper() {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}

beforeEach(() => {
  createMutateAsync.mockReset();
  activateMutateAsync.mockReset();
  usePlannerOperationMock.mockReset();
  usePlannerOperationMock.mockReturnValue({ operation: null, connection: { state: "idle" } });
  qc = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
});

afterEach(() => {
  qc.clear();
});

describe("usePlannerSessionController — field-scoped rollback", () => {
  it("submitReply rollback preserves a concurrent SSE update to current_operation_id", async () => {
    // Pre-seed the detail cache so applyPlanningSessionFields has a
    // live entry to read pre-write values from.
    const session = buildSession({ conversation_state: "ready" });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

    // mutateAsync rejects only AFTER an SSE-style cache patch lands.
    createMutateAsync.mockImplementationOnce(() => {
      // Simulate an SSE update that arrives while the create
      // operation is in flight. A sibling formalize finishing,
      // for instance, would land current_operation_id=op_other on
      // the cache before the reply's mutation rejects.
      applyPlanningSessionFields(qc, "ps_1", { current_operation_id: "op_other_just_arrived" });
      return Promise.reject(new Error("network gone"));
    });

    const { result } = renderHook(() => usePlannerSessionController(session), {
      wrapper: makeWrapper(),
    });

    await expect(
      act(async () => {
        await result.current.submitReply({ text: "hi", answer: { text: "hi" } });
      }),
    ).rejects.toThrow("network gone");

    // The optimistic conversation_state was reverted (back to
    // "ready") AND the SSE-applied current_operation_id survived —
    // because takeSnapshot only captured conversation_state, not
    // the entire session object.
    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.conversation_state).toBe("ready");
    expect(detail?.current_operation_id).toBe("op_other_just_arrived");
  });

  it("regenerate rollback preserves a concurrent SSE update to context_snapshot_id", async () => {
    const session = buildSession({ conversation_state: "ready", context_snapshot_id: "snap_1" });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);
    qc.setQueryData(plannerKeys.messages("ps_1"), {
      pages: [
        {
          planning_session_id: "ps_1",
          items: [
            {
              planning_message_id: "assistant-1",
              planning_session_id: "ps_1",
              turn_index: 1,
              role: "assistant",
              content_markdown: "older",
              created_at: "2026-04-21T10:00:00Z",
              variant_index: 0,
              active: true,
            },
          ],
          total_count: 1,
          first_item_index: 0,
          previous_cursor: undefined,
        },
      ],
      pageParams: [undefined],
    });

    createMutateAsync.mockImplementationOnce(() => {
      // Concurrent /context/refresh succeeds and updates the cache
      // mid-flight. Without field-scoped snapshots this update would
      // be reverted along with the regenerate's optimistic write.
      applyPlanningSessionFields(qc, "ps_1", { context_snapshot_id: "snap_just_refreshed" });
      return Promise.reject(new Error("regenerate failed"));
    });

    const { result } = renderHook(() => usePlannerSessionController(session), {
      wrapper: makeWrapper(),
    });

    await expect(
      act(async () => {
        await result.current.regenerate({ turnIndex: 1 });
      }),
    ).rejects.toThrow("regenerate failed");

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.conversation_state).toBe("ready"); // reverted
    expect(detail?.context_snapshot_id).toBe("snap_just_refreshed"); // survived
  });

  it("startFormalize rollback does not touch session detail (empty field set)", async () => {
    // Formalize's optimistic write of current_operation_id /
    // conversation_state happens AFTER mutateAsync resolves, so the
    // catch-path snapshot has no detail fields to revert. This test
    // pins the empty-fieldNames behaviour: detail cache is untouched
    // by the rollback even when the mutation fails.
    const session = buildSession({
      conversation_state: "ready",
      current_operation_id: undefined,
    });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

    createMutateAsync.mockImplementationOnce(() => {
      // Sibling SSE patch lands during the in-flight create.
      applyPlanningSessionFields(qc, "ps_1", { current_operation_id: "op_concurrent" });
      return Promise.reject(new Error("formalize failed"));
    });

    const { result } = renderHook(() => usePlannerSessionController(session), {
      wrapper: makeWrapper(),
    });

    await expect(
      act(async () => {
        await result.current.startFormalize();
      }),
    ).rejects.toThrow("formalize failed");

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    // Both the SSE-applied current_operation_id and the original
    // conversation_state survive — formalize's rollback was a
    // pure no-op on the session detail.
    expect(detail?.current_operation_id).toBe("op_concurrent");
    expect(detail?.conversation_state).toBe("ready");
  });

  it("takeSnapshot reads pre-write values from the LIVE cache, not the closure-captured prop", async () => {
    // The session prop and the cache start in sync, but a same-tick
    // SSE patch lands BEFORE the optimistic write. takeSnapshot must
    // read from the cache so the rollback target reflects the
    // post-SSE state, not the stale prop.
    const session = buildSession({ conversation_state: "ready" });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

    // Simulate an SSE patch BEFORE submitReply runs. The cache now
    // has conversation_state=ready (already set) but, hypothetically,
    // a different value than the prop would carry if it were stale.
    // For this test we assert that the rollback target equals the
    // CACHE value at takeSnapshot time, by mutating the cache after
    // the controller's session prop was captured.
    const { result, rerender } = renderHook(({ s }) => usePlannerSessionController(s), {
      initialProps: { s: session },
      wrapper: makeWrapper(),
    });

    // Cache patches conversation_state to a divergent value before
    // the controller method runs. The session prop closure still
    // says "ready"; the live cache says "processing-from-elsewhere".
    applyPlanningSessionFields(qc, "ps_1", {
      conversation_state: "processing",
      current_operation_id: "op_external",
    });

    // Re-render so the controller's useMemo sees the prop is the
    // same reference (we didn't change it). The takeSnapshot
    // executes inside submitReply and reads the live cache.
    rerender({ s: session });

    createMutateAsync.mockImplementationOnce(() => Promise.reject(new Error("boom")));

    await expect(
      act(async () => {
        await result.current.submitReply({ text: "hi", answer: { text: "hi" } });
      }),
    ).rejects.toThrow("boom");

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    // Rollback target was the live cache value at snapshot time
    // (`processing`), not the stale prop value (`ready`). Without
    // the live-cache read, this assertion would fail.
    expect(detail?.conversation_state).toBe("processing");
    // current_operation_id was never optimistically touched, so
    // it remains as the SSE patch left it.
    expect(detail?.current_operation_id).toBe("op_external");
  });
});

describe("usePlannerSessionController — retention across transient SSE failure", () => {
  it("submitReply keeps activeOperationId attached when usePlannerOperation reports connection.failed", async () => {
    // A transient SSE failure must not clear the active operation id.
    // Connection state is a UI concern; controller-owned operation state
    // stays attached until a real terminal transition arrives.
    const session = buildSession({ conversation_state: "ready" });
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

    createMutateAsync.mockResolvedValueOnce({ operation_id: "op_real" });

    // Start with an actively streaming operation.
    usePlannerOperationMock.mockReturnValue({
      operation: {
        kind: "reply",
        operationId: "op_real",
        status: "pending",
        lastEventSeq: 0,
        pendingReply: null,
        replyText: "",
        formalizeText: "",
        error: null,
      },
      connection: { state: "open", attempt: 1 },
    });

    const { result, rerender } = renderHook(({ s }) => usePlannerSessionController(s), {
      initialProps: { s: session },
      wrapper: makeWrapper(),
    });

    await act(async () => {
      await result.current.submitReply({ text: "hi", answer: { text: "hi" } });
    });

    // After submitReply resolves, the controller has set
    // activeOperationId from the mutateAsync return value.
    expect(result.current.activeOperationId).toBe("op_real");

    // Then flip only the transport state to failed; the operation itself
    // remains non-terminal.
    usePlannerOperationMock.mockReturnValue({
      operation: {
        kind: "reply",
        operationId: "op_real",
        status: "pending",
        lastEventSeq: 0,
        pendingReply: null,
        replyText: "",
        formalizeText: "",
        error: null,
      },
      connection: { state: "failed", attempt: 6, lastError: "EOF" },
    });
    rerender({ s: session });

    // RETENTION: activeOperationId still attached. The controller
    // never clears based on connection state — that's a UI concern,
    // not a controller concern.
    expect(result.current.activeOperationId).toBe("op_real");
    // The connection state IS exposed via the controller's return
    // value so the page can pass it through to PlannerThread.
    expect(result.current.connection.state).toBe("failed");
  });

	it("clears activeOperationId once the streamed operation ends in error", async () => {
		const session = buildSession({ conversation_state: "ready" });
		qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

		createMutateAsync.mockResolvedValueOnce({ operation_id: "op_real" });
		usePlannerOperationMock.mockReturnValue({
			operation: {
				kind: "reply",
				operationId: "op_real",
				status: "pending",
				lastEventSeq: 0,
				pendingReply: null,
				replyText: "",
				formalizeText: "",
				error: null,
			},
			connection: { state: "open", attempt: 1 },
		});

		const { result, rerender } = renderHook(({ s }) => usePlannerSessionController(s), {
			initialProps: { s: session },
			wrapper: makeWrapper(),
		});

		await act(async () => {
			await result.current.submitReply({ text: "hi", answer: { text: "hi" } });
		});

		expect(result.current.activeOperationId).toBe("op_real");

		usePlannerOperationMock.mockReturnValue({
			operation: {
				kind: "reply",
				operationId: "op_real",
				status: "error",
				lastEventSeq: 4,
				pendingReply: null,
				replyText: "",
				formalizeText: "",
				error: "provider failed",
			},
			connection: { state: "idle", attempt: 0 },
		});
		rerender({ s: session });

		expect(result.current.activeOperationId).toBeUndefined();
	});

	it("clears activeOperationId once the streamed operation is cancelled", async () => {
		const session = buildSession({ conversation_state: "ready" });
		qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), session);

		createMutateAsync.mockResolvedValueOnce({ operation_id: "op_real" });
		usePlannerOperationMock.mockReturnValue({
			operation: {
				kind: "formalize",
				operationId: "op_real",
				status: "pending",
				lastEventSeq: 0,
				pendingReply: null,
				replyText: "",
				formalizeText: "",
				error: null,
			},
			connection: { state: "open", attempt: 1 },
		});

		const { result, rerender } = renderHook(({ s }) => usePlannerSessionController(s), {
			initialProps: { s: session },
			wrapper: makeWrapper(),
		});

		await act(async () => {
			await result.current.startFormalize();
		});

		expect(result.current.activeOperationId).toBe("op_real");

		usePlannerOperationMock.mockReturnValue({
			operation: {
				kind: "formalize",
				operationId: "op_real",
				status: "cancelled",
				lastEventSeq: 2,
				pendingReply: null,
				replyText: "",
				formalizeText: "partial draft",
				error: "operation cancelled during shutdown",
			},
			connection: { state: "idle", attempt: 0 },
		});
		rerender({ s: session });

		expect(result.current.activeOperationId).toBeUndefined();
	});
});
