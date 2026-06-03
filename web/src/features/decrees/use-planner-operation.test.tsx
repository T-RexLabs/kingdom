import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  PlanningOperationDetail,
  PlanningSessionDetail,
} from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { usePlannerOperation } from "./use-planner-operation";

// SSE seam mocks. The whole point of subscribeSSE is to be the only
// piece of network plumbing under test, so the mock captures the
// onConnectionChange callback and lets the test fire a `failed`
// transition immediately. The Promise resolves on abort to mirror
// production behaviour and avoid leaking a forever-pending promise
// across the SSE-effect's re-runs.
const subscribeSSEMock = vi.fn();
vi.mock("@/lib/sse-stream", () => ({
  subscribeSSE: (args: unknown) => subscribeSSEMock(args),
}));

// apiFetch is the polled fallback's transport. The mock is reset per
// test and configured with the exact response sequence the poll loop
// should observe.
const apiFetchMock = vi.fn();
vi.mock("@/api/client", () => ({
  API_BASE: "/api/v1",
  apiFetch: (...args: unknown[]) => apiFetchMock(...args),
  buildAuthHeaders: (headers?: HeadersInit) => ({ ...(headers as object) }),
}));

function buildOperation(
  overrides: Partial<PlanningOperationDetail> = {},
): PlanningOperationDetail {
  return {
    operation_id: "op_running",
    planning_session_id: "ps_1",
    type: "reply",
    status: "running",
    request: {},
    error: null,
    result_ref: null,
    created_at: "2026-04-21T10:00:00Z",
    finished_at: null,
    ...overrides,
  } as PlanningOperationDetail;
}

function buildSession(
  overrides: Partial<PlanningSessionDetail> = {},
): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    title: "Planner kickoff",
    title_state: "custom",
    lifecycle_state: "active",
    conversation_state: "processing",
    current_question_message_id: "msg_1",
    current_operation_id: "op_running",
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

// Drain pending microtasks. Combined with `vi.advanceTimersByTimeAsync`
// this lets the poll loop's await-chain settle deterministically
// without depending on `waitFor` (which interacts poorly with fake
// timers — its retry interval is itself a setTimeout that gets faked).
async function flush(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

beforeEach(() => {
  subscribeSSEMock.mockReset();
  apiFetchMock.mockReset();
  vi.useFakeTimers();
  qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
});

afterEach(() => {
  vi.useRealTimers();
  qc.clear();
});

describe("usePlannerOperation — SSE-fail → poll-fallback → terminal hydrate", () => {
  it("transitions through connection.failed → polling → idle and hydrates the operation when the poll finds a terminal status", async () => {
    let sseArgs: {
      onConnectionChange: (next: { state: string; attempt?: number; lastError?: string }) => void;
      signal: AbortSignal;
    } | null = null;
    subscribeSSEMock.mockImplementation((args: typeof sseArgs) => {
      sseArgs = args;
      return new Promise<void>((resolve) => {
        args!.signal.addEventListener("abort", () => resolve(), { once: true });
      });
    });

    // Two polls: first returns `running` (still in flight, no
    // hydrate), then a terminal `succeeded` row that drives the
    // hydrate + invalidate cascade. Subsequent calls (if any) also
    // return the terminal so the hydrate path is idempotent.
    apiFetchMock
      .mockResolvedValueOnce(buildOperation({ status: "running" }))
      .mockResolvedValue(
        buildOperation({
          status: "succeeded",
          finished_at: "2026-04-21T10:06:00Z",
          result_ref: "msg_done",
        }),
      );

    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    const invalidateSpy = vi.spyOn(qc, "invalidateQueries");

    // Stable operation reference — recreating it per render would
    // re-fire the SSE effect on every dispatch.
    const op = buildOperation();
    const { result } = renderHook(() => usePlannerOperation(op), {
      wrapper: makeWrapper(),
    });

    // SSE subscription was wired up.
    await flush();
    expect(subscribeSSEMock).toHaveBeenCalled();
    expect(sseArgs).not.toBeNull();

    // Drive SSE to `failed`. The reducer surfaces it through
    // `connection.state` only, and the poll fallback arms itself.
    await act(async () => {
      sseArgs!.onConnectionChange({ state: "failed", attempt: 6, lastError: "EOF" });
    });
    expect(result.current.connection.state).toBe("failed");
    // Connection failure must not mutate the operation into a terminal
    // state. Callers should treat this as a transport problem while the
    // server-side operation keeps running.
    expect(result.current.operation?.status).toBe("pending");
    expect(result.current.operation?.error).toBeNull();

    // First poll runs immediately on effect mount. Flush microtasks
    // so its `running` result is observed before assertions.
    await flush();
    expect(apiFetchMock).toHaveBeenCalledTimes(1);

    // Load-bearing safety property: the cache must NOT have been
    // patched to "ready" yet — the operation is genuinely still in
    // flight server-side. A premature flip would let the user kick
    // off a second operation that the partial-unique-index then 409s.
    {
      const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
      expect(detail?.conversation_state).toBe("processing");
      expect(detail?.current_operation_id).toBe("op_running");
    }

    // Advance the 5s poll interval and flush so the second poll
    // resolves with the terminal row, hydrate fires, and the
    // invalidation fan-out runs.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await flush();

    expect(apiFetchMock.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(result.current.operation?.status).toBe("done");
    expect(result.current.connection.state).toBe("idle");

    const invalidatedKeys = invalidateSpy.mock.calls
      .map((call) => (call[0] as { queryKey?: readonly unknown[] } | undefined)?.queryKey)
      .filter((k): k is readonly unknown[] => Array.isArray(k))
      .map((k) => JSON.stringify(k));
    const expectedKeys = [
      plannerKeys.operation("op_running"),
      plannerKeys.detail("ps_1"),
      plannerKeys.messages("ps_1"),
      plannerKeys.plan("ps_1"),
      plannerKeys.listPrefix(),
    ].map((k) => JSON.stringify(k));
    for (const key of expectedKeys) {
      expect(invalidatedKeys).toContain(key);
    }
  });

  it("keeps polling silently when the API throws — never flips the cache to ready until a real terminal row arrives", async () => {
    let sseArgs: {
      onConnectionChange: (next: { state: string; attempt?: number; lastError?: string }) => void;
      signal: AbortSignal;
    } | null = null;
    subscribeSSEMock.mockImplementation((args: typeof sseArgs) => {
      sseArgs = args;
      return new Promise<void>((resolve) => {
        args!.signal.addEventListener("abort", () => resolve(), { once: true });
      });
    });

    apiFetchMock.mockRejectedValue(new Error("ECONNREFUSED"));

    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    const op = buildOperation();
    const { result } = renderHook(() => usePlannerOperation(op), {
      wrapper: makeWrapper(),
    });

    await flush();
    await act(async () => {
      sseArgs!.onConnectionChange({ state: "failed", attempt: 6, lastError: "EOF" });
    });

    // Walk through several poll cycles. Each one rejects; the loop
    // catches and waits the 5s interval. The cache must stay
    // "processing" throughout — a phantom "ready" flip is the worst
    // possible bug here (composer re-enables → user 409s themselves).
    await flush();
    expect(apiFetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await flush();
    expect(apiFetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await flush();
    expect(apiFetchMock).toHaveBeenCalledTimes(3);

    expect(result.current.connection.state).toBe("failed");
    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.conversation_state).toBe("processing");
    expect(detail?.current_operation_id).toBe("op_running");
  });
});
