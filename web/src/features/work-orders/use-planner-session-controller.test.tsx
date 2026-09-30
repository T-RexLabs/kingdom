import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setCSRFToken, ApiError } from "@/api/client";
import type { PlanningSessionDetail } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { plannerScope, plannerWorkspaceState } from "./planner-workspace-state";
import { usePlannerSessionController } from "./use-planner-session-controller";

const createOperation = vi.fn();
const mockUsePlanningOperation = vi.fn();
const mockUsePlannerOperation = vi.fn();

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: () => "variant-key",
  useActivateVariant: () => ({ mutateAsync: vi.fn() }),
  useCreatePlanningOperation: () => ({ mutateAsync: createOperation, error: null }),
  usePlanningOperation: (...args: unknown[]) => mockUsePlanningOperation(...args),
}));

vi.mock("./use-planner-operation", () => ({
  usePlannerOperation: (...args: unknown[]) => mockUsePlannerOperation(...args),
}));

const session: PlanningSessionDetail = {
  planning_session_id: "planning-session-1",
  acp_session_id: "acp-session-1",
  title: "Planner",
  title_state: "custom",
  auto_title_eligible: false,
  lifecycle_state: "active",
  conversation_state: "ready",
  active_leaf_message_id: "message-1",
  created_at: "2026-08-18T10:00:00Z",
  updated_at: "2026-08-18T10:00:00Z",
  project_root: "/tmp/project",
  model_selection: { model: "github-copilot/gpt-5.4-mini" },
  external_sources: [],
};

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={new QueryClient()}>{children}</QueryClientProvider>;
}

function pendingError() {
  return new ApiError(409, {
    message: "operation reservation pending",
    code: "idempotency_pending",
    retryable: true,
    category: "core",
    details: {},
    correlationId: "test",
  });
}

describe("usePlannerSessionController mutation recovery", () => {
  beforeEach(() => {
    sessionStorage.clear();
    createOperation.mockReset();
    mockUsePlanningOperation.mockReset();
    mockUsePlanningOperation.mockReturnValue({
      data: null,
      error: null,
      isError: false,
      isFetching: false,
      isPending: false,
      refetch: vi.fn(),
    });
    mockUsePlannerOperation.mockReset();
    mockUsePlannerOperation.mockReturnValue({ operation: null, connection: { state: "idle" } });
  });

  it.each(["reply", "regenerate", "formalize", "stop"] as const)("does not send %s if the exact request cannot be journaled", async (kind) => {
    const client = new QueryClient();
    const currentSession = kind === "stop" ? { ...session, current_run_id: "active-run", conversation_state: "processing" as const } : session;
    const view = renderHook(() => usePlannerSessionController(currentSession), { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    const send = vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("response lost"));
    createOperation.mockRejectedValue(new TypeError("response lost"));
    const storage = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new DOMException("quota", "QuotaExceededError"); });
    try {
      await act(async () => {
        const request = kind === "reply" ? view.result.current.submitReply({ text: "Keep this intent" }) : kind === "regenerate" ? view.result.current.regenerate({ messageId: "message-1" }) : kind === "formalize" ? view.result.current.startFormalize() : view.result.current.stop.onStop();
        await expect(request).rejects.toThrow(/recovery request could not be saved/);
      });
      expect(createOperation).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
      expect(plannerWorkspaceState(client).getSession(plannerScope(session)).request).toBeUndefined();
      expect(view.result.current.createOperation.error?.message).toMatch(/recovery request could not be saved/);
    } finally { storage.mockRestore(); send.mockRestore(); }
  });

  it.each([
    [
      "reply",
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.submitReply({ text: "Ship it" }),
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.retryReplyRecovery(),
    ],
    [
      "regenerate",
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.regenerate({ messageId: "message-1" }),
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.regenerate({ messageId: "message-1" }),
    ],
    [
      "formalize",
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.startFormalize(),
      (controller: ReturnType<typeof usePlannerSessionController>) => controller.startFormalize(),
    ],
  ])("reuses the exact ACP %s request after idempotency_pending", async (_kind, invoke, retry) => {
    createOperation
      .mockRejectedValueOnce(pendingError())
      .mockResolvedValueOnce({ run_id: "acp-run-1", status: "pending" });
    const { result } = renderHook(() => usePlannerSessionController(session), { wrapper });

    await act(async () => {
      await expect(invoke(result.current)).rejects.toThrow("operation reservation pending");
    });
    const firstRequest = createOperation.mock.calls[0]?.[0];

    await act(async () => {
      await retry(result.current);
    });

    expect(createOperation.mock.calls[1]?.[0].body).toBe(firstRequest.body);
    expect(createOperation.mock.calls[1]?.[0].key).toBe(firstRequest.key);
  });

  it.each([
    ["idempotency_pending", () => pendingError()],
    ["a transport failure", () => new Error("connection reset")],
  ])("blocks a different reply during %s recovery and retries the retained reply explicitly", async (_failureKind, makeFailure) => {
    createOperation
      .mockRejectedValueOnce(makeFailure())
      .mockResolvedValueOnce({ run_id: "acp-run-a", status: "pending" })
      .mockResolvedValueOnce({ run_id: "acp-run-b", status: "pending" });
    const { result } = renderHook(() => usePlannerSessionController(session), { wrapper });

    await act(async () => {
      await expect(result.current.submitReply({ text: "Reply A" })).rejects.toThrow();
    });
    const firstRequest = createOperation.mock.calls[0]?.[0];
    expect(result.current.recoveryKind).toBe("reply");

    await act(async () => {
      await expect(result.current.submitReply({ text: "Reply B" })).rejects.toThrow(
        "Retry the pending reply request before sending another reply",
      );
    });
    expect(createOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      await result.current.retryReplyRecovery();
    });

    expect(createOperation.mock.calls[1]?.[0].body).toBe(firstRequest?.body);
    expect(createOperation.mock.calls[1]?.[0].key).toBe(firstRequest?.key);
    expect(result.current.recoveryKind).toBeNull();

    await act(async () => {
      await result.current.submitReply({ text: "Reply B" });
    });

    expect(createOperation.mock.calls[2]?.[0].body.reply?.content_markdown).toBe("Reply B");
    expect(createOperation.mock.calls[2]?.[0].key).not.toBe(firstRequest?.key);
  });

  it("retains a failed operation after the session clears its live run pointer", async () => {
    const failedOperation = {
      kind: "reply" as const,
      operationId: "acp-run-1",
      status: "error" as const,
      lastEventSeq: 3,
      pendingReply: null,
      replyText: "",
      formalizeText: "",
      error: "Planner provider failed",
      events: [],
      activityItems: [],
    };
    mockUsePlanningOperation.mockReturnValue({
      data: { run_id: "acp-run-1" },
      error: null,
      isError: false,
      isFetching: false,
      isPending: false,
      refetch: vi.fn(),
    });
    mockUsePlannerOperation.mockReturnValue({ operation: failedOperation, connection: { state: "idle" } });
    const processingSession: PlanningSessionDetail = {
      ...session,
      conversation_state: "processing" as const,
      current_run_id: "acp-run-1",
    };
    const { result, rerender } = renderHook(
      ({ currentSession }) => usePlannerSessionController(currentSession),
      { wrapper, initialProps: { currentSession: processingSession } },
    );

    await waitFor(() => expect(result.current.activeOperation?.status).toBe("error"));

    rerender({ currentSession: { ...processingSession, conversation_state: "ready", current_run_id: undefined } });

    expect(result.current.activeOperation).toEqual(failedOperation);
    expect(result.current.activeOperationId).toBeUndefined();
  });
});


describe("planner session ownership", () => {
  beforeEach(() => {
    sessionStorage.clear(); createOperation.mockReset();
    mockUsePlanningOperation.mockReturnValue({ data: null, error: null, isError: false, isFetching: false, isPending: false, refetch: vi.fn() });
    mockUsePlannerOperation.mockReturnValue({ operation: null, connection: { state: "idle" } });
  });

  it("keeps both sessions' ambiguous replies and their exact keys across navigation", async () => {
    createOperation.mockRejectedValueOnce(pendingError()).mockRejectedValueOnce(pendingError()).mockResolvedValueOnce({ run_id: "op-a", status: "created" });
    const client = new QueryClient();
    const view = renderHook(({ currentSession }) => usePlannerSessionController(currentSession), { initialProps: { currentSession: session }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    await act(async () => { await expect(view.result.current.submitReply({ text: "A" })).rejects.toThrow(); });
    const original = createOperation.mock.calls[0][0];
    view.rerender({ currentSession: { ...session, planning_session_id: "session-b" } });
    expect(view.result.current.recoveryKind).toBeNull();
    await act(async () => { await expect(view.result.current.submitReply({ text: "B" })).rejects.toThrow(); });
    view.rerender({ currentSession: session });
    expect(view.result.current.recoveryKind).toBe("reply");
    await act(async () => { await view.result.current.retryReplyRecovery(); });
    expect(createOperation.mock.calls[2][0]).toEqual(original);
    view.rerender({ currentSession: { ...session, planning_session_id: "session-b" } });
    expect(view.result.current.recoveryKind).toBe("reply");
  });

  it("settles A in A's registry after the controller switches to B", async () => {
    let finish!: (value: { run_id: string; status: string }) => void;
    createOperation.mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }));
    const client = new QueryClient();
    const view = renderHook(({ currentSession }) => usePlannerSessionController(currentSession), { initialProps: { currentSession: session }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    let request!: Promise<void>;
    act(() => { request = view.result.current.submitReply({ text: "A" }); });
    view.rerender({ currentSession: { ...session, planning_session_id: "session-b" } });
    await act(async () => { finish({ run_id: "op-a", status: "created" }); await request; });
    expect(view.result.current.activeOperationId).toBeUndefined();
    expect(view.result.current.recoveryKind).toBeNull();
    view.rerender({ currentSession: session });
    expect(view.result.current.activeOperationId).toBe("op-a");
  });
});


describe("planner outcome resolution and Stop", () => {
  beforeEach(() => {
    sessionStorage.clear(); createOperation.mockReset();
    mockUsePlanningOperation.mockReturnValue({ data: null, error: null, isError: false, isFetching: false, isPending: false, refetch: vi.fn() });
    mockUsePlannerOperation.mockReturnValue({ operation: null, connection: { state: "idle" } });
    setCSRFToken("csrf", "original-binding");
  });

  it.each([
    ["unauthorized", 401, "unknown", "api.auth"],
    ["CSRF", 403, "policy_violation", "api.auth.csrf"],
    ["conflicting key", 409, "idempotency_conflict", "idempotency-middleware"],
    ["expired key", 400, "idempotency_key_expired", "idempotency-middleware"],
  ] as const)("retains unresolved reply and Stop requests after a rejected %s replay", async (_label, status, code, correlationId) => {
    const error = new ApiError(status, { message: "Replay rejected", code, category: "core", details: {}, retryable: false, correlationId });
    createOperation.mockRejectedValueOnce(new TypeError("Response lost")).mockRejectedValueOnce(error);
    const client = new QueryClient();
    const reply = renderHook(() => usePlannerSessionController(session), { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    await act(async () => { await expect(reply.result.current.submitReply({ text: "Original" })).rejects.toThrow("Response lost"); });
    const original = plannerWorkspaceState(client).getSession(plannerScope(session)).request;
    await act(async () => { await expect(reply.result.current.retryReplyRecovery()).rejects.toThrow("Replay rejected"); });
    expect(plannerWorkspaceState(client).getSession(plannerScope(session)).request).toEqual(original);
    reply.unmount();

    const active = { ...session, planning_session_id: "stop-session", current_run_id: "running-operation", conversation_state: "processing" as const };
    const stopClient = new QueryClient();
    const stop = renderHook(() => usePlannerSessionController(active), { wrapper: ({ children }) => <QueryClientProvider client={stopClient}>{children}</QueryClientProvider> });
    const send = vi.spyOn(globalThis, "fetch").mockRejectedValueOnce(new TypeError("Response lost")).mockResolvedValueOnce(new Response(JSON.stringify({ error: { code, message: "Replay rejected", category: "core", details: {}, retryable: false, correlation_id: correlationId } }), { status }));
    try {
      await act(async () => { await expect(stop.result.current.stop.onStop()).rejects.toThrow("Response lost"); });
      const stopped = plannerWorkspaceState(stopClient).getSession(plannerScope(active)).request;
      await act(async () => { await expect(stop.result.current.stop.onStop()).rejects.toThrow("Replay rejected"); });
      expect(plannerWorkspaceState(stopClient).getSession(plannerScope(active)).request).toEqual(stopped);
    } finally { send.mockRestore(); }
  });

  it("resolves a restored request through authoritative readback without a replacement POST", async () => {
    createOperation.mockRejectedValueOnce(pendingError());
    const firstClient = new QueryClient();
    const original = renderHook(() => usePlannerSessionController(session), { wrapper: ({ children }) => <QueryClientProvider client={firstClient}>{children}</QueryClientProvider> });
    await act(async () => { await expect(original.result.current.submitReply({ text: "Original A" })).rejects.toThrow(); });
    const sent = createOperation.mock.calls[0][0];
    original.unmount();
    setCSRFToken("new-csrf", "replacement-binding");
    const recoveredRun = { run_id: "recovered-run", planning_session_id: session.planning_session_id, session_id: "acp-session", agent_name: "planner-reply", status: "completed", created_at: "2026-09-30T10:00:00Z" };
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({ state: "completed", response_status: 201, response_body: recoveredRun }), { status: 200 })).mockResolvedValueOnce(new Response(JSON.stringify(recoveredRun), { status: 200 }));
    const client = new QueryClient();
    const restored = renderHook(() => usePlannerSessionController(session), { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    try {
      expect(restored.result.current.recoveryBlocked).toBe(true);
      await act(async () => { await restored.result.current.recovery.onCheck(); });
      expect(fetch.mock.calls[0][0]).toContain(`/mutation-outcomes/${sent.key}?operation=planning-operation-create`);
      expect(createOperation).toHaveBeenCalledTimes(1);
      expect(restored.result.current.recoveryKind).toBeNull();
      expect(client.getQueryData(plannerKeys.operation("recovered-run"))).toEqual(recoveredRun);
    } finally { fetch.mockRestore(); }
  });

  it.each(["unknown", "pending"])("retains the exact journal when outcome readback is %s", async (state) => {
    createOperation.mockRejectedValueOnce(pendingError());
    const client = new QueryClient();
    const hook = renderHook(() => usePlannerSessionController(session), { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    await act(async () => { await expect(hook.result.current.submitReply({ text: "Original" })).rejects.toThrow(); });
    const key = createOperation.mock.calls[0][0].key;
    setCSRFToken("new-csrf", "replacement-binding");
    hook.rerender();
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({ state }), { status: 200 }));
    try {
      await act(async () => { await hook.result.current.recovery.onCheck(); });
      expect(hook.result.current.recoveryBlocked).toBe(true);
      expect(plannerWorkspaceState(client).getSession(plannerScope(session)).request?.key).toBe(key);
      expect(createOperation).toHaveBeenCalledTimes(1);
    } finally { fetch.mockRestore(); }
  });

  it("cancels the originating run without optimistically cancelling another session", async () => {
    const client = new QueryClient();
    const activeSession = { ...session, current_run_id: "op-to-stop", conversation_state: "processing" as const };
    let finish!: (response: Response) => void;
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }));
    const hook = renderHook(({ currentSession }) => usePlannerSessionController(currentSession), { initialProps: { currentSession: activeSession as PlanningSessionDetail }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
    try {
      let request!: Promise<void>;
      act(() => { request = hook.result.current.stop.onStop(); });
      expect(hook.result.current.stop.pending).toBe(true);
      expect(hook.result.current.activeOperation?.status).not.toBe("cancelled");
      hook.rerender({ currentSession: { ...session, planning_session_id: "session-b", current_run_id: "op-b", conversation_state: "processing" } });
      const cancelled = { run_id: "op-to-stop", planning_session_id: session.planning_session_id, session_id: "acp", agent_name: "planner-reply", status: "cancelled", created_at: "2026-09-30T10:00:00Z" };
      await act(async () => { finish(new Response(JSON.stringify(cancelled), { status: 200 })); await request; });
      expect(fetch.mock.calls[0][0]).toBe("/api/v1/acp/runs/op-to-stop/cancel");
      expect(fetch.mock.calls[0][1]?.method).toBe("POST");
      expect(hook.result.current.activeOperationId).toBe("op-b");
      expect(client.getQueryData(plannerKeys.operation("op-to-stop"))).toEqual(cancelled);
    } finally { fetch.mockRestore(); }
  });
});
