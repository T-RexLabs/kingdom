import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import type { PlanningSessionDetail } from "@/api/hooks";
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
