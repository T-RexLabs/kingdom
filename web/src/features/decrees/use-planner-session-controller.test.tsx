import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import type { PlanningSessionDetail } from "@/api/hooks";
import { usePlannerSessionController } from "./use-planner-session-controller";

const createOperation = vi.fn();

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: () => "variant-key",
  useActivateVariant: () => ({ mutateAsync: vi.fn() }),
  useCreatePlanningOperation: () => ({ mutateAsync: createOperation, error: null }),
  usePlanningOperation: () => ({ data: null }),
}));

vi.mock("./use-planner-operation", () => ({
  usePlannerOperation: () => ({ operation: null, connection: "idle" }),
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
  beforeEach(() => createOperation.mockReset());

  it.each([
    ["reply", (controller: ReturnType<typeof usePlannerSessionController>) => controller.submitReply({ text: "Ship it" })],
    ["regenerate", (controller: ReturnType<typeof usePlannerSessionController>) => controller.regenerate({ messageId: "message-1" })],
    ["formalize", (controller: ReturnType<typeof usePlannerSessionController>) => controller.startFormalize()],
  ])("reuses the exact ACP %s request after idempotency_pending", async (_kind, invoke) => {
    createOperation
      .mockRejectedValueOnce(pendingError())
      .mockResolvedValueOnce({ run_id: "acp-run-1", status: "pending" });
    const { result } = renderHook(() => usePlannerSessionController(session), { wrapper });

    await act(async () => {
      await expect(invoke(result.current)).rejects.toThrow("operation reservation pending");
    });
    const firstRequest = createOperation.mock.calls[0]?.[0];

    await act(async () => {
      await invoke(result.current);
    });

    expect(createOperation.mock.calls[1]?.[0].body).toBe(firstRequest.body);
    expect(createOperation.mock.calls[1]?.[0].key).toBe(firstRequest.key);
  });
});
