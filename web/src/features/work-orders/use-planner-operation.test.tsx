import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AcpRun } from "@/api/hooks";
import { usePlannerOperation } from "./use-planner-operation";

const subscribeSSEMock = vi.fn();
vi.mock("@/lib/sse-stream", () => ({
  subscribeSSE: (args: unknown) => subscribeSSEMock(args),
}));

vi.mock("@/api/client", () => ({
  API_BASE: "/api/v1",
  buildAuthHeaders: () => ({}),
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

  it("keeps successful terminal streams open until activity reconciliation", async () => {
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
    expect(subscriptions[0].signal.aborted).toBe(false);
    expect(subscriptions[0].isTerminalEventType("run.completed")).toBe(false);
    expect(subscriptions[0].isTerminalEventType("run.activity.reconciled")).toBe(true);
    expect(hook.result.current.operation?.replyText).toBe("terminal answer");

    await act(async () => {
      await subscriptions[0].onEvent({
        id: "acp-run-1:1",
        type: "generic",
        data: JSON.stringify({
          kind: "kingdom.tool",sequence:1,input:{availability:"not_provided"},output:{availability:"not_provided"},message:{availability:"not_provided"},reasoning:{availability:"not_provided"},
          activity_id: "recovered-tool",
          tool_name: "webfetch",
          label: "Fetched https://example.com",
        }),
      });
    });
    expect(hook.result.current.operation?.activityItems).toHaveLength(1);

    await act(async () => {
      await subscriptions[0].onEvent({
        id: "acp-run-1:2",
        type: "run.activity.reconciled",
        data: JSON.stringify({ recovered_activity_count: 1 }),
      });
    });
    expect(hook.result.current.operation?.activityReconciled).toBe(true);
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

});
