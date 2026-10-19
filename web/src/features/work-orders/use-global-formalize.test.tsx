import { act, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StrictMode } from "react";
import { NotificationContext } from "@/components/notifications/notification-context";
import { GlobalFormalizeContext } from "./global-formalize-context";
import type { PlannerOperationState } from "./planner-operation-controller";
import { FormalizeNotifier } from "./use-global-formalize";

vi.mock("@/router", () => ({
  router: { navigate: vi.fn() },
}));

const terminalFormalize: PlannerOperationState = {
  kind: "formalize",
  operationId: "operation-1",
  status: "done",
  lastEventSeq: 2,
  pendingReply: null,
  replyText: "",
  formalizeText: "Plan",
  error: null,
  activityItems: [],
};

describe("formalize notifier", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("keeps terminal cleanup scheduled across streamed object rehydration", () => {
    vi.useFakeTimers();
    const addNotification = vi.fn();
    const clearFormalize = vi.fn();
    const current = {
      workspace: "/tmp/project",
      kind: "formalize" as const,
      sessionId: "session-1",
      operationId: "operation-1",
      sessionTitle: "Plan the work",
    };
    const notificationContext = {
      notifications: new Map(),
      addNotification,
      updateNotification: vi.fn(),
      removeNotification: vi.fn(),
    };
    const formalizeContext = {
      entries: [current],
      markNotified: vi.fn(),
      trackFormalize: vi.fn(),
      clearFormalize,
    };
    const renderNotifier = (streamed: PlannerOperationState) => (
      <StrictMode>
        <NotificationContext.Provider value={notificationContext}>
          <GlobalFormalizeContext.Provider value={formalizeContext}>
            <FormalizeNotifier entry={current} streamed={streamed} />
          </GlobalFormalizeContext.Provider>
        </NotificationContext.Provider>
      </StrictMode>
    );

    const view = render(renderNotifier(terminalFormalize));
    expect(addNotification).toHaveBeenCalledTimes(1);

    view.rerender(renderNotifier({ ...terminalFormalize, formalizeText: "Plan rehydrated" }));
    act(() => vi.advanceTimersByTime(5_000));

    expect(addNotification).toHaveBeenCalledTimes(1);
    expect(clearFormalize).toHaveBeenCalledTimes(1);
  });
});


it("notifies and clears two formalizations independently", () => {
  vi.useFakeTimers();
  const addNotification = vi.fn();
  const clearFormalize = vi.fn();
  const markNotified = vi.fn();
  const a = { workspace: "/tmp/project", sessionId: "a", operationId: "op-a", kind: "formalize" as const, sessionTitle: "A" };
  const b = { ...a, sessionId: "b", operationId: "op-b", sessionTitle: "B" };
  const context = { entries: [a, b], trackFormalize: vi.fn(), clearFormalize, markNotified };
  const notifications = { notifications: new Map(), addNotification, updateNotification: vi.fn(), removeNotification: vi.fn() };
  try {
    render(<NotificationContext.Provider value={notifications}><GlobalFormalizeContext.Provider value={context}>
      <FormalizeNotifier entry={a} streamed={{ ...terminalFormalize, operationId: "op-a" }} />
      <FormalizeNotifier entry={b} streamed={{ ...terminalFormalize, operationId: "op-b" }} />
    </GlobalFormalizeContext.Provider></NotificationContext.Provider>);
    expect(addNotification.mock.calls.map(([notice]) => notice.message)).toEqual(["A", "B"]);
    act(() => vi.advanceTimersByTime(5_000));
    expect(clearFormalize.mock.calls).toEqual([["op-a"], ["op-b"]]);
    expect(addNotification).toHaveBeenCalledTimes(2);
  } finally { vi.useRealTimers(); }
});
