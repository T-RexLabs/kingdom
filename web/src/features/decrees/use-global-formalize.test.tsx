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
      current,
      trackFormalize: vi.fn(),
      clearFormalize,
    };
    const renderNotifier = (streamed: PlannerOperationState) => (
      <StrictMode>
        <NotificationContext.Provider value={notificationContext}>
          <GlobalFormalizeContext.Provider value={formalizeContext}>
            <FormalizeNotifier streamed={streamed} />
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
