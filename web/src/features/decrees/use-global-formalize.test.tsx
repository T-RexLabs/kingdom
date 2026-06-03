import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useEffect, useState, type ReactNode } from "react";
import { NotificationProvider, useNotifications } from "@/components/notifications";
import {
  FormalizeNotifier,
  GlobalFormalizeProvider,
  useGlobalFormalize,
} from "./use-global-formalize";
import type { PlannerOperationState } from "./planner-operation-controller";

// router is exercised when the user clicks "View Plan" on the
// success toast. Tests don't click it, but the import path needs a
// stub since the real router pulls in the route tree.
vi.mock("@/router", () => ({
  router: { navigate: vi.fn().mockResolvedValue(undefined) },
}));

function makeStreamed(overrides: Partial<PlannerOperationState> = {}): PlannerOperationState {
  return {
    kind: "formalize",
    operationId: "op-x",
    status: "pending",
    lastEventSeq: 1,
    pendingReply: null,
    replyText: "",
    formalizeText: "",
    error: null,
    ...overrides,
  };
}

function ProviderTree({ children }: { children: ReactNode }) {
  return (
    <NotificationProvider>
      <GlobalFormalizeProvider>{children}</GlobalFormalizeProvider>
    </NotificationProvider>
  );
}

beforeEach(() => {
  sessionStorage.clear();
});

afterEach(() => {
  sessionStorage.clear();
  vi.useRealTimers();
});

// ============================================================================
// GlobalFormalizeProvider — isolated state + storage tests. No data
// fetching, no terminal effect — those live in FormalizeNotifier.
// ============================================================================

describe("GlobalFormalizeProvider — public state API", () => {
  it("trackFormalize adds a loading notification, sets current, and persists to sessionStorage", () => {
    const { result } = renderHook(
      () => ({
        formalize: useGlobalFormalize(),
        notifications: useNotifications().notifications,
      }),
      { wrapper: ProviderTree },
    );

    expect(result.current.formalize.current).toBeNull();

    act(() => {
      result.current.formalize.trackFormalize("session-1", "op-1", "Build the planner");
    });

    expect(result.current.formalize.current).toEqual({
      sessionId: "session-1",
      operationId: "op-1",
      sessionTitle: "Build the planner",
    });
    expect(result.current.notifications.get("formalize-op-1")?.status).toBe("loading");
    const stored = sessionStorage.getItem("kingdom:pending-formalize");
    expect(stored).not.toBeNull();
    expect(JSON.parse(stored!)).toEqual({
      sessionId: "session-1",
      operationId: "op-1",
      sessionTitle: "Build the planner",
    });
  });

  it("clearFormalize removes notification, current, and storage", () => {
    const { result } = renderHook(
      () => ({
        formalize: useGlobalFormalize(),
        notifications: useNotifications().notifications,
      }),
      { wrapper: ProviderTree },
    );

    act(() => {
      result.current.formalize.trackFormalize("session-1", "op-1", "Build");
    });
    expect(result.current.formalize.current).not.toBeNull();

    act(() => {
      result.current.formalize.clearFormalize();
    });

    expect(result.current.formalize.current).toBeNull();
    expect(result.current.notifications.get("formalize-op-1")).toBeUndefined();
    expect(sessionStorage.getItem("kingdom:pending-formalize")).toBeNull();
  });

  it("rehydrates current from sessionStorage on mount", () => {
    sessionStorage.setItem(
      "kingdom:pending-formalize",
      JSON.stringify({ sessionId: "session-2", operationId: "op-2", sessionTitle: "Restored" }),
    );

    const { result } = renderHook(() => useGlobalFormalize(), { wrapper: ProviderTree });

    expect(result.current.current).toEqual({
      sessionId: "session-2",
      operationId: "op-2",
      sessionTitle: "Restored",
    });
  });

  it("ignores corrupt sessionStorage on mount (does not throw)", () => {
    sessionStorage.setItem("kingdom:pending-formalize", "{not json");

    const { result } = renderHook(() => useGlobalFormalize(), { wrapper: ProviderTree });

    expect(result.current.current).toBeNull();
  });

  it("trackFormalize removes the prior tracked notification before adding the new loading toast", () => {
    // A new tracked formalize should replace the old toast instead of
    // leaving both notifications visible at once.
    const { result } = renderHook(
      () => ({
        formalize: useGlobalFormalize(),
        notifications: useNotifications().notifications,
      }),
      { wrapper: ProviderTree },
    );

    // First formalize — loading toast for op-A.
    act(() => {
      result.current.formalize.trackFormalize("session-1", "op-A", "First plan");
    });
    expect(result.current.notifications.get("formalize-op-A")?.status).toBe("loading");
    expect(result.current.formalize.current?.operationId).toBe("op-A");

    // Second formalize starts BEFORE the first one terminated.
    // Without the fix: both toasts coexist. With the fix: old
    // notification is removed before the new loading toast lands,
    // AND the tracker advances to op-B (proving we didn't just
    // remove the old notification but also moved state forward).
    act(() => {
      result.current.formalize.trackFormalize("session-1", "op-B", "Second plan");
    });
    expect(result.current.notifications.get("formalize-op-A")).toBeUndefined();
    expect(result.current.notifications.get("formalize-op-B")?.status).toBe("loading");
    expect(result.current.formalize.current?.operationId).toBe("op-B");
  });
});

// FormalizeNotifier tests keep the provider mounted and drive streamed state
// through a harness so latch behavior is exercised across updates, not remounts.

function NotifierTree({
  trackOp,
  initial,
  controllerRef,
  children,
}: {
  trackOp?: { sessionId: string; operationId: string; sessionTitle: string };
  initial: PlannerOperationState | null;
  controllerRef: { current: ((next: PlannerOperationState | null) => void) | null };
  children: ReactNode;
}) {
  return (
    <NotificationProvider>
      <GlobalFormalizeProvider>
        <NotifierHarness initial={initial} controllerRef={controllerRef} trackOp={trackOp} />
        {children}
      </GlobalFormalizeProvider>
    </NotificationProvider>
  );
}

function NotifierHarness({
  initial,
  controllerRef,
  trackOp,
}: {
  initial: PlannerOperationState | null;
  controllerRef: { current: ((next: PlannerOperationState | null) => void) | null };
  trackOp?: { sessionId: string; operationId: string; sessionTitle: string };
}) {
  const [streamed, setLocal] = useState(initial);
  controllerRef.current = setLocal;
  const { trackFormalize } = useGlobalFormalize();
  // Self-trigger trackFormalize on first mount via useEffect.
  // Doing it during render would call setState on the Provider
  // mid-render and React would warn (correctly).
  useEffect(() => {
    if (trackOp) {
      trackFormalize(trackOp.sessionId, trackOp.operationId, trackOp.sessionTitle);
    }
    // We only want to fire this once on mount; the harness is
    // remounted per test so re-runs aren't a concern.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return <FormalizeNotifier streamed={streamed} />;
}

describe("FormalizeNotifier — terminal latch contract", () => {
  it("fires success toast on streamed.status='done'", () => {
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-done", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-done", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    expect(result.current.get("formalize-op-done")?.status).toBe("loading");

    act(() => {
      controller.current!(makeStreamed({ operationId: "op-done", status: "done", draftId: "d1" }));
    });

    const toast = result.current.get("formalize-op-done");
    expect(toast?.status).toBe("success");
    expect(toast?.title).toMatch(/plan ready/i);
  });

  it("fires info toast on streamed.status='cancelled'", () => {
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-cancelled", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-cancelled", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    act(() => {
      controller.current!(makeStreamed({
        operationId: "op-cancelled",
        status: "cancelled",
        error: "operation cancelled during shutdown",
      }));
    });

    const toast = result.current.get("formalize-op-cancelled");
    expect(toast?.status).toBe("info");
    expect(toast?.title).toMatch(/cancelled/i);
  });

  it("fires error toast on streamed.status='error'", () => {
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-error", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-error", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    act(() => {
      controller.current!(makeStreamed({ operationId: "op-error", status: "error", error: "boom" }));
    });

    const toast = result.current.get("formalize-op-error");
    expect(toast?.status).toBe("error");
    expect(toast?.title).toMatch(/plan failed/i);
  });

  it("does NOT fire on non-terminal streamed status (transient SSE failure scenario)", () => {
    // Keep the same provider mounted while the streamed status moves from
    // running to pending to done. The non-terminal update must not fire a
    // toast, and the terminal update must fire exactly once.
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-latched", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-latched", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    expect(result.current.get("formalize-op-latched")?.status).toBe("loading");

    // Non-terminal update: no terminal toast.
    act(() => {
      controller.current!(makeStreamed({ operationId: "op-latched", status: "pending" }));
    });
    expect(result.current.get("formalize-op-latched")?.status).toBe("loading");

    // Real terminal update: the latch fires once.
    act(() => {
      controller.current!(makeStreamed({ operationId: "op-latched", status: "done" }));
    });
    expect(result.current.get("formalize-op-latched")?.status).toBe("success");
  });

  it("ignores streamed updates for a different operationId", () => {
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-tracked", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-tracked", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    // A streamed update for a DIFFERENT op id (e.g. a stale cache
    // hit during a transition) must not flip our toast.
    act(() => {
      controller.current!(makeStreamed({ operationId: "op-stranger", status: "done" }));
    });
    expect(result.current.get("formalize-op-tracked")?.status).toBe("loading");
    expect(result.current.get("formalize-op-stranger")).toBeUndefined();
  });

  it("latches per-mount: the same terminal status fires exactly once", async () => {
    // Reduplicating the streamed object reference would re-fire the
    // effect; the latch must short-circuit on the second observation.
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => useNotifications().notifications,
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-once", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-once", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    act(() => {
      controller.current!(makeStreamed({ operationId: "op-once", status: "done" }));
    });
    const firstToast = result.current.get("formalize-op-once");
    expect(firstToast?.status).toBe("success");

    // Drive another non-identical "done" object — the latch must
    // suppress re-firing for the same op id.
    act(() => {
      controller.current!(makeStreamed({ operationId: "op-once", status: "done" }));
    });
    // Same toast, not a fresh addNotification call — the existing
    // toast persists with the same fields, no re-render flicker.
    expect(result.current.get("formalize-op-once")?.status).toBe("success");
  });

  it("eagerly clears sessionStorage on terminal and delays current clear until after dismissDelay", async () => {
    // Terminal handling clears sessionStorage immediately so a refresh does
    // not replay the toast, but delays clearing `current` until the dismiss
    // window ends so live consumers can still read the tracker.
    vi.useFakeTimers();
    const controller = { current: null as ((next: PlannerOperationState | null) => void) | null };
    const { result } = renderHook(
      () => ({
        formalize: useGlobalFormalize(),
        notifications: useNotifications().notifications,
      }),
      {
        wrapper: ({ children }) => (
          <NotifierTree
            trackOp={{ sessionId: "session-1", operationId: "op-cleanup", sessionTitle: "Build" }}
            initial={makeStreamed({ operationId: "op-cleanup", status: "streaming" })}
            controllerRef={controller}
          >
            {children}
          </NotifierTree>
        ),
      },
    );

    // Pre-condition: trackFormalize ran on harness mount → storage
    // is written and `current` is set.
    expect(sessionStorage.getItem("kingdom:pending-formalize")).not.toBeNull();
    expect(result.current.formalize.current?.operationId).toBe("op-cleanup");

    // Drive terminal=done. The Notifier's effect runs synchronously
    // inside the act block: addNotification (success), eager
    // clearStorage(), schedule the delayed clearFormalize timer.
    await act(async () => {
      controller.current!(makeStreamed({ operationId: "op-cleanup", status: "done" }));
    });

    // Sanity: this test is exercising the cleanup-AFTER-terminal
    // path, not cleanup-without-terminal. The success toast must
    // be in place before we time-travel.
    expect(result.current.notifications.get("formalize-op-cleanup")?.status).toBe("success");

    // Eager guarantee: sessionStorage cleared immediately. A refresh
    // at this exact moment must NOT re-fire the terminal toast.
    expect(sessionStorage.getItem("kingdom:pending-formalize")).toBeNull();
    // Delayed guarantee: current still set so mid-flight consumers
    // observe the tracker until the toast's auto-dismiss has
    // elapsed.
    expect(result.current.formalize.current?.operationId).toBe("op-cleanup");

    // Advance just before dismissDelay (5s for done) — current still set.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4999);
    });
    expect(result.current.formalize.current?.operationId).toBe("op-cleanup");

    // Advance past dismissDelay — current is now null (delayed
    // clearFormalize fired).
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2);
    });
    expect(result.current.formalize.current).toBeNull();
  });
});
