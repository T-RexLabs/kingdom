import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { usePlanningOperation } from "@/api/hooks";
import { useNotifications } from "@/components/notifications";
import { router } from "@/router";
import { isPlannerOperationTerminal, type PlannerOperationState } from "./planner-operation-controller";
import { usePlannerOperation } from "./use-planner-operation";

const STORAGE_KEY = "kingdom:pending-formalize";

type FormalizeEntry = {
  sessionId: string;
  operationId: string;
  sessionTitle: string;
};

type GlobalFormalizeContextValue = {
  current: FormalizeEntry | null;
  trackFormalize: (sessionId: string, operationId: string, sessionTitle: string) => void;
  clearFormalize: () => void;
};

const GlobalFormalizeContext = createContext<GlobalFormalizeContextValue | null>(null);

function readStorage(): FormalizeEntry | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as FormalizeEntry;
    if (parsed.sessionId && parsed.operationId) return parsed;
  } catch {
    // ignore corrupt storage
  }
  return null;
}

function writeStorage(entry: FormalizeEntry) {
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(entry));
}

function clearStorage() {
  sessionStorage.removeItem(STORAGE_KEY);
}

function notificationId(operationId: string) {
  return `formalize-${operationId}`;
}

/**
 * Owns the public loading-toast behavior plus the persisted entry
 * state. NO data fetching, NO terminal-effect orchestration — those
 * live in FormalizeNotifier so the Provider remains test-driveable
 * without mocking data hooks, and the orchestrator can be exercised
 * with plain props.
 */
export function GlobalFormalizeProvider({ children }: { children: ReactNode }) {
  const [entry, setEntry] = useState<FormalizeEntry | null>(() => readStorage());
  const { addNotification, removeNotification } = useNotifications();

  const trackFormalize = useCallback(
    (sessionId: string, operationId: string, sessionTitle: string) => {
      // Remove any prior tracked notification before starting a fresh
      // formalize. Without this, a new formalize started during the
      // previous one's dismiss window would leave the old terminal
      // toast ("Plan Ready"/"Plan Failed"/"Plan Cancelled") visible
      // alongside the new "Generating Plan" loading toast — confusing
      // UX with mismatched titles. Mirrors clearFormalize's shape:
      // both methods need to know `entry` to clean up its
      // notification before mutating the tracker.
      if (entry) {
        removeNotification(notificationId(entry.operationId));
      }
      const next = { sessionId, operationId, sessionTitle };
      setEntry(next);
      writeStorage(next);
      addNotification({
        id: notificationId(operationId),
        title: "Generating Plan",
        message: sessionTitle || undefined,
        status: "loading",
      });
    },
    [entry, addNotification, removeNotification],
  );

  const clearFormalize = useCallback(() => {
    if (entry) {
      removeNotification(notificationId(entry.operationId));
    }
    setEntry(null);
    clearStorage();
  }, [entry, removeNotification]);

  return (
    <GlobalFormalizeContext.Provider value={{ current: entry, trackFormalize, clearFormalize }}>
      {children}
    </GlobalFormalizeContext.Provider>
  );
}

export function useGlobalFormalize() {
  const ctx = useContext(GlobalFormalizeContext);
  if (!ctx) throw new Error("useGlobalFormalize must be used within GlobalFormalizeProvider");
  return ctx;
}

/**
 * Pure orchestrator: reads `current` from the public context, takes
 * `streamed` as a prop, runs the latch + addNotification on terminal.
 * Tests drive this with plain useState-backed props instead of
 * mocking the underlying data hooks.
 *
 * The dismiss path uses the existing public clearFormalize from
 * context — no new context method needed. By the time dismissDelay
 * elapses, the toast has already auto-faded (autoDismissMs <
 * dismissDelay for every terminal branch), so clearFormalize's
 * removeNotification is an idempotent no-op against a missing id;
 * what it actually accomplishes is clearing the in-memory entry +
 * (idempotent) clearStorage so subsequent trackFormalize calls
 * start clean.
 */
export function FormalizeNotifier({ streamed }: { streamed: PlannerOperationState | null }) {
  const { current, clearFormalize } = useGlobalFormalize();
  const { addNotification } = useNotifications();
  const terminalHandledRef = useRef<string | null>(null);

  // Reset the latch whenever the tracked operation changes (or
  // clears entirely). Op IDs are server-issued and never collide,
  // so the previous-op latch wouldn't actually swallow a fresh
  // operation's terminal — but resetting on transition makes the
  // lifecycle easier to reason about and shields against any
  // future ID source change.
  useEffect(() => {
    if (terminalHandledRef.current && terminalHandledRef.current !== current?.operationId) {
      terminalHandledRef.current = null;
    }
  }, [current?.operationId]);

  useEffect(() => {
    if (!current || !streamed) return;
    if (streamed.operationId !== current.operationId) return;
    if (terminalHandledRef.current === current.operationId) return;
    if (!isPlannerOperationTerminal(streamed.status)) return;

    terminalHandledRef.current = current.operationId;

    let dismissDelay = 5000;
    if (streamed.status === "done") {
      addNotification({
        id: notificationId(current.operationId),
        title: "Plan Ready",
        message: current.sessionTitle || undefined,
        status: "success",
        autoDismissMs: 4000,
        action: {
          label: "View Plan",
          onClick: () => {
            void router.navigate({
              to: "/decrees/planning/$sessionId",
              params: { sessionId: current.sessionId },
              search: { show_plan: true },
            });
          },
        },
      });
    } else if (streamed.status === "cancelled") {
      addNotification({
        id: notificationId(current.operationId),
        title: "Plan Generation Cancelled",
        message: streamed.error ?? "Server restarted before the plan finished generating.",
        status: "info",
        autoDismissMs: 4000,
      });
    } else {
      // status === "error"
      addNotification({
        id: notificationId(current.operationId),
        title: "Plan Failed",
        message: streamed.error ?? "An error occurred",
        status: "error",
        autoDismissMs: 8000,
      });
      dismissDelay = 9000;
    }

    // Eager storage clear (refresh-safety): without this, a refresh
    // during the dismiss window would re-mount the Provider, read
    // sessionStorage, and re-fire the terminal effect → duplicate
    // toast on every refresh until dismissDelay elapsed.
    clearStorage();

    // Delayed entry clear via the existing public clearFormalize.
    // By dismissDelay elapsed, the toast has auto-faded; clearFormalize
    // calls removeNotification (idempotent on missing id), setEntry(null),
    // and clearStorage (idempotent on already-empty storage).
    const t = setTimeout(() => clearFormalize(), dismissDelay);
    return () => clearTimeout(t);
  }, [current, streamed, addNotification, clearFormalize]);

  return null;
}

/**
 * Production composer: subscribes to the operation row + SSE stream
 * for the currently tracked entry, passes the streamed state to
 * FormalizeNotifier. Mounted exactly once at the app root inside
 * GlobalFormalizeProvider. Tests that exercise the latch can mount
 * FormalizeNotifier directly with controlled props instead.
 */
export function FormalizeNotifierMount() {
  const { current } = useGlobalFormalize();
  const operationQuery = usePlanningOperation(current?.operationId);
  const { operation: streamed } = usePlannerOperation(operationQuery.data ?? null);
  return <FormalizeNotifier streamed={streamed} />;
}
