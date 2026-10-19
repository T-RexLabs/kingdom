import { useCallback, useEffect, useMemo, useRef, type ReactNode } from "react";
import { ApiError } from "@/api/client";
import { usePlanningOperation } from "@/api/hooks";
import { useNotifications } from "@/components/notifications";
import { router } from "@/router";
import { GlobalFormalizeContext, useGlobalFormalize, type FormalizeEntry } from "./global-formalize-context";
import { isPlannerOperationTerminal, type PlannerOperationState } from "./planner-operation-controller";
import { usePlannerOperation } from "./use-planner-operation";
import { useTrackedPlannerOperations } from "./use-planner-workspace-state";
import type { TrackedPlannerOperation } from "./planner-workspace-state";

function notificationId(operationId: string) { return `formalize-${operationId}`; }

export function GlobalFormalizeProvider({ children }: { children: ReactNode }) {
  const { store, operations } = useTrackedPlannerOperations();
  const { addNotification, removeNotification } = useNotifications();
  const entries = useMemo(() => operations.filter((entry) => entry.kind === "formalize"), [operations]);
  const trackFormalize = useCallback((sessionId: string, operationId: string, sessionTitle: string) => {
    // The controller registers the known operation before notifying. Never
    // invent a workspace identity or replace another session's registration.
    const entry = store.getOperations().find((item) => item.operationId === operationId && item.sessionId === sessionId);
    if (!entry) return;
    store.trackOperation({ ...entry, sessionTitle });
    addNotification({ id: notificationId(operationId), title: "Generating Plan", message: sessionTitle || undefined, status: "loading" });
  }, [store, addNotification]);
  const clearFormalize = useCallback((operationId: string) => {
    removeNotification(notificationId(operationId));
    store.finishOperation(operationId);
  }, [store, removeNotification]);
  const markNotified = useCallback((operationId: string) => store.markNotified(operationId), [store]);
  return <GlobalFormalizeContext.Provider value={{ entries, trackFormalize, clearFormalize, markNotified }}>{children}</GlobalFormalizeContext.Provider>;
}

export function FormalizeNotifier({ entry, streamed }: { entry: FormalizeEntry; streamed: PlannerOperationState | null }) {
  const { clearFormalize, markNotified } = useGlobalFormalize();
  const { addNotification } = useNotifications();
  const handledRef = useRef(entry.notified ?? false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dismissedRef = useRef(false);
  useEffect(() => () => { if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; } }, []);
  useEffect(() => {
    if (!streamed || streamed.operationId !== entry.operationId || !isPlannerOperationTerminal(streamed.status)) return;
    if (!handledRef.current) {
      handledRef.current = true;
      markNotified(entry.operationId);
      if (streamed.status === "done") {
        addNotification({
          id: notificationId(entry.operationId), title: "Plan Ready", message: entry.sessionTitle || undefined, status: "success", autoDismissMs: 4000,
          action: { label: "View Plan", onClick: () => { void router.navigate({ to: "/work-orders/planning/$sessionId", params: { sessionId: entry.sessionId }, search: { show_plan: true } }); } },
        });
      } else {
        addNotification({
          id: notificationId(entry.operationId), title: streamed.status === "cancelled" ? "Plan Generation Cancelled" : "Plan Failed",
          message: streamed.error ?? (streamed.status === "cancelled" ? "Plan generation was cancelled." : "An error occurred"),
          status: streamed.status === "cancelled" ? "info" : "error", autoDismissMs: streamed.status === "cancelled" ? 4000 : 8000,
        });
      }
    }
    if (timerRef.current || dismissedRef.current) return;
    timerRef.current = setTimeout(() => { timerRef.current = null; dismissedRef.current = true; clearFormalize(entry.operationId); }, streamed.status === "error" ? 9000 : 5000);
  }, [entry, streamed, addNotification, clearFormalize, markNotified]);
  return null;
}

function PlannerOperationTracker({ entry }: { entry: TrackedPlannerOperation }) {
  const { store } = useTrackedPlannerOperations();
  const { clearFormalize } = useGlobalFormalize();
  const operationQuery = usePlanningOperation(entry.operationId);
  useEffect(() => {
    if (operationQuery.error instanceof ApiError && operationQuery.error.status === 404) clearFormalize(entry.operationId);
  }, [operationQuery.error, entry.operationId, clearFormalize]);
  const { operation } = usePlannerOperation(operationQuery.data ?? null);
  useEffect(() => {
    if (!operation || operation.operationId !== entry.operationId) return;
    store.observeOperation(entry, operation);
    if (entry.kind !== "formalize" && isPlannerOperationTerminal(operation.status)) store.finishOperation(entry.operationId);
  }, [entry, operation, store]);
  return entry.kind === "formalize" ? <FormalizeNotifier entry={entry} streamed={operation} /> : null;
}

/** One tracker per active operation; no historical sessions mount. */
export function FormalizeNotifierMount() {
  const { operations } = useTrackedPlannerOperations();
  return <>{operations.map((entry) => <PlannerOperationTracker key={entry.operationId} entry={entry} />)}</>;
}
