import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useSyncExternalStore } from "react";
import { plannerWorkspaceState, type PlannerScope } from "./planner-workspace-state";

export function usePlannerWorkspaceState() { return plannerWorkspaceState(useQueryClient()); }
export function usePlannerSessionUI(scope: PlannerScope) {
  const store = usePlannerWorkspaceState();
  const { workspace, sessionId } = scope;
  const subscribe = useCallback((listener: () => void) => store.subscribeSession({ workspace, sessionId }, listener), [store, workspace, sessionId]);
  const snapshot = useCallback(() => store.getSession({ workspace, sessionId }), [store, workspace, sessionId]);
  return { store, state: useSyncExternalStore(subscribe, snapshot, snapshot) };
}
export function useTrackedPlannerOperations() {
  const store = usePlannerWorkspaceState();
  return { store, operations: useSyncExternalStore(store.subscribeOperations, store.getOperations, store.getOperations) };
}
