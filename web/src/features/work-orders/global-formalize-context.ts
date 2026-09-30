import { createContext, useContext } from "react";
import type { TrackedPlannerOperation } from "./planner-workspace-state";

export type FormalizeEntry = TrackedPlannerOperation;
export type GlobalFormalizeContextValue = {
  entries: FormalizeEntry[];
  trackFormalize: (sessionId: string, operationId: string, sessionTitle: string) => void;
  clearFormalize: (operationId: string) => void;
  markNotified: (operationId: string) => void;
};
export const GlobalFormalizeContext = createContext<GlobalFormalizeContextValue | null>(null);
export function useGlobalFormalize() {
  const context = useContext(GlobalFormalizeContext);
  if (!context) throw new Error("useGlobalFormalize must be used within GlobalFormalizeProvider");
  return context;
}
