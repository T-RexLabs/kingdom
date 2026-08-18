import { createContext, useContext } from "react";

export type FormalizeEntry = {
  sessionId: string;
  operationId: string;
  sessionTitle: string;
};

export type GlobalFormalizeContextValue = {
  current: FormalizeEntry | null;
  trackFormalize: (sessionId: string, operationId: string, sessionTitle: string) => void;
  clearFormalize: () => void;
};

export const GlobalFormalizeContext = createContext<GlobalFormalizeContextValue | null>(null);

export function useGlobalFormalize() {
  const context = useContext(GlobalFormalizeContext);
  if (!context) throw new Error("useGlobalFormalize must be used within GlobalFormalizeProvider");
  return context;
}
