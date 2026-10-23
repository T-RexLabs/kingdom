import { createContext, useContext } from "react";
export type CentralSession = {
  session_id: string;
  csrf_token: string;
  user: { id: string; display_name: string; team_owner: boolean };
};
export const SessionContext = createContext<CentralSession | null>(null);
export function useCentralSession(): CentralSession {
  const session = useContext(SessionContext);
  if (!session) throw new Error("A signed-in Kingdom session is required.");
  return session;
}

