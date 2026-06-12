import { newIdempotencyKey, useGeneratePlanningSessionTitle, type PlanningSessionDetail } from "@/api/hooks";
import { useEffect, useRef } from "react";

export function usePlanningSessionAutoTitleCatchup(sessionId: string, session: PlanningSessionDetail | undefined): void {
  const generateTitle = useGeneratePlanningSessionTitle(sessionId);
  const attemptedSessionIdRef = useRef<string | null>(null);

  useEffect(() => {
    attemptedSessionIdRef.current = null;
  }, [sessionId]);

  useEffect(() => {
    if (!session) {
      return;
    }
    if (!session.auto_title_eligible) {
      return;
    }
    if (session.title_state !== "placeholder") {
      return;
    }
    if (session.current_title_run_id) {
      return;
    }
    if (attemptedSessionIdRef.current === sessionId || generateTitle.isPending) {
      return;
    }

    attemptedSessionIdRef.current = sessionId;
    void generateTitle.mutateAsync({ key: newIdempotencyKey("generate-title") }).catch(() => {
      // One catch-up attempt per mounted session keeps the UI responsive
      // without retry-looping on persistent provider/setup failures.
    });
  }, [generateTitle, session, sessionId]);
}
