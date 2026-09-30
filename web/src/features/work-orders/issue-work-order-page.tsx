import { useNavigate, useSearch } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { WorkOrderForm, type WorkOrderDispatchResult } from "./work-order-form";

export function IssueWorkOrderPage() {
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const [dispatchResult, setDispatchResult] =
    useState<WorkOrderDispatchResult | null>(null);
  const openedRun = useRef<string | null>(null);

  useEffect(() => {
    const runId = dispatchResult?.run.run_id;
    if (!runId || openedRun.current === runId) return;
    // The form commits its successful draft reset before leaving this page.
    // A repeated effect must not add another navigation for the same result.
    openedRun.current = runId;
    void navigate({ to: "/runs/$runId", params: { runId }, replace: true });
  }, [dispatchResult, navigate]);

  return (
    <div className="mx-auto w-full max-w-3xl">
      <WorkOrderForm
        initialPlanningLineage={search}
        onDispatched={setDispatchResult}
      />
    </div>
  );
}
