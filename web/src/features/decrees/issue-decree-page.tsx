import { useSearch } from "@tanstack/react-router";
import { Link } from "@tanstack/react-router";
import { ArrowRight, CheckCircle2 } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { runDisplayBadgeStatus, runDisplayStateLabel } from "@/features/runs/run-display-state";
import { DecreeForm, type DecreeDispatchResult } from "./decree-form";
import { RecentDecreeRunsCard } from "./recent-decree-runs-card";

export function IssueDecreePage() {
  const search = useSearch({ strict: false });
  const [dispatchResult, setDispatchResult] = useState<DecreeDispatchResult | null>(null);
  const confirmationCardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!dispatchResult || !confirmationCardRef.current || typeof confirmationCardRef.current.scrollIntoView !== "function") {
      return;
    }

    const confirmationCard = confirmationCardRef.current;
    const prefersReducedMotion = typeof window !== "undefined"
      && typeof window.matchMedia === "function"
      && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    confirmationCard.scrollIntoView({
      behavior: prefersReducedMotion ? "auto" : "smooth",
      block: "start",
      inline: "nearest",
    });
  }, [dispatchResult]);

  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,2.0fr)_minmax(300px,1fr)]">
      <div className="min-w-0">
        <DecreeForm
          initialPlanningLineage={search}
          onDispatched={setDispatchResult}
        />
      </div>
      <div className="space-y-4">
        {dispatchResult ? (
          <div
            ref={confirmationCardRef}
            role="region"
            aria-labelledby="decree-dispatched-heading"
            aria-live="polite"
            aria-atomic="true"
          >
            <Card>
              <CardHeader>
                <CardTitle className="text-status-succeeded">
                  <span
                    id="decree-dispatched-heading"
                    className="flex items-center gap-2"
                  >
                    <CheckCircle2 className="h-4 w-4" />
                    Decree Dispatched
                  </span>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="space-y-2">
                  <div className="rounded-md border border-border bg-surface-raised px-3 py-2">
                    <p className="text-xs text-foreground-muted">Decree</p>
                    <p className="font-mono text-xs text-wrap-anywhere">{dispatchResult.decreeId}</p>
                  </div>
                  <div className="rounded-md border border-border bg-surface-raised px-3 py-2">
                    <p className="text-xs text-foreground-muted">Run</p>
                    <p className="font-mono text-xs text-wrap-anywhere">{dispatchResult.run.run_id}</p>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <span className="text-xs text-foreground-muted">Status:</span>
                  <Badge variant="status" status={runDisplayBadgeStatus(dispatchResult.run)}>
                    {runDisplayStateLabel(dispatchResult.run)}
                  </Badge>
                </div>

                <Link to="/runs/$runId" params={{ runId: dispatchResult.run.run_id }}>
                  <Button size="sm" className="w-full">
                    <ArrowRight className="h-3.5 w-3.5" />
                    View Run
                  </Button>
                </Link>
              </CardContent>
            </Card>
          </div>
        ) : null}

        <RecentDecreeRunsCard />
      </div>
    </div>
  );
}
