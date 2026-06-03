import { useRuns } from "@/api/hooks";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatTimestamp } from "@/lib/utils";
import { Link } from "@tanstack/react-router";
import { ArrowRight, Crown, Scroll, Shield } from "lucide-react";
import { describeManualGovernanceState } from "@/features/runs/manual-governance-state";
import { runDisplayBadgeStatus, runDisplayStateLabel } from "@/features/runs/run-display-state";

export function RecentDecreeRunsCard() {
  const { data: runs, isLoading: runsLoading } = useRuns({ limit: 10 });
  const recentRuns = runs?.items ?? [];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm">
          <Crown className="h-4 w-4" />
          Recent Decree Runs
        </CardTitle>
      </CardHeader>
      <CardContent>
        {runsLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : recentRuns.length === 0 ? (
          <div className="py-8 text-center">
            <Scroll className="mx-auto mb-2 h-6 w-6 text-foreground-subtle/40" />
            <p className="text-xs text-foreground-subtle">No runs yet. Issue a decree to begin.</p>
          </div>
        ) : (
            <div className="divide-y divide-border">
              {recentRuns.map((run) => {
                const governanceState = describeManualGovernanceState(run);
                const isActionRequired = governanceState.isAwaitingPlanApproval || governanceState.isAwaitingFinalApproval;
                return (
                  <Link
                    key={run.run_id}
                    to="/runs/$runId"
                    params={{ runId: run.run_id }}
                    className="-mx-2 flex items-center justify-between rounded px-2 py-2 transition-colors hover:bg-surface-raised"
                  >
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-xs font-medium text-foreground">{run.decree_title || run.run_id}</p>
                      <p className="text-[10px] text-foreground-subtle">
                        {run.workflow_id} · {formatTimestamp(run.created_at)}
                      </p>
                    </div>
                    <div className="ml-2 flex items-center gap-1.5">
                      {isActionRequired ? (
                        <span className="inline-flex items-center gap-1 rounded-md bg-gold/10 px-1.5 py-0.5 text-[10px] font-medium text-gold">
                          <Shield className="h-2.5 w-2.5" />
                          Action Required
                        </span>
                      ) : null}
                      <Badge variant="status" status={runDisplayBadgeStatus(run)} className="text-[10px]">
                        {runDisplayStateLabel(run)}
                      </Badge>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}

        {recentRuns.length > 0 && (
          <Link to="/runs" className="mt-3 block">
            <Button variant="ghost" size="sm" className="w-full text-xs">
              View All Runs
              <ArrowRight className="ml-1 h-3 w-3" />
            </Button>
          </Link>
        )}
      </CardContent>
    </Card>
  );
}
