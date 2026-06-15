import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  newIdempotencyKey,
  useCreateFinalApproval,
  useRetryNode,
  useRerunFromNode,
  type RunSummary,
  type RunGraphNode,
} from "@/api/hooks";
import {
  AlertTriangle,
  Gavel,
  Play,
  RefreshCw,
} from "lucide-react";
import { useState, type Ref } from "react";
import { nodeExecutionSemantics } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { runStatusBannerClass } from "./run-detail-tones";
import { ApprovalDecisionForm } from "./approval-decision-form";
import { describeManualGovernanceState } from "./manual-governance-state";

interface GovernanceActionsProps {
  run: RunSummary;
  failedNodes?: RunGraphNode[];
  showFinalApproval?: boolean;
  showNodeActions?: boolean;
  finalApprovalCardRef?: Ref<HTMLDivElement>;
}

export function GovernanceActions({ run, failedNodes = [], showFinalApproval = true, showNodeActions = true, finalApprovalCardRef }: GovernanceActionsProps) {
  const governanceState = describeManualGovernanceState(run);
  const isAwaitingApproval = governanceState.isAwaitingFinalApproval;
  const isFailedRun = run.status === "failed";

  // Only show governance panel if there are actionable items
  const hasActions =
    (showFinalApproval && isAwaitingApproval) || (showNodeActions && isFailedRun && failedNodes.length > 0);

  if (!hasActions) return null;

  return (
    <div className="space-y-3">
      {showFinalApproval && isAwaitingApproval && <FinalApprovalCard run={run} cardRef={finalApprovalCardRef} />}
      {showNodeActions && isFailedRun && failedNodes.length > 0 && (
        <NodeActionsCard runId={run.run_id} failedNodes={failedNodes} />
      )}
    </div>
  );
}

/* ── Final Approval Card ── */

export function FinalApprovalCard({ run, cardRef }: { run: RunSummary; cardRef?: Ref<HTMLDivElement> }) {
  const approval = useCreateFinalApproval(run.run_id);
  const governanceState = describeManualGovernanceState(run);

  const handleConfirm = async (notes: string) => {
    const body = {
      action: "confirm" as const,
      ...(notes !== "" ? { reason: notes } : {}),
    };
    await approval.mutateAsync({
      body,
      key: newIdempotencyKey("approval"),
    });
  };

  const handleRequestRework = async (notes: string) => {
    await approval.mutateAsync({
      body: { action: "rework", reason: notes },
      key: newIdempotencyKey("approval"),
    });
  };

  return (
    <div ref={cardRef} data-testid="run-final-approval-card" tabIndex={-1}>
      <Card className="border-gold/30">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Gavel className="h-4 w-4 text-gold" />
            <span className="text-gold">Awaiting Your Final Approval</span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ApprovalDecisionForm
            description={governanceState.noticeMessage ?? "This run is paused until you confirm completion or request rework."}
            notesLabel="Notes"
            notesHint="Optional when confirming. Required when requesting rework."
            notesPlaceholder="Add optional approval notes or explain what should be reworked."
            confirmLabel="Confirm Completion"
            confirmPendingLabel="Confirming..."
            reworkLabel="Request Rework"
            reworkPendingLabel="Requesting Rework..."
            reworkValidationMessage="Add rework guidance before requesting rework."
            errorMessage={approval.error?.message ?? null}
            onConfirm={handleConfirm}
            onRequestRework={handleRequestRework}
          />
        </CardContent>
      </Card>
    </div>
  );
}

/* ── Node Actions Card (retry / rerun for failed nodes) ── */

export function NodeActionsCard({
  runId,
  failedNodes,
}: {
  runId: string;
  failedNodes: RunGraphNode[];
}) {
  const retry = useRetryNode(runId);
  const rerun = useRerunFromNode(runId);
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [actionFeedback, setActionFeedback] = useState<string>("");

  const handleRetry = async (nodeKey: string) => {
    setPendingAction(`retry-${nodeKey}`);
    setActionFeedback("");
    try {
      const result = await retry.mutateAsync({
        value: nodeKey,
        key: newIdempotencyKey("retry"),
      });
      setActionFeedback(`Retry queued for ${result.node_key}. This reruns only the selected node.`);
    } catch {
      // error available via retry.error
    } finally {
      setPendingAction(null);
    }
  };

  const handleRerun = async (nodeKey: string) => {
    setPendingAction(`rerun-${nodeKey}`);
    setActionFeedback("");
    try {
      const result = await rerun.mutateAsync({
        value: nodeKey,
        key: newIdempotencyKey("rerun"),
      });
      if (result.from_node !== nodeKey) {
        setActionFeedback(`Rerun queued from ${result.from_node}. The original target was promoted to rebuild plan and downstream execution.`);
      } else {
        setActionFeedback(`Rerun queued from ${result.from_node}. This reruns from that node and downstream.`);
      }
    } catch {
      // error available via rerun.error
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <Card className="border-status-failed/20">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-sm">
          <AlertTriangle className="h-4 w-4 text-status-failed" />
          <span className="text-status-failed">
            Failed Nodes ({failedNodes.length})
          </span>
        </CardTitle>
        <p className="text-xs text-foreground-subtle">
          Retry reruns only the selected node. Rerun replays from the selected node and downstream (royal closure rerun promotes to plan drafting).
        </p>
      </CardHeader>
      <CardContent className="space-y-2">
        {actionFeedback && (
          <div className="rounded-md border border-status-succeeded/30 bg-status-succeeded/10 px-3 py-2 text-sm text-status-succeeded">
            {actionFeedback}
          </div>
        )}
        {(retry.error || rerun.error) && (
          <ErrorBanner
            message={
              (retry.error ?? rerun.error)?.message ?? "Action failed"
            }
          />
        )}
        {failedNodes.map((node) => {
          const { executionModeLabel, roleLabel } = nodeExecutionSemantics(node.execution_ownership, node.required_role);

          return (
            <div
              key={node.node_id}
              className="flex items-center justify-between rounded-md border border-border bg-surface-raised px-3 py-2"
            >
              <div className="min-w-0 flex-1">
	                <p className="text-wrap-anywhere text-sm font-medium">{node.node_id}</p>
                <div className="flex items-center gap-2 text-xs text-foreground-subtle">
                  <Badge variant="status" status={node.state} className="text-[10px]">
                    {statusLabel(node.state)}
                  </Badge>
                  <span className="text-foreground-subtle">{executionModeLabel}</span>
                  <span className="text-foreground-subtle">{roleLabel}</span>
                </div>
	                <p className="mt-1 text-wrap-anywhere text-xs text-foreground-subtle">
                  {node.failure_reason ?? "Failure reason unavailable."}
                </p>
              </div>
              <div className="flex items-center gap-1.5">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => handleRetry(node.node_id)}
                  disabled={pendingAction !== null}
                  title="Retry this node"
                >
                  <RefreshCw
                    className={`h-3 w-3 ${
                      pendingAction === `retry-${node.node_id}` ? "animate-spin" : ""
                    }`}
                  />
                  Retry
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => handleRerun(node.node_id)}
                  disabled={pendingAction !== null}
                  title="Rerun from this node"
                >
                  <Play className="h-3 w-3" />
                  Rerun
                </Button>
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}

/* ── Shared ── */

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className={`rounded-md border px-3 py-2 text-sm ${runStatusBannerClass("failed")}`}>
      {message}
    </div>
  );
}
