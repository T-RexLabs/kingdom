import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  useCreateFinalApproval,
  useRetryNode,
  useRerunFromNode,
  type CreateFinalApprovalRequest,
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
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

interface GovernanceActionsProps {
  run: RunSummary;
  failedNodes?: RunGraphNode[];
  cancelledNodes?: RunGraphNode[];
  showFinalApproval?: boolean;
  showNodeActions?: boolean;
  finalApprovalCardRef?: Ref<HTMLDivElement>;
}

export function GovernanceActions({ run, failedNodes = [], cancelledNodes = [], showFinalApproval = true, showNodeActions = true, finalApprovalCardRef }: GovernanceActionsProps) {
  const governanceState = describeManualGovernanceState(run);
  const isAwaitingApproval = governanceState.isAwaitingFinalApproval;
  const isFailedRun = run.status === "failed";
  const isCancelledRun = run.status === "cancelled";
  const actionNodes = isCancelledRun ? cancelledNodes : failedNodes;

  // Only show governance panel if there are actionable items
  const hasActions =
    (showFinalApproval && isAwaitingApproval) || (showNodeActions && (isFailedRun || isCancelledRun) && actionNodes.length > 0);

  if (!hasActions) return null;

  return (
    <div className="space-y-3">
      {showFinalApproval && isAwaitingApproval && <FinalApprovalCard run={run} cardRef={finalApprovalCardRef} />}
      {showNodeActions && (isFailedRun || isCancelledRun) && actionNodes.length > 0 && (
        <NodeActionsCard runId={run.run_id} nodes={actionNodes} cancelled={isCancelledRun} />
      )}
    </div>
  );
}

/* ── Final Approval Card ── */

export function FinalApprovalCard({ run, cardRef }: { run: RunSummary; cardRef?: Ref<HTMLDivElement> }) {
  const approval = useCreateFinalApproval(run.run_id);
  const recoverableApproval = useRecoverableMutation(
    "approval",
    (body: CreateFinalApprovalRequest, key) => approval.mutateAsync({ body, key }),
    run.run_id,
  );
  const governanceState = describeManualGovernanceState(run);

  const handleConfirm = async (notes: string) => {
    const body = {
      action: "confirm" as const,
      ...(notes !== "" ? { reason: notes } : {}),
    };
    await recoverableApproval.mutateAsync(body);
  };

  const handleRequestRework = async (notes: string) => {
    await recoverableApproval.mutateAsync({ action: "rework", reason: notes });
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
            recoveryAction={recoverableApproval.pendingRequest?.action ?? null}
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
  nodes,
  cancelled,
}: {
  runId: string;
  nodes: RunGraphNode[];
  cancelled: boolean;
}) {
  const retry = useRetryNode(runId);
  const rerun = useRerunFromNode(runId);
  const recoverableRetry = useRecoverableMutation(
    "retry",
    (nodeKey: string, key) => retry.mutateAsync({ value: nodeKey, key }),
    runId,
  );
  const recoverableRerun = useRecoverableMutation(
    "rerun",
    (nodeKey: string, key) => rerun.mutateAsync({ value: nodeKey, key }),
    runId,
  );
  const [pendingAction, setPendingAction] = useState<string | null>(null);

  // Queuing succeeds by removing the node from this list, which unmounts the
  // card — so a success banner here could only ever flash and vanish. The run
  // timeline records "Retry queued" and "Rerun queued" durably, which is where
  // an operator can actually go back and read what happened.
  const queueAction = async (action: "retry" | "rerun", nodeKey: string) => {
    setPendingAction(`${action}-${nodeKey}`);
    try {
      await (action === "retry" ? recoverableRetry : recoverableRerun).mutateAsync(nodeKey);
    } catch {
      // Failure is the case worth showing, and it is rendered from
      // retry.error / rerun.error below.
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <Card className="border-status-failed/20">
      <CardHeader className="pb-3">
        {/* Named for what these nodes have in common — the run stopped and they
            are where it can be picked up again — because the list can hold a
            reviewer that asked for rework alongside steps that genuinely broke.
            Each row's badge names its own state. */}
        <CardTitle className="flex items-center gap-2 text-sm">
          <AlertTriangle className="h-4 w-4 text-status-failed" />
          <span className="text-status-failed">
            {cancelled ? "Cancelled Run Nodes" : "Nodes to Recover"} ({nodes.length})
          </span>
        </CardTitle>
        <p className="text-xs text-foreground-subtle">
          Retry reruns only the selected node. Rerun replays from the selected node and downstream (royal closure rerun promotes to plan drafting).
        </p>
      </CardHeader>
      <CardContent className="space-y-2">
        {(retry.error || rerun.error) && (
          <ErrorBanner
            message={
              (retry.error ?? rerun.error)?.message ?? "Action failed"
            }
          />
        )}
        {nodes.map((node) => {
          const { executionModeLabel, roleLabel } = nodeExecutionSemantics(node.execution_ownership, node.required_role);
          const reason = node.failure_reason ?? (cancelled ? "" : "Failure reason unavailable.");
          const recoveryPending = recoverableRetry.hasPendingRecovery || recoverableRerun.hasPendingRecovery;
          const canRetryRecovery = recoverableRetry.pendingRequest === node.node_id;
          const canRerunRecovery = recoverableRerun.pendingRequest === node.node_id;

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
	                {reason ? <p className="mt-1 text-wrap-anywhere text-xs text-foreground-subtle">{reason}</p> : null}
              </div>
              <div className="flex items-center gap-1.5">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => void queueAction("retry", node.node_id)}
                  disabled={pendingAction !== null || (recoveryPending && !canRetryRecovery)}
                  title="Retry this node"
                >
                  <RefreshCw
                    className={`h-3 w-3 ${
                      pendingAction === `retry-${node.node_id}` ? "animate-spin" : ""
                    }`}
                  />
                  {canRetryRecovery ? "Retry Again" : "Retry"}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => void queueAction("rerun", node.node_id)}
                  disabled={pendingAction !== null || (recoveryPending && !canRerunRecovery)}
                  title="Rerun from this node"
                >
                  <Play className="h-3 w-3" />
                  {canRerunRecovery ? "Retry Rerun" : "Rerun"}
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
