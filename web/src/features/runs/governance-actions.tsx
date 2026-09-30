import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Modal, ModalFooter } from "@/components/ui/modal";
import { SelectField } from "@/components/ui/select-field";
import {
  useCreateFinalApproval,
  useRetryNode,
  useRerunFromNode,
  type CreateFinalApprovalRequest,
  type RunSummary,
  type RunGraphNode,
} from "@/api/hooks";
import {
  Gavel,
  Play,
  RefreshCw,
} from "lucide-react";
import { useId, useRef, useState, type Ref } from "react";
import { nodeLabel } from "@/lib/utils";
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
        <RunRecoveryActions key={run.run_id} runId={run.run_id} nodes={actionNodes} />
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
      <Card className="border-accent/30">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Gavel className="h-4 w-4 text-accent" />
            <span className="text-accent">Awaiting Your Final Approval</span>
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

/* ── Run Recovery ── */

type RecoveryAction = "retry" | "rerun";

function RunRecoveryActions({ runId, nodes }: { runId: string; nodes: RunGraphNode[] }) {
  const retry = useRetryNode(runId);
  const rerun = useRerunFromNode(runId);
  // These controllers outlive the dialog: dismissing an uncertain response must
  // never discard its exact target or idempotency key.
  const recoverableRetry = useRecoverableMutation(
    "retry", (nodeKey: string, key) => retry.mutateAsync({ value: nodeKey, key }), runId,
  );
  const recoverableRerun = useRecoverableMutation(
    "rerun", (nodeKey: string, key) => rerun.mutateAsync({ value: nodeKey, key }), runId,
  );
  const pickerId = useId();
  const submitting = useRef(false);
  const [pending, setPending] = useState(false);
  const [action, setAction] = useState<RecoveryAction | null>(null);
  const [selectedId, setSelectedId] = useState("");
  const [error, setError] = useState<string | null>(null);
  const recoveryAction = recoverableRetry.hasPendingRecovery ? "retry"
    : recoverableRerun.hasPendingRecovery ? "rerun" : null;
  const retainedId = recoverableRetry.pendingRequest ?? recoverableRerun.pendingRequest;
  const targetId = retainedId ?? selectedId;
  const selected = nodes.find((node) => node.node_id === targetId);
  const retryLabel = recoveryAction === "retry" ? "Retry Again" : "Retry";
  const rerunLabel = recoveryAction === "rerun" ? "Retry Rerun" : "Rerun";

  const openAction = (next: RecoveryAction) => {
    if (submitting.current || (recoveryAction && recoveryAction !== next)) return;
    setSelectedId(retainedId ?? (nodes.length === 1 ? nodes[0].node_id : ""));
    if (!recoveryAction) setError(null);
    setAction(next);
  };
  const close = () => { if (!submitting.current) setAction(null); };
  const queueAction = async () => {
    if (!action || submitting.current || (!selected && !retainedId)) return;
    submitting.current = true;
    setPending(true);
    setError(null);
    try {
      await (action === "retry" ? recoverableRetry : recoverableRerun).mutateAsync(targetId);
      setAction(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to queue recovery. Try again.");
    } finally {
      submitting.current = false;
      setPending(false);
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <Button size="sm" variant="outline" onClick={() => openAction("retry")}
          disabled={pending || recoveryAction === "rerun"}>
          <RefreshCw className="h-3.5 w-3.5" />{retryLabel}
        </Button>
        <Button size="sm" variant="ghost" onClick={() => openAction("rerun")}
          disabled={pending || recoveryAction === "retry"}>
          <Play className="h-3.5 w-3.5" />{rerunLabel}
        </Button>
      </div>
      {error && !action && <div role="alert"><ErrorBanner message={error} /></div>}
      <Modal open={action !== null} onClose={close} size="sm"
        title={action === "retry" ? "Retry Step" : "Rerun from Step"}
        description={action === "retry"
          ? "Repeat one workflow step in this run."
          : "Repeat a step and its downstream workflow in this run. Completion restarts at plan drafting."}
        footer={<ModalFooter>
          <Button variant="ghost" onClick={close} disabled={pending}>Cancel</Button>
          <Button onClick={() => void queueAction()} disabled={pending || (!selected && !retainedId)}>
            {pending ? "Queuing…" : recoveryAction === "retry" ? "Retry Again"
              : recoveryAction === "rerun" ? "Retry Rerun"
              : action === "retry" ? "Retry Step" : "Rerun from Step"}
          </Button>
        </ModalFooter>}>
        <div className="space-y-3">
          <SelectField id={pickerId} label="Step" value={targetId}
            placeholder="Choose a workflow step…" onChange={setSelectedId}
            disabled={pending || recoveryAction !== null}
            options={[
              ...nodes.map((node) => ({ value: node.node_id, label: `${nodeLabel(node.node_id)} · ${statusLabel(node.state)}` })),
              ...(retainedId && !selected ? [{ value: retainedId, label: nodeLabel(retainedId) }] : []),
            ]} />
          {selected?.failure_reason && <p className="text-wrap-anywhere text-sm text-foreground-muted">{selected.failure_reason}</p>}
          {recoveryAction && <p className="text-sm text-foreground-muted">The previous response was not confirmed. Retry the same request to recover its outcome.</p>}
          {error && <div role="alert"><ErrorBanner message={error} /></div>}
        </div>
      </Modal>
    </div>
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
