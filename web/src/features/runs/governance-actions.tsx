import { PublishResult } from "@/features/central/publish-result";
import { runtimeMode } from "@/lib/runtime-mode";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Modal, ModalFooter } from "@/components/ui/modal";
import { SelectField } from "@/components/ui/select-field";
import {
  useCreateFinalApproval,
  useResumeRun,
  useRerunFromNode,
  useRunResumePlan,
  type CreateFinalApprovalRequest,
  type ResumeRunRequest,
  type RunResumePlan,
  type RunSummary,
} from "@/api/hooks";
import {
  Gavel,
  Play,
  RotateCcw,
} from "lucide-react";
import { useId, useRef, useState, type Ref } from "react";
import { formatTimestamp, nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { runStatusBannerClass } from "./run-detail-tones";
import { ApprovalDecisionForm } from "./approval-decision-form";
import { describeManualGovernanceState } from "./manual-governance-state";
import type { RunRecoverySelection } from "./run-recovery-selection";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

interface GovernanceActionsProps {
  run: RunSummary;
  recovery?: RunRecoverySelection;
  showFinalApproval?: boolean;
  showNodeActions?: boolean;
  finalApprovalCardRef?: Ref<HTMLDivElement>;
}

export function GovernanceActions({ run, recovery = { nodes: [] }, showFinalApproval = true, showNodeActions = true, finalApprovalCardRef }: GovernanceActionsProps) {
  const governanceState = describeManualGovernanceState(run);
  const isAwaitingApproval = governanceState.isAwaitingFinalApproval;
  const isFailedRun = run.status === "failed";
  const isCancelledRun = run.status === "cancelled";
  const actionNodes = recovery.nodes;

  // Only show governance panel if there are actionable items
  const canPublish = run.status === "succeeded" && runtimeMode() === "hosted-project";
  const hasActions = canPublish ||
    (showFinalApproval && isAwaitingApproval) || (showNodeActions && (isFailedRun || isCancelledRun) && actionNodes.length > 0);

  if (!hasActions) return null;

  return (
    <div className="space-y-3">
      {canPublish && <PublishResult key={run.run_id} runID={run.run_id} />}
      {showFinalApproval && isAwaitingApproval && <FinalApprovalCard run={run} cardRef={finalApprovalCardRef} />}
      {showNodeActions && (isFailedRun || isCancelledRun) && actionNodes.length > 0 && (
        <RunRecoveryActions key={run.run_id} runId={run.run_id} recovery={recovery} />
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

type RecoveryAction = "resume" | "rerun";

function resumeRequest(plan: RunResumePlan | undefined): ResumeRunRequest | null {
  if (!plan?.available) return null;
  return {
    reason: "manual resume from run detail",
    steps: plan.steps.map((step) => ({ node_id: step.node_id, source_attempt_id: step.source_attempt_id ?? "" })),
  };
}

function RunRecoveryActions({ runId, recovery }: { runId: string; recovery: RunRecoverySelection }) {
  const { nodes, defaultNodeId } = recovery;
  const resumePlan = useRunResumePlan(runId);
  const resume = useResumeRun(runId);
  const rerun = useRerunFromNode(runId);
  // These controllers outlive the dialog: dismissing an uncertain response must
  // never discard its exact confirmed request or idempotency key.
  const recoverableResume = useRecoverableMutation(
    "resume", (body: ResumeRunRequest, key) => resume.mutateAsync({ body, key }), runId,
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
  const recoveryAction = recoverableResume.hasPendingRecovery ? "resume"
    : recoverableRerun.hasPendingRecovery ? "rerun" : null;
  const plan = resumePlan.data;
  const confirmed = recoverableResume.pendingRequest ?? resumeRequest(plan);
  const retainedId = recoverableRerun.pendingRequest;
  const targetId = retainedId ?? selectedId;
  const selected = nodes.find((node) => node.node_id === targetId);
  const resumeLabel = recoveryAction === "resume" ? "Retry Resume" : "Resume";
  const rerunLabel = recoveryAction === "rerun" ? "Retry Rerun" : "Rerun";
  const canConfirm = action === "resume" ? confirmed !== null : Boolean(selected || retainedId);

  const openAction = (next: RecoveryAction) => {
    if (submitting.current || (recoveryAction && recoveryAction !== next)) return;
    if (next === "resume") void resumePlan.refetch();
    setSelectedId(retainedId ?? (defaultNodeId ?? (nodes.length === 1 ? nodes[0].node_id : "")));
    if (!recoveryAction) setError(null);
    setAction(next);
  };
  const switchToRerun = () => {
    if (submitting.current || recoveryAction) return;
    setError(null);
    setAction("rerun");
  };
  const close = () => { if (!submitting.current) setAction(null); };
  const queueAction = async () => {
    if (!action || submitting.current || !canConfirm) return;
    submitting.current = true;
    setPending(true);
    setError(null);
    try {
      if (action === "resume") await recoverableResume.mutateAsync(confirmed!);
      else await recoverableRerun.mutateAsync(targetId);
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
        <Button size="sm" variant="outline" onClick={() => openAction("resume")}
          disabled={pending || recoveryAction === "rerun"}>
          <RotateCcw className="h-3.5 w-3.5" />{resumeLabel}
        </Button>
        <Button size="sm" variant="ghost" onClick={() => openAction("rerun")}
          disabled={pending || recoveryAction === "resume"}>
          <Play className="h-3.5 w-3.5" />{rerunLabel}
        </Button>
      </div>
      {error && !action && <div role="alert"><ErrorBanner message={error} /></div>}
      <Modal open={action !== null} onClose={close} size="sm"
        title={action === "resume" ? "Resume Interrupted Work" : "Rerun from Step"}
        description={action === "resume"
          ? "Continue each interrupted step from its saved conversation and partial files in a new attempt. Completed steps are kept."
          : "Repeat a step and its downstream workflow from the trusted baseline. Completion restarts at plan drafting."}
        footer={<ModalFooter>
          <Button variant="ghost" onClick={close} disabled={pending}>Cancel</Button>
          {action === "resume" && !confirmed && !recoveryAction && plan && (
            <Button variant="outline" onClick={switchToRerun} disabled={pending}>Rerun Instead</Button>
          )}
          <Button onClick={() => void queueAction()} disabled={pending || !canConfirm}>
            {pending ? "Queuing…" : recoveryAction === "resume" ? "Retry Resume"
              : recoveryAction === "rerun" ? "Retry Rerun"
              : action === "resume" ? "Resume" : "Rerun from Step"}
          </Button>
        </ModalFooter>}>
        {action === "resume" ? (
          <ResumePlanDetails plan={plan} loading={resumePlan.isPending} loadError={resumePlan.error instanceof Error ? resumePlan.error.message : null} retained={recoveryAction === "resume"} />
        ) : (
          <div className="space-y-3">
            <SelectField id={pickerId} label="Step" value={targetId}
              placeholder="Choose a workflow step…" onChange={setSelectedId}
              disabled={pending || recoveryAction !== null}
              options={[
                ...nodes.map((node) => ({ value: node.node_id, label: `${nodeLabel(node.node_id)} · ${statusLabel(node.state)}` })),
                ...(retainedId && !selected ? [{ value: retainedId, label: nodeLabel(retainedId) }] : []),
              ]} />
            {selected?.failure_reason && <p className="text-wrap-anywhere text-sm text-foreground-muted">{selected.failure_reason}</p>}
          </div>
        )}
        {(recoveryAction || error) && (
          <div className="mt-3 space-y-3">
            {recoveryAction && <p className="text-sm text-foreground-muted">The previous response was not confirmed. Retry the same request to recover its outcome.</p>}
            {error && <div role="alert"><ErrorBanner message={error} /></div>}
          </div>
        )}
      </Modal>
    </div>
  );
}

function ResumePlanDetails({ plan, loading, loadError, retained }: { plan?: RunResumePlan; loading: boolean; loadError: string | null; retained: boolean }) {
  if (loading && !plan) return <p className="text-sm text-foreground-muted">Checking saved checkpoints…</p>;
  if (!plan) return <ErrorBanner message={loadError ?? "Unable to load the resume plan."} />;
  return (
    <div className="space-y-3">
      <ul className="space-y-2" aria-label="Interrupted steps">
        {plan.steps.map((step) => (
          <li key={step.node_id} className="rounded-md border border-border px-3 py-2 text-sm">
            <div className="font-medium text-foreground">{nodeLabel(step.node_id)}</div>
            {step.unavailable_reason
              ? <p className="text-wrap-anywhere text-foreground-muted">{step.unavailable_reason}</p>
              : step.checkpoint_captured_at && <p className="text-foreground-muted">Checkpoint saved {formatTimestamp(step.checkpoint_captured_at)}</p>}
          </li>
        ))}
      </ul>
      {plan.unavailable_reason && <p className="text-wrap-anywhere text-sm text-foreground-muted">{plan.unavailable_reason}</p>}
      {!plan.available && !retained && (
        <p className="text-sm text-foreground-muted">Resume is unavailable. Rerun repeats a step from its trusted baseline instead.</p>
      )}
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
