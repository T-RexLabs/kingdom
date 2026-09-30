import { useState, type Ref } from "react";
import { Badge } from "@/components/ui/badge";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { RelativeTime } from "@/components/ui/relative-time";
import {
  useCreatePlanApproval,
  useRunLatestGovernanceDecision,
  useRunGovernanceDecisions,
  useRunPlan,
  type RunSummary,
  type CreatePlanApprovalRequest,
} from "@/api/hooks";
import { ChevronDown, ChevronRight, Clock3, FileText, Gavel } from "lucide-react";
import { ApprovalDecisionForm } from "./approval-decision-form";
import { GovernanceDecisionHistoryModal } from "./governance-decision-history-modal";
import { governanceDecisionLabel, governanceDecisionStageLabel } from "./governance-decision-labels";
import { describeManualGovernanceState, runPlanStateLabel } from "./manual-governance-state";
import { accentBannerClass, runStatusBannerClass } from "./run-detail-tones";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

export function CurrentPlanPanel({
  run,
  approvalSectionRef,
}: {
  run: RunSummary;
  approvalSectionRef?: Ref<HTMLDivElement>;
}) {
  const [isOpen, setIsOpen] = useState(true);
  const [historyOpen, setHistoryOpen] = useState(false);
  const planApproval = useCreatePlanApproval(run.run_id);
  const recoverableApproval = useRecoverableMutation(
    "plan-approval",
    (body: CreatePlanApprovalRequest, key) => planApproval.mutateAsync({ body, key }),
    run.run_id,
  );
  const { data: plan, isLoading, error } = useRunPlan(run.run_id);
  const latestGovernanceDecision = useRunLatestGovernanceDecision(run.run_id);
  const governanceHistory = useRunGovernanceDecisions(run.run_id, { enabled: historyOpen });

  const content = plan?.content ?? "";
  const blockingPlanError = !plan ? error : null;
  const planRefreshWarning = plan && error
    ? "Plan refresh failed. Showing the last successful snapshot while the latest refresh recovers."
    : null;
	const governanceState = describeManualGovernanceState(run);
	const isAwaitingPlanApproval = governanceState.isAwaitingPlanApproval;
	const planRevisionStatus = plan?.revision_status ?? "pending";
	const hasCurrentPlanRevision = planRevisionStatus === "ready" && (plan?.plan_revision_id?.trim() ?? "") !== "";
  const isPendingPlanRevision = isAwaitingPlanApproval && !blockingPlanError && (isLoading || planRevisionStatus === "pending");
  const isUnavailablePlanRevision = isAwaitingPlanApproval && !isPendingPlanRevision && (!!blockingPlanError || !hasCurrentPlanRevision);
  const latestDecision = latestGovernanceDecision.data?.items[0] ?? null;
  const historyItems = governanceHistory.data?.pages.flatMap((page) => page.items) ?? (latestDecision ? [latestDecision] : []);
  const latestGuidance = latestDecision && latestDecision.feedback_markdown.trim() !== "" ? latestDecision : null;

	const submitConfirm = async (notes: string) => {
		if (!hasCurrentPlanRevision || !plan?.plan_revision_id) {
			return;
		}
		const body = {
			action: "confirm" as const,
			plan_revision_id: plan.plan_revision_id,
			...(notes !== "" ? { reason: notes } : {}),
		};
    await recoverableApproval.mutateAsync(body);
  };

	const submitRework = async (notes: string) => {
		if (!hasCurrentPlanRevision || !plan?.plan_revision_id) {
			return;
		}
		await recoverableApproval.mutateAsync({
				action: "rework",
				plan_revision_id: plan.plan_revision_id,
				reason: notes,
			});
  };

  return (
    <section className="space-y-3 rounded-md border border-border bg-surface-raised/40 p-3">
      <div className="rounded-md border border-border bg-background p-3">
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          className="mb-0 flex w-full items-center gap-2 text-left"
        >
          <div className="flex min-w-0 items-center gap-2">
            {isOpen ? <ChevronDown className="h-3.5 w-3.5 text-foreground-subtle" /> : <ChevronRight className="h-3.5 w-3.5 text-foreground-subtle" />}
            <FileText className="h-4 w-4 text-gold" />
             <h2 className="text-sm font-semibold text-foreground">Current Plan</h2>
             <Badge variant="status" status={run.plan_state === "approved" ? "succeeded" : run.status === "failed" ? "failed" : run.status === "running" ? "running" : "pending"}>
              {runPlanStateLabel(run)}
             </Badge>
          </div>
        </button>

        {isOpen && (
          <>
            {planRefreshWarning ? (
              <div className={`mt-3 rounded-md border px-3 py-2 text-sm ${accentBannerClass("gold")}`}>
                {planRefreshWarning}
              </div>
            ) : null}
            <div className="mt-3">
              {isPendingPlanRevision ? (
                <PendingPlanRevisionState />
              ) : blockingPlanError ? (
                <p className="text-sm text-status-failed">Current plan revision is unavailable right now.</p>
              ) : isLoading || !content.trim() ? (
                <p className="text-sm text-foreground-subtle">Plan has not yet been drafted.</p>
              ) : (
                <div className="space-y-2">
                  <div className="max-h-[44vh] overflow-auto rounded-md border border-border bg-background p-3">
                    <MarkdownBlock content={content} className="text-sm text-foreground-muted text-wrap-anywhere" />
                  </div>
                </div>
              )}
            </div>
            {latestDecision && (
              <div className="mt-3 rounded-md border border-border bg-surface p-3">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-foreground-subtle">Allfather Guidance</p>
                  <button
                    type="button"
                    onClick={() => setHistoryOpen(true)}
                    className="text-xs font-medium text-gold transition-colors hover:text-gold-bright"
                  >
                    View History
                  </button>
                </div>
                {latestGuidance ? (
                  <button
                    type="button"
                    onClick={() => setHistoryOpen(true)}
                    className="mt-2 block w-full rounded-md border border-border bg-background p-3 text-left transition-colors hover:bg-surface-raised/40"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
	                      <div className="flex flex-wrap items-center gap-2">
	                        <Badge>{governanceDecisionStageLabel(latestGuidance)}</Badge>
	                        <p className="text-sm font-semibold text-foreground">{governanceDecisionLabel(latestGuidance)}</p>
	                      </div>
	                      <span className="text-xs text-foreground-subtle"><RelativeTime iso={latestGuidance.created_at} /></span>
	                    </div>
                    <div className="mt-3">
                      <MarkdownBlock content={latestGuidance.feedback_markdown} className="text-sm text-foreground-muted text-wrap-anywhere" />
                    </div>
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={() => setHistoryOpen(true)}
                    className="mt-2 block w-full rounded-md border border-border bg-background px-3 py-2 text-left text-sm text-foreground-muted transition-colors hover:bg-surface-raised/40"
                  >
                    View the complete Allfather decision history for this run.
                  </button>
                )}
              </div>
            )}
          </>
        )}
      </div>

      {isAwaitingPlanApproval && (
        <div
          ref={approvalSectionRef}
          data-testid="run-plan-approval-card"
          tabIndex={-1}
          className="rounded-md border border-gold/30 bg-background p-3"
        >
          <p className="mb-2 flex items-center gap-2 text-sm font-semibold text-gold">
            <Gavel className="h-4 w-4" />
            Awaiting Your Plan Approval
          </p>

          {isPendingPlanRevision ? (
            <PendingPlanRevisionNotice />
          ) : isUnavailablePlanRevision ? (
            <UnavailablePlanRevisionNotice />
          ) : (
            <ApprovalDecisionForm
              description={governanceState.noticeMessage ?? "Execution is paused until you confirm this plan or request rework."}
              notesLabel="Notes"
              notesHint="Optional when confirming. Required when requesting rework."
              notesPlaceholder="Add optional approval notes or explain the rework you want."
              confirmLabel="Confirm Plan"
              confirmPendingLabel="Confirming..."
              reworkLabel="Request Rework"
              reworkPendingLabel="Requesting Rework..."
              reworkValidationMessage="Add rework guidance before requesting rework."
              errorMessage={planApproval.error?.message ?? null}
              recoveryAction={recoverableApproval.pendingRequest?.action ?? null}
              onConfirm={submitConfirm}
              onRequestRework={submitRework}
            />
          )}
        </div>
      )}

		<GovernanceDecisionHistoryModal
			open={historyOpen}
			onClose={() => setHistoryOpen(false)}
			items={historyItems}
			hasMore={historyOpen ? governanceHistory.hasNextPage : false}
			isLoadingMore={governanceHistory.isFetchingNextPage}
			onLoadMore={() => {
				void governanceHistory.fetchNextPage();
			}}
		/>
    </section>
  );
}

function PendingPlanRevisionState() {
  return (
    <div className="rounded-md border border-border bg-background px-3 py-4 text-sm text-foreground-subtle">
      Syncing the current council plan revision for review.
    </div>
  );
}

function PendingPlanRevisionNotice() {
  return (
    <div className={`rounded-md border px-3 py-2 text-sm ${accentBannerClass("gold")}`}>
      <div className="flex items-start gap-2">
        <Clock3 className="mt-0.5 h-4 w-4 shrink-0" />
        <p>
          The council plan revision is still syncing into the approval view. Approval actions will unlock automatically once the current revision is ready.
        </p>
      </div>
    </div>
  );
}

function UnavailablePlanRevisionNotice() {
  return (
    <div className={`rounded-md border px-3 py-2 text-sm ${runStatusBannerClass("failed")}`}>
      The current council plan revision could not be loaded. Wait for the snapshot to recover before deciding.
    </div>
  );
}
