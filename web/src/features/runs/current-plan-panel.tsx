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
import { Clock3 } from "lucide-react";
import { ApprovalDecisionForm } from "./approval-decision-form";
import { GovernanceDecisionHistoryModal } from "./governance-decision-history-modal";
import {
  governanceDecisionLabel,
  governanceDecisionStageLabel,
} from "./governance-decision-labels";
import {
  describeManualGovernanceState,
  runPlanStateLabel,
} from "./manual-governance-state";
import { accentBannerClass, runStatusBannerClass } from "./run-detail-tones";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

export function CurrentPlanPanel({
  run,
  approvalSectionRef,
}: {
  run: RunSummary;
  approvalSectionRef?: Ref<HTMLDivElement>;
}) {
  const [historyOpen, setHistoryOpen] = useState(false);
  const planApproval = useCreatePlanApproval(run.run_id);
  const recoverableApproval = useRecoverableMutation(
    "plan-approval",
    (body: CreatePlanApprovalRequest, key) =>
      planApproval.mutateAsync({ body, key }),
    run.run_id,
  );
  const { data: plan, isLoading, error } = useRunPlan(run.run_id);
  const latestGovernanceDecision = useRunLatestGovernanceDecision(run.run_id);
  const governanceHistory = useRunGovernanceDecisions(run.run_id, {
    enabled: historyOpen,
  });

  const content = plan?.content ?? "";
  const blockingPlanError = !plan ? error : null;
  const planRefreshWarning =
    plan && error
      ? "Plan refresh failed. Showing the last successful snapshot while the latest refresh recovers."
      : null;
  const governanceState = describeManualGovernanceState(run);
  const isAwaitingPlanApproval = governanceState.isAwaitingPlanApproval;
  const planRevisionStatus = plan?.revision_status ?? "pending";
  const hasCurrentPlanRevision =
    planRevisionStatus === "ready" &&
    (plan?.plan_revision_id?.trim() ?? "") !== "";
  const isPendingPlanRevision =
    isAwaitingPlanApproval &&
    !blockingPlanError &&
    (isLoading || planRevisionStatus === "pending");
  const isUnavailablePlanRevision =
    isAwaitingPlanApproval &&
    !isPendingPlanRevision &&
    (!!blockingPlanError || !hasCurrentPlanRevision);
  const latestDecision = latestGovernanceDecision.data?.items[0] ?? null;
  const historyItems =
    governanceHistory.data?.pages.flatMap((page) => page.items) ??
    (latestDecision ? [latestDecision] : []);
  const latestGuidance =
    latestDecision && latestDecision.feedback_markdown.trim() !== ""
      ? latestDecision
      : null;

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
    <section className="mx-auto max-w-3xl space-y-8">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-lg font-semibold text-foreground">Current Plan</h2>
        <Badge
          variant="status"
          status={
            run.plan_state === "approved"
              ? "succeeded"
              : run.status === "failed"
                ? "failed"
                : run.status === "running"
                  ? "running"
                  : "pending"
          }
        >
          {runPlanStateLabel(run)}
        </Badge>
      </header>
      {planRefreshWarning && (
        <p role="status" className="text-sm text-status-blocked">
          {planRefreshWarning}
        </p>
      )}
      <section aria-label="Current plan document">
        {isPendingPlanRevision ? (
          <PendingPlanRevisionState />
        ) : blockingPlanError ? (
          <p className="text-sm text-status-failed">
            Current plan revision is unavailable right now.
          </p>
        ) : isLoading || !content.trim() ? (
          <p className="text-sm text-foreground-subtle">
            Plan has not yet been drafted.
          </p>
        ) : (
          <MarkdownBlock
            content={content}
            className="text-[15px] leading-7 text-foreground"
          />
        )}
      </section>
      {latestDecision && (
        <section
          className="space-y-3 border-t border-border/60 pt-5"
          aria-label="Operator guidance"
        >
          <div className="flex items-center justify-between gap-3">
            <h3 className="text-sm font-medium text-foreground-muted">
              Operator Guidance
            </h3>
            <button
              type="button"
              onClick={() => setHistoryOpen(true)}
              className="min-h-9 text-sm text-accent hover:text-accent-bright"
            >
              View History
            </button>
          </div>
          {latestGuidance ? (
            <>
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-foreground-muted">
                <span>
                  {governanceDecisionStageLabel(latestGuidance)} ·{" "}
                  {governanceDecisionLabel(latestGuidance)}
                </span>
                <RelativeTime iso={latestGuidance.created_at} />
              </div>
              <MarkdownBlock
                content={latestGuidance.feedback_markdown}
                className="text-sm leading-6 text-foreground-muted"
              />
            </>
          ) : (
            <p className="text-sm text-foreground-muted">
              No written guidance accompanied the latest decision.
            </p>
          )}
        </section>
      )}

      {isAwaitingPlanApproval && (
        <div
          ref={approvalSectionRef}
          data-testid="run-plan-approval-card"
          tabIndex={-1}
          className="border-t border-border/60 pt-6 outline-none"
        >
          <p className="mb-2 flex items-center gap-2 text-sm font-semibold text-accent">
            Awaiting Your Plan Approval
          </p>

          {isPendingPlanRevision ? (
            <PendingPlanRevisionNotice />
          ) : isUnavailablePlanRevision ? (
            <UnavailablePlanRevisionNotice />
          ) : (
            <ApprovalDecisionForm
              description={
                governanceState.noticeMessage ??
                "Execution is paused until you confirm this plan or request rework."
              }
              notesLabel="Notes"
              notesHint="Optional when confirming. Required when requesting rework."
              notesPlaceholder="Add optional approval notes or explain the rework you want."
              confirmLabel="Confirm Plan"
              confirmPendingLabel="Confirming..."
              reworkLabel="Request Rework"
              reworkPendingLabel="Requesting Rework..."
              reworkValidationMessage="Add rework guidance before requesting rework."
              errorMessage={planApproval.error?.message ?? null}
              recoveryAction={
                recoverableApproval.pendingRequest?.action ?? null
              }
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
    <div className="py-2 text-sm text-foreground-muted">
      Syncing the current plan revision for review.
    </div>
  );
}

function PendingPlanRevisionNotice() {
  return (
    <div
      className={`rounded-md border px-3 py-2 text-sm ${accentBannerClass("accent")}`}
    >
      <div className="flex items-start gap-2">
        <Clock3 className="mt-0.5 h-4 w-4 shrink-0" />
        <p>
          The plan revision is still syncing into the approval view. Approval
          actions will unlock automatically once the current revision is ready.
        </p>
      </div>
    </div>
  );
}

function UnavailablePlanRevisionNotice() {
  return (
    <div
      className={`rounded-md border px-3 py-2 text-sm ${runStatusBannerClass("failed")}`}
    >
      The current plan revision could not be loaded. Wait for the snapshot to
      recover before deciding.
    </div>
  );
}
