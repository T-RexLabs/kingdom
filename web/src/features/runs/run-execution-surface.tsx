import type { RunDetailSummary, RunExecution, RunExecutionDetail, RunGraphEdge, RunGraphNode, TimelineEvent } from "@/api/hooks";
import { Badge } from "@/components/ui/badge";
import { PopoverListbox } from "@/components/ui/popover-listbox";
import { RelativeTime } from "@/components/ui/relative-time";
import { nodeLabel } from "@/lib/utils";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useId, useMemo, useState } from "react";
import { statusLabel } from "@/lib/status-system";
import { operatorFacingExecutionFailureText } from "./completion-receipt-failures";
import type { ActivityStepGroup } from "./run-activity-selection";
import { useNodeActivityRecency } from "./use-node-activity-recency";
import { RunActivityLanes } from "./run-activity-lanes";
import { RunExecutionActivity } from "./run-execution-activity";
import { RunExecutionsPanel } from "./run-executions-panel";
import { executionDisplayStatus, executionProgressLabel, isActiveExecutionStatus, isBlockedExecutionStatus, isCurrentActivityExecutionStatus, isQueuedExecutionStatus, operatorExecutionStatusText } from "./run-status-semantics";
import { TimelinePanel } from "./timeline-panel";

export function RunExecutionSurface({
	run,
	executions,
	stepGroups,
	executionsErrorMessage,
	showPrimaryExecutionError,
	isSummaryLoading,
	onRetryExecutions,
	selectedNodeId,
	selectedAttemptId,
	selectedAttemptSource,
	onSelectStep,
	onSelectAttempt,
	detail,
	isDetailLoading,
	detailErrorMessage,
	detailPlaceholderMessage,
	onRetryDetail,
	events,
	graph,
	hasMoreEvents,
	onLoadMoreEvents,
	isLoadingMoreEvents,
}: {
	run: RunDetailSummary;
	executions: RunExecution[];
	stepGroups: ActivityStepGroup[];
	executionsErrorMessage: string | null;
	showPrimaryExecutionError: boolean;
	isSummaryLoading: boolean;
	onRetryExecutions: () => void;
	selectedNodeId: string | null;
	selectedAttemptId: string | null;
	selectedAttemptSource: "auto" | "manual" | null;
	onSelectStep: (nodeId: string) => void;
	onSelectAttempt: (attemptId: string) => void;
	detail: (RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } }) | null;
	isDetailLoading: boolean;
	detailErrorMessage: string | null;
	detailPlaceholderMessage?: string | null;
	onRetryDetail: () => void;
	events: TimelineEvent[];
	graph?: { nodes: RunGraphNode[]; edges: RunGraphEdge[] };
	hasMoreEvents: boolean;
	onLoadMoreEvents: () => void;
	isLoadingMoreEvents: boolean;
}) {
	const [timelineOpen, setTimelineOpen] = useState(false);
	const currentStepGroup = stepGroups.find((item) => item.nodeId === selectedNodeId)
		?? (selectedAttemptSource === "manual" ? null : stepGroups[0] ?? null);
	const currentExecution = currentStepGroup
		? currentStepGroup.attempts.find((item) => item.attempt_id === selectedAttemptId)
			?? currentStepGroup.attempts.find((item) => item.attempt_id === currentStepGroup.preferredAttemptId)
			?? currentStepGroup.attempts[0]
			?? null
		: null;
	const primaryExecutionErrorVisible = showPrimaryExecutionError && typeof executionsErrorMessage === "string" && executionsErrorMessage.trim() !== "";
	const currentExecutionDisplayStatus = selectedExecutionDisplayStatus(currentExecution, detail);
	const summary = executionSummary(run, currentExecution, currentExecutionDisplayStatus, detail, executionsErrorMessage, primaryExecutionErrorVisible, isSummaryLoading);
	const isExecutionRunning = isActiveExecutionStatus(currentExecution?.status);
	const nodeActivityAt = useNodeActivityRecency(run.run_id, currentExecution?.attempt_id ?? selectedAttemptId, isExecutionRunning);
	// Prefer honest ACP activity recency; fall back to the coarse execution timestamp
	// only before any agent activity has been recorded.
	const summaryActivityAt = nodeActivityAt ?? currentExecution?.activity_at ?? run.last_activity_at;
	const activityHeroLabel = primaryExecutionErrorVisible ? "Execution Steps" : activityHeroLabelText(currentExecutionDisplayStatus, selectedAttemptSource);
	const stepSelectorId = useId();
	const attemptSelectorId = useId();
	const timelinePanelId = useId();
	const stepOptions = useMemo(() => {
		return stepGroups.map((item) => ({
			id: item.nodeId,
			label: nodeLabel(item.nodeId),
		}));
	}, [stepGroups]);
	const attemptOptions = (currentStepGroup?.attempts ?? []).map((item) => ({
		id: item.attempt_id,
		label: `Attempt ${item.attempt}`,
	}));
	const stepSelectorFooter = executionsErrorMessage && !primaryExecutionErrorVisible ? (
		<div className="space-y-2 text-xs text-status-blocked">
			<div>{executionsErrorMessage}</div>
			<button
				type="button"
				onClick={onRetryExecutions}
				className="rounded-md border border-status-blocked/30 px-2.5 py-1 font-medium text-status-blocked hover:bg-status-blocked/10"
			>
				Retry step list
			</button>
		</div>
	) : null;

	return (
		<div className="space-y-3">
			<RunActivityLanes
				runId={run.run_id}
				executions={executions}
				selectedNodeId={selectedNodeId}
				onSelectStep={onSelectStep}
			/>
			{primaryExecutionErrorVisible && (
				<section className="rounded-xl border border-status-blocked/40 bg-status-blocked/10 p-4 text-status-blocked">
					<div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
						<div className="space-y-1">
							<p className="text-xs uppercase tracking-[0.16em] text-status-blocked/80">Execution Steps</p>
							<p className="text-sm font-medium text-foreground">The execution step list could not be loaded.</p>
							<p className="text-sm text-status-blocked">{executionsErrorMessage}</p>
						</div>
						<button
							type="button"
							onClick={onRetryExecutions}
							className="inline-flex items-center justify-center rounded-md border border-status-blocked/40 px-3 py-2 text-sm font-medium text-status-blocked hover:bg-status-blocked/10"
						>
							Retry step list
						</button>
					</div>
				</section>
			)}
			<section className="rounded-xl border border-border bg-background/70 p-4">
				<div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
					<div className="min-w-0 flex-1">
						<p className="text-xs uppercase tracking-[0.16em] text-foreground-muted">{selectedAttemptSource === "manual" ? "Selected Step" : activityHeroLabel}</p>
						<PopoverListbox
							triggerId={stepSelectorId}
							triggerRole="button"
							triggerAriaLabel="Current step"
							triggerClassName="mt-2 flex w-full items-center gap-2 rounded-md text-left transition-colors hover:text-gold"
							triggerContent={(
								<>
									<span className="truncate text-lg font-semibold text-foreground sm:text-xl">{summary.title}</span>
									<ChevronDown className="h-4 w-4 shrink-0 text-foreground-subtle" />
								</>
							)}
							items={stepOptions}
							selectedId={currentStepGroup?.nodeId ?? null}
							onSelect={onSelectStep}
							footer={stepSelectorFooter}
							minWidth={220}
							panelClassName="flex flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"
						/>
						<p className="mt-2 max-w-3xl text-wrap-anywhere text-sm leading-relaxed text-foreground-subtle">{summary.body}</p>
						<div className="mt-3 text-xs text-foreground-subtle">
							Last activity <RelativeTime iso={summaryActivityAt} />
						</div>
					</div>
					<div className="flex flex-col items-start gap-2 sm:items-end">
						{currentExecution && currentExecutionDisplayStatus
							? <Badge variant="status" status={currentExecutionDisplayStatus}>{statusLabel(currentExecutionDisplayStatus)}</Badge>
							: null}
						{currentStepGroup && currentStepGroup.attempts.length > 1 ? (
							<div className="min-w-[6.75rem] sm:min-w-[7rem]">
								<PopoverListbox
									triggerId={attemptSelectorId}
									triggerRole="button"
									triggerAriaLabel="Selected attempt"
									triggerClassName="flex w-full items-center justify-between gap-1.5 rounded-md border border-border bg-surface px-2.5 py-1.5 text-left text-xs text-foreground transition-colors hover:border-gold/40 hover:text-gold"
									triggerContent={(
										<>
											<span className="truncate">{`Attempt ${currentExecution?.attempt ?? currentStepGroup.attempts[0]?.attempt ?? 1}`}</span>
											<ChevronDown className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
										</>
									)}
									items={attemptOptions}
									selectedId={selectedAttemptId}
									onSelect={onSelectAttempt}
									minWidth={112}
									panelClassName="flex flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"
								/>
							</div>
						) : null}
					</div>
				</div>
			</section>

			<RunExecutionActivity
				runId={run.run_id}
				attemptId={currentExecution?.attempt_id ?? selectedAttemptId}
				isRunning={isActiveExecutionStatus(currentExecution?.status)}
			/>

			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={isDetailLoading}
				detailErrorMessage={detailErrorMessage}
				detailPlaceholderMessage={detailPlaceholderMessage}
				onRetryDetail={onRetryDetail}
			/>

			<section className="rounded-xl border border-border bg-surface-raised/20">
				<button
					type="button"
					onClick={() => setTimelineOpen((current) => !current)}
					aria-expanded={timelineOpen}
					aria-controls={timelinePanelId}
					className="flex w-full items-center justify-between gap-3 px-3 py-3 text-left"
				>
					<div className="min-w-0">
						<p className="text-xs uppercase tracking-[0.16em] text-foreground-muted">Timeline</p>
						<p className="mt-1 text-xs text-foreground-subtle">Latest activity first. Expand to inspect step-by-step events.</p>
					</div>
					{timelineOpen
						? <ChevronDown className="h-4 w-4 shrink-0 text-foreground-subtle" />
						: <ChevronRight className="h-4 w-4 shrink-0 text-foreground-subtle" />}
				</button>
				{timelineOpen ? (
					<div id={timelinePanelId} className="border-t border-border px-3 py-3">
						<div className="h-[26rem] min-h-[26rem]">
							<TimelinePanel
								events={events}
								graph={graph}
								executions={executions}
								hasMore={hasMoreEvents}
								onLoadMore={onLoadMoreEvents}
								isLoadingMore={isLoadingMoreEvents}
							/>
						</div>
					</div>
				) : null}
			</section>
		</div>
	);
}

function activityHeroLabelText(
	status: string | null,
	selectionSource: "auto" | "manual" | null,
): string {
	if (selectionSource === "manual") {
		return "Selected Step";
	}
	if (status === null || isCurrentActivityExecutionStatus(status)) {
		return "Current Activity";
	}
	if (status === "succeeded") {
		return "Latest Completed Step";
	}
	return "Latest Step";
}

function executionSummary(
	run: RunDetailSummary,
	execution: RunExecution | null,
	displayStatus: string | null,
	detail: (RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } }) | null,
	executionsErrorMessage: string | null,
	showPrimaryExecutionError: boolean,
	isSummaryLoading: boolean,
): { title: string; body: string } {
	if (!execution) {
		if (showPrimaryExecutionError && executionsErrorMessage) {
			return {
				title: "Execution steps unavailable",
				body: "Retry the step list to restore the latest execution view.",
			};
		}
		if (isSummaryLoading) {
			return {
				title: "Loading current step",
				body: "Kingdom is loading the latest execution summary from recorded activity.",
			};
		}
		return {
			title: "Waiting to start",
			body: run.status === "failed"
				? "This run failed before a useful step was recorded."
				: "This run has not reached a meaningful execution step yet.",
		};
	}

	const title = nodeLabel(execution.node_id);
	const state = execution.status;
	const progressLabel = executionProgressLabel(
		state,
		detail?.progress_reason_code,
		detail?.progress_event_type,
		detail?.live_signal?.live_label,
	);

	if (displayStatus === "rework") {
		return { title, body: "Changes were requested for this step." };
	}
	if (detail?.output_verdict === "REWORK") {
		return { title, body: "Changes were requested for this step." };
	}
	if (detail?.settlement_status === "pending") {
		return {
			title,
			body: operatorExecutionStatusText("verifying", progressLabel) ?? "Kingdom is still confirming the final result for this step.",
		};
	}
	if (isQueuedExecutionStatus(state)) {
		return {
			title,
			body: operatorExecutionStatusText(state, progressLabel, execution.handoff_state) ?? "This step is queued to start.",
		};
	}
	if (isActiveExecutionStatus(state)) {
		return { title, body: operatorExecutionStatusText(state, progressLabel, execution.handoff_state) ?? "This step is still running." };
	}
	if (isBlockedExecutionStatus(state)) {
		return {
			title,
			body: operatorExecutionStatusText(state, progressLabel, execution.handoff_state) ?? "This step is blocked while Kingdom decides or performs recovery.",
		};
	}
	if (state === "failed") {
		const matchingDetail = detail?.attempt_id === execution.attempt_id ? detail : null;
		return {
			title,
			body: operatorFacingExecutionFailureText(
				matchingDetail?.failure_code ?? execution.failure_code,
				matchingDetail?.completion_invalid_reason ?? execution.completion_invalid_reason,
				matchingDetail?.failure_text,
				execution.failure_text,
			) ?? "This step failed.",
		};
	}
	if (state === "cancelled") {
		return {
			title,
			body: run.status === "failed" ? "This step stopped after the run failed elsewhere." : "This step stopped before completion.",
		};
	}
	return { title, body: "This step completed." };
}

function selectedExecutionDisplayStatus(
	execution: RunExecution | null,
	detail: (RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } }) | null,
): string | null {
	if (!execution) {
		return null;
	}
	if (detail && detail.attempt_id === execution.attempt_id) {
		if (detail.settlement_status === "pending") {
			return "verifying";
		}
		return executionDisplayStatus(detail);
	}
	return executionDisplayStatus(execution);
}
