import type { RunDetailSummary, RunExecutionDetail, RunGraphEdge, RunGraphNode, RunNodeAttemptHistoryItem, RunNodeExecution, TimelineEvent } from "@/api/hooks";
import { nodeLabel } from "@/lib/utils";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useId, useRef, useState } from "react";
import { RunExecutionActivity } from "./run-execution-activity";
import { RunExecutionsPanel } from "./run-executions-panel";
import { presentAttemptHistoryItem, presentRunNodeExecution } from "./run-execution-presentation";
import { isActiveExecutionStatus } from "./run-status-semantics";
import { TimelinePanel } from "./timeline-panel";
import { WorkflowNodeCards } from "./workflow-node-cards";

export function RunExecutionSurface({
	run,
	nodes,
	attempts,
	executionsErrorMessage,
	showPrimaryExecutionError,
	isSummaryLoading,
	onRetryExecutions,
	selectedNodeId,
	selectedAttemptId,
	onSelectStep,
	onSelectAttempt,
	onSelectCurrentAttempt,
	historyOpen,
	onToggleHistory,
	hasMoreAttempts,
	onLoadMoreAttempts,
	isLoadingMoreAttempts,
	isHistoryLoading,
	detail,
	isDetailLoading,
	detailErrorMessage,
	detailPlaceholderMessage,
	onRetryDetail,
	onRetryStop,
	isStopRetryPending,
	stopRetryErrorMessage,
	events,
	graph,
	hasMoreEvents,
	onLoadMoreEvents,
	isLoadingMoreEvents,
}: {
	run: RunDetailSummary;
	nodes: RunNodeExecution[];
	attempts: RunNodeAttemptHistoryItem[];
	executionsErrorMessage: string | null;
	showPrimaryExecutionError: boolean;
	isSummaryLoading: boolean;
	onRetryExecutions: () => void;
	selectedNodeId: string | null;
	selectedAttemptId: string | null;
	onSelectStep: (nodeId: string) => void;
	onSelectAttempt: (attemptId: string) => void;
	onSelectCurrentAttempt: () => void;
	historyOpen: boolean;
	onToggleHistory: () => void;
	hasMoreAttempts: boolean;
	onLoadMoreAttempts: () => void;
	isLoadingMoreAttempts: boolean;
	isHistoryLoading: boolean;
	detail: (RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } }) | null;
	isDetailLoading: boolean;
	detailErrorMessage: string | null;
	detailPlaceholderMessage?: string | null;
	onRetryDetail: () => void;
	onRetryStop: (attemptID: string) => void;
	isStopRetryPending: boolean;
	stopRetryErrorMessage: string | null;
	events: TimelineEvent[];
	graph?: { nodes: RunGraphNode[]; edges: RunGraphEdge[] };
	hasMoreEvents: boolean;
	onLoadMoreEvents: () => void;
	isLoadingMoreEvents: boolean;
}) {
	const [timelineOpen, setTimelineOpen] = useState(false);
	const historyPanelId = useId();
	const timelinePanelId = useId();
	const currentAttemptActionRef = useRef<HTMLButtonElement>(null);
	const selectedNode = nodes.find((node) => node.node_id === selectedNodeId) ?? nodes[0] ?? null;
	const currentExecution = selectedNode?.current_execution ?? null;
	const selectedHistory = attempts.find((attempt) => attempt.attempt_id === selectedAttemptId) ?? null;
	const selectedDetail = detail?.attempt_id === selectedAttemptId ? detail : null;
	const selectedExecution = selectedHistory ?? selectedDetail ?? currentExecution;
	const selectedPresentation = selectedNode ? presentRunNodeExecution(selectedNode, selectedExecution) : null;
	const historicalSelected = !!selectedAttemptId && selectedExecution?.attempt_id === selectedAttemptId && selectedAttemptId !== currentExecution?.attempt_id;
	const primaryExecutionErrorVisible = showPrimaryExecutionError && !!executionsErrorMessage?.trim();
	const stopRetryAvailable = !!selectedExecution && selectedExecution.attempt_id === currentExecution?.attempt_id
		&& selectedPresentation?.status === "paused"
		&& selectedExecution.stop_retry_available;

	return (
		<div className="space-y-3">
			{primaryExecutionErrorVisible ? (
				<section className="rounded-xl border border-status-blocked/40 bg-status-blocked/10 p-4 text-status-blocked">
					<p className="text-sm font-medium text-foreground">The execution step list could not be loaded.</p>
					<p className="mt-1 text-sm">{executionsErrorMessage}</p>
					<button
						type="button"
						onClick={onRetryExecutions}
						className="mt-3 inline-flex min-h-11 items-center rounded-md border border-status-blocked/40 px-3 text-sm font-medium hover:bg-status-blocked/10"
					>
						Retry Step List
					</button>
				</section>
			) : null}
			<WorkflowNodeCards
				nodes={nodes}
				selectedNodeId={selectedNode?.node_id}
				onSelectNode={onSelectStep}
				emptyMessage={!isSummaryLoading && !primaryExecutionErrorVisible ? "No execution steps have been recorded yet." : undefined}
			/>

			{selectedNode ? (
				<section className="rounded-xl border border-border bg-background/70">
					<div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
						<div>
							<p className="text-xs font-medium uppercase tracking-[0.16em] text-foreground-muted">Selected Node</p>
							<p className="mt-1 font-medium text-foreground">{nodeLabel(selectedNode.node_id)}</p>
						</div>
						{currentExecution ? (
							<button
								ref={currentAttemptActionRef}
								type="button"
								onClick={onSelectCurrentAttempt}
								className="min-h-11 rounded-md border border-border px-3 text-sm font-medium text-foreground hover:bg-surface-raised"
							>
								{selectedPresentation?.currentAttemptLabel} {currentExecution.attempt}
							</button>
						) : null}
					</div>
					{historicalSelected ? (
						<div className="flex flex-wrap items-center justify-between gap-3 border-b border-gold/30 bg-gold/5 px-4 py-3">
							<p className="text-sm font-medium text-gold">{selectedPresentation?.historicalAttemptLabel} {selectedExecution?.attempt}</p>
							<button
								type="button"
								onClick={() => {
									onSelectCurrentAttempt();
									currentAttemptActionRef.current?.focus();
								}}
								className="min-h-11 rounded-md border border-gold/40 px-3 text-sm font-medium text-gold hover:bg-gold/10"
							>
								Return to Current Attempt
							</button>
						</div>
					) : null}
					<button
						type="button"
						onClick={onToggleHistory}
						aria-expanded={historyOpen}
						aria-controls={historyPanelId}
						className="flex min-h-11 w-full items-center justify-between gap-3 px-4 py-3 text-left"
					>
						<span className="text-sm font-medium text-foreground">Attempt History ({selectedNode.attempt_count})</span>
						{historyOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
					</button>
					{historyOpen ? (
						<div id={historyPanelId} className="border-t border-border px-4 py-3">
							<AttemptHistory
								attempts={attempts}
								currentAttemptId={currentExecution?.attempt_id}
								loading={isHistoryLoading}
								hasMore={hasMoreAttempts}
								onLoadMore={onLoadMoreAttempts}
								loadingMore={isLoadingMoreAttempts}
								onSelect={onSelectAttempt}
							/>
						</div>
					) : null}
				</section>
			) : null}

			{stopRetryAvailable && selectedExecution ? (
				<div className="flex flex-wrap items-center gap-3">
					<button
						type="button"
						onClick={() => onRetryStop(selectedExecution.attempt_id)}
						disabled={isStopRetryPending}
						className="inline-flex min-h-11 items-center justify-center rounded-md border border-status-blocked/40 px-3 py-2 text-sm font-medium text-status-blocked hover:bg-status-blocked/10 disabled:opacity-60"
					>
						{isStopRetryPending ? "Retrying Stop..." : "Retry Stop"}
					</button>
					{stopRetryErrorMessage ? <p role="alert" className="text-sm text-status-failed">{stopRetryErrorMessage}</p> : null}
				</div>
			) : null}
			<RunExecutionActivity
				runId={run.run_id}
				attemptId={selectedExecution?.attempt_id ?? null}
				isRunning={isActiveExecutionStatus(selectedExecution?.status)}
			/>
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={isDetailLoading}
				detailErrorMessage={detailErrorMessage}
				detailPlaceholderMessage={detailPlaceholderMessage}
				onRetryDetail={onRetryDetail}
				failure={!historicalSelected ? selectedNode?.failure : selectedHistory?.failure}
			/>
			<section className="rounded-xl border border-border bg-surface-raised/20">
				<button
					type="button"
					onClick={() => setTimelineOpen((current) => !current)}
					aria-expanded={timelineOpen}
					aria-controls={timelinePanelId}
					className="flex min-h-11 w-full items-center justify-between gap-3 px-3 py-3 text-left"
				>
					<span>
						<span className="block text-xs font-medium uppercase tracking-[0.16em] text-foreground-muted">Timeline</span>
						<span className="mt-1 block text-xs text-foreground-subtle">Latest activity first. Expand to inspect step-by-step events.</span>
					</span>
					{timelineOpen ? <ChevronDown className="h-4 w-4 shrink-0" /> : <ChevronRight className="h-4 w-4 shrink-0" />}
				</button>
				{timelineOpen ? (
					<div id={timelinePanelId} className="border-t border-border px-3 py-3">
						<div className="h-[26rem] min-h-[26rem]">
							<TimelinePanel
								events={events}
								graph={graph}
								executions={nodes.flatMap((node) => node.current_execution ? [node.current_execution] : [])}
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

function AttemptHistory({
	attempts,
	currentAttemptId,
	loading,
	hasMore,
	onLoadMore,
	loadingMore,
	onSelect,
}: {
	attempts: RunNodeAttemptHistoryItem[];
	currentAttemptId?: string;
	loading: boolean;
	hasMore: boolean;
	onLoadMore: () => void;
	loadingMore: boolean;
	onSelect: (attemptId: string) => void;
}) {
	if (loading) {
		return <p className="text-sm text-foreground-subtle">Loading attempt history...</p>;
	}

	return (
		<div className="space-y-2">
			{attempts.map((attempt) => (
				<AttemptHistoryRow
					key={attempt.attempt_id}
					attempt={attempt}
					current={attempt.attempt_id === currentAttemptId}
					onSelect={onSelect}
				/>
			))}
			{attempts.length === 0 ? <p className="text-sm text-foreground-subtle">No attempt history is available.</p> : null}
			{hasMore ? (
				<button
					type="button"
					onClick={onLoadMore}
					disabled={loadingMore}
					className="min-h-11 text-sm font-medium text-gold hover:text-gold-bright disabled:opacity-60"
				>
					{loadingMore ? "Loading Attempts..." : "Load Older Attempts"}
				</button>
			) : null}
		</div>
	);
}

function AttemptHistoryRow({
	attempt,
	current,
	onSelect,
}: {
	attempt: RunNodeAttemptHistoryItem;
	current: boolean;
	onSelect: (attemptId: string) => void;
}) {
	const presentation = presentAttemptHistoryItem(attempt, current ? attempt.attempt_id : undefined);

	return (
		<article className="rounded-lg border border-border bg-surface p-3">
			<button type="button" onClick={() => onSelect(attempt.attempt_id)} className="min-h-11 w-full text-left">
				<span className="font-medium text-foreground">{presentation.attemptLabel}</span>
				<span className="mt-1 block text-sm text-foreground-subtle">Outcome: {presentation.outcome}</span>
				<span className="mt-1 block text-xs text-foreground-subtle">
					Started: {attempt.started_at ?? attempt.launch_started_at ?? "Not started"} · Finished: {attempt.committed_at ?? attempt.terminal_observed_at ?? "In progress"}
				</span>
				{presentation.error ? <span className="mt-1 block text-sm text-status-failed">Error: {presentation.error}</span> : null}
				{attempt.superseded ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						Superseded{attempt.superseded_at ? ` at ${attempt.superseded_at}` : ""}
					</span>
				) : null}
				{presentation.retryRelationships.map((relationship, index) => (
					<span key={`${attempt.attempt_id}-${index}`} className="mt-1 block text-xs text-foreground-subtle">
						{relationship}
					</span>
				))}
				{attempt.token_usage ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						Token Usage: {attempt.token_usage.total_tokens} total ({attempt.token_usage.input_tokens} input, {attempt.token_usage.output_tokens} output)
					</span>
				) : null}
			</button>
			<details className="mt-2 text-xs text-foreground-subtle">
				<summary className="min-h-11 cursor-pointer py-2 font-medium text-foreground">Technical Details</summary>
				{attempt.provider_invocation_id ? (
					<p>
						Provider Invocation ID: <code className="text-wrap-anywhere">{attempt.provider_invocation_id}</code>
					</p>
				) : (
					<p>No provider invocation ID was recorded.</p>
				)}
			</details>
		</article>
	);
}
