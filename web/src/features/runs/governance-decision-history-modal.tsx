import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ModalDialog } from "@/components/ui/modal-dialog";
import {
	MODAL_BODY_TEXT_CLASS,
	ModalContentCard,
	ModalContentSection,
	ModalShell,
	modalShellSizeClass,
} from "@/components/ui/modal-shell";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import type { GovernanceDecision } from "@/api/hooks";
import { cn } from "@/lib/utils";

function formatGovernanceTimestamp(iso: string): string {
	const date = new Date(iso);
	if (Number.isNaN(date.getTime())) {
		return iso;
	}

	const pad = (value: number) => value.toString().padStart(2, "0");
	return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function decisionLabel(item: GovernanceDecision): string {
	if (item.stage === "plan") {
		return item.decision === "needs_rework" ? "Requested Plan Rework" : "Approved Plan";
	}
	return item.decision === "needs_rework" ? "Requested Final Rework" : "Confirmed Completion";
}

function stageLabel(item: GovernanceDecision): string {
	return item.stage === "plan" ? "Plan" : "Final";
}

export function GovernanceDecisionHistoryModal({
	open,
	onClose,
	items,
	hasMore,
	isLoadingMore,
	onLoadMore,
}: {
	open: boolean;
	onClose: () => void;
	items: GovernanceDecision[];
	hasMore?: boolean;
	isLoadingMore?: boolean;
	onLoadMore?: () => void;
}) {
	return (
		<ModalDialog open={open} onClose={onClose} ariaLabel="Allfather decision history" panelClassName={cn("w-full", modalShellSizeClass("md"))}>
			<ModalShell title="Allfather Decision History" caption="Governance History" onClose={onClose}>
				<div className="space-y-3 pr-1">
					{items.length === 0 ? (
						<div className="rounded-md border border-dashed border-border px-3 py-4 text-sm text-foreground-subtle">
							No Allfather decisions recorded yet.
						</div>
					) : (
						items.map((item) => {
							const hasNote = item.feedback_markdown.trim() !== "";
							return (
								<ModalContentCard key={item.decision_id}>
									<div className="flex flex-wrap items-center justify-between gap-3">
										<div className="flex flex-wrap items-center gap-2">
											<Badge>{stageLabel(item)}</Badge>
											<p className="text-sm font-semibold text-foreground">{decisionLabel(item)}</p>
										</div>
										<span className="text-xs text-foreground-subtle">{formatGovernanceTimestamp(item.created_at)}</span>
									</div>
									{hasNote ? (
										<ModalContentSection className="mt-3">
											<MarkdownBlock content={item.feedback_markdown} className={`${MODAL_BODY_TEXT_CLASS} text-wrap-anywhere`} />
										</ModalContentSection>
									) : null}
								</ModalContentCard>
							);
						})
					)}
					{hasMore ? (
						<div className="flex justify-center pt-1">
							<Button type="button" variant="outline" onClick={onLoadMore} disabled={isLoadingMore || !onLoadMore}>
								{isLoadingMore ? "Loading Older Decisions..." : "Load Older Decisions"}
							</Button>
						</div>
					) : null}
				</div>
			</ModalShell>
		</ModalDialog>
	);
}

export function governanceDecisionLabel(item: GovernanceDecision): string {
	return decisionLabel(item);
}

export function governanceDecisionStageLabel(item: GovernanceDecision): string {
	return stageLabel(item);
}
