import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
	MODAL_BODY_TEXT_CLASS,
	Modal,
	ModalContentCard,
	ModalContentSection,
	ModalFooter,
} from "@/components/ui/modal-shell";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import type { GovernanceDecision } from "@/api/hooks";
import { governanceDecisionLabel, governanceDecisionStageLabel } from "./governance-decision-labels";

function formatGovernanceTimestamp(iso: string): string {
	const date = new Date(iso);
	if (Number.isNaN(date.getTime())) {
		return iso;
	}

	const pad = (value: number) => value.toString().padStart(2, "0");
	return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
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
		// Paging belongs in the footer: inside the scrolling body the control
		// scrolls away exactly when the operator has reached the end and wants it.
		<Modal
			open={open}
			onClose={onClose}
			size="md"
			title="Allfather Decision History"
			caption="Governance History"
			footer={hasMore ? (
				<ModalFooter>
					<Button type="button" variant="outline" onClick={onLoadMore} disabled={isLoadingMore || !onLoadMore}>
						{isLoadingMore ? "Loading Older Decisions..." : "Load Older Decisions"}
					</Button>
				</ModalFooter>
			) : undefined}
		>
				<div className="space-y-3">
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
											<Badge>{governanceDecisionStageLabel(item)}</Badge>
											<p className="text-sm font-semibold text-foreground">{governanceDecisionLabel(item)}</p>
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
				</div>
		</Modal>
	);
}
