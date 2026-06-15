import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ModalDialog } from "@/components/ui/modal-dialog";
import {
  MODAL_BODY_TEXT_CLASS,
  MODAL_SUPPORTING_TEXT_CLASS,
  ModalContentCard,
  ModalShell,
  modalShellSizeClass,
} from "@/components/ui/modal-shell";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { Spinner } from "@/components/ui/spinner";
import type { PlanningDraft } from "@/api/hooks";
import { isPlannerOperationTerminal, type PlannerOperationState } from "./planner-operation-controller";
import { useTextReveal } from "@/lib/use-text-reveal";

export function PlanResultModal({
  open,
  draft,
  sessionId,
  contextSnapshotId,
  operation,
  onClose,
}: {
  open: boolean;
  draft: PlanningDraft | null;
  sessionId: string;
  contextSnapshotId?: string;
  operation: PlannerOperationState | null;
  onClose: () => void;
}) {
  const isGenerating = operation?.kind === "formalize" && !isPlannerOperationTerminal(operation.status);
  const generationError = operation?.kind === "formalize" ? operation.error : null;
  const { displayText: revealedFormalize, isStreamActive: isFormalizeStreaming } = useTextReveal(operation?.formalizeText ?? "", open && isGenerating);

  if (!open) return null;

  return (
    <ModalDialog
      open={open}
      onClose={onClose}
      ariaLabel="Generated plan"
      panelClassName={`w-full ${modalShellSizeClass("lg")}`}
    >
			<ModalShell
				title={draft?.decree_title ?? (isGenerating ? "Generating plan" : "Plan result")}
				caption="Generated Plan"
				onClose={onClose}
				footer={(
					<div className="flex items-center justify-end gap-3">
						<Button variant="ghost" onClick={onClose}>
							Close
						</Button>
						<Link
							to="/decrees/new"
							search={{
								planning_session_id: sessionId,
								planning_draft_id: draft?.planning_draft_id ?? "",
								...(contextSnapshotId ? { planning_context_snapshot_id: contextSnapshotId } : {}),
							}}
						>
							<Button disabled={!draft}>
								Issue Decree from Plan
								<ArrowRight className="ml-1.5 h-3.5 w-3.5" />
							</Button>
						</Link>
					</div>
				)}
			>
	          <ModalContentCard data-testid="plan-result-card">
	            {generationError ? (
	              <div className="rounded-lg border border-status-failed/30 bg-status-failed/10 p-4 text-sm text-status-failed">
	                {generationError}
	              </div>
	            ) : null}

	            {!generationError && isGenerating ? (
	              <div className="space-y-4">
	                <div className={`flex items-center gap-3 ${MODAL_BODY_TEXT_CLASS}`}>
	                  <Spinner className="h-5 w-5" />
	                  <span>Formalizing decree draft...</span>
	                </div>
	                {revealedFormalize ? (
	                  isFormalizeStreaming ? (
	                    <div data-streaming>
	                      <p className={`${MODAL_BODY_TEXT_CLASS} whitespace-pre-wrap text-wrap-anywhere`}>{revealedFormalize}</p>
	                    </div>
	                  ) : (
	                    <MarkdownBlock content={operation?.formalizeText ?? ""} className={MODAL_BODY_TEXT_CLASS} />
	                  )
	                ) : (
	                  <p className={MODAL_SUPPORTING_TEXT_CLASS}>Preparing initial preview...</p>
	                )}
	              </div>
	            ) : null}

	            {!generationError && !isGenerating && draft ? (
	              <MarkdownBlock content={draft.decree_text} className={MODAL_BODY_TEXT_CLASS} />
	            ) : null}
	          </ModalContentCard>
			</ModalShell>
    </ModalDialog>
  );
}
