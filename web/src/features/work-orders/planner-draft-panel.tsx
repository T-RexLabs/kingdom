import { Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import {
  MODAL_BODY_TEXT_CLASS,
  MODAL_SUPPORTING_TEXT_CLASS,
  Modal,
  ModalContentCard,
  ModalFooter,
} from "@/components/ui/modal-shell";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { Spinner } from "@/components/ui/spinner";
import type { PlanningDraft } from "@/api/hooks";
import type { components } from "@/api/schema";
import { isPlannerOperationTerminal, type PlannerOperationState } from "./planner-operation-controller";
import { useTextReveal } from "@/lib/use-text-reveal";

export function PlanResultModal({
  open,
  draft,
  planningLineage,
  operation,
  onClose,
}: {
  open: boolean;
  draft: PlanningDraft | null;
  planningLineage?: components["schemas"]["PlanningLineage"];
  operation: PlannerOperationState | null;
  onClose: () => void;
}) {
  const isGenerating = operation?.kind === "formalize" && !isPlannerOperationTerminal(operation.status);
  const generationError = operation?.kind === "formalize" ? operation.error : null;
  const { displayText: revealedFormalize, isStreamActive: isFormalizeStreaming } = useTextReveal(operation?.formalizeText ?? "", open && isGenerating);

  if (!open) return null;

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="lg"
      title={draft?.work_order_title ?? (isGenerating ? "Generating plan" : "Plan result")}
      caption="Generated Plan"
      footer={(
					<ModalFooter>
						<Button variant="ghost" onClick={onClose}>
							Close
						</Button>
						{draft && planningLineage ? (
							<Link className={buttonVariants()} to="/work-orders/new" search={planningLineage}>
									Issue Work Order from Plan
									<ArrowRight className="ml-1.5 h-3.5 w-3.5" />
							</Link>
						) : (
							<Button disabled>
								Issue Work Order from Plan
								<ArrowRight className="ml-1.5 h-3.5 w-3.5" />
							</Button>
						)}
					</ModalFooter>
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
	                  <span>Formalizing work order draft...</span>
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
	              <MarkdownBlock content={draft.work_order_text} className={MODAL_BODY_TEXT_CLASS} />
	            ) : null}
	          </ModalContentCard>
    </Modal>
  );
}
