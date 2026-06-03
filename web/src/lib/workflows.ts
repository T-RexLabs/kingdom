export const SKIP_REVIEW_VALIDATION_WORKFLOW_ID = "royal-decree-skip-review-validation-v1";

export function isSkipReviewValidationWorkflowId(workflowId: string | null | undefined): boolean {
  return (workflowId?.trim() ?? "") === SKIP_REVIEW_VALIDATION_WORKFLOW_ID;
}
