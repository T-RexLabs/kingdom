package workflow

const (
	DefaultWorkflowID              = "work-order-v1"
	SkipReviewValidationWorkflowID = "work-order-skip-review-validation-v1"
)

func BuiltInWorkflowID(skipReviewsAndValidations bool) string {
	if skipReviewsAndValidations {
		return SkipReviewValidationWorkflowID
	}
	return DefaultWorkflowID
}

func IsSkipReviewValidationWorkflowID(workflowID string) bool {
	return workflowID == SkipReviewValidationWorkflowID
}
