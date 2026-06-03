package workflow

const (
	DefaultWorkflowID              = "royal-decree-v1"
	SkipReviewValidationWorkflowID = "royal-decree-skip-review-validation-v1"
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
