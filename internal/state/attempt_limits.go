package state

import coreworkflow "kingdom/internal/core/workflow"

const unboundedLoopAttemptLimit = 1_000_000

func NodeAttemptLimitForClass(nodeClass string, settings coreworkflow.LoopSettings) int {
	normalized := coreworkflow.NormalizeLoopSettings(settings)
	if normalized.RunUntilAllApprove && (nodeClass == coreworkflow.NodeClassExecutionReview || nodeClass == coreworkflow.NodeClassValidationGates || nodeClass == coreworkflow.NodeClassDelegatedExecution) {
		return unboundedLoopAttemptLimit
	}

	limit := DefaultMaxAttemptsPerNode

	switch nodeClass {
	case coreworkflow.NodeClassExecutionReview, coreworkflow.NodeClassDelegatedExecution:
		combined := 1
		if normalized.HasExecutionReview {
			combined += normalized.MaxExecutionReviewCycles
		}
		if normalized.HasValidationGates {
			combined += normalized.MaxValidationReworkCycles
		}
		if combined > limit {
			limit = combined
		}
	case coreworkflow.NodeClassValidationGates:
		combined := 1
		if normalized.HasValidationGates {
			combined += normalized.MaxValidationReworkCycles
		}
		if combined > limit {
			limit = combined
		}
	}

	return limit
}
