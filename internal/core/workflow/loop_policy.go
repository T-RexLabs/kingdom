package workflow

import "strings"

type LoopSettings struct {
	RunUntilAllApprove        bool
	HasExecutionReview        bool
	HasValidationGates        bool
	MaxExecutionReviewCycles  int
	MaxValidationReworkCycles int
}

type LoopSettingsOverrides struct {
	RunUntilAllApprove        *bool
	MaxExecutionReviewCycles  *int
	MaxValidationReworkCycles *int
}

func DefinitionLoopSettings(definition Definition, runUntilAllApprove bool) LoopSettings {
	hasExecutionReview := definitionHasNodeClass(definition.Nodes, NodeClassExecutionReview)
	hasValidationGates := definitionHasNodeClass(definition.Nodes, NodeClassValidationGates)
	return NormalizeLoopSettings(LoopSettings{
		RunUntilAllApprove:        runUntilAllApprove,
		HasExecutionReview:        hasExecutionReview,
		HasValidationGates:        hasValidationGates,
		MaxExecutionReviewCycles:  definitionLoopCapValue(definition.Metadata.MaxExecutionReviewCycles, hasExecutionReview, defaultMaxExecutionReviewCycles),
		MaxValidationReworkCycles: definitionLoopCapValue(definition.Metadata.MaxValidationReworkCycles, hasValidationGates, defaultMaxValidationReworkCycles),
	})
}

func ResolveRunLoopSettings(definition Definition, overrides LoopSettingsOverrides) LoopSettings {
	settings := DefinitionLoopSettings(definition, false)
	if overrides.RunUntilAllApprove != nil {
		settings.RunUntilAllApprove = *overrides.RunUntilAllApprove
	}
	if settings.HasExecutionReview {
		if overrides.MaxExecutionReviewCycles != nil {
			settings.MaxExecutionReviewCycles = *overrides.MaxExecutionReviewCycles
		}
	} else {
		settings.MaxExecutionReviewCycles = 0
	}
	if settings.HasValidationGates {
		if overrides.MaxValidationReworkCycles != nil {
			settings.MaxValidationReworkCycles = *overrides.MaxValidationReworkCycles
		}
	} else {
		settings.MaxValidationReworkCycles = 0
	}
	return NormalizeLoopSettings(settings)
}

func NormalizeLoopSettings(settings LoopSettings) LoopSettings {
	normalized := settings
	if !normalized.HasExecutionReview {
		normalized.MaxExecutionReviewCycles = 0
	} else if normalized.MaxExecutionReviewCycles < 0 {
		normalized.MaxExecutionReviewCycles = defaultMaxExecutionReviewCycles
	}
	if !normalized.HasValidationGates {
		normalized.MaxValidationReworkCycles = 0
	} else if normalized.MaxValidationReworkCycles < 0 {
		normalized.MaxValidationReworkCycles = defaultMaxValidationReworkCycles
	}
	if !normalized.HasExecutionReview && !normalized.HasValidationGates {
		normalized.RunUntilAllApprove = false
	}
	return normalized
}

func HasAnyExecutionLoops(settings LoopSettings) bool {
	normalized := NormalizeLoopSettings(settings)
	return normalized.HasExecutionReview || normalized.HasValidationGates
}

func CanStartExecutionReviewCycle(settings LoopSettings, failedReviewCycles int) (bool, bool) {
	normalized := NormalizeLoopSettings(settings)
	if !normalized.HasExecutionReview {
		return false, true
	}
	if normalized.RunUntilAllApprove {
		return true, false
	}
	if normalized.MaxExecutionReviewCycles <= 0 {
		return false, true
	}
	if failedReviewCycles < normalized.MaxExecutionReviewCycles {
		return true, false
	}
	return false, true
}

func CanQueueExecutionReviewRework(settings LoopSettings, failedReviewCycles int) (bool, bool) {
	normalized := NormalizeLoopSettings(settings)
	if !normalized.HasExecutionReview {
		return false, true
	}
	if normalized.RunUntilAllApprove {
		return true, false
	}
	if normalized.MaxExecutionReviewCycles <= 0 {
		return false, true
	}
	if failedReviewCycles <= normalized.MaxExecutionReviewCycles {
		return true, false
	}
	return false, true
}

func CanStartValidationReworkCycle(settings LoopSettings, failedValidationWaves int) (bool, bool) {
	normalized := NormalizeLoopSettings(settings)
	if !normalized.HasValidationGates {
		return false, true
	}
	if normalized.RunUntilAllApprove {
		return true, false
	}
	if normalized.MaxValidationReworkCycles <= 0 {
		return false, true
	}
	if failedValidationWaves < normalized.MaxValidationReworkCycles {
		return true, false
	}
	return false, true
}

func CanQueueValidationRework(settings LoopSettings, failedValidationWaves int) (bool, bool) {
	normalized := NormalizeLoopSettings(settings)
	if !normalized.HasValidationGates {
		return false, true
	}
	if normalized.RunUntilAllApprove {
		return true, false
	}
	if normalized.MaxValidationReworkCycles <= 0 {
		return false, true
	}
	if failedValidationWaves <= normalized.MaxValidationReworkCycles {
		return true, false
	}
	return false, true
}

func ReadyNodeAutoSucceedsAtLoopCap(settings LoopSettings, nodeClass string, wave, failedReviewCyclesBeforeWave, failedValidationWavesBeforeWave int) bool {
	if wave <= 0 {
		return false
	}

	normalized := NormalizeLoopSettings(settings)
	switch strings.TrimSpace(nodeClass) {
	case NodeClassExecutionReview:
		if !normalized.HasExecutionReview || normalized.RunUntilAllApprove {
			return false
		}
		if normalized.MaxExecutionReviewCycles <= 0 {
			return true
		}
		return maxInt(failedReviewCyclesBeforeWave, 0) >= normalized.MaxExecutionReviewCycles
	case NodeClassValidationGates:
		if !normalized.HasValidationGates || normalized.RunUntilAllApprove {
			return false
		}
		if normalized.MaxValidationReworkCycles <= 0 {
			return true
		}
		return maxInt(failedValidationWavesBeforeWave, 0) >= normalized.MaxValidationReworkCycles
	default:
		return false
	}
}

func definitionLoopCapValue(value *int, hasNodeClass bool, defaultValue int) int {
	if !hasNodeClass {
		return 0
	}
	if value == nil {
		return defaultValue
	}
	return *value
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func LoopCapAutoSuccessReasonCode(nodeClass string) string {
	switch strings.TrimSpace(nodeClass) {
	case NodeClassExecutionReview:
		return "execution_review_cap_auto_succeeded"
	case NodeClassValidationGates:
		return "validation_gate_cap_auto_succeeded"
	default:
		return ""
	}
}

func IsLoopCapAutoSuccessReasonCode(nodeClass, reasonCode string) bool {
	return strings.TrimSpace(reasonCode) == LoopCapAutoSuccessReasonCode(nodeClass)
}
