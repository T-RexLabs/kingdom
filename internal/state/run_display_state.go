package state

// DeriveRunDisplayState returns the operator-facing run display state from the
// canonical run status and plan state persisted in SQLite.
func DeriveRunDisplayState(status, planState string) string {
	switch {
	case status == "awaiting_human_confirmation":
		return "awaiting_final_decision"
	case status == "running" && planState == "awaiting_king_decision":
		return "awaiting_plan_decision"
	case status == "pending":
		return "pending"
	case status == "running":
		return "running"
	case status == "succeeded":
		return "succeeded"
	case status == "failed":
		return "failed"
	case status == "cancelled":
		return "cancelled"
	default:
		return "running"
	}
}
