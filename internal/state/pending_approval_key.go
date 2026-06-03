package state

import (
	"fmt"
	"strings"
)

// ManualPlanApprovalActiveSelectExpr returns a SQL expression that is true when
// a run is actively awaiting manual plan approval.
func ManualPlanApprovalActiveSelectExpr(runAlias string) string {
	trimmedAlias := normalizePendingApprovalRunAlias(runAlias)
	return fmt.Sprintf(`(%[1]s.run_status = 'running' AND %[1]s.plan_state = 'awaiting_king_decision')`, trimmedAlias)
}

// ManualFinalApprovalActiveSelectExpr returns a SQL expression that is true
// when a run is actively awaiting manual final approval.
func ManualFinalApprovalActiveSelectExpr(runAlias string) string {
	trimmedAlias := normalizePendingApprovalRunAlias(runAlias)
	return fmt.Sprintf(`(%s.run_status = 'awaiting_human_confirmation')`, trimmedAlias)
}

// ManualApprovalActiveSelectExpr returns a SQL expression that is true when a
// run is actively awaiting any manual approval.
func ManualApprovalActiveSelectExpr(runAlias string) string {
	return fmt.Sprintf(`(%s OR %s)`, ManualPlanApprovalActiveSelectExpr(runAlias), ManualFinalApprovalActiveSelectExpr(runAlias))
}

// PendingApprovalKeySelectExpr returns a SQL expression that projects the
// canonical manual approval gate identity stored on workflow_runs.
func PendingApprovalKeySelectExpr(runAlias string) string {
	trimmedAlias := normalizePendingApprovalRunAlias(runAlias)
	trimmedKeyExpr := fmt.Sprintf(`TRIM(COALESCE(%s.pending_approval_key, ''))`, trimmedAlias)

	return fmt.Sprintf(`CASE
		WHEN COALESCE(%[1]s.autonomous_approvals, 1) <> 0 THEN NULL
		WHEN %[2]s AND %[3]s LIKE 'plan:%%' THEN %[3]s
		WHEN %[4]s AND %[3]s LIKE 'final:%%' THEN %[3]s
		ELSE NULL
	END`, trimmedAlias, ManualPlanApprovalActiveSelectExpr(trimmedAlias), trimmedKeyExpr, ManualFinalApprovalActiveSelectExpr(trimmedAlias))
}

func normalizePendingApprovalRunAlias(runAlias string) string {
	trimmedAlias := strings.TrimSpace(runAlias)
	if trimmedAlias == "" {
		return "wr"
	}
	return trimmedAlias
}

func pendingApprovalGateKey(scope string, req TransitionRequest) (string, error) {
	trimmedScope := strings.TrimSpace(scope)
	if trimmedScope == "" {
		return "", fmt.Errorf("pending approval scope is required")
	}
	gateID := firstNonEmptyTrimmed(req.TransitionID, req.EventID)
	if gateID == "" {
		return "", fmt.Errorf("pending %s approval gate id is required", trimmedScope)
	}
	return trimmedScope + ":" + gateID, nil
}
