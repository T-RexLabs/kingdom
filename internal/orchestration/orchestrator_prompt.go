package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/verdict"
	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

var controlCommandLinePattern = regexp.MustCompile(`(?i)^\s*(ROYAL_VERDICT|VERDICT)\s*:`)
var controlTokenLiteralPattern = regexp.MustCompile(`(?i)\b(ROYAL_VERDICT|VERDICT)\s*:`)

func (o *orchestrationLoop) workOrderTextForRun(ctx context.Context, runID string) (providerexecution.ContentOpener, error) {
	var workOrderID string
	err := o.db.QueryRowContext(ctx, `
SELECT d.work_order_id
FROM workflow_runs wr
JOIN work_orders d ON d.work_order_id = wr.work_order_id
WHERE wr.run_id = ?`, runID).Scan(&workOrderID)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (io.ReadCloser, error) {
		return state.OpenWorkOrderText(ctx, o.db, workOrderID)
	}, nil
}

func (o *orchestrationLoop) workOrderTitleForRun(ctx context.Context, runID string) (string, error) {
	var workOrderTitle string
	err := o.db.QueryRowContext(ctx, `
SELECT d.work_order_title
FROM workflow_runs wr
JOIN work_orders d ON d.work_order_id = wr.work_order_id
WHERE wr.run_id = ?`, runID).Scan(&workOrderTitle)
	if err != nil {
		return "", err
	}
	return workOrderTitle, nil
}

func currentAssignmentForNodeClass(nodeClass, delegatorRoleID string) string {
	lines := []string{fmt.Sprintf("- Assignment: %s", humanizeNodeClass(nodeClass))}
	if trimmedDelegatorRole := strings.TrimSpace(delegatorRoleID); trimmedDelegatorRole != "" {
		lines = append(lines, fmt.Sprintf("- Upstream Role: %s", humanizePromptIdentifier(trimmedDelegatorRole)))
	}
	return strings.Join(lines, "\n")
}

func humanizeNodeClass(nodeClass string) string {
	switch strings.TrimSpace(nodeClass) {
	case workflow.NodeClassWorkIntake:
		return "Work Intake"
	case workflow.NodeClassAdvisorConsultation:
		return "Advisory Consultation"
	case workflow.NodeClassAdvisorySynthesis:
		return "Advisory Synthesis"
	case workflow.NodeClassPlanDrafting:
		return "Plan Drafting"
	case workflow.NodeClassPlanDecision:
		return "Governance Plan Decision"
	case workflow.NodeClassExecutionKickoff:
		return "Execution Kickoff"
	case workflow.NodeClassDelegatedExecution:
		return "Delegated Execution"
	case workflow.NodeClassExecutionReview:
		return "Execution Review"
	case workflow.NodeClassValidationGates:
		return "Validation Gates"
	case workflow.NodeClassCompletion:
		return "Completion"
	default:
		return humanizePromptIdentifier(nodeClass)
	}
}

func humanizePromptIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == '-' || r == '_'
	})
	for idx, part := range parts {
		lower := strings.ToLower(strings.TrimSpace(part))
		if lower == "" {
			continue
		}
		if len(lower) <= 2 {
			parts[idx] = strings.ToUpper(lower)
			continue
		}
		parts[idx] = strings.ToUpper(lower[:1]) + lower[1:]
	}
	return strings.Join(parts, " ")
}

func stripControlCommandLines(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	lines := strings.Split(trimmed, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if controlCommandLinePattern.MatchString(line) {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.TrimSpace(strings.Join(filtered, "\n"))
}

func sanitizeFailureContextText(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	rewritten := controlTokenLiteralPattern.ReplaceAllStringFunc(trimmed, func(token string) string {
		normalized := strings.ToUpper(strings.TrimSpace(token))
		switch {
		case strings.HasPrefix(normalized, "ROYAL_VERDICT"):
			return "royal closure verdict token "
		default:
			return "verdict token "
		}
	})
	return strings.TrimSpace(rewritten)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type validationGateReworkError struct {
	reason string
}

func (e *validationGateReworkError) Error() string {
	return e.reason
}

type invalidValidationVerdictError struct {
	reason string
}

func (e *invalidValidationVerdictError) Error() string {
	return e.reason
}

type executionReviewReworkError struct {
	reason string
}

func (e *executionReviewReworkError) Error() string {
	return e.reason
}

type invalidExecutionReviewVerdictError struct {
	reason string
}

func (e *invalidExecutionReviewVerdictError) Error() string {
	return e.reason
}

func detectExecutionReviewRework(nodeID, outputText string) error {
	return detectExecutionReviewReworkForClass(nodeID, "", outputText)
}

func detectExecutionReviewReworkForClass(nodeID, nodeClass, outputText string) error {
	if resolvedNodeClass(nodeID, nodeClass) != workflow.NodeClassExecutionReview {
		return nil
	}
	verdict, err := parseNodeVerdict(outputText, "execution review")
	if err != nil {
		return err
	}
	if verdict == "REWORK" {
		return &executionReviewReworkError{reason: "execution review requested rework with VERDICT: REWORK"}
	}
	return nil
}

func parseNodeVerdict(outputText, ownerLabel string) (string, error) {
	resolvedVerdict, found, err := verdict.FindNearTop(outputText)
	if err != nil {
		return "", fmt.Errorf("%s response contains invalid verdict lines: %w", ownerLabel, invalidVerdictErrorForOwner(ownerLabel))
	}
	if !found {
		return "", fmt.Errorf("%s response missing required verdict line near top: VERDICT: PASS|REWORK: %w", ownerLabel, invalidVerdictErrorForOwner(ownerLabel))
	}
	return resolvedVerdict, nil
}

func invalidVerdictErrorForOwner(ownerLabel string) error {
	if ownerLabel == "execution review" {
		return &invalidExecutionReviewVerdictError{reason: "invalid execution review verdict"}
	}
	return &invalidValidationVerdictError{reason: "invalid validation verdict"}
}

func detectValidationGateRework(nodeID, outputText string) error {
	return detectValidationGateReworkForClass(nodeID, "", outputText)
}

func detectValidationGateReworkForClass(nodeID, nodeClass, outputText string) error {
	if resolvedNodeClass(nodeID, nodeClass) != workflow.NodeClassValidationGates {
		return nil
	}
	verdict, err := parseNodeVerdict(outputText, "validation")
	if err != nil {
		return err
	}
	if verdict == "REWORK" {
		return &validationGateReworkError{reason: "validation gate requested rework with VERDICT: REWORK"}
	}
	return nil
}

func (o *orchestrationLoop) roleIDForAgent(ctx context.Context, agentID string) string {
	trimmedAgentID := strings.TrimSpace(agentID)
	if o == nil || o.db == nil || trimmedAgentID == "" {
		return ""
	}

	var roleID string
	if err := o.db.QueryRowContext(ctx, `SELECT role_id FROM agents WHERE agent_id = ? LIMIT 1`, trimmedAgentID).Scan(&roleID); err != nil {
		return ""
	}
	return strings.TrimSpace(roleID)
}

func (o *orchestrationLoop) operatorReworkFeedbackForAdvisoryAttempt(ctx context.Context, runID string, attempt int) string {
	trimmedRunID := strings.TrimSpace(runID)
	if o == nil || o.db == nil || trimmedRunID == "" || attempt < 1 {
		return ""
	}
	review, ok, err := state.NewRunPlanReviewStore(o.db).GetNeedsReworkForQueuedAdvisoryAttempt(ctx, trimmedRunID, attempt)
	if err != nil || !ok {
		return ""
	}
	feedback := strings.TrimSpace(stripControlCommandLines(review.FeedbackMarkdown))
	if feedback == "" {
		return ""
	}
	return fmt.Sprintf("Operator Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) operatorPlanReworkFeedbackForPlanAttempt(ctx context.Context, runID string, attempt int) string {
	trimmedRunID := strings.TrimSpace(runID)
	if o == nil || o.db == nil || trimmedRunID == "" || attempt < 1 {
		return ""
	}
	review, ok, err := state.NewRunPlanReviewStore(o.db).GetNeedsReworkForQueuedPlanAttempt(ctx, trimmedRunID, attempt)
	if err != nil || !ok {
		return ""
	}
	feedback := strings.TrimSpace(stripControlCommandLines(review.FeedbackMarkdown))
	if feedback == "" {
		return ""
	}
	return fmt.Sprintf("Operator Plan Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) operatorFinalReworkFeedbackForPlanAttempt(ctx context.Context, runID string, attempt int) string {
	trimmedRunID := strings.TrimSpace(runID)
	if o == nil || o.db == nil || trimmedRunID == "" || attempt < 1 {
		return ""
	}
	review, ok, err := state.NewRunFinalReviewStore(o.db).GetNeedsReworkForQueuedPlanAttempt(ctx, trimmedRunID, attempt)
	if err != nil || !ok {
		return ""
	}
	feedback := strings.TrimSpace(stripControlCommandLines(review.FeedbackMarkdown))
	if feedback == "" {
		return ""
	}
	return fmt.Sprintf("Operator Final Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) latestScopedInstructionForNode(ctx context.Context, runID, nodeID, fromAgentID, toAgentID string) string {
	trimmedRunID := strings.TrimSpace(runID)
	trimmedNodeID := strings.TrimSpace(nodeID)
	trimmedFromAgentID := strings.TrimSpace(fromAgentID)
	trimmedToAgentID := strings.TrimSpace(toAgentID)
	if o == nil || o.db == nil || trimmedRunID == "" || trimmedNodeID == "" || trimmedFromAgentID == "" || trimmedToAgentID == "" {
		return ""
	}
	var instruction sql.NullString
	err := o.db.QueryRowContext(ctx, `
SELECT COALESCE(json_extract(mm.payload_json, '$.delegator_instruction_scoped'), '')
FROM mailbox_messages mm
WHERE mm.run_id = ?
  AND mm.from_agent_id = ?
  AND mm.to_agent_id = ?
  AND json_extract(mm.payload_json, '$.communication_type') = 'instruction_dispatched'
  AND json_extract(mm.payload_json, '$.node_id') = ?
ORDER BY mm.message_seq DESC
LIMIT 1`, trimmedRunID, trimmedFromAgentID, trimmedToAgentID, trimmedNodeID).Scan(&instruction)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(instruction.String)
}

func failureReasonLabel(nodeID, nodeClass, reasonCode string) string {
	trimmedReasonCode := strings.ToLower(strings.TrimSpace(reasonCode))
	switch trimmedReasonCode {
	case "execution_review_rework":
		return "Rework Required"
	case "validation_gate_rework":
		return "Validation Rework"
	case "llm_invalid_response", "provider_empty_output", "provider_incomplete_turn":
		if resolvedNodeClass(strings.TrimSpace(nodeID), strings.TrimSpace(nodeClass)) == workflow.NodeClassExecutionReview {
			return "Rework Required"
		}
		if resolvedNodeClass(strings.TrimSpace(nodeID), strings.TrimSpace(nodeClass)) == workflow.NodeClassValidationGates {
			return "Validation Rework"
		}
		return "Action Required"
	default:
		return "Action Required"
	}
}

func humanizedFailureActorName(nodeID string) string {
	trimmed := strings.TrimSpace(nodeID)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "execution_review") {
		return "Reviewer Reviewer"
	}
	if strings.HasPrefix(trimmed, "quality_validation") {
		return "Intelligence QA Validator"
	}
	if strings.HasPrefix(trimmed, "security_validation") {
		return "Intelligence Security Validator"
	}
	if strings.HasPrefix(trimmed, "validation_gates") {
		return "Intelligence Validator"
	}
	return ""
}

func isOwnerFailureReasonCode(reasonCode string) bool {
	if verdict.IsReworkReasonCode(reasonCode) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(reasonCode)) {
	case "llm_invalid_response", "provider_empty_output", "provider_incomplete_turn":
		return true
	default:
		return false
	}
}

func shouldUseErrorAsResponseFallback(nodeID string, err error, reasonCode string) bool {
	if err == nil {
		return false
	}
	reasonCode = firstNonEmptyString(strings.TrimSpace(reasonCode), classifyExecutionReasonCode(err))
	if reasonCode == "provider_incomplete_turn" {
		return !isExecutionOwnerNode(nodeID)
	}
	return !isOwnerFailureReasonCode(reasonCode)
}

func formatFailureContextResponse(nodeID, nodeClass, reasonCode, response string) string {
	trimmedReasonCode := strings.ToLower(strings.TrimSpace(reasonCode))
	trimmedResponse := strings.TrimSpace(sanitizeFailureContextText(stripControlCommandLines(response)))
	if trimmedResponse != "" {
		return trimmedResponse
	}
	if trimmedReasonCode == "llm_invalid_response" || trimmedReasonCode == "provider_empty_output" || trimmedReasonCode == "provider_incomplete_turn" {
		classID := resolvedNodeClass(strings.TrimSpace(nodeID), strings.TrimSpace(nodeClass))
		if classID == workflow.NodeClassExecutionReview || classID == workflow.NodeClassValidationGates {
			return "(empty response)"
		}
	}
	return ""
}

func isExecutionOwnerNode(nodeID string) bool {
	switch nodeClassForNodeID(strings.TrimSpace(nodeID)) {
	case "execution_review", "validation_gates":
		return true
	default:
		return false
	}
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return strings.TrimSpace(value.String)
}
