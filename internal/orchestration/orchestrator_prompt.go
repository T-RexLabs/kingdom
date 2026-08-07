package orchestration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/verdict"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

var controlCommandLinePattern = regexp.MustCompile(`(?i)^\s*(ROYAL_VERDICT|VERDICT)\s*:`)
var controlTokenLiteralPattern = regexp.MustCompile(`(?i)\b(ROYAL_VERDICT|VERDICT)\s*:`)

func (o *orchestrationLoop) decreeTextForRun(ctx context.Context, runID string) (string, error) {
	var decreeText string
	err := o.db.QueryRowContext(ctx, `
SELECT d.decree_text
FROM workflow_runs wr
JOIN decrees d ON d.decree_id = wr.decree_id
WHERE wr.run_id = ?`, runID).Scan(&decreeText)
	if err != nil {
		return "", err
	}
	return decreeText, nil
}

func (o *orchestrationLoop) decreeTitleForRun(ctx context.Context, runID string) (string, error) {
	var decreeTitle string
	err := o.db.QueryRowContext(ctx, `
SELECT d.decree_title
FROM workflow_runs wr
JOIN decrees d ON d.decree_id = wr.decree_id
WHERE wr.run_id = ?`, runID).Scan(&decreeTitle)
	if err != nil {
		return "", err
	}
	return decreeTitle, nil
}

func (o *orchestrationLoop) buildNodePrompt(ctx context.Context, runID, nodeID, nodeRunID, roleID, nodeClass, decreeText, delegatorRoleID, approvedPlan, delegatorInstruction, priorWithholdContext string) string {
	classID := resolvedNodeClass(nodeID, nodeClass)
	projectedDecree := projectDecreeForNode(classID, decreeText)
	approvedPlan = stripControlCommandLines(approvedPlan)
	delegatorInstruction = stripControlCommandLines(delegatorInstruction)
	systemPreamble := o.renderSystemPrompt(roleID, runID, nodeID, nodeRunID, projectedDecree)
	planningConstraints := ""
	if classID == "plan_drafting" {
		planningConstraints = strings.Join([]string{
			"- Return planning output only: plan deltas, acceptance criteria, dependency notes, and verification scope.",
			"- Do not claim completed implementation work.",
			"- Do not present code changes, delivered files, or test execution as finished work.",
			"- Resolve unspecified implementation details using repository evidence and, when correctness depends on current external facts, authoritative external sources. State material assumptions.",
			"- Treat investigation, design, security hardening, implementation, and verification as executable plan work rather than approval gates.",
			"- Preserve gates required by the decree, runtime policy, or accepted architecture, but do not invent internal ratification. If an unavailable external dependency affects one slice, isolate it and plan all independent work.",
		}, "\n")
	}
	responseOnlyConstraints := ""
	if classID == workflow.NodeClassAdvisorConsultation || classID == workflow.NodeClassAdvisorySynthesis || classID == workflow.NodeClassPlanDrafting {
		responseOnlyConstraints = strings.Join([]string{
			"- You may inspect current project files when needed.",
			"- Do not create, edit, rename, or delete project files for this node.",
			"- Return advice, synthesis, or planning output only.",
			"- Implementation changes belong to delegated execution after plan approval.",
		}, "\n")
	}
	protectedRuntimePaths := ""
	if classID == workflow.NodeClassDelegatedExecution || classID == workflow.NodeClassExecutionReview || classID == workflow.NodeClassValidationGates {
		protectedRuntimePaths = strings.Join([]string{
			"- Treat `.kingdom/` and `kingdom-artifacts/` as Kingdom-managed runtime/operator state, not decree deliverables.",
			"- Ignore those paths when checking whether only the intended project files changed, unless the decree explicitly targets them.",
			"- Never delete, modify, or \"clean up\" those runtime-managed paths as part of decree work or rework.",
		}, "\n")
	}
	responseContract := nodeResponseContractForClass(classID)
	rulesBlocks := []string{}
	if trimmedConstraints := strings.TrimSpace(planningConstraints); trimmedConstraints != "" {
		rulesBlocks = append(rulesBlocks, renderPromptSection("Planning Constraints", trimmedConstraints))
	}
	if trimmedConstraints := strings.TrimSpace(responseOnlyConstraints); trimmedConstraints != "" {
		rulesBlocks = append(rulesBlocks, renderPromptSection("Response-Only Constraints", trimmedConstraints))
	}
	if trimmedConstraints := strings.TrimSpace(protectedRuntimePaths); trimmedConstraints != "" {
		rulesBlocks = append(rulesBlocks, renderPromptSection("Protected Runtime Paths", trimmedConstraints))
	}
	if responseContract != "" {
		rulesBlocks = append(rulesBlocks, renderPromptSection("Response Format Requirements", responseContract))
	}

	runContextBlocks := []string{
		renderPromptSection("Current Assignment", currentAssignmentForNodeClass(classID, delegatorRoleID)),
		renderPromptSection("Current Decree", firstNonEmptyString(strings.TrimSpace(projectedDecree), "No decree text was provided.")),
	}
	if trimmedPlan := strings.TrimSpace(approvedPlan); trimmedPlan != "" && classID != workflow.NodeClassDelegatedExecution {
		runContextBlocks = append(runContextBlocks, renderPromptSection("Approved Plan", trimmedPlan))
	}
	if trimmedInstruction := strings.TrimSpace(delegatorInstruction); trimmedInstruction != "" {
		runContextBlocks = append(runContextBlocks, renderPromptSection("Upstream Guidance", trimmedInstruction))
	}
	if classID == "execution_review" || classID == "validation_gates" {
		if builderEvidence := o.builderEvidenceManifestForRun(ctx, runID); strings.TrimSpace(builderEvidence) != "" {
			runContextBlocks = append(runContextBlocks, renderPromptSection("Builder Evidence Summary", builderEvidence))
		}
	}
	rulesBody := joinPromptBlocks(rulesBlocks...)
	if strings.TrimSpace(rulesBody) == "" {
		rulesBody = "- No additional node-specific rules."
	}

	_ = priorWithholdContext
	sections := []string{
		renderPromptSection("System Role", systemPreamble),
		renderPromptSection("Kingdom Rules", rulesBody),
		renderPromptSection("Run Context", joinPromptBlocks(runContextBlocks...)),
		renderPromptSection("Final Checks", finalChecksForNodeClass(classID)),
	}
	return joinInstructionBlocks(sections...) + "\n"
}

func finalChecksForNodeClass(nodeClass string) string {
	lines := []string{
		"- Call `kingdom_complete_attempt` exactly once when your final response is complete.",
		"- Put the full final canonical response in `final_text`.",
		"- Do not rely on plain assistant text, stream endings, or process exit to finish the node.",
		"- If your response is not final yet, do not call the tool.",
	}
	if strings.TrimSpace(nodeClass) == workflow.NodeClassExecutionReview || strings.TrimSpace(nodeClass) == workflow.NodeClassValidationGates {
		lines = append(lines, "- Your first non-empty line in `final_text` must be `VERDICT: PASS` or `VERDICT: REWORK`.")
	}
	return strings.Join(lines, "\n")
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
	case workflow.NodeClassDecreeIntake:
		return "Decree Intake"
	case workflow.NodeClassAdvisorConsultation:
		return "Advisory Consultation"
	case workflow.NodeClassAdvisorySynthesis:
		return "Advisory Synthesis"
	case workflow.NodeClassPlanDrafting:
		return "Plan Drafting"
	case workflow.NodeClassKingPlanDecision:
		return "King Plan Decision"
	case workflow.NodeClassExecutionKickoff:
		return "Execution Kickoff"
	case workflow.NodeClassDelegatedExecution:
		return "Delegated Execution"
	case workflow.NodeClassExecutionReview:
		return "Execution Review"
	case workflow.NodeClassValidationGates:
		return "Validation Gates"
	case workflow.NodeClassRoyalClosure:
		return "Royal Closure"
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

func renderPromptSection(title, body string) string {
	trimmedBody := strings.TrimSpace(body)
	if trimmedBody == "" {
		return ""
	}
	return fmt.Sprintf("%s:\n%s", strings.TrimSpace(title), trimmedBody)
}

func joinPromptBlocks(blocks ...string) string {
	trimmed := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if section := strings.TrimSpace(block); section != "" {
			trimmed = append(trimmed, section)
		}
	}
	return strings.Join(trimmed, "\n\n")
}

func nodeResponseContractForClass(nodeClass string) string {
	switch strings.TrimSpace(nodeClass) {
	case "execution_review":
		return strings.Join([]string{
			"- You are issuing the final review outcome for this node now, not a progress update.",
			"- RequiredControlLine: VERDICT: PASS|REWORK",
			"- The first non-empty line must be exactly: VERDICT: PASS or VERDICT: REWORK.",
			"- Do not place any text before the verdict line.",
			"- If implementation evidence is missing, incomplete, or uncertain, choose VERDICT: REWORK.",
			"- After the verdict line, give concise rationale and actionable rework guidance when needed.",
		}, "\n")
	case "validation_gates":
		return strings.Join([]string{
			"- You are issuing the final validation outcome for this node now, not a progress update.",
			"- RequiredControlLine: VERDICT: PASS|REWORK",
			"- The first non-empty line must be exactly: VERDICT: PASS or VERDICT: REWORK.",
			"- Do not place any text before the verdict line.",
			"- If any requirement remains blocked, unverified, or uncertain, choose VERDICT: REWORK.",
			"- After the verdict line, give concise rationale and include blocking gates or fix items when useful.",
		}, "\n")
	default:
		return ""
	}
}

func projectDecreeForNode(nodeClass, decreeText string) string {
	_ = nodeClass
	return strings.TrimSpace(stripControlCommandLines(decreeText))
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

func (o *orchestrationLoop) renderSystemPrompt(roleID, runID, nodeID, nodeRunID, decreeText string) string {
	rendered := strings.TrimSpace(renderRoleTemplateOrFallback(
		o.roleComm,
		roleID,
		func(cfg rolecomm.RoleTemplate) string { return cfg.SystemPromptTemplate },
		map[string]string{
			"RoleID":     strings.TrimSpace(roleID),
			"RunID":      strings.TrimSpace(runID),
			"NodeID":     strings.TrimSpace(nodeID),
			"NodeRunID":  strings.TrimSpace(nodeRunID),
			"DecreeText": strings.TrimSpace(decreeText),
		},
		"You are a Kingdom agent. Execute your assigned role with discipline and return valid output.",
	))
	if rendered == "" {
		return "You are a Kingdom agent. Execute your assigned role with discipline and return valid output."
	}
	return rendered
}

func renderRoleTemplateOrFallback(policy rolecomm.Policy, roleID string, extract func(rolecomm.RoleTemplate) string, data map[string]string, fallback string) string {
	if extract == nil {
		return fallback
	}
	selected := policy.DefaultRoleTemplate
	if templateConfig, ok := policy.Roles[strings.TrimSpace(roleID)]; ok {
		selected = templateConfig
	}

	rawTemplate := strings.TrimSpace(extract(selected))
	if rawTemplate == "" {
		return fallback
	}

	tmpl, err := template.New("role_prompt").Option("missingkey=zero").Parse(rawTemplate)
	if err != nil {
		return fallback
	}

	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, data); err != nil {
		return fallback
	}

	rendered := strings.TrimSpace(buffer.String())
	if rendered == "" {
		return fallback
	}
	return rendered
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

func (o *orchestrationLoop) approvedPlanForRun(ctx context.Context, runID string) string {
	trimmedRunID := strings.TrimSpace(runID)
	if o == nil || o.db == nil || trimmedRunID == "" {
		return ""
	}
	revisionStore := state.NewRunPlanRevisionStore(o.db)
	reviewStore := state.NewRunPlanReviewStore(o.db)
	approvedReview, ok, err := reviewStore.LatestApprovedRunPlanReview(ctx, trimmedRunID)
	if err == nil && ok {
		revision, revisionOK, revisionErr := revisionStore.RunPlanRevision(ctx, approvedReview.ReviewedPlanRevisionID)
		if revisionErr == nil && revisionOK {
			return revision.ContentMarkdown
		}
		return ""
	}

	var planState string
	if err := o.db.QueryRowContext(ctx,
		`SELECT plan_state
		 FROM workflow_runs
		 WHERE run_id = ?
		 LIMIT 1`,
		trimmedRunID,
	).Scan(&planState); err != nil {
		return ""
	}
	if strings.TrimSpace(planState) != "approved" {
		return ""
	}
	revision, ok := state.RunPlanRevision{}, false
	revision, ok, err = revisionStore.CurrentRunPlanRevision(ctx, trimmedRunID)
	if err != nil || !ok {
		return ""
	}
	return revision.ContentMarkdown
}

func (o *orchestrationLoop) delegatorInstructionForDispatch(ctx context.Context, runID, nodeID string, attempt int, delegatorAgentID, delegateAgentID string) string {
	return o.delegatorInstructionForDispatchForClass(ctx, runID, nodeID, "", attempt, delegatorAgentID, delegateAgentID)
}

func (o *orchestrationLoop) delegatorInstructionForDispatchForClass(ctx context.Context, runID, nodeID, nodeClass string, attempt int, delegatorAgentID, delegateAgentID string) string {
	trimmedRunID := strings.TrimSpace(runID)
	trimmedNodeID := strings.TrimSpace(nodeID)
	trimmedDelegatorAgentID := strings.TrimSpace(delegatorAgentID)
	trimmedDelegateAgentID := strings.TrimSpace(delegateAgentID)
	if o == nil || o.db == nil || trimmedRunID == "" || trimmedNodeID == "" || trimmedDelegatorAgentID == "" {
		return ""
	}
	classID := resolvedNodeClass(trimmedNodeID, nodeClass)

	scopedInstruction := ""
	var instruction sql.NullString
	if trimmedDelegateAgentID != "" {
		query := `
SELECT COALESCE(json_extract(mm.payload_json, '$.delegator_instruction_scoped'), '')
FROM mailbox_messages mm
WHERE mm.run_id = ?
  AND mm.from_agent_id = ?
  AND mm.to_agent_id = ?
  AND json_extract(mm.payload_json, '$.communication_type') = 'instruction_dispatched'
  AND json_extract(mm.payload_json, '$.node_id') = ?`
		args := []any{trimmedRunID, trimmedDelegatorAgentID, trimmedDelegateAgentID, trimmedNodeID}
		query += `
ORDER BY mm.message_seq DESC
	LIMIT 1`
		err := o.db.QueryRowContext(ctx, query, args...).Scan(&instruction)
		if err == nil {
			scopedInstruction = strings.TrimSpace(stripControlCommandLines(instruction.String))
			if scopedInstruction != "" && classID != "advisory_synthesis" && classID != "delegated_execution" {
				return scopedInstruction
			}
		}
	}
	if classID == "advisory_synthesis" {
		return mergeDelegatorInstructionBlocks(scopedInstruction, o.allfatherReworkFeedbackForAdvisoryAttempt(ctx, trimmedRunID, attempt), o.advisorSynthesisInstructionForRun(ctx, trimmedRunID))
	}
	if classID == "advisor_consultation" {
		return ""
	}
	if classID == "plan_drafting" {
		planReworkFeedback := o.allfatherPlanReworkFeedbackForPlanAttempt(ctx, trimmedRunID, attempt)
		finalReworkFeedback := o.allfatherFinalReworkFeedbackForPlanAttempt(ctx, trimmedRunID, attempt)
		planningResponse := ""
		if guidance, ok, err := o.planningOutputStore().LatestRunPlanningOutput(ctx, trimmedRunID, state.RunPlanningOutputKindAdvisoryGuidance); err == nil && ok {
			planningResponse = stripControlCommandLines(guidance.ContentMarkdown)
		}
		if strings.TrimSpace(planningResponse) != "" {
			return mergeDelegatorInstructionBlocks(planReworkFeedback, finalReworkFeedback, planningResponse)
		}
		upstreamInstruction := stripControlCommandLines(o.latestScopedInstructionForNode(ctx, trimmedRunID, "advisory_synthesis", deterministicAgentIDForRole("king"), trimmedDelegatorAgentID))
		return mergeDelegatorInstructionBlocks(planReworkFeedback, finalReworkFeedback, firstNonEmptyString(strings.TrimSpace(upstreamInstruction), planningSafeInstructionFallback(classID)))
	}
	if classID == "delegated_execution" {
		return o.builderExecutionInstructionForRun(ctx, trimmedRunID, scopedInstruction)
	}
	return ""
}

func (o *orchestrationLoop) allfatherReworkFeedbackForAdvisoryAttempt(ctx context.Context, runID string, attempt int) string {
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
	return fmt.Sprintf("Allfather Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) allfatherPlanReworkFeedbackForPlanAttempt(ctx context.Context, runID string, attempt int) string {
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
	return fmt.Sprintf("Allfather Plan Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) allfatherFinalReworkFeedbackForPlanAttempt(ctx context.Context, runID string, attempt int) string {
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
	return fmt.Sprintf("Allfather Final Rework Feedback:\n%s", feedback)
}

func (o *orchestrationLoop) builderExecutionInstructionForRun(ctx context.Context, runID, scopedInstruction string) string {
	latestPlan, baselinePlan := o.latestCouncilPlansForRun(ctx, runID)
	failureContext := o.latestExecutionFailureContextsForRun(ctx, runID)

	primary := firstNonEmptyString(stripControlCommandLines(latestPlan), stripControlCommandLines(o.approvedPlanForRun(ctx, runID)))
	baseline := firstNonEmptyString(stripControlCommandLines(baselinePlan), stripControlCommandLines(o.approvedPlanForRun(ctx, runID)), stripControlCommandLines(latestPlan))
	formattedFailureContext := formatExecutionFailureContexts(failureContext)

	planningSections := []string{
		fmt.Sprintf("Primary Council Rework Plan:\n%s", firstNonEmptyString(strings.TrimSpace(primary), "none")),
	}
	if baselineText := strings.TrimSpace(baseline); baselineText != "" && strings.TrimSpace(primary) != baselineText {
		planningSections = append(planningSections, fmt.Sprintf("Approved Plan Baseline:\n%s", baselineText))
	}
	reworkSections := make([]string, 0, 2)
	if trimmedFailureContext := strings.TrimSpace(formattedFailureContext); trimmedFailureContext != "" {
		reworkSections = append(reworkSections, fmt.Sprintf("Failure Context:\n%s", trimmedFailureContext))
	}
	trimmedScopedInstruction := sanitizeDelegatedExecutionScopedInstruction(scopedInstruction)
	if trimmedScopedInstruction != "" {
		reworkSections = append(reworkSections, fmt.Sprintf("Scoped Delegator Instruction:\n%s", trimmedScopedInstruction))
	}
	groups := []string{
		"Execution directive: prioritize the latest council rework plan for implementation updates.",
		strings.Join(planningSections, "\n\n"),
	}
	if len(reworkSections) > 0 {
		failurePriorityBlock := append([]string{"Address all Failure Context items first."}, reworkSections...)
		groups = append(groups, strings.Join(failurePriorityBlock, "\n\n"))
	}
	return joinInstructionBlocks(groups...)
}

func sanitizeDelegatedExecutionScopedInstruction(scopedInstruction string) string {
	trimmed := strings.TrimSpace(stripControlCommandLines(scopedInstruction))
	if trimmed == "" {
		return ""
	}
	for _, marker := range []string{
		"Primary Council Rework Plan:",
		"Approved Plan Baseline:",
		"Failure Context:",
		"Scoped Delegator Instruction:",
	} {
		if strings.Contains(trimmed, marker) {
			return ""
		}
	}
	return trimmed
}

func (o *orchestrationLoop) latestCouncilPlansForRun(ctx context.Context, runID string) (string, string) {
	trimmedRunID := strings.TrimSpace(runID)
	if o == nil || o.db == nil || trimmedRunID == "" {
		return "", ""
	}
	revisionStore := state.NewRunPlanRevisionStore(o.db)
	current, ok, err := revisionStore.CurrentRunPlanRevision(ctx, trimmedRunID)
	if err != nil || !ok {
		return "", ""
	}
	baseline, baselineOK, err := revisionStore.PreviousRunPlanRevision(ctx, trimmedRunID, current.PlanRevisionID)
	if err != nil || !baselineOK {
		return current.ContentMarkdown, ""
	}
	return current.ContentMarkdown, baseline.ContentMarkdown
}

func planningSafeInstructionFallback(nodeClass string) string {
	if nodeClass == "plan_drafting" {
		return "Return a planning-only output: scoped execution delta, dependencies, acceptance criteria, and verification checkpoints. Do not claim completed implementation."
	}
	return "Return planning-only instructions and acceptance criteria."
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

type executionFailureContext struct {
	NodeID     string
	NodeClass  string
	ActorName  string
	ReasonCode string
	Response   string
}

func formatExecutionFailureContexts(contexts []executionFailureContext) string {
	if len(contexts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(contexts))
	for _, item := range contexts {
		nodeID := strings.TrimSpace(item.NodeID)
		nodeClass := strings.TrimSpace(item.NodeClass)
		actorName := strings.TrimSpace(item.ActorName)
		reasonCode := strings.TrimSpace(item.ReasonCode)
		response := strings.TrimSpace(item.Response)
		if response == "" {
			continue
		}
		header := firstNonEmptyString(actorName, humanizedFailureActorName(nodeID), "Unknown Agent")
		header = fmt.Sprintf("%s - %s", header, failureReasonLabel(nodeID, nodeClass, reasonCode))
		parts = append(parts, fmt.Sprintf("%s\n%s", header, response))
	}
	return joinInstructionBlocks(parts...)
}

func (o *orchestrationLoop) latestExecutionFailureContextsForRun(ctx context.Context, runID string) []executionFailureContext {
	trimmedRunID := strings.TrimSpace(runID)
	store := o.canonicalExecutionStore()
	if store == nil || trimmedRunID == "" {
		return nil
	}

	records, err := store.ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{
		RunID:       trimmedRunID,
		NodeClasses: []string{"execution_review", "validation_gates"},
		States:      []string{"committed_success", "committed_failure"},
	})
	if err != nil {
		return nil
	}

	contexts := make([]executionFailureContext, 0)
	for _, record := range records {
		trimmedNodeID := strings.TrimSpace(record.NodeID)
		trimmedReasonCode := firstNonEmptyString(nullStringValue(record.OutcomeReasonCode), nullStringValue(record.FailureCode))
		if !isOwnerFailureReasonCode(trimmedReasonCode) {
			continue
		}
		trimmedResponse := formatFailureContextResponse(trimmedNodeID, record.NodeClass, trimmedReasonCode, nullStringValue(record.OutputText))
		contexts = append(contexts, executionFailureContext{
			NodeID:     trimmedNodeID,
			NodeClass:  strings.TrimSpace(record.NodeClass),
			ActorName:  strings.TrimSpace(nullStringValue(record.AgentDisplayName)),
			ReasonCode: trimmedReasonCode,
			Response:   trimmedResponse,
		})
	}
	if len(contexts) == 0 {
		return nil
	}
	sort.SliceStable(contexts, func(i, j int) bool {
		leftClass := resolvedNodeClass(contexts[i].NodeID, contexts[i].NodeClass)
		rightClass := resolvedNodeClass(contexts[j].NodeID, contexts[j].NodeClass)
		if leftClass != rightClass {
			if leftClass == "execution_review" {
				return true
			}
			if rightClass == "execution_review" {
				return false
			}
		}
		return contexts[i].NodeID < contexts[j].NodeID
	})
	return contexts
}

func failureReasonLabel(nodeID, nodeClass, reasonCode string) string {
	trimmedReasonCode := strings.ToLower(strings.TrimSpace(reasonCode))
	switch trimmedReasonCode {
	case "execution_review_rework", "invalid_execution_review_verdict":
		return "Rework Required"
	case "validation_gate_rework", "invalid_validation_verdict":
		return "Validation Rework"
	case "provider_empty_output", "provider_incomplete_turn":
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
		return "Reviewer Shieldmaiden"
	}
	if strings.HasPrefix(trimmed, "validation_gates_qa_scout") {
		return "Intelligence QA Scout"
	}
	if strings.HasPrefix(trimmed, "validation_gates_nightraven") {
		return "Intelligence Nightraven"
	}
	if strings.HasPrefix(trimmed, "validation_gates") {
		return "Intelligence Validator"
	}
	return ""
}

func isOwnerFailureReasonCode(reasonCode string) bool {
	switch strings.ToLower(strings.TrimSpace(reasonCode)) {
	case "execution_review_rework", "validation_gate_rework", "invalid_validation_verdict", "invalid_execution_review_verdict", "provider_empty_output", "provider_incomplete_turn", "execution_not_started":
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
	if trimmedReasonCode == "provider_empty_output" || trimmedReasonCode == "provider_incomplete_turn" || trimmedReasonCode == "invalid_validation_verdict" || trimmedReasonCode == "invalid_execution_review_verdict" || trimmedReasonCode == "execution_not_started" {
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

func mergeDelegatorInstructionBlocks(parts ...string) string {
	trimmedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		trimmedParts = append(trimmedParts, trimmed)
	}
	return joinInstructionBlocks(trimmedParts...)
}

func (o *orchestrationLoop) advisorSynthesisInstructionForRun(ctx context.Context, runID string) string {
	trimmedRunID := strings.TrimSpace(runID)
	store := o.canonicalExecutionStore()
	if store == nil || trimmedRunID == "" {
		return ""
	}

	records, err := store.ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{
		RunID:       trimmedRunID,
		NodeClasses: []string{"advisor_consultation"},
		States:      []string{"committed_success"},
	})
	if err != nil {
		return ""
	}

	type advisorInput struct {
		nodeID       string
		responseText string
		agentName    string
		agentID      string
	}
	inputs := make([]advisorInput, 0)
	for _, record := range records {
		input := advisorInput{
			nodeID:       strings.TrimSpace(record.NodeID),
			responseText: strings.TrimSpace(stripControlCommandLines(nullStringValue(record.OutputText))),
			agentName:    strings.TrimSpace(nullStringValue(record.AgentDisplayName)),
			agentID:      strings.TrimSpace(record.AgentID),
		}
		if input.nodeID == "" {
			continue
		}
		if input.responseText == "" {
			continue
		}
		inputs = append(inputs, input)
	}
	if len(inputs) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.WriteString("Synthesize all advisor counsel into a single decision-ready brief for council deliberation.\n\n")
	builder.WriteString("Synthesis requirements:\n")
	builder.WriteString("- Treat advisor responses as counsel and evidence, not decisions or approval authority.\n")
	builder.WriteString("- Preserve explicit decree requirements and accepted architecture while using counsel to fill unspecified details and state material assumptions.\n")
	builder.WriteString("- Merge aligned advice into one coherent direction.\n")
	builder.WriteString("- Call out major tradeoffs and conflicts among advisors.\n")
	builder.WriteString("- Separate executable investigation, safeguards, and verification from genuinely unavailable external dependencies; do not invent approval gates.\n")
	builder.WriteString("- Keep output strategic, concrete, and ready for council planning.\n\n")
	builder.WriteString("Advisor Responses:\n")
	for idx, input := range inputs {
		if idx > 0 {
			builder.WriteString(instructionSectionSeparator)
			builder.WriteString("\n\n")
		}
		label := firstNonEmptyString(strings.TrimSpace(input.agentName), strings.TrimSpace(input.agentID), input.nodeID)
		builder.WriteString(fmt.Sprintf("%s:\n", label))
		builder.WriteString(input.responseText)
		builder.WriteString("\n\n")
	}

	return strings.TrimSpace(builder.String())
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return strings.TrimSpace(value.String)
}

func joinInstructionBlocks(parts ...string) string {
	trimmed := make([]string, 0, len(parts))
	for _, part := range parts {
		if section := strings.TrimSpace(part); section != "" {
			trimmed = append(trimmed, section)
		}
	}
	if len(trimmed) == 0 {
		return ""
	}
	return strings.Join(trimmed, "\n\n"+instructionSectionSeparator+"\n\n")
}

func defaultDelegatorInstructionForDispatch(nodeID, roleID, decreeText string) string {
	if nodeClassForNodeID(nodeID) != "advisor_consultation" {
		return ""
	}
	decreeSummary := strings.TrimSpace(decreeText)
	decreeSummary = strings.TrimSpace(stripControlCommandLines(decreeSummary))
	if decreeSummary == "" {
		decreeSummary = "No decree text was provided."
	}
	roleLens := strings.TrimSpace(roleID)
	if roleLens == "" {
		roleLens = "advisor"
	}
	lensGuidance := "Focus on the strongest recommendation, why it follows from your role's lens, and the most important risks and tradeoffs."
	switch roleLens {
	case "noble-delegate":
		lensGuidance = "Focus on whether the decree satisfies the requested behavior, fits the existing architecture and public contracts, and avoids harmful precedent or system drift."
	case "clergy-delegate":
		lensGuidance = "Focus on trust boundaries, abuse resistance, blast radius, recovery posture, and any residual risk that remains after mitigation."
	case "burgher-delegate":
		lensGuidance = "Focus on performance under load, scalability as demand grows, resource efficiency, likely bottlenecks, and long-term maintainability."
	case "peasant-delegate":
		lensGuidance = "Focus on usability, operator burden, support burden, day-2 friction, and what ordinary users or operators can realistically sustain."
	}
	return fmt.Sprintf(`Provide advisory counsel for this decree through the %s lens.

- Return advice only. Do not claim implementation work, code edits, or produced artifacts.
- Keep the response freeform but decision-ready: make a recommendation, explain it through your role's lens, and call out the most important risks and tradeoffs.
- %s
- If key evidence is missing, say what uncertainty materially affects the recommendation.

Decree focus:
%s`, roleLens, lensGuidance, decreeSummary)
}
