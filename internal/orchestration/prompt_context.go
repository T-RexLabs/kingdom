package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/prompts"
	"kingdom/internal/securefs"
	"kingdom/internal/state"
)

type promptAssembly struct {
	ctx   context.Context
	files []*promptFile
}
type opencodePromptOpener func(context.Context) (io.ReadCloser, error)

func (a *promptAssembly) close() {
	for _, file := range a.files {
		_ = file.Close()
	}
}
func (a *promptAssembly) prepare(doc promptDocument) (*promptFile, error) {
	file, err := preparePromptDocument(a.ctx, doc)
	if err == nil {
		a.files = append(a.files, file)
	}
	return file, err
}
func promptPrepared(file *promptFile) promptDocument {
	if file == nil {
		return promptDocument{}
	}
	return promptSource(file.source())
}
func firstPromptFile(files ...*promptFile) *promptFile {
	for _, file := range files {
		if file != nil && file.size > 0 {
			return file
		}
	}
	return nil
}

func (o *orchestrationLoop) approvedPlanDocument(ctx context.Context, runID string) (promptDocument, error) {
	revisions := state.NewRunPlanRevisionStore(o.db)
	review, found, err := state.NewRunPlanReviewStore(o.db).LatestApprovedRunPlanReview(ctx, runID)
	if err != nil {
		return promptDocument{}, err
	}
	if found {
		revision, found, err := revisions.RunPlanRevision(ctx, review.ReviewedPlanRevisionID)
		if err != nil {
			return promptDocument{}, err
		}
		if !found {
			return promptDocument{}, fmt.Errorf("approved plan revision is missing")
		}
		return promptSource(func(ctx context.Context) (io.ReadCloser, error) { return revisions.OpenContent(ctx, revision) }), nil
	}
	var planState string
	if err := o.db.QueryRowContext(ctx, `SELECT plan_state FROM workflow_runs WHERE run_id=?`, runID).Scan(&planState); err != nil {
		return promptDocument{}, err
	}
	if planState != "approved" {
		return promptDocument{}, nil
	}
	revision, found, err := revisions.CurrentRunPlanRevision(ctx, runID)
	if err != nil || !found {
		return promptDocument{}, err
	}
	return promptSource(func(ctx context.Context) (io.ReadCloser, error) { return revisions.OpenContent(ctx, revision) }), nil
}

func (o *orchestrationLoop) executionOutputDocument(record executionstore.RunExecutionOutputRecord) promptDocument {
	if record.OutputTextRef == nil {
		return promptLiteral(nullStringValue(record.OutputText))
	}
	ref := *record.OutputTextRef
	return promptSource(func(ctx context.Context) (io.ReadCloser, error) {
		return o.canonicalExecutionStore().OpenProviderContent(ctx, ref)
	})
}

func (o *orchestrationLoop) advisorSynthesisDocument(ctx context.Context, runID string, a *promptAssembly) (promptDocument, error) {
	records, err := o.canonicalExecutionStore().ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{RunID: runID, NodeClasses: []string{"advisor_consultation"}, States: []string{"committed_success"}})
	if err != nil {
		return promptDocument{}, err
	}
	var responses []promptDocument
	for _, record := range records {
		if strings.TrimSpace(record.NodeID) == "" {
			continue
		}
		body, err := a.prepare(promptFiltered(o.executionOutputDocument(record)))
		if err != nil {
			return promptDocument{}, err
		}
		if body.size == 0 {
			continue
		}
		label := firstNonEmptyString(nullStringValue(record.AgentDisplayName), strings.TrimSpace(record.AgentID), strings.TrimSpace(record.NodeID))
		responses = append(responses, promptSection(label, promptPrepared(body)))
	}
	if len(responses) == 0 {
		return promptDocument{}, nil
	}
	prefix := strings.Join([]string{
		"Synthesize all advisor counsel into a single decision-ready brief for plan development.", "", "Synthesis requirements:",
		"- Treat advisor responses as counsel and evidence, not decisions or approval authority.",
		"- Preserve explicit work order requirements and accepted architecture while using counsel to fill unspecified details and state material assumptions.",
		"- Merge aligned advice into one coherent direction.", "- Call out major tradeoffs and conflicts among advisors.",
		"- Separate executable investigation, safeguards, and verification from genuinely unavailable external dependencies; do not invent approval gates.",
		"- Keep output strategic, concrete, and ready for plan drafting."}, "\n")
	return promptJoin("\n\n", promptLiteral(prefix), promptSection("Advisor Responses", promptInstructions(responses...))), nil
}

func (o *orchestrationLoop) failureContextDocument(ctx context.Context, runID string, a *promptAssembly) (promptDocument, error) {
	records, err := o.canonicalExecutionStore().ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{RunID: runID, NodeClasses: []string{"execution_review", "validation_gates"}, States: []string{"committed_success", "committed_failure"}})
	if err != nil {
		return promptDocument{}, err
	}
	sort.SliceStable(records, func(i, j int) bool {
		left, right := resolvedNodeClass(records[i].NodeID, records[i].NodeClass), resolvedNodeClass(records[j].NodeID, records[j].NodeClass)
		if left != right {
			if left == "execution_review" {
				return true
			}
			if right == "execution_review" {
				return false
			}
		}
		return records[i].NodeID < records[j].NodeID
	})
	var parts []promptDocument
	for _, record := range records {
		reason := firstNonEmptyString(nullStringValue(record.OutcomeReasonCode), nullStringValue(record.FailureCode))
		if !isOwnerFailureReasonCode(reason) {
			continue
		}
		doc := promptFiltered(o.executionOutputDocument(record))
		doc.sanitize = true
		body, err := a.prepare(doc)
		if err != nil {
			return promptDocument{}, err
		}
		response := promptPrepared(body)
		if body.size == 0 {
			response = promptLiteral(formatFailureContextResponse(record.NodeID, record.NodeClass, reason, ""))
			if response.text == "" {
				continue
			}
		}
		label := fmt.Sprintf("%s - %s", firstNonEmptyString(nullStringValue(record.AgentDisplayName), humanizedFailureActorName(record.NodeID), "Unknown Agent"), failureReasonLabel(record.NodeID, record.NodeClass, reason))
		parts = append(parts, promptJoin("\n", promptLiteral(label), response))
	}
	return promptInstructions(parts...), nil
}

func (o *orchestrationLoop) builderInstructionDocument(ctx context.Context, runID string, scoped, approved promptDocument, a *promptAssembly) (promptDocument, error) {
	revisions := state.NewRunPlanRevisionStore(o.db)
	current, found, err := revisions.CurrentRunPlanRevision(ctx, runID)
	if err != nil {
		return promptDocument{}, err
	}
	var latest, previous promptDocument
	if found {
		latest = promptSource(func(ctx context.Context) (io.ReadCloser, error) { return revisions.OpenContent(ctx, current) })
		prior, ok, err := revisions.PreviousRunPlanRevision(ctx, runID, current.PlanRevisionID)
		if err != nil {
			return promptDocument{}, err
		}
		if ok {
			previous = promptSource(func(ctx context.Context) (io.ReadCloser, error) { return revisions.OpenContent(ctx, prior) })
		}
	}
	currentFile, err := a.prepare(promptFiltered(latest))
	if err != nil {
		return promptDocument{}, err
	}
	previousFile, err := a.prepare(promptFiltered(previous))
	if err != nil {
		return promptDocument{}, err
	}
	approvedFile, err := a.prepare(promptFiltered(approved))
	if err != nil {
		return promptDocument{}, err
	}
	primary := firstPromptFile(currentFile, approvedFile)
	baseline := firstPromptFile(previousFile, approvedFile, currentFile)
	primaryDoc := promptPrepared(primary)
	primaryDoc.fallback = "none"
	plans := []promptDocument{promptSection("Primary Rework Plan", primaryDoc)}
	if baseline != nil && (primary == nil || primary.size != baseline.size || primary.sha != baseline.sha) {
		plans = append(plans, promptSection("Approved Plan Baseline", promptPrepared(baseline)))
	}
	failure, err := o.failureContextDocument(ctx, runID, a)
	if err != nil {
		return promptDocument{}, err
	}
	scoped = promptFiltered(scoped)
	scoped.dropComposed = true
	scopedFile, err := a.prepare(scoped)
	if err != nil {
		return promptDocument{}, err
	}
	failureFile, err := a.prepare(failure)
	if err != nil {
		return promptDocument{}, err
	}
	groups := []promptDocument{promptLiteral("Execution directive: prioritize the latest rework plan for implementation updates."), promptJoin("\n\n", plans...)}
	if failureFile.size > 0 || scopedFile.size > 0 {
		groups = append(groups, promptJoin("\n\n", promptLiteral("Address all Failure Context items first."), promptSection("Failure Context", promptPrepared(failureFile)), promptSection("Scoped Delegator Instruction", promptPrepared(scopedFile))))
	}
	return promptInstructions(groups...), nil
}

func (o *orchestrationLoop) dispatchGuidanceDocument(ctx context.Context, runID, nodeID, nodeClass string, attempt int, delegator, delegate string, approved promptDocument, a *promptAssembly) (promptDocument, string, error) {
	class := resolvedNodeClass(nodeID, nodeClass)
	if delegator == "" {
		return promptDocument{}, "", nil
	}
	scoped := strings.TrimSpace(stripControlCommandLines(o.latestScopedInstructionForNode(ctx, runID, nodeID, delegator, delegate)))
	if scoped != "" && class != "advisory_synthesis" && class != "delegated_execution" {
		return promptLiteral(scoped), scoped, nil
	}
	switch class {
	case workflow.NodeClassAdvisorySynthesis:
		advisors, err := o.advisorSynthesisDocument(ctx, runID, a)
		if err != nil {
			return promptDocument{}, "", err
		}
		return promptInstructions(promptLiteral(scoped), promptLiteral(o.operatorReworkFeedbackForAdvisoryAttempt(ctx, runID, attempt)), advisors), scoped, nil
	case workflow.NodeClassPlanDrafting:
		feedback := []promptDocument{promptLiteral(o.operatorPlanReworkFeedbackForPlanAttempt(ctx, runID, attempt)), promptLiteral(o.operatorFinalReworkFeedbackForPlanAttempt(ctx, runID, attempt))}
		output, found, err := o.planningOutputStore().LatestRunPlanningOutput(ctx, runID, state.RunPlanningOutputKindAdvisoryGuidance)
		if err != nil {
			return promptDocument{}, "", err
		}
		if found {
			guidance, err := a.prepare(promptFiltered(promptSource(func(ctx context.Context) (io.ReadCloser, error) {
				return o.planningOutputStore().OpenContent(ctx, output)
			})))
			if err != nil {
				return promptDocument{}, "", err
			}
			if guidance.size > 0 {
				return promptInstructions(append(feedback, promptPrepared(guidance))...), scoped, nil
			}
		}
		upstream := stripControlCommandLines(o.latestScopedInstructionForNode(ctx, runID, "advisory_synthesis", deterministicAgentIDForRole("governance"), delegator))
		return promptInstructions(append(feedback, promptLiteral(upstream))...), scoped, nil
	case workflow.NodeClassDelegatedExecution:
		doc, err := o.builderInstructionDocument(ctx, runID, promptLiteral(scoped), approved, a)
		return doc, scoped, err
	default:
		return promptDocument{}, scoped, nil
	}
}

type dispatchPromptConfiguration struct {
	SystemPrompt string
}

func (o *orchestrationLoop) prepareDispatchPrompt(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, delegator string) (*promptFile, string, dispatchPromptConfiguration, error) {
	a := &promptAssembly{ctx: ctx}
	defer a.close()
	workOrder, err := o.workOrderTextForRun(ctx, run.RunID)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, err
	}
	approved, err := o.approvedPlanDocument(ctx, run.RunID)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, err
	}
	guidance, scoped, err := o.dispatchGuidanceDocument(ctx, run.RunID, node.NodeID, node.NodeClass, attempt.Attempt, delegator, attempt.AgentID, approved, a)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, err
	}
	prepared, err := a.prepare(guidance)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, err
	}
	guidance = promptPrepared(prepared)
	agentConfigurationSnapshot, err := state.LoadRunAgentConfigurationSnapshot(ctx, o.db, run.RunID)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, fmt.Errorf("load run agent configuration snapshot: %w", err)
	}
	roleConfiguration := agentConfigurationSnapshot.ForRole(node.RequiredRole)
	behavior := workflow.BehaviorForNode(node.NodeID, node.NodeClass)
	systemPrompt, err := agentConfigurationSnapshot.PromptCatalog.ExecutionSystemPrompt(prompts.ExecutionSystemPromptInput{
		RoleID: node.RequiredRole, NodeClass: resolvedNodeClass(node.NodeID, node.NodeClass),
		CommunicationStyle: roleConfiguration.Voice.Instruction, AgentGuidance: roleConfiguration.GuidanceText,
	})
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, fmt.Errorf("compose run-pinned agent system prompt: %w", err)
	}
	finalChecks := agentConfigurationSnapshot.PromptCatalog.FinalChecksSection(
		resolvedNodeClass(node.NodeID, node.NodeClass),
		behavior.ProviderCompletionMode == workflow.ProviderCompletionModeServerResponse,
	)
	doc := o.buildNodeContextDocument(ctx, run.RunID, node.NodeID, node.NodeClass, o.roleIDForAgent(ctx, delegator), finalChecks, promptSource(workOrder), approved, guidance)
	file, err := preparePromptDocument(ctx, doc)
	if err != nil {
		return nil, "", dispatchPromptConfiguration{}, err
	}
	// The streamed context framing ends with one newline.
	if _, err := file.WriteString("\n"); err != nil {
		file.Close()
		return nil, "", dispatchPromptConfiguration{}, err
	}
	file.size++
	h := sha256.New()
	if _, err := securefs.Copy(ctx, h, io.NewSectionReader(file.File, 0, file.size)); err != nil {
		file.Close()
		return nil, "", dispatchPromptConfiguration{}, err
	}
	file.sha = hex.EncodeToString(h.Sum(nil))
	return file, scoped, dispatchPromptConfiguration{SystemPrompt: systemPrompt}, nil
}
