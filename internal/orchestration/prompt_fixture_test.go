package orchestration

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

func projectDecreeForNode(_ string, text string) string {
	return strings.TrimSpace(stripControlCommandLines(text))
}
func joinInstructionBlocks(parts ...string) string {
	var nonempty []string
	for _, part := range parts {
		if text := strings.TrimSpace(part); text != "" {
			nonempty = append(nonempty, text)
		}
	}
	return strings.Join(nonempty, "\n\n"+instructionSectionSeparator+"\n\n")
}

type executionFailureContext struct{ NodeID, NodeClass, ActorName, ReasonCode, Response string }

func formatExecutionFailureContexts(contexts []executionFailureContext) string {
	var parts []string
	for _, item := range contexts {
		if strings.TrimSpace(item.Response) == "" {
			continue
		}
		label := firstNonEmptyString(strings.TrimSpace(item.ActorName), humanizedFailureActorName(item.NodeID), "Unknown Agent")
		parts = append(parts, fmt.Sprintf("%s - %s\n%s", label, failureReasonLabel(item.NodeID, item.NodeClass, item.ReasonCode), strings.TrimSpace(item.Response)))
	}
	return joinInstructionBlocks(parts...)
}

// Independent string oracle for exact section/stripping compatibility tests.
func (o *orchestrationLoop) buildNodePrompt(ctx context.Context, runID, nodeID, nodeRunID, roleID, nodeClass, decreeText, delegatorRoleID, approvedPlan, delegatorInstruction, priorWithholdContext string) string {
	classID := resolvedNodeClass(nodeID, nodeClass)
	frame := o.buildNodePromptFrame(ctx, runID, nodeID, nodeRunID, roleID, nodeClass, delegatorRoleID)
	blocks := []string{renderPromptSection("Current Assignment", frame.Assignment), renderPromptSection("Current Decree", firstNonEmptyString(projectDecreeForNode(classID, decreeText), "No decree text was provided."))}
	if plan := stripControlCommandLines(approvedPlan); plan != "" && classID != workflow.NodeClassDelegatedExecution {
		blocks = append(blocks, renderPromptSection("Approved Plan", plan))
	}
	if guidance := stripControlCommandLines(delegatorInstruction); guidance != "" {
		blocks = append(blocks, renderPromptSection("Upstream Guidance", guidance))
	}
	if frame.Evidence != "" {
		blocks = append(blocks, renderPromptSection("Builder Evidence Summary", frame.Evidence))
	}
	return joinInstructionBlocks(renderPromptSection("System Role", frame.System), renderPromptSection("Kingdom Rules", frame.Rules), renderPromptSection("Run Context", joinPromptBlocks(blocks...)), renderPromptSection("Final Checks", frame.Checks)) + "\n"
}

// Only fixture assertions materialize these replay streams. Keeping these
// helpers out of production prevents accidental return to string composition.
func promptFixtureText(ctx context.Context, doc promptDocument) string {
	file, err := preparePromptDocument(ctx, doc)
	if err != nil {
		return ""
	}
	defer file.Close()
	raw, err := io.ReadAll(io.NewSectionReader(file.File, 0, file.size))
	if err != nil {
		return ""
	}
	return string(raw)
}
func (o *orchestrationLoop) approvedPlanForRun(ctx context.Context, runID string) string {
	doc, err := o.approvedPlanDocument(ctx, runID)
	if err != nil {
		return ""
	}
	return promptFixtureText(ctx, doc)
}
func (o *orchestrationLoop) delegatorInstructionForDispatch(ctx context.Context, runID, nodeID string, attempt int, delegator, delegate string) string {
	return o.delegatorInstructionForDispatchForClass(ctx, runID, nodeID, "", attempt, delegator, delegate)
}
func (o *orchestrationLoop) delegatorInstructionForDispatchForClass(ctx context.Context, runID, nodeID, nodeClass string, attempt int, delegator, delegate string) string {
	a := &promptAssembly{ctx: ctx}
	defer a.close()
	approved, err := o.approvedPlanDocument(ctx, runID)
	if err != nil {
		return ""
	}
	doc, _, err := o.dispatchGuidanceDocument(ctx, runID, nodeID, nodeClass, attempt, delegator, delegate, approved, a)
	if err != nil {
		return ""
	}
	return promptFixtureText(ctx, doc)
}
func (o *orchestrationLoop) advisorSynthesisInstructionForRun(ctx context.Context, runID string) string {
	a := &promptAssembly{ctx: ctx}
	defer a.close()
	doc, err := o.advisorSynthesisDocument(ctx, runID, a)
	if err != nil {
		return ""
	}
	return promptFixtureText(ctx, doc)
}
func (o *orchestrationLoop) latestCouncilPlansForRun(ctx context.Context, runID string) (string, string) {
	s := state.NewRunPlanRevisionStore(o.db)
	current, found, err := s.CurrentRunPlanRevision(ctx, runID)
	if err != nil || !found {
		return "", ""
	}
	var latest, previous strings.Builder
	if err := s.CopyContent(ctx, current, &latest); err != nil {
		return "", ""
	}
	prior, found, err := s.PreviousRunPlanRevision(ctx, runID, current.PlanRevisionID)
	if err == nil && found {
		if err := s.CopyContent(ctx, prior, &previous); err != nil {
			return latest.String(), ""
		}
	}
	return latest.String(), previous.String()
}
func (o *orchestrationLoop) latestExecutionFailureContextsForRun(ctx context.Context, runID string) []executionFailureContext {
	records, err := o.canonicalExecutionStore().ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{RunID: runID, NodeClasses: []string{"execution_review", "validation_gates"}, States: []string{"committed_success", "committed_failure"}})
	if err != nil {
		return nil
	}
	var result []executionFailureContext
	for _, record := range records {
		reason := firstNonEmptyString(nullStringValue(record.OutcomeReasonCode), nullStringValue(record.FailureCode))
		if !isOwnerFailureReasonCode(reason) {
			continue
		}
		text := promptFixtureText(ctx, o.executionOutputDocument(record))
		result = append(result, executionFailureContext{NodeID: record.NodeID, NodeClass: record.NodeClass, ActorName: nullStringValue(record.AgentDisplayName), ReasonCode: reason, Response: formatFailureContextResponse(record.NodeID, record.NodeClass, reason, text)})
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := resolvedNodeClass(result[i].NodeID, result[i].NodeClass), resolvedNodeClass(result[j].NodeID, result[j].NodeClass)
		if a != b {
			if a == "execution_review" {
				return true
			}
			if b == "execution_review" {
				return false
			}
		}
		return result[i].NodeID < result[j].NodeID
	})
	return result
}
