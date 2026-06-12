package acp

import (
	"strings"

	"github.com/google/uuid"
)

const (
	PlannerReplyAgentName         = "planner-reply"
	PlannerRegenerateAgentName    = "planner-regenerate"
	PlannerFormalizeAgentName     = "planner-formalize"
	PlannerGenerateTitleAgentName = "planner-generate-title"

	RunCreatedEventType    = "run.created"
	RunInProgressEventType = "run.in-progress"
	MessagePartEventType   = "message.part"
	MessageDoneEventType   = "message.completed"
	RunCompletedEventType  = "run.completed"
	RunFailedEventType     = "run.failed"
	RunCancelledEventType  = "run.cancelled"
	ErrorEventType         = "error"
	GenericEventType       = "generic"

	KingdomPhaseKind     = "kingdom.phase"
	KingdomToolKind      = "kingdom.tool"
	KingdomReasoningKind = "kingdom.reasoning"
	KingdomProgressKind  = "kingdom.progress"

	PlannerRunStatusCreated    = "created"
	PlannerRunStatusInProgress = "in-progress"
	PlannerRunStatusAwaiting   = "awaiting"
	PlannerRunStatusCompleted  = "completed"
	PlannerRunStatusFailed     = "failed"
	PlannerRunStatusCancelled  = "cancelled"
)

var (
	plannerSessionNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://kingdom.local/acp/planner/session"))
	plannerRunNamespace     = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://kingdom.local/acp/planner/run"))
)

type PlannerAgent struct {
	Name          string
	Title         string
	Description   string
	OperationType string
}

func PlannerAgents() []PlannerAgent {
	return []PlannerAgent{
		{
			Name:          PlannerReplyAgentName,
			Title:         "Planner Reply",
			Description:   "Continue the planning conversation and produce the next assistant reply.",
			OperationType: "reply",
		},
		{
			Name:          PlannerRegenerateAgentName,
			Title:         "Planner Regenerate",
			Description:   "Generate a new assistant variant for an existing planning turn.",
			OperationType: "regenerate",
		},
		{
			Name:          PlannerFormalizeAgentName,
			Title:         "Planner Formalize",
			Description:   "Formalize the current planning session into the working draft.",
			OperationType: "formalize",
		},
		{
			Name:          PlannerGenerateTitleAgentName,
			Title:         "Planner Generate Title",
			Description:   "Generate a session title from the current planning conversation.",
			OperationType: "generate_title",
		},
	}
}

func PlannerAgentByName(name string) (PlannerAgent, bool) {
	trimmed := strings.TrimSpace(name)
	for _, agent := range PlannerAgents() {
		if agent.Name == trimmed {
			return agent, true
		}
	}
	return PlannerAgent{}, false
}

func PlannerAgentNameForOperationType(operationType string) string {
	switch strings.TrimSpace(operationType) {
	case "reply":
		return PlannerReplyAgentName
	case "regenerate":
		return PlannerRegenerateAgentName
	case "formalize":
		return PlannerFormalizeAgentName
	case "generate_title":
		return PlannerGenerateTitleAgentName
	default:
		return ""
	}
}

func PlannerSessionID(planningSessionID string) string {
	trimmed := strings.TrimSpace(planningSessionID)
	if trimmed == "" {
		return ""
	}
	return uuid.NewSHA1(plannerSessionNamespace, []byte(trimmed)).String()
}

func PlannerRunID(operationID string) string {
	trimmed := strings.TrimSpace(operationID)
	if trimmed == "" {
		return ""
	}
	return uuid.NewSHA1(plannerRunNamespace, []byte(trimmed)).String()
}
