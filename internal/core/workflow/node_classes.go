package workflow

const CodeInvalidNodeClass = "invalid_node_class"

const (
	NodeClassDecreeIntake                = "decree_intake"
	NodeClassAdvisorConsultation         = "advisor_consultation"
	NodeClassAdvisorySynthesis           = "advisory_synthesis"
	NodeClassPlanDrafting                = "plan_drafting"
	NodeClassKingPlanDecision            = "king_plan_decision"
	NodeClassExecutionKickoff            = "execution_kickoff"
	NodeClassDelegatedExecution          = "delegated_execution"
	NodeClassExecutionReview             = "execution_review"
	NodeClassValidationGates             = "validation_gates"
	NodeClassRoyalClosure                = "royal_closure"
)

var canonicalNodeClasses = []string{
	NodeClassDecreeIntake,
	NodeClassAdvisorConsultation,
	NodeClassAdvisorySynthesis,
	NodeClassPlanDrafting,
	NodeClassKingPlanDecision,
	NodeClassExecutionKickoff,
	NodeClassDelegatedExecution,
	NodeClassExecutionReview,
	NodeClassValidationGates,
	NodeClassRoyalClosure,
}

var canonicalNodeClassSet = func() map[string]struct{} {
	allowed := make(map[string]struct{}, len(canonicalNodeClasses))
	for _, nodeClass := range canonicalNodeClasses {
		allowed[nodeClass] = struct{}{}
	}
	return allowed
}()

func CanonicalNodeClasses() []string {
	return append([]string(nil), canonicalNodeClasses...)
}

func IsCanonicalNodeClass(nodeClass string) bool {
	_, ok := canonicalNodeClassSet[nodeClass]
	return ok
}
