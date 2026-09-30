package rolecomm

import "strings"

// NeutralStyleID identifies the sole execution voice. Presentation themes never
// enter this package or prompt resolution.
const NeutralStyleID = "neutral-v1"

// NeutralInstruction is shared by interactive planning and workflow execution.
func NeutralInstruction() string {
	return "Communicate in clear, concise, neutral language. Preserve the assigned responsibilities, evidence requirements, output schemas, and exact control lines. Do not adopt a ceremonial or themed persona."
}

// ResolveTemplate selects responsibility instructions under the neutral voice.
func ResolveTemplate(policy Policy, roleID string) RoleTemplate {
	selected := policy.DefaultRoleTemplate
	if configured, ok := policy.Roles[strings.TrimSpace(roleID)]; ok {
		selected = configured
	}
	selected.SystemPromptTemplate = NeutralInstruction() + "\n\n" + selected.SystemPromptTemplate
	return selected
}
