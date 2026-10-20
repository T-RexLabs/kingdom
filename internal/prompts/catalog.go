// Package prompts owns authored role instructions and protected prompt
// contracts. Its embedded catalog is the single source used by runtime
// prompt construction and immutable resource snapshots.
package prompts

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

//go:embed catalog/*.yaml
var catalogFiles embed.FS

type Catalog struct {
	Version       int                     `json:"version"`
	RolePolicy    RoleCommunicationPolicy `json:"role_policy"`
	NodeContracts map[string]NodeContract `json:"node_contracts"`
	PlanningModes PlanningModes           `json:"planning_modes"`
	NeutralStyle  string                  `json:"neutral_style"`
	StylePreamble string                  `json:"style_preamble"`
}

type RoleCommunicationPolicy struct {
	Version                string                    `yaml:"version" json:"version"`
	PolicyID               string                    `yaml:"policy_id" json:"policy_id"`
	DefaultMailboxTemplate MailboxTemplate           `yaml:"default_mailbox_template" json:"default_mailbox_template"`
	Roles                  map[string]RoleDefinition `yaml:"roles" json:"roles"`
}

type RoleDefinition struct {
	ProtectedInstructionTemplate string          `yaml:"protected_instruction_template" json:"protected_instruction_template"`
	DefaultGuidance              string          `yaml:"default_guidance" json:"default_guidance"`
	MailboxTemplate              MailboxTemplate `yaml:"mailbox_template" json:"mailbox_template"`
}

type MailboxTemplate struct {
	SalutationTemplate string `yaml:"salutation_template" json:"salutation_template"`
	BodyTemplate       string `yaml:"body_template" json:"body_template"`
	SignoffTemplate    string `yaml:"signoff_template" json:"signoff_template"`
	SignatureTemplate  string `yaml:"signature_template" json:"signature_template"`
}

type NodeContract struct {
	AuthorityAndEvidence []string `yaml:"authority_and_evidence" json:"authority_and_evidence"`
	PlanningConstraints  []string `yaml:"planning_constraints" json:"planning_constraints"`
	ResponseOnly         []string `yaml:"response_only" json:"response_only"`
	AdvisorResponse      []string `yaml:"advisor_response" json:"advisor_response"`
	ProtectedPaths       []string `yaml:"protected_paths" json:"protected_paths"`
	InspectionWorkspace  []string `yaml:"inspection_workspace" json:"inspection_workspace"`
	ResponseFormat       []string `yaml:"response_format" json:"response_format"`
	FinalChecks          []string `yaml:"final_checks" json:"final_checks"`
	ResumeContinuation   []string `yaml:"resume_continuation" json:"resume_continuation,omitempty"`
}

type PlanningModes struct {
	InvocationContext string           `yaml:"invocation_context" json:"invocation_context"`
	SharedSafety      []string         `yaml:"shared_safety" json:"shared_safety"`
	Reply             PlanningContract `yaml:"reply" json:"reply"`
	Formalize         PlanningContract `yaml:"formalize" json:"formalize"`
	GenerateTitle     PlanningContract `yaml:"generate_title" json:"generate_title"`
}

type PlanningContract struct {
	Instruction    string        `yaml:"instruction" json:"instruction"`
	Rules          []string      `yaml:"rules" json:"rules"`
	Sections       []PlanSection `yaml:"sections" json:"sections"`
	OutputContract string        `yaml:"output_contract" json:"output_contract"`
	FinalChecks    []string      `yaml:"final_checks" json:"final_checks"`
}

type PlanSection struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
}

// LoadDefault loads and validates the embedded prompt catalog against the
// canonical role IDs supplied by application state. The catalog deliberately
// does not own or duplicate the role registry.
func LoadDefault(requiredRoleIDs []string) (Catalog, error) {
	var catalog Catalog
	catalog.Version = 1
	guidance := struct {
		Roles map[string]string `yaml:"roles"`
	}{}
	for name, target := range map[string]any{
		"roles.yaml":            &catalog.RolePolicy,
		"default_guidance.yaml": &guidance,
		"node_contracts.yaml":   &catalog.NodeContracts,
		"planning_modes.yaml":   &catalog.PlanningModes,
		"communication_styles.yaml": &struct {
			Neutral       *string `yaml:"neutral"`
			StylePreamble *string `yaml:"style_preamble"`
		}{Neutral: &catalog.NeutralStyle, StylePreamble: &catalog.StylePreamble},
	} {
		raw, err := catalogFiles.ReadFile("catalog/" + name)
		if err != nil {
			return Catalog{}, fmt.Errorf("read prompt catalog %s: %w", name, err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		decoder.KnownFields(true)
		if err := decoder.Decode(target); err != nil {
			return Catalog{}, fmt.Errorf("decode prompt catalog %s: %w", name, err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return Catalog{}, fmt.Errorf("prompt catalog %s must contain one YAML document", name)
		}
	}
	for roleID, role := range catalog.RolePolicy.Roles {
		role.DefaultGuidance = guidance.Roles[roleID]
		catalog.RolePolicy.Roles[roleID] = role
	}
	if err := catalog.Validate(requiredRoleIDs); err != nil {
		return Catalog{}, err
	}
	// Only new revisions must carry it; historical revisions stay valid.
	if _, err := catalog.ResumeContinuationSection(); err != nil {
		return Catalog{}, err
	}
	return catalog, nil
}

func (c Catalog) Validate(requiredRoleIDs []string) error {
	if c.Version != 1 {
		return fmt.Errorf("prompt catalog version must be 1")
	}
	if c.RolePolicy.Version != "v1" || strings.TrimSpace(c.RolePolicy.PolicyID) == "" {
		return fmt.Errorf("prompt role policy identity is invalid")
	}
	if err := validateMailboxTemplate("default_mailbox_template", c.RolePolicy.DefaultMailboxTemplate); err != nil {
		return err
	}
	required := make(map[string]struct{}, len(requiredRoleIDs))
	for _, roleID := range requiredRoleIDs {
		roleID = strings.TrimSpace(roleID)
		if roleID != "" {
			required[roleID] = struct{}{}
		}
	}
	for roleID := range required {
		role, ok := c.RolePolicy.Roles[roleID]
		if !ok {
			return fmt.Errorf("prompt catalog is missing required role %q", roleID)
		}
		if err := validateRole(roleID, role); err != nil {
			return err
		}
	}
	for roleID, role := range c.RolePolicy.Roles {
		if _, ok := required[roleID]; !ok {
			return fmt.Errorf("prompt catalog contains unknown role %q", roleID)
		}
		if err := validateRole(roleID, role); err != nil {
			return err
		}
	}
	if strings.TrimSpace(c.NeutralStyle) == "" || strings.TrimSpace(c.StylePreamble) == "" {
		return fmt.Errorf("prompt catalog neutral style and scope are required")
	}
	shared := c.NodeContracts["shared"]
	requiredRules := []struct {
		name  string
		lines []string
	}{
		{"shared.protected_paths", shared.ProtectedPaths},
		{"shared.authority_and_evidence", shared.AuthorityAndEvidence},
		{"shared.response_only", shared.ResponseOnly},
		{"plan_drafting.planning_constraints", c.NodeContracts["plan_drafting"].PlanningConstraints},
		{"advisory_synthesis.planning_constraints", c.NodeContracts["advisory_synthesis"].PlanningConstraints},
		{"advisor_consultation.advisor_response", c.NodeContracts["advisor_consultation"].AdvisorResponse},
		{"execution_review.inspection_workspace", c.NodeContracts["execution_review"].InspectionWorkspace},
		{"execution_review.response_format", c.NodeContracts["execution_review"].ResponseFormat},
		{"validation_gates.inspection_workspace", c.NodeContracts["validation_gates"].InspectionWorkspace},
		{"validation_gates.response_format", c.NodeContracts["validation_gates"].ResponseFormat},
	}
	for _, name := range []string{"default", "review", "validation", "runtime_owned"} {
		requiredRules = append(requiredRules, struct {
			name  string
			lines []string
		}{name + ".final_checks", c.NodeContracts[name].FinalChecks})
	}
	for _, rules := range requiredRules {
		if err := validateRequiredRules(rules.name, rules.lines); err != nil {
			return err
		}
	}
	if strings.TrimSpace(c.PlanningModes.InvocationContext) == "" || len(c.PlanningModes.SharedSafety) == 0 {
		return fmt.Errorf("planning invocation context and shared safety rules are required")
	}
	if err := validatePlanningContract("reply", c.PlanningModes.Reply); err != nil {
		return err
	}
	if err := validatePlanningContract("formalize", c.PlanningModes.Formalize); err != nil {
		return err
	}
	if err := validatePlanningContract("generate_title", c.PlanningModes.GenerateTitle); err != nil {
		return err
	}
	if len(c.PlanningModes.Formalize.Sections) == 0 {
		return fmt.Errorf("formalize prompt contract requires plan sections")
	}
	for _, section := range c.PlanningModes.Formalize.Sections {
		if strings.TrimSpace(section.Name) == "" || strings.TrimSpace(section.Description) == "" {
			return fmt.Errorf("formalize plan sections require a name and description")
		}
	}
	return nil
}

func validateRole(roleID string, role RoleDefinition) error {
	if strings.TrimSpace(role.ProtectedInstructionTemplate) == "" || strings.TrimSpace(role.DefaultGuidance) == "" {
		return fmt.Errorf("prompt role %q requires protected instruction and default guidance", roleID)
	}
	return validateMailboxTemplate(roleID, role.MailboxTemplate)
}

func validateMailboxTemplate(name string, template MailboxTemplate) error {
	if strings.TrimSpace(template.SalutationTemplate) == "" || strings.TrimSpace(template.BodyTemplate) == "" || strings.TrimSpace(template.SignoffTemplate) == "" || strings.TrimSpace(template.SignatureTemplate) == "" {
		return fmt.Errorf("prompt %q has an incomplete mailbox template", name)
	}
	return nil
}

func validatePlanningContract(name string, contract PlanningContract) error {
	if strings.TrimSpace(contract.Instruction) == "" || len(contract.Rules) == 0 || strings.TrimSpace(contract.OutputContract) == "" || len(contract.FinalChecks) == 0 {
		return fmt.Errorf("planning prompt contract %q is incomplete", name)
	}
	return validateRequiredRules("planning."+name+".final_checks", contract.FinalChecks)
}

func validateRequiredRules(name string, lines []string) error {
	if len(lines) == 0 {
		return fmt.Errorf("prompt contract %q requires rules", name)
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			return fmt.Errorf("prompt contract %q contains an empty rule", name)
		}
	}
	return nil
}

// Revision returns canonical JSON and its content digest for immutable
// persistence. encoding/json sorts map keys, making the digest deterministic.
func (c Catalog) Revision() ([]byte, string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, "", fmt.Errorf("encode prompt catalog revision: %w", err)
	}
	digest := sha256.Sum256(raw)
	return raw, hex.EncodeToString(digest[:]), nil
}

func ParseRevision(raw []byte, expectedDigest string, requiredRoleIDs []string) (Catalog, error) {
	expectedDigest = strings.TrimSpace(expectedDigest)
	digest := sha256.Sum256(raw)
	if expectedDigest == "" || hex.EncodeToString(digest[:]) != expectedDigest {
		return Catalog{}, fmt.Errorf("prompt catalog revision digest mismatch")
	}
	var catalog Catalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return Catalog{}, fmt.Errorf("decode prompt catalog revision: %w", err)
	}
	if requiredRoleIDs == nil {
		// Historical snapshots validate against the role set embedded in that
		// revision, which can differ from the current role registry.
		requiredRoleIDs = catalog.RoleIDs()
	}
	if err := catalog.Validate(requiredRoleIDs); err != nil {
		return Catalog{}, err
	}
	return catalog, nil
}

func (c Catalog) RoleIDs() []string {
	roles := make([]string, 0, len(c.RolePolicy.Roles))
	for roleID := range c.RolePolicy.Roles {
		roles = append(roles, roleID)
	}
	sort.Strings(roles)
	return roles
}

type ExecutionSystemPromptInput struct {
	RoleID             string
	NodeClass          string
	CommunicationStyle string
	AgentGuidance      string
}

func (c Catalog) ExecutionSystemPrompt(input ExecutionSystemPromptInput) (string, error) {
	roleInstruction, err := c.ProtectedRoleInstruction(input.RoleID)
	if err != nil {
		return "", err
	}
	var blocks []string
	blocks = append(blocks, promptSection("Protected Role Instruction", roleInstruction))
	blocks = append(blocks, promptSection("Communication Style", c.CommunicationStyleInstruction(input.CommunicationStyle)))
	if guidance := strings.TrimSpace(input.AgentGuidance); guidance != "" {
		blocks = append(blocks, promptSection("Agent Guidance", guidance))
	}
	blocks = append(blocks, promptSection("Kingdom Contract", c.KingdomRules(input.NodeClass)))
	return strings.Join(blocks, "\n\n"), nil
}

func (c Catalog) PlanningSystemPrompt(communicationStyle, guidance, mode string) (string, error) {
	roleInstruction, err := c.ProtectedRoleInstruction("plan-author")
	if err != nil {
		return "", err
	}
	style := c.CommunicationStyleInstruction(communicationStyle)
	contract, ok := c.PlanningModes.forMode(mode)
	if !ok {
		return "", fmt.Errorf("planning prompt mode %q is unsupported", mode)
	}
	modeSections := []string{contract.Instruction}
	modeSections = append(modeSections, bulletLines(contract.Rules)...)
	if strings.TrimSpace(mode) == "formalize" {
		modeSections = append(modeSections, "Produce a work order plan with these sections (omit a section if the conversation produced nothing settled for it rather than padding):")
		sections := make([]string, 0, len(contract.Sections))
		for _, section := range contract.Sections {
			sections = append(sections, fmt.Sprintf("- **%s**: %s", section.Name, section.Description))
		}
		modeSections = append(modeSections, strings.Join(sections, "\n"))
	}
	modeSections = append(modeSections, contract.OutputContract)
	blocks := []string{
		promptSection("Protected Role Instruction", roleInstruction),
		promptSection("Communication Style", style),
	}
	if guidance := strings.TrimSpace(guidance); guidance != "" {
		blocks = append(blocks, promptSection("Agent Guidance", guidance))
	}
	shared := c.NodeContracts["shared"]
	kingdomContract := []string{
		promptSection("Authority and Evidence", strings.Join(bulletLines(shared.AuthorityAndEvidence), "\n")),
		promptSection("Planning Invocation", c.PlanningModes.InvocationContext),
		promptSection("Planning Safety", strings.Join(bulletLines(c.PlanningModes.SharedSafety), "\n")),
		promptSection("Planning Mode Contract", strings.Join(nonEmptyStrings(modeSections), "\n\n")),
	}
	blocks = append(blocks, promptSection("Kingdom Contract", strings.Join(kingdomContract, "\n\n")))
	return strings.Join(blocks, "\n\n"), nil
}

func (c Catalog) IsRecognizedPlanSectionHeading(heading string) bool {
	heading = strings.ToLower(strings.TrimSpace(heading))
	for _, section := range c.PlanningModes.Formalize.Sections {
		if heading == strings.ToLower(strings.TrimSpace(section.Name)) {
			return true
		}
	}
	return false
}

func (c Catalog) KingdomRules(nodeClass string) string {
	class := strings.TrimSpace(nodeClass)
	shared := c.NodeContracts["shared"]
	node := c.NodeContracts[class]
	var responseOnly []string
	if class == "plan_drafting" || class == "advisor_consultation" || class == "advisory_synthesis" {
		responseOnly = shared.ResponseOnly
	}
	sections := []struct {
		name string
		text []string
	}{
		{"Authority and Evidence", shared.AuthorityAndEvidence},
		{"Planning Constraints", node.PlanningConstraints},
		{"Response-Only Constraints", responseOnly},
		{"Protected Runtime Paths", append(append([]string(nil), shared.ProtectedPaths...), node.ProtectedPaths...)},
		{"Inspection Workspace", node.InspectionWorkspace},
		{"Advisor Response Requirements", node.AdvisorResponse},
		{"Response Format Requirements", node.ResponseFormat},
	}
	var blocks []string
	for _, section := range sections {
		if len(section.text) != 0 {
			blocks = append(blocks, promptSection(section.name, strings.Join(bulletLines(section.text), "\n")))
		}
	}
	return strings.Join(blocks, "\n\n")
}

// PlanningFinalChecksSection resolves the selected mode from the pinned catalog.
// Callers append it after the complete transcript and external context.
func (c Catalog) PlanningFinalChecksSection(mode string) (string, error) {
	contract, ok := c.PlanningModes.forMode(mode)
	if !ok {
		return "", fmt.Errorf("planning prompt mode %q is unsupported", mode)
	}
	return promptSection("Final Checks", strings.Join(bulletLines(contract.FinalChecks), "\n")), nil
}

func (c Catalog) finalChecks(nodeClass string, providerCompletion bool) []string {
	class := strings.TrimSpace(nodeClass)
	finalChecksKey := "runtime_owned"
	if providerCompletion {
		finalChecksKey = "default"
	}
	if class == "execution_review" {
		finalChecksKey = "review"
	} else if class == "validation_gates" {
		finalChecksKey = "validation"
	}
	return c.NodeContracts[finalChecksKey].FinalChecks
}

// ResumeContinuationSection explains a checkpointed continuation. Revisions
// pinned before Resume existed lack it, so their runs cannot be resumed.
func (c Catalog) ResumeContinuationSection() (string, error) {
	lines := bulletLines(c.NodeContracts["shared"].ResumeContinuation)
	if len(lines) == 0 {
		return "", fmt.Errorf("the run's pinned prompt catalog predates Resume")
	}
	return promptSection("Resumed Attempt", strings.Join(lines, "\n")), nil
}

func (c Catalog) FinalChecksSection(nodeClass string, providerCompletion bool) string {
	checks := strings.Join(bulletLines(c.finalChecks(nodeClass, providerCompletion)), "\n")
	return promptSection("Final Checks", checks)
}

func (c Catalog) ProtectedRoleInstruction(roleID string) (string, error) {
	roleID = strings.TrimSpace(roleID)
	role, ok := c.RolePolicy.Roles[roleID]
	if !ok {
		return "", fmt.Errorf("prompt role %q is not defined", roleID)
	}
	content, err := renderInstruction(role.ProtectedInstructionTemplate, map[string]string{"RoleID": roleID})
	if err != nil {
		return "", fmt.Errorf("render protected instruction for role %q: %w", roleID, err)
	}
	return content, nil
}

func (c Catalog) CommunicationStyleInstruction(instruction string) string {
	instruction = strings.TrimSpace(instruction)
	if instruction == "" {
		return c.NeutralStyle
	}
	return strings.TrimSpace(c.StylePreamble) + "\n\n" + instruction
}

func (p PlanningModes) forMode(mode string) (PlanningContract, bool) {
	switch strings.TrimSpace(mode) {
	case "reply", "regenerate":
		return p.Reply, true
	case "formalize":
		return p.Formalize, true
	case "generate_title":
		return p.GenerateTitle, true
	default:
		return PlanningContract{}, false
	}
}

func renderInstruction(raw string, data map[string]string) (string, error) {
	tmpl, err := template.New("protected_instruction").Option("missingkey=error").Parse(raw)
	if err != nil {
		return "", err
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, data); err != nil {
		return "", err
	}
	return strings.TrimSpace(rendered.String()), nil
}

func promptSection(title, body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	return title + ":\n" + body
}

func nonEmptyStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func bulletLines(values []string) []string {
	lines := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			lines = append(lines, "- "+value)
		}
	}
	return lines
}
