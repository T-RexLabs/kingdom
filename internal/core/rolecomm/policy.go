package rolecomm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

type Policy struct {
	Version             string                  `yaml:"version" json:"version"`
	PolicyID            string                  `yaml:"policy_id" json:"policy_id"`
	DefaultRoleTemplate RoleCommunicationConfig `yaml:"default_role_template" json:"default_role_template"`
	Roles               map[string]RoleTemplate `yaml:"roles" json:"roles"`
}

type RoleTemplate struct {
	SystemPromptTemplate string                     `yaml:"system_prompt_template" json:"system_prompt_template"`
	MailboxTemplate      MailboxCommunicationConfig `yaml:"mailbox_template" json:"mailbox_template"`
}

type RoleCommunicationConfig = RoleTemplate

type MailboxCommunicationConfig struct {
	SalutationTemplate string `yaml:"salutation_template" json:"salutation_template"`
	BodyTemplate       string `yaml:"body_template" json:"body_template"`
	SignoffTemplate    string `yaml:"signoff_template" json:"signoff_template"`
	SignatureTemplate  string `yaml:"signature_template" json:"signature_template"`
}

func LoadPolicyFromFile(policyPath string, schemaPath string, requiredRoleIDs []string) (Policy, error) {
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		return Policy{}, fmt.Errorf("read role communication policy %q: %w", policyPath, err)
	}

	if err := validatePolicySchema(raw, schemaPath); err != nil {
		return Policy{}, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	var policy Policy
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("decode role communication policy %q: %w", policyPath, err)
	}

	if err := normalizeAndValidate(&policy, requiredRoleIDs); err != nil {
		return Policy{}, fmt.Errorf("validate role communication policy %q: %w", policyPath, err)
	}

	return policy, nil
}

func normalizeAndValidate(policy *Policy, requiredRoleIDs []string) error {
	policy.Version = strings.TrimSpace(policy.Version)
	policy.PolicyID = strings.TrimSpace(policy.PolicyID)

	if policy.Version != "v1" {
		return fmt.Errorf("role communication policy version must be \"v1\", got %q", policy.Version)
	}
	if policy.PolicyID == "" {
		return fmt.Errorf("role communication policy id is required")
	}
	if err := normalizeTemplate(&policy.DefaultRoleTemplate); err != nil {
		return fmt.Errorf("default_role_template: %w", err)
	}
	if len(policy.Roles) == 0 {
		return fmt.Errorf("role communication policy roles is required")
	}

	required := make(map[string]struct{}, len(requiredRoleIDs))
	for _, roleID := range requiredRoleIDs {
		trimmed := strings.TrimSpace(roleID)
		if trimmed == "" {
			continue
		}
		required[trimmed] = struct{}{}
	}

	missing := make([]string, 0)
	for roleID := range required {
		template, ok := policy.Roles[roleID]
		if !ok {
			missing = append(missing, roleID)
			continue
		}
		if err := normalizeTemplate(&template); err != nil {
			return fmt.Errorf("roles[%s]: %w", roleID, err)
		}
		policy.Roles[roleID] = template
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing required role templates: %s", strings.Join(missing, ", "))
	}

	unknown := make([]string, 0)
	for roleID, template := range policy.Roles {
		trimmedRole := strings.TrimSpace(roleID)
		if trimmedRole == "" {
			return fmt.Errorf("roles contains empty role id key")
		}
		if _, ok := required[trimmedRole]; !ok {
			unknown = append(unknown, trimmedRole)
		}
		if err := normalizeTemplate(&template); err != nil {
			return fmt.Errorf("roles[%s]: %w", trimmedRole, err)
		}
		if trimmedRole != roleID {
			delete(policy.Roles, roleID)
			policy.Roles[trimmedRole] = template
		} else {
			policy.Roles[trimmedRole] = template
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("unknown role templates are not allowed: %s", strings.Join(unknown, ", "))
	}

	return nil
}

func normalizeTemplate(template *RoleTemplate) error {
	template.SystemPromptTemplate = strings.TrimSpace(template.SystemPromptTemplate)
	template.MailboxTemplate.SalutationTemplate = strings.TrimSpace(template.MailboxTemplate.SalutationTemplate)
	template.MailboxTemplate.BodyTemplate = strings.TrimSpace(template.MailboxTemplate.BodyTemplate)
	template.MailboxTemplate.SignoffTemplate = strings.TrimSpace(template.MailboxTemplate.SignoffTemplate)
	template.MailboxTemplate.SignatureTemplate = strings.TrimSpace(template.MailboxTemplate.SignatureTemplate)

	if template.SystemPromptTemplate == "" {
		return fmt.Errorf("system_prompt_template is required")
	}
	if template.MailboxTemplate.SalutationTemplate == "" {
		return fmt.Errorf("mailbox_template.salutation_template is required")
	}
	if template.MailboxTemplate.BodyTemplate == "" {
		return fmt.Errorf("mailbox_template.body_template is required")
	}
	if template.MailboxTemplate.SignoffTemplate == "" {
		return fmt.Errorf("mailbox_template.signoff_template is required")
	}
	if template.MailboxTemplate.SignatureTemplate == "" {
		return fmt.Errorf("mailbox_template.signature_template is required")
	}

	return nil
}

func validatePolicySchema(rawYAML []byte, schemaPath string) error {
	if strings.TrimSpace(schemaPath) == "" {
		return fmt.Errorf("role communication policy schema path is required")
	}

	var value any
	if err := yaml.Unmarshal(rawYAML, &value); err != nil {
		return fmt.Errorf("decode role communication policy for schema validation: %w", err)
	}

	jsonValue, err := toJSONCompatible(value)
	if err != nil {
		return fmt.Errorf("normalize role communication policy for schema validation: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	absSchemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		return fmt.Errorf("resolve role communication policy schema path %q: %w", schemaPath, err)
	}
	schema, err := compiler.Compile("file://" + filepath.ToSlash(absSchemaPath))
	if err != nil {
		return fmt.Errorf("compile role communication policy schema %q: %w", schemaPath, err)
	}

	if err := schema.Validate(jsonValue); err != nil {
		return fmt.Errorf("validate role communication policy against schema %q: %w", schemaPath, err)
	}

	return nil
}

func toJSONCompatible(v any) (any, error) {
	switch value := v.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(value))
		for key, nested := range value {
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}
		return normalized, nil
	case map[any]any:
		normalized := make(map[string]any, len(value))
		for key, nested := range value {
			keyString, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("non-string map key %T", key)
			}
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[keyString] = converted
		}
		return normalized, nil
	case []any:
		normalized := make([]any, len(value))
		for i, nested := range value {
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[i] = converted
		}
		return normalized, nil
	default:
		return value, nil
	}
}
