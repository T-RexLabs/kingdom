package rolecomm

import (
	"bytes"
	"strings"
	"text/template"

	"kingdom/internal/prompts"
)

type Policy struct {
	DefaultTemplate MailboxCommunicationConfig
	Roles           map[string]MailboxCommunicationConfig
}

type MailboxCommunicationConfig struct {
	SalutationTemplate string
	BodyTemplate       string
	SignoffTemplate    string
	SignatureTemplate  string
}

func FromCatalog(catalog prompts.Catalog) Policy {
	policy := Policy{
		DefaultTemplate: mailboxTemplateFromCatalog(catalog.RolePolicy.DefaultMailboxTemplate),
		Roles:           make(map[string]MailboxCommunicationConfig, len(catalog.RolePolicy.Roles)),
	}
	for roleID, role := range catalog.RolePolicy.Roles {
		policy.Roles[roleID] = mailboxTemplateFromCatalog(role.MailboxTemplate)
	}
	return policy
}

func mailboxTemplateFromCatalog(template prompts.MailboxTemplate) MailboxCommunicationConfig {
	return MailboxCommunicationConfig{
		SalutationTemplate: template.SalutationTemplate,
		BodyTemplate:       template.BodyTemplate,
		SignoffTemplate:    template.SignoffTemplate,
		SignatureTemplate:  template.SignatureTemplate,
	}
}

func (p Policy) MailboxTemplateForRole(roleID string) MailboxCommunicationConfig {
	if template, ok := p.Roles[strings.TrimSpace(roleID)]; ok {
		return template
	}
	return p.DefaultTemplate
}

func RenderTemplate(raw string, data map[string]string, fallback string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	tmpl, err := template.New("role_communication").Option("missingkey=zero").Parse(raw)
	if err != nil {
		return fallback
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, data); err != nil {
		return fallback
	}
	if result := strings.TrimSpace(rendered.String()); result != "" {
		return result
	}
	return fallback
}
