package opencode

import "encoding/json"

const configSchemaURL = "https://opencode.ai/config.json"

const shareModeDisabled = "disabled"

type permissionAction string

const permissionActionDeny permissionAction = "deny"

type sessionInlineConfig struct {
	Schema     string                      `json:"$schema,omitempty"`
	Share      string                      `json:"share,omitempty"`
	Server     sessionServerConfig         `json:"server,omitempty"`
	Permission map[string]permissionAction `json:"permission,omitempty"`
}

type sessionServerConfig struct {
	MDNS bool `json:"mdns"`
}

func PlannerSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"external_directory": permissionActionDeny}
	return marshalSessionInlineConfig(config)
}

// ExternalPlannerSessionInlineConfig denies every tool. Linked issue text is
// attacker-controlled and must never be able to induce file, shell, network,
// subagent, skill, or custom-tool access.
func ExternalPlannerSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"*": permissionActionDeny, "external_directory": permissionActionDeny, "question": permissionActionDeny}
	return marshalSessionInlineConfig(config)
}

func ExecutionSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"external_directory": permissionActionDeny, "question": permissionActionDeny}
	return marshalSessionInlineConfig(config)
}

func hardenedSessionInlineConfig() sessionInlineConfig {
	return sessionInlineConfig{
		Schema: configSchemaURL,
		Share:  shareModeDisabled,
		Server: sessionServerConfig{MDNS: false},
	}
}

func marshalSessionInlineConfig(cfg sessionInlineConfig) (string, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
