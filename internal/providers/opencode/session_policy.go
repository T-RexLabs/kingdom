package opencode

import "encoding/json"

const configSchemaURL = "https://opencode.ai/config.json"

const shareModeDisabled = "disabled"

type permissionAction string

const permissionActionDeny permissionAction = "deny"

type sessionInlineConfig struct {
	Schema     string                  `json:"$schema,omitempty"`
	Share      string                  `json:"share,omitempty"`
	Server     sessionServerConfig     `json:"server,omitempty"`
	Permission sessionPermissionConfig `json:"permission,omitempty"`
}

type sessionServerConfig struct {
	MDNS bool `json:"mdns"`
}

type sessionPermissionConfig struct {
	ExternalDirectory permissionAction `json:"external_directory,omitempty"`
	Question          permissionAction `json:"question,omitempty"`
}

func PlannerSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = sessionPermissionConfig{ExternalDirectory: permissionActionDeny}
	return marshalSessionInlineConfig(config)
}

func ExecutionSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = sessionPermissionConfig{
		ExternalDirectory: permissionActionDeny,
		Question:          permissionActionDeny,
	}
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
