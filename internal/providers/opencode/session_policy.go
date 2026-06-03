package opencode

import "encoding/json"

const configSchemaURL = "https://opencode.ai/config.json"

type permissionAction string

const permissionActionDeny permissionAction = "deny"

type sessionInlineConfig struct {
	Schema     string                  `json:"$schema,omitempty"`
	Permission sessionPermissionConfig `json:"permission,omitempty"`
}

type sessionPermissionConfig struct {
	ExternalDirectory permissionAction `json:"external_directory,omitempty"`
	Question          permissionAction `json:"question,omitempty"`
}

func PlannerSessionInlineConfig() (string, error) {
	return marshalSessionInlineConfig(sessionInlineConfig{
		Schema: configSchemaURL,
		Permission: sessionPermissionConfig{
			ExternalDirectory: permissionActionDeny,
		},
	})
}

func ExecutionSessionInlineConfig() (string, error) {
	return marshalSessionInlineConfig(sessionInlineConfig{
		Schema: configSchemaURL,
		Permission: sessionPermissionConfig{
			ExternalDirectory: permissionActionDeny,
			Question:          permissionActionDeny,
		},
	})
}

func marshalSessionInlineConfig(cfg sessionInlineConfig) (string, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
