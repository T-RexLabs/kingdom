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

// A note for anyone reaching for a broader denial here: OpenCode does not
// enforce a `"*"` wildcard in this permission block. Kingdom shipped
// `permission: {"*": "deny"}` for planner turns carrying linked issue text and
// believed it removed every tool; measured against a live provider it removed
// none — `list`, `glob`, `grep`, and `webfetch` all still ran. What is actually
// enforced is the agent profile's `tools:` block, which is why the planner
// genuinely has no shell and cannot write. Deny a capability there, not here,
// and verify it against a real provider before claiming it.
func PlannerSessionInlineConfig() (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"external_directory": permissionActionDeny}
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
