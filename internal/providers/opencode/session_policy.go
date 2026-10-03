package opencode

import (
	"encoding/json"
	"fmt"
	"strings"
)

const configSchemaURL = "https://opencode.ai/config.json"

const shareModeDisabled = "disabled"

type permissionAction string

const permissionActionDeny permissionAction = "deny"

type sessionInlineConfig struct {
	Schema     string                        `json:"$schema,omitempty"`
	Share      string                        `json:"share,omitempty"`
	Server     sessionServerConfig           `json:"server,omitempty"`
	Permission map[string]permissionAction   `json:"permission,omitempty"`
	Agent      map[string]sessionAgentConfig `json:"agent,omitempty"`
}

type sessionServerConfig struct {
	MDNS bool `json:"mdns"`
}

type sessionAgentConfig struct {
	Prompt string `json:"prompt"`
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
	return planningInlineConfig("")
}

func ExecutionSessionInlineConfig() (string, error) {
	return executionInlineConfig("")
}

func PlannerAgentInlineConfig(agentPrompt string) (string, error) {
	if strings.TrimSpace(agentPrompt) == "" {
		return "", fmt.Errorf("agent prompt is required for planner invocation")
	}
	return planningInlineConfig(agentPrompt)
}

func ExecutionAgentInlineConfig(agentName, agentPrompt string) (string, error) {
	agentName = strings.TrimSpace(agentName)
	if agentName == "" || strings.TrimSpace(agentPrompt) == "" {
		return "", fmt.Errorf("agent name and prompt are required for invocation")
	}
	return executionInlineConfigForAgent(agentName, agentPrompt)
}

func planningInlineConfig(agentPrompt string) (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"external_directory": permissionActionDeny}
	if strings.TrimSpace(agentPrompt) != "" {
		config.Agent = map[string]sessionAgentConfig{"planning-assistant": {Prompt: strings.TrimSpace(agentPrompt)}}
	}
	return marshalSessionInlineConfig(config)
}

func executionInlineConfig(agentPrompt string) (string, error) {
	return executionInlineConfigForAgent("", agentPrompt)
}

func executionInlineConfigForAgent(agentName, agentPrompt string) (string, error) {
	config := hardenedSessionInlineConfig()
	config.Permission = map[string]permissionAction{"external_directory": permissionActionDeny, "question": permissionActionDeny}
	if strings.TrimSpace(agentPrompt) != "" {
		if agentName == "" {
			return "", fmt.Errorf("agent name is required for invocation prompt")
		}
		config.Agent = map[string]sessionAgentConfig{agentName: {Prompt: strings.TrimSpace(agentPrompt)}}
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
