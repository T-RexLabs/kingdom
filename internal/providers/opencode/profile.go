package opencode

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
)

const pluginRPCName = "kingdom"

// profileActions are the stock permissions a managed role may use. Every other
// action, including background work and subagents, stays denied.
func profileActions(agent string) []string {
	actions := []string{"read", "glob", "grep"}
	switch agent {
	case "builder":
		return append(actions, "edit", "shell")
	case "qa-validator", "security-validator", "resilience-validator":
		return append(actions, "shell")
	default:
		return actions
	}
}

// profile is one request's immutable agent definition. The server resolves an
// agent by ID at every step, so a profile is registered once and never changed.
type profile struct {
	id        string
	requestID string
	system    string
	actions   []string
}

func newProfile(system string, actions []string) (profile, error) {
	profileID, err := randomID("kd_")
	if err != nil {
		return profile{}, err
	}
	requestID, err := randomID("msg_")
	if err != nil {
		return profile{}, err
	}
	return profile{id: profileID, requestID: requestID, system: system, actions: actions}, nil
}

func randomID(prefix string) (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate identifier: %w", err)
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

type profileRegistration struct {
	InstanceID string `json:"instanceID"`
	Generation string `json:"generation"`
	Location   string `json:"location"`
}

// registerProfile creates the profile through the plugin and then verifies that
// the server resolves exactly the definition Kingdom intended.
func registerProfile(ctx context.Context, client *Client, directory, instanceID string, p profile) (string, error) {
	var registered profileRegistration
	input := map[string]any{"profileID": p.id, "requestID": p.requestID, "system": p.system, "actions": p.actions}
	if err := client.CallPlugin(ctx, directory, pluginRPCName, "register", input, &registered); err != nil {
		return "", fmt.Errorf("register OpenCode profile: %w", err)
	}
	if registered.InstanceID != instanceID || registered.Location != directory || registered.Generation == "" {
		return "", fmt.Errorf("OpenCode plugin registered the profile for a different service or location")
	}
	agent, err := client.Agent(ctx, directory, p.id)
	if err != nil {
		return "", errors.Join(fmt.Errorf("verify OpenCode profile: %w", err), retireProfile(ctx, client, directory, p.id))
	}
	want := []Permission{{Action: "*", Resource: "*", Effect: "deny"}}
	for _, action := range p.actions {
		want = append(want, Permission{Action: action, Resource: "*", Effect: "allow"})
	}
	if agent.System != p.system || agent.Mode != "primary" || !slices.Equal(agent.Permissions, want) {
		return "", errors.Join(fmt.Errorf("OpenCode resolved a different profile than Kingdom registered"), retireProfile(ctx, client, directory, p.id))
	}
	return registered.Generation, nil
}

func retireProfile(ctx context.Context, client *Client, directory, profileID string) error {
	var retired struct {
		Retired bool `json:"retired"`
	}
	return client.CallPlugin(ctx, directory, pluginRPCName, "retire", map[string]string{"profileID": profileID}, &retired)
}
