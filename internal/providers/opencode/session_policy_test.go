package opencode

import (
	"encoding/json"
	"testing"
)

func TestPlannerSessionInlineConfig(t *testing.T) {
	configContent, err := PlannerSessionInlineConfig()
	if err != nil {
		t.Fatalf("build planner inline config: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(configContent), &payload); err != nil {
		t.Fatalf("decode planner inline config: %v", err)
	}
	if payload["$schema"] != configSchemaURL {
		t.Fatalf("expected schema %q, got %#v", configSchemaURL, payload["$schema"])
	}
	permission, ok := payload["permission"].(map[string]any)
	if !ok {
		t.Fatalf("expected planner permission object, got %#v", payload["permission"])
	}
	if permission["external_directory"] != string(permissionActionDeny) {
		t.Fatalf("expected planner external_directory deny, got %#v", permission["external_directory"])
	}
	if _, ok := permission["question"]; ok {
		t.Fatalf("expected planner inline config to leave question permission unset, got %#v", permission["question"])
	}
}

func TestExecutionSessionInlineConfig(t *testing.T) {
	configContent, err := ExecutionSessionInlineConfig()
	if err != nil {
		t.Fatalf("build execution inline config: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(configContent), &payload); err != nil {
		t.Fatalf("decode execution inline config: %v", err)
	}
	if payload["$schema"] != configSchemaURL {
		t.Fatalf("expected schema %q, got %#v", configSchemaURL, payload["$schema"])
	}
	permission, ok := payload["permission"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution permission object, got %#v", payload["permission"])
	}
	if permission["external_directory"] != string(permissionActionDeny) {
		t.Fatalf("expected execution external_directory deny, got %#v", permission["external_directory"])
	}
	if permission["question"] != string(permissionActionDeny) {
		t.Fatalf("expected execution question deny, got %#v", permission["question"])
	}
}
