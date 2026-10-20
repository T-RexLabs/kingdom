package opencode

import (
	"encoding/json"
	"testing"
)

func assertCommonSessionInlineConfig(t *testing.T, payload map[string]any) {
	t.Helper()
	if payload["$schema"] != configSchemaURL {
		t.Fatalf("expected schema %q, got %#v", configSchemaURL, payload["$schema"])
	}
	if payload["share"] != shareModeDisabled {
		t.Fatalf("expected share mode %q, got %#v", shareModeDisabled, payload["share"])
	}
	server, ok := payload["server"].(map[string]any)
	if !ok {
		t.Fatalf("expected server object, got %#v", payload["server"])
	}
	if server["mdns"] != false {
		t.Fatalf("expected server.mdns false, got %#v", server["mdns"])
	}
}

func TestPlannerSessionInlineConfig(t *testing.T) {
	configContent, err := PlannerSessionInlineConfig()
	if err != nil {
		t.Fatalf("build planner inline config: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(configContent), &payload); err != nil {
		t.Fatalf("decode planner inline config: %v", err)
	}
	assertCommonSessionInlineConfig(t, payload)
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
	assertCommonSessionInlineConfig(t, payload)
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

func TestInvocationInlineConfigsPinAgentPromptAndPreserveHardening(t *testing.T) {
	for name, build := range map[string]func() (string, error){
		"planner":   func() (string, error) { return PlannerAgentInlineConfig("pinned planner prompt") },
		"execution": func() (string, error) { return ExecutionAgentInlineConfig("reviewer", "pinned reviewer prompt") },
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := build()
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				t.Fatal(err)
			}
			assertCommonSessionInlineConfig(t, payload)
			permission, ok := payload["permission"].(map[string]any)
			if !ok || permission["external_directory"] != string(permissionActionDeny) {
				t.Fatalf("invocation config lost external-directory denial: %#v", payload["permission"])
			}
			if name == "execution" && permission["question"] != string(permissionActionDeny) {
				t.Fatalf("execution invocation lost question denial: %#v", permission)
			}
			agents, ok := payload["agent"].(map[string]any)
			if !ok || len(agents) != 1 {
				t.Fatalf("invocation agent prompt missing: %#v", payload["agent"])
			}
			for _, value := range agents {
				agent, ok := value.(map[string]any)
				if !ok || agent["prompt"] == "" {
					t.Fatalf("invocation agent prompt is empty: %#v", value)
				}
			}
		})
	}
}
