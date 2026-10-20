package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/theme"
)

func TestAgentConfigurationEndpointsRequireThemeRevisionAndResetRoleOverrides(t *testing.T) {
	store, err := theme.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ThemeStore: store})
	request := func(method, path, key string, body any, status int) []byte {
		t.Helper()
		var fields map[string]any
		if body != nil {
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
		}
		response := performJSONRequest(t, handler, requestSpec{method: method, path: "/api/v1" + path, idempotencyKey: key, body: fields})
		if response.Code != status {
			t.Fatalf("%s %s: status=%d body=%s", method, path, response.Code, response.Body.String())
		}
		return response.Body.Bytes()
	}

	source := `{"schemaVersion":1,"resolverVersion":1,"id":"star-wars","version":"1.0.0","name":"Star Wars","agentVoice":"Use measured, cinematic language.","base":{"id":"obsidian-court","version":"1.1.0"}}`
	var saved gen.ThemeSource
	if err := json.Unmarshal(request(http.MethodPut, "/themes/star-wars/source", "save-star-wars", gen.ThemeSaveRequest{Source: source, Assets: map[string][]byte{}, ExpectedSourceDigest: ""}, http.StatusOK), &saved); err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "/themes/star-wars/publish", "publish-star-wars", gen.PublishThemeJSONBody{ExpectedSourceDigest: saved.SourceDigest}, http.StatusCreated)

	var catalog gen.ThemeCatalog
	if err := json.Unmarshal(request(http.MethodGet, "/themes", "", nil, http.StatusOK), &catalog); err != nil {
		t.Fatal(err)
	}
	var voiceThemeID, voiceThemeDigest string
	var voiceThemeInstruction *string
	for i := range catalog.Items {
		if catalog.Items[i].Id == "star-wars" {
			voiceThemeID = catalog.Items[i].Id
			voiceThemeDigest = catalog.Items[i].ResolvedDigest
			voiceThemeInstruction = catalog.Items[i].AgentVoice
			break
		}
	}
	if voiceThemeID == "" || voiceThemeInstruction == nil || *voiceThemeInstruction != "Use measured, cinematic language." {
		t.Fatalf("theme catalog omitted its authored agent voice: %#v", catalog.Items)
	}

	var initial gen.AgentConfiguration
	if err := json.Unmarshal(request(http.MethodGet, "/agent-configuration", "", nil, http.StatusOK), &initial); err != nil {
		t.Fatal(err)
	}
	if initial.Workspace.RowVersion != 1 || initial.Workspace.Voice.Source.Kind != gen.AgentVoiceSourceKind("neutral-v1") {
		t.Fatalf("unexpected initial agent configuration: %#v", initial.Workspace)
	}
	if len(initial.PromptCatalogDigest) != 64 {
		t.Fatalf("agent configuration omitted its prompt catalog revision: %q", initial.PromptCatalogDigest)
	}

	voiceSelection := map[string]any{"kind": "theme", "theme_id": voiceThemeID, "theme_digest": voiceThemeDigest}
	var updatedWorkspace gen.AgentWorkspaceConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/workspace-voice", "set-workspace-voice", map[string]any{
		"expected_row_version": initial.Workspace.RowVersion,
		"voice":                voiceSelection,
	}, http.StatusOK), &updatedWorkspace); err != nil {
		t.Fatal(err)
	}
	if updatedWorkspace.RowVersion != 2 || updatedWorkspace.Voice.Instruction == nil || *updatedWorkspace.Voice.Instruction != *voiceThemeInstruction {
		t.Fatalf("workspace voice was not resolved from the exact theme revision: %#v", updatedWorkspace)
	}
	request(http.MethodPut, "/agent-configuration/workspace-voice", "stale-workspace-voice", map[string]any{
		"expected_row_version": initial.Workspace.RowVersion,
		"voice":                map[string]any{"kind": "neutral-v1"},
	}, http.StatusConflict)
	staleCatalogDigest := "0" + initial.PromptCatalogDigest[1:]
	if staleCatalogDigest == initial.PromptCatalogDigest {
		staleCatalogDigest = "1" + initial.PromptCatalogDigest[1:]
	}
	request(http.MethodPut, "/agent-configuration/roles/builder", "stale-prompt-catalog", map[string]any{
		"expected_row_version":           0,
		"expected_prompt_catalog_digest": staleCatalogDigest,
		"guidance_override":              "stale default guidance",
		"voice_override":                 nil,
	}, http.StatusConflict)

	var role gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/roles/builder", "set-builder-guidance", map[string]any{
		"expected_row_version":           0,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              "Keep implementation summaries concise.",
		"voice_override":                 map[string]any{"kind": "neutral-v1"},
	}, http.StatusOK), &role); err != nil {
		t.Fatal(err)
	}
	if role.RowVersion != 1 || role.EffectiveGuidance != "Keep implementation summaries concise." || role.GuidanceOverride == nil || role.VoiceOverride == nil || role.VoiceOverride.Source.Kind != gen.AgentVoiceSourceKind("neutral-v1") {
		t.Fatalf("unexpected role configuration: %#v", role)
	}
	var planAuthor gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/roles/plan-author", "set-plan-author-voice", map[string]any{
		"expected_row_version":           0,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              "Ask one clear question at a time.",
		"voice_override":                 voiceSelection,
	}, http.StatusOK), &planAuthor); err != nil {
		t.Fatal(err)
	}
	request(http.MethodPut, "/agent-configuration/roles/plan-author", "missing-role-style", map[string]any{
		"expected_row_version":           planAuthor.RowVersion,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              "This partial update must not mutate saved style.",
	}, http.StatusBadRequest)
	var afterPartialUpdate gen.AgentConfiguration
	if err := json.Unmarshal(request(http.MethodGet, "/agent-configuration", "", nil, http.StatusOK), &afterPartialUpdate); err != nil {
		t.Fatal(err)
	}
	var persistedPlanAuthor gen.AgentRoleConfiguration
	for _, configuredRole := range afterPartialUpdate.Roles {
		if configuredRole.RoleId == "plan-author" {
			persistedPlanAuthor = configuredRole
			break
		}
	}
	if persistedPlanAuthor.RowVersion != planAuthor.RowVersion || persistedPlanAuthor.VoiceOverride == nil || persistedPlanAuthor.VoiceOverride.Source.Kind != gen.AgentVoiceSourceKind("theme") || persistedPlanAuthor.EffectiveGuidance != "Ask one clear question at a time." {
		t.Fatalf("omitting the required voice selection mutated saved role configuration: %#v", persistedPlanAuthor)
	}
	var inheritedAdvisor gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/roles/architecture-advisor", "inherit-workspace-style", map[string]any{
		"expected_row_version":           0,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              nil,
		"voice_override":                 nil,
	}, http.StatusOK), &inheritedAdvisor); err != nil {
		t.Fatal(err)
	}
	if inheritedAdvisor.VoiceOverride != nil || inheritedAdvisor.EffectiveVoice.Source.Kind != gen.AgentVoiceSourceKind("theme") || inheritedAdvisor.EffectiveVoice.Instruction == nil || *inheritedAdvisor.EffectiveVoice.Instruction != *voiceThemeInstruction {
		t.Fatalf("explicit null must preserve workspace inheritance: %#v", inheritedAdvisor)
	}
	request(http.MethodDelete, "/themes/star-wars", "unpublish-star-wars", nil, http.StatusNoContent)

	var preservedWorkspace gen.AgentWorkspaceConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/workspace-voice", "preserve-unpublished-workspace-voice", map[string]any{
		"expected_row_version": updatedWorkspace.RowVersion,
		"voice":                voiceSelection,
	}, http.StatusOK), &preservedWorkspace); err != nil {
		t.Fatal(err)
	}
	if preservedWorkspace.RowVersion != updatedWorkspace.RowVersion+1 || preservedWorkspace.Voice.Instruction == nil || *preservedWorkspace.Voice.Instruction != *voiceThemeInstruction {
		t.Fatalf("unpublishing a theme broke its saved workspace voice: %#v", preservedWorkspace)
	}
	var preservedPlanAuthor gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/roles/plan-author", "preserve-unpublished-role-voice", map[string]any{
		"expected_row_version":           planAuthor.RowVersion,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              "Ask one focused question at a time.",
		"voice_override":                 voiceSelection,
	}, http.StatusOK), &preservedPlanAuthor); err != nil {
		t.Fatal(err)
	}
	if preservedPlanAuthor.RowVersion != planAuthor.RowVersion+1 || preservedPlanAuthor.VoiceOverride == nil || preservedPlanAuthor.VoiceOverride.Instruction == nil || *preservedPlanAuthor.VoiceOverride.Instruction != *voiceThemeInstruction {
		t.Fatalf("unpublishing a theme broke its saved role voice: %#v", preservedPlanAuthor)
	}
	request(http.MethodPut, "/agent-configuration/roles/builder", "reject-client-instruction", map[string]any{
		"expected_row_version":           1,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              "",
		"voice_override":                 map[string]any{"kind": "theme", "theme_id": voiceThemeID, "theme_digest": voiceThemeDigest, "instruction": "untrusted client text"},
	}, http.StatusBadRequest)
	request(http.MethodPost, "/agent-configuration/roles/builder/guidance/reset", "stale-reset-catalog", map[string]any{
		"expected_row_version":           role.RowVersion,
		"expected_prompt_catalog_digest": staleCatalogDigest,
	}, http.StatusConflict)

	var reset gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPost, "/agent-configuration/roles/builder/guidance/reset", "reset-builder-settings", map[string]any{
		"expected_row_version":           role.RowVersion,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
	}, http.StatusOK), &reset); err != nil {
		t.Fatal(err)
	}
	if reset.RowVersion != 2 || reset.GuidanceOverride != nil || reset.EffectiveGuidance != reset.DefaultGuidance || reset.VoiceOverride == nil || reset.VoiceOverride.Source.Kind != gen.AgentVoiceSourceKind("neutral-v1") {
		t.Fatalf("guidance reset did not restore the role default while preserving communication style: %#v", reset)
	}

	customInstruction := "Use spare, practical language."
	var customWorkspace gen.AgentWorkspaceConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/workspace-voice", "set-custom-workspace-style", map[string]any{
		"expected_row_version": preservedWorkspace.RowVersion,
		"voice":                map[string]any{"kind": "custom", "instruction": customInstruction},
	}, http.StatusOK), &customWorkspace); err != nil {
		t.Fatal(err)
	}
	if customWorkspace.Voice.Source.Kind != gen.AgentVoiceSourceKind("custom") || customWorkspace.Voice.Instruction == nil || *customWorkspace.Voice.Instruction != customInstruction {
		t.Fatalf("workspace custom style was not stored: %#v", customWorkspace)
	}
	roleCustomInstruction := "Use a careful advisory voice."
	var customRole gen.AgentRoleConfiguration
	if err := json.Unmarshal(request(http.MethodPut, "/agent-configuration/roles/reviewer", "set-role-custom-style", map[string]any{
		"expected_row_version":           0,
		"expected_prompt_catalog_digest": initial.PromptCatalogDigest,
		"guidance_override":              nil,
		"voice_override":                 map[string]any{"kind": "custom", "instruction": roleCustomInstruction},
	}, http.StatusOK), &customRole); err != nil {
		t.Fatal(err)
	}
	if customRole.EffectiveVoice.Source.Kind != gen.AgentVoiceSourceKind("custom") || customRole.EffectiveVoice.Instruction == nil || *customRole.EffectiveVoice.Instruction != roleCustomInstruction || customRole.GuidanceOverride != nil || customRole.EffectiveGuidance != customRole.DefaultGuidance {
		t.Fatalf("role custom style or inherited default guidance was not resolved: %#v", customRole)
	}
}

func TestAgentConfigurationRejectsInvalidUTF8BeforeJSONDecoding(t *testing.T) {
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{})
	tests := []struct {
		path   string
		prefix string
		suffix string
	}{
		{
			path:   "/api/v1/agent-configuration/workspace-voice",
			prefix: `{"expected_row_version":1,"voice":{"kind":"custom","instruction":"`,
			suffix: `"}}`,
		},
		{
			path:   "/api/v1/agent-configuration/roles/builder",
			prefix: `{"expected_row_version":0,"expected_prompt_catalog_digest":"` + strings.Repeat("a", 64) + `","guidance_override":"`,
			suffix: `"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			body := append([]byte(tt.prefix), 0xff)
			body = append(body, []byte(tt.suffix)...)
			req := httptest.NewRequest(http.MethodPut, tt.path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", testIdempotencyKey(t, "invalid-utf8-"+tt.path, time.Now()))
			authenticateAPITestRequest(req)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid UTF-8 status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}
