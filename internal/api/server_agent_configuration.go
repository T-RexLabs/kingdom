package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
	"kingdom/internal/theme"
)

type agentVoiceSelectionError struct {
	status  int
	message string
	cause   error
}

func (e *agentVoiceSelectionError) Error() string { return e.message }
func (e *agentVoiceSelectionError) Unwrap() error { return e.cause }

func (s *Server) handleGetAgentConfiguration(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.agent_configuration"); !ok {
		return
	}
	configuration, err := state.GetAgentConfiguration(r.Context(), s.db)
	if err != nil {
		s.writeInternalError(w, "read agent configuration failed")
		return
	}
	writeJSON(w, http.StatusOK, presentAgentConfiguration(configuration))
}

func (s *Server) handleUpdateWorkspaceAgentVoice(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.agent_configuration"); !ok {
		return
	}
	var request gen.UpdateWorkspaceAgentVoiceRequest
	if _, err := decodeAgentConfigurationJSON(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	configuration, err := state.GetAgentConfiguration(r.Context(), s.db)
	if err != nil {
		s.writeInternalError(w, "read agent configuration failed")
		return
	}
	existing := configuration.Workspace.Voice
	voice, err := s.resolveAgentVoiceSelection(request.Voice, &existing)
	if err != nil {
		s.writeAgentVoiceSelectionError(w, err)
		return
	}
	updated, err := state.UpdateWorkspaceAgentVoice(r.Context(), s.db, int64(request.ExpectedRowVersion), voice, s.now().UTC().Format(fixedUTCTimestampLayout))
	if errors.Is(err, state.ErrAgentConfigurationConflict) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "Agent settings changed in another window. Reload the latest settings and try again.", "api.agent_configuration", nil)
		return
	}
	if err != nil {
		var validation *state.AgentConfigurationValidationError
		if errors.As(err, &validation) {
			s.writeBadRequest(w, validation.Error())
		} else {
			s.writeInternalError(w, "update workspace agent voice failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, presentAgentWorkspaceConfiguration(updated))
}

func (s *Server) handleUpdateAgentRoleConfiguration(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.agent_configuration"); !ok {
		return
	}
	var request gen.UpdateAgentRoleConfigurationRequest
	if _, err := decodeAgentConfigurationJSON(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	var voiceOverride *state.AgentVoice
	if request.VoiceOverride != nil {
		configuration, err := state.GetAgentConfiguration(r.Context(), s.db)
		if err != nil {
			s.writeInternalError(w, "read agent configuration failed")
			return
		}
		var existing *state.AgentVoice
		for _, role := range configuration.Roles {
			if role.RoleID == strings.TrimSpace(r.PathValue("roleId")) && role.VoiceOverride != nil {
				voice := *role.VoiceOverride
				existing = &voice
				break
			}
		}
		voice, err := s.resolveAgentVoiceSelection(*request.VoiceOverride, existing)
		if err != nil {
			s.writeAgentVoiceSelectionError(w, err)
			return
		}
		voiceOverride = &voice
	}
	updated, err := state.UpdateAgentRoleConfiguration(r.Context(), s.db, r.PathValue("roleId"), request.GuidanceOverride, voiceOverride, int64(request.ExpectedRowVersion), request.ExpectedPromptCatalogDigest, s.now().UTC().Format(fixedUTCTimestampLayout))
	if errors.Is(err, state.ErrAgentConfigurationConflict) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "This agent's settings changed in another window. Reload the latest settings and try again.", "api.agent_configuration", nil)
		return
	}
	if errors.Is(err, state.ErrAgentPromptCatalogConflict) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "Agent guidance defaults changed. Reload the latest settings before saving.", "api.agent_configuration", nil)
		return
	}
	if err != nil {
		var validation *state.AgentConfigurationValidationError
		if errors.As(err, &validation) {
			s.writeBadRequest(w, validation.Error())
		} else {
			s.writeInternalError(w, "update role agent configuration failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, presentAgentRoleConfiguration(updated))
}

func decodeAgentConfigurationJSON(w http.ResponseWriter, r *http.Request, destination any) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errors.New("agent settings request is too large or unreadable")
	}
	if !utf8.Valid(rawBody) {
		return nil, errors.New("agent settings must be valid UTF-8")
	}
	r.Body = io.NopCloser(bytes.NewReader(rawBody))
	if err := decodeStrictJSON(r, destination); err != nil {
		return nil, err
	}
	return rawBody, nil
}

func (s *Server) handleResetAgentRoleGuidance(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.agent_configuration"); !ok {
		return
	}
	var request gen.ResetAgentRoleGuidanceRequest
	if _, err := decodeAgentConfigurationJSON(w, r, &request); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	updated, err := state.ResetAgentRoleGuidance(r.Context(), s.db, r.PathValue("roleId"), int64(request.ExpectedRowVersion), request.ExpectedPromptCatalogDigest, s.now().UTC().Format(fixedUTCTimestampLayout))
	if errors.Is(err, state.ErrAgentConfigurationConflict) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "This agent's settings changed in another window. Reload the latest settings and try again.", "api.agent_configuration", nil)
		return
	}
	if errors.Is(err, state.ErrAgentPromptCatalogConflict) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "Agent guidance defaults changed. Reload the latest settings before resetting.", "api.agent_configuration", nil)
		return
	}
	if err != nil {
		var validation *state.AgentConfigurationValidationError
		if errors.As(err, &validation) {
			s.writeBadRequest(w, validation.Error())
		} else {
			s.writeInternalError(w, "reset role agent guidance failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, presentAgentRoleConfiguration(updated))
}

func (s *Server) resolveAgentVoiceSelection(selection gen.AgentVoiceSelection, existing *state.AgentVoice) (state.AgentVoice, error) {
	kind := strings.TrimSpace(string(selection.Kind))
	themeID := strings.TrimSpace(optionalStringValue(selection.ThemeId))
	themeDigest := strings.TrimSpace(optionalStringValue(selection.ThemeDigest))
	switch kind {
	case state.AgentVoiceKindNeutral:
		if themeID != "" || themeDigest != "" || selection.Instruction != nil {
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusBadRequest, message: "neutral voice cannot include theme revision data"}
		}
		return state.NeutralAgentVoice(), nil
	case state.AgentVoiceKindTheme:
		if themeID == "" || themeDigest == "" || selection.Instruction != nil {
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusBadRequest, message: "theme voice requires an exact theme revision"}
		}
		// A saved selection remains valid after its theme is unpublished because
		// the resolved instruction is copied into workspace agent configuration.
		if existing != nil && existing.Source.Kind == kind && existing.Source.ThemeID == themeID && existing.Source.ThemeDigest == themeDigest {
			return *existing, nil
		}
		if s.themes == nil {
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusServiceUnavailable, message: "theme storage is unavailable"}
		}
		pkg, err := s.themes.ReadRevision(themeID, themeDigest)
		if err != nil {
			if errors.Is(err, theme.ErrNotFound) {
				return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusNotFound, message: "the selected theme revision is no longer available", cause: err}
			}
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusInternalServerError, message: "theme revision could not be read", cause: err}
		}
		if strings.TrimSpace(pkg.Manifest.AgentVoice) == "" {
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusBadRequest, message: "selected theme revision does not define an agent voice"}
		}
		return state.AgentVoice{
			Source:      state.AgentVoiceSource{Kind: kind, ThemeID: themeID, ThemeDigest: themeDigest},
			Instruction: pkg.Manifest.AgentVoice,
		}, nil
	case state.AgentVoiceKindCustom:
		if themeID != "" || themeDigest != "" || selection.Instruction == nil {
			return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusBadRequest, message: "custom communication style requires instruction text and cannot include theme revision data"}
		}
		return state.AgentVoice{Source: state.AgentVoiceSource{Kind: kind}, Instruction: *selection.Instruction}, nil
	default:
		return state.AgentVoice{}, &agentVoiceSelectionError{status: http.StatusBadRequest, message: "voice kind must be neutral-v1, theme, or custom"}
	}
}

func presentAgentConfiguration(configuration state.AgentConfiguration) gen.AgentConfiguration {
	roles := make([]gen.AgentRoleConfiguration, 0, len(configuration.Roles))
	for _, role := range configuration.Roles {
		roles = append(roles, presentAgentRoleConfiguration(role))
	}
	return gen.AgentConfiguration{Workspace: presentAgentWorkspaceConfiguration(configuration.Workspace), Roles: roles, PromptCatalogDigest: configuration.PromptCatalogDigest}
}

func presentAgentWorkspaceConfiguration(configuration state.AgentWorkspaceConfiguration) gen.AgentWorkspaceConfiguration {
	return gen.AgentWorkspaceConfiguration{Voice: presentAgentVoice(configuration.Voice), RowVersion: int(configuration.RowVersion)}
}

func presentAgentRoleConfiguration(configuration state.AgentRoleConfiguration) gen.AgentRoleConfiguration {
	result := gen.AgentRoleConfiguration{
		RoleId: configuration.RoleID, DefaultGuidance: configuration.DefaultGuidance,
		GuidanceOverride: configuration.GuidanceOverride, EffectiveGuidance: configuration.EffectiveGuidance,
		EffectiveVoice: presentAgentVoice(configuration.EffectiveVoice),
		RowVersion:     int(configuration.RowVersion),
	}
	if configuration.VoiceOverride != nil {
		voice := presentAgentVoice(*configuration.VoiceOverride)
		result.VoiceOverride = &voice
	}
	return result
}

func presentAgentVoice(voice state.AgentVoice) gen.AgentVoice {
	source := gen.AgentVoiceSource{Kind: gen.AgentVoiceSourceKind(voice.Source.Kind)}
	if voice.Source.ThemeID != "" {
		themeID := voice.Source.ThemeID
		source.ThemeId = &themeID
	}
	if voice.Source.ThemeDigest != "" {
		digest := voice.Source.ThemeDigest
		source.ThemeDigest = &digest
	}
	result := gen.AgentVoice{Source: source}
	if voice.Instruction != "" {
		instruction := voice.Instruction
		result.Instruction = &instruction
	}
	return result
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Server) writeAgentVoiceSelectionError(w http.ResponseWriter, err error) {
	var selectionError *agentVoiceSelectionError
	if !errors.As(err, &selectionError) {
		s.writeInternalError(w, "resolve selected agent voice failed")
		return
	}
	switch selectionError.status {
	case http.StatusBadRequest:
		s.writeBadRequest(w, selectionError.message)
	case http.StatusNotFound:
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, selectionError.message, "api.agent_configuration", nil)
	case http.StatusServiceUnavailable:
		writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeDependencyBlocked, selectionError.message, "api.agent_configuration", nil)
	default:
		s.writeInternalError(w, "resolve selected agent voice failed")
	}
}
