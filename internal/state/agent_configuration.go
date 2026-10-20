package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"kingdom/internal/core/rolecomm"
	"kingdom/internal/prompts"
)

const (
	AgentVoiceKindNeutral = rolecomm.NeutralStyleID
	AgentVoiceKindTheme   = "theme"
	AgentVoiceKindCustom  = "custom"
	AgentGuidanceLimit    = 8000
	AgentVoiceTextLimit   = 2000
)

var ErrAgentConfigurationConflict = errors.New("agent configuration changed since it was read")
var ErrAgentPromptCatalogConflict = errors.New("agent prompt catalog changed since settings were read")

type AgentConfigurationValidationError struct{ Message string }

func (e *AgentConfigurationValidationError) Error() string {
	if e == nil {
		return "invalid agent configuration"
	}
	return e.Message
}

func invalidAgentConfiguration(err error) error {
	return &AgentConfigurationValidationError{Message: err.Error()}
}

type AgentVoiceSource struct {
	Kind        string `json:"kind"`
	ThemeID     string `json:"theme_id,omitempty"`
	ThemeDigest string `json:"theme_digest,omitempty"`
}

type AgentVoice struct {
	Source      AgentVoiceSource `json:"source"`
	Instruction string           `json:"instruction,omitempty"`
}

type AgentWorkspaceConfiguration struct {
	Voice      AgentVoice `json:"voice"`
	RowVersion int64      `json:"row_version"`
}

type AgentRoleConfiguration struct {
	RoleID            string      `json:"role_id"`
	DefaultGuidance   string      `json:"default_guidance"`
	GuidanceOverride  *string     `json:"guidance_override"`
	EffectiveGuidance string      `json:"effective_guidance"`
	EffectiveVoice    AgentVoice  `json:"effective_voice"`
	VoiceOverride     *AgentVoice `json:"voice_override,omitempty"`
	RowVersion        int64       `json:"row_version"`
}

type AgentConfiguration struct {
	Workspace           AgentWorkspaceConfiguration `json:"workspace"`
	Roles               []AgentRoleConfiguration    `json:"roles"`
	PromptCatalogDigest string                      `json:"prompt_catalog_digest"`
}

type AgentRolePromptConfiguration struct {
	GuidanceText string     `json:"guidance_text"`
	Voice        AgentVoice `json:"voice"`
}

type AgentExecutionConfigurationSnapshot struct {
	Version             int                                     `json:"version"`
	PromptCatalogDigest string                                  `json:"prompt_catalog_digest"`
	WorkspaceVoice      AgentVoice                              `json:"workspace_voice"`
	Roles               map[string]AgentRolePromptConfiguration `json:"roles"`
	PromptCatalog       prompts.Catalog                         `json:"-"`
}

type PlanningAgentConfigurationSnapshot struct {
	Version             int                          `json:"version"`
	PromptCatalogDigest string                       `json:"prompt_catalog_digest"`
	Role                AgentRolePromptConfiguration `json:"role"`
}

type PlanningPromptConfigurationSnapshot struct {
	Role          AgentRolePromptConfiguration
	PromptCatalog prompts.Catalog
}

type agentConfigurationQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func NeutralAgentVoice() AgentVoice {
	return AgentVoice{Source: AgentVoiceSource{Kind: AgentVoiceKindNeutral}}
}

func AgentRoleIDs() []string {
	roles := CanonicalRoleIDs()
	result := make([]string, 0, len(roles))
	for _, roleID := range roles {
		if strings.TrimSpace(roleID) != "" && roleID != "operator" {
			result = append(result, roleID)
		}
	}
	return result
}

var loadCanonicalPromptCatalog = sync.OnceValue(func() struct {
	catalog prompts.Catalog
	err     error
} {
	catalog, err := prompts.LoadDefault(CanonicalRoleIDs())
	return struct {
		catalog prompts.Catalog
		err     error
	}{catalog: catalog, err: err}
})

func CanonicalPromptCatalog() (prompts.Catalog, error) {
	loaded := loadCanonicalPromptCatalog()
	return loaded.catalog, loaded.err
}

func GetAgentConfiguration(ctx context.Context, db *sql.DB) (AgentConfiguration, error) {
	if db == nil {
		return AgentConfiguration{}, fmt.Errorf("database handle is required")
	}
	return readAgentConfiguration(ctx, db)
}

func readAgentConfiguration(ctx context.Context, q agentConfigurationQuerier) (AgentConfiguration, error) {
	catalog, err := CanonicalPromptCatalog()
	if err != nil {
		return AgentConfiguration{}, fmt.Errorf("load canonical prompt catalog: %w", err)
	}
	_, digest, err := catalog.Revision()
	if err != nil {
		return AgentConfiguration{}, err
	}
	var config AgentConfiguration
	config.PromptCatalogDigest = digest
	var workspace AgentWorkspaceConfiguration
	var kind, themeID, voiceDigest, instruction string
	if err := q.QueryRowContext(ctx, `
SELECT voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version
	FROM agent_workspace_configuration WHERE singleton = 1`).Scan(&kind, &themeID, &voiceDigest, &instruction, &workspace.RowVersion); err != nil {
		return AgentConfiguration{}, fmt.Errorf("read workspace agent configuration: %w", err)
	}
	workspace.Voice = AgentVoice{Source: AgentVoiceSource{Kind: kind, ThemeID: themeID, ThemeDigest: voiceDigest}, Instruction: instruction}
	if err := validateAgentVoice(workspace.Voice); err != nil {
		return AgentConfiguration{}, fmt.Errorf("invalid workspace agent voice: %w", err)
	}
	config.Workspace = workspace
	byRole := make(map[string]AgentRoleConfiguration)
	rows, err := q.QueryContext(ctx, `
SELECT role_id, guidance_override, voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version
FROM agent_role_configuration`)
	if err != nil {
		return AgentConfiguration{}, fmt.Errorf("list role agent configuration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var role AgentRoleConfiguration
		var guidanceOverride, overrideKind sql.NullString
		var overrideThemeID, overrideDigest, overrideInstruction string
		if err := rows.Scan(&role.RoleID, &guidanceOverride, &overrideKind, &overrideThemeID, &overrideDigest, &overrideInstruction, &role.RowVersion); err != nil {
			return AgentConfiguration{}, fmt.Errorf("scan role agent configuration: %w", err)
		}
		definition, ok := catalog.RolePolicy.Roles[role.RoleID]
		if !ok {
			return AgentConfiguration{}, fmt.Errorf("agent configuration references unknown role %q", role.RoleID)
		}
		role.DefaultGuidance = definition.DefaultGuidance
		role.EffectiveGuidance = definition.DefaultGuidance
		if guidanceOverride.Valid {
			value := guidanceOverride.String
			role.GuidanceOverride = &value
			role.EffectiveGuidance = value
		}
		if overrideKind.Valid {
			voice := AgentVoice{Source: AgentVoiceSource{Kind: overrideKind.String, ThemeID: overrideThemeID, ThemeDigest: overrideDigest}, Instruction: overrideInstruction}
			if err := validateAgentVoice(voice); err != nil {
				return AgentConfiguration{}, fmt.Errorf("invalid agent voice for role %q: %w", role.RoleID, err)
			}
			role.VoiceOverride = &voice
		}
		role.EffectiveVoice = workspace.Voice
		if role.VoiceOverride != nil {
			role.EffectiveVoice = *role.VoiceOverride
		}
		if role.GuidanceOverride != nil {
			if err := validateRoleGuidance(role.RoleID, *role.GuidanceOverride); err != nil {
				return AgentConfiguration{}, err
			}
		}
		byRole[role.RoleID] = role
	}
	if err := rows.Err(); err != nil {
		return AgentConfiguration{}, fmt.Errorf("read role agent configuration: %w", err)
	}
	config.Roles = make([]AgentRoleConfiguration, 0, len(AgentRoleIDs()))
	for _, roleID := range AgentRoleIDs() {
		role, ok := byRole[roleID]
		if !ok {
			definition := catalog.RolePolicy.Roles[roleID]
			role = AgentRoleConfiguration{RoleID: roleID, DefaultGuidance: definition.DefaultGuidance, EffectiveGuidance: definition.DefaultGuidance, EffectiveVoice: workspace.Voice}
		}
		config.Roles = append(config.Roles, role)
	}
	return config, nil
}

func UpdateWorkspaceAgentVoice(ctx context.Context, db *sql.DB, expectedVersion int64, voice AgentVoice, updatedAt string) (AgentWorkspaceConfiguration, error) {
	if db == nil {
		return AgentWorkspaceConfiguration{}, fmt.Errorf("database handle is required")
	}
	if expectedVersion < 1 {
		return AgentWorkspaceConfiguration{}, invalidAgentConfiguration(fmt.Errorf("workspace agent configuration version must be positive"))
	}
	voice = normalizeAgentVoice(voice)
	if err := validateAgentVoice(voice); err != nil {
		return AgentWorkspaceConfiguration{}, invalidAgentConfiguration(err)
	}
	result, err := db.ExecContext(ctx, `
UPDATE agent_workspace_configuration
SET voice_kind = ?, voice_theme_id = ?, voice_theme_digest = ?, voice_instruction = ?,
    row_version = row_version + 1, updated_at = ?
WHERE singleton = 1 AND row_version = ?`,
		voice.Source.Kind, voice.Source.ThemeID, voice.Source.ThemeDigest, voice.Instruction, strings.TrimSpace(updatedAt), expectedVersion)
	if err != nil {
		return AgentWorkspaceConfiguration{}, fmt.Errorf("update workspace agent voice: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return AgentWorkspaceConfiguration{}, fmt.Errorf("confirm workspace agent voice update: %w", err)
	}
	if changed != 1 {
		return AgentWorkspaceConfiguration{}, ErrAgentConfigurationConflict
	}
	return AgentWorkspaceConfiguration{Voice: voice, RowVersion: expectedVersion + 1}, nil
}

func UpdateAgentRoleConfiguration(ctx context.Context, db *sql.DB, roleID string, guidanceOverride *string, voiceOverride *AgentVoice, expectedVersion int64, expectedPromptCatalogDigest, updatedAt string) (AgentRoleConfiguration, error) {
	if db == nil {
		return AgentRoleConfiguration{}, fmt.Errorf("database handle is required")
	}
	roleID = strings.TrimSpace(roleID)
	if expectedVersion < 0 {
		return AgentRoleConfiguration{}, invalidAgentConfiguration(fmt.Errorf("role agent configuration version cannot be negative"))
	}
	if err := validateExpectedPromptCatalogDigest(expectedPromptCatalogDigest); err != nil {
		return AgentRoleConfiguration{}, err
	}
	if err := validateRoleGuidance(roleID, ""); err != nil {
		return AgentRoleConfiguration{}, invalidAgentConfiguration(err)
	}
	if guidanceOverride != nil {
		if err := validateRoleGuidance(roleID, *guidanceOverride); err != nil {
			return AgentRoleConfiguration{}, invalidAgentConfiguration(err)
		}
	}
	if voiceOverride != nil {
		normalized := normalizeAgentVoice(*voiceOverride)
		voiceOverride = &normalized
	}
	voiceKind := any(nil)
	themeID, digest, instruction := "", "", ""
	if voiceOverride != nil {
		if err := validateAgentVoice(*voiceOverride); err != nil {
			return AgentRoleConfiguration{}, invalidAgentConfiguration(err)
		}
		voiceKind = voiceOverride.Source.Kind
		themeID, digest, instruction = voiceOverride.Source.ThemeID, voiceOverride.Source.ThemeDigest, voiceOverride.Instruction
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRoleConfiguration{}, fmt.Errorf("begin role agent configuration update: %w", err)
	}
	defer tx.Rollback()
	if expectedVersion == 0 {
		_, err := tx.ExecContext(ctx, `
INSERT INTO agent_role_configuration (role_id, guidance_override, voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 1, ?)`, roleID, nullableGuidanceValue(guidanceOverride), voiceKind, themeID, digest, instruction, strings.TrimSpace(updatedAt))
		if err != nil {
			if IsUniqueViolationOn(err, "agent_role_configuration.role_id") {
				return AgentRoleConfiguration{}, ErrAgentConfigurationConflict
			}
			return AgentRoleConfiguration{}, fmt.Errorf("create role agent configuration: %w", err)
		}
	} else {
		result, err := tx.ExecContext(ctx, `
UPDATE agent_role_configuration
SET guidance_override = ?, voice_kind = ?, voice_theme_id = ?, voice_theme_digest = ?, voice_instruction = ?,
    row_version = row_version + 1, updated_at = ?
WHERE role_id = ? AND row_version = ?`,
			nullableGuidanceValue(guidanceOverride), voiceKind, themeID, digest, instruction, strings.TrimSpace(updatedAt), roleID, expectedVersion)
		if err != nil {
			return AgentRoleConfiguration{}, fmt.Errorf("update role agent configuration: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return AgentRoleConfiguration{}, fmt.Errorf("confirm role agent configuration update: %w", err)
		}
		if changed != 1 {
			return AgentRoleConfiguration{}, ErrAgentConfigurationConflict
		}
	}
	configuration, err := readAgentConfiguration(ctx, tx)
	if err != nil {
		return AgentRoleConfiguration{}, err
	}
	role, ok := agentRoleFromConfiguration(configuration, roleID)
	if !ok {
		return AgentRoleConfiguration{}, fmt.Errorf("updated agent role %q is not available", roleID)
	}
	if err := tx.Commit(); err != nil {
		return AgentRoleConfiguration{}, fmt.Errorf("commit role agent configuration update: %w", err)
	}
	return role, nil
}

func nullableGuidanceValue(guidance *string) any {
	if guidance == nil {
		return nil
	}
	return *guidance
}

func ResetAgentRoleGuidance(ctx context.Context, db *sql.DB, roleID string, expectedVersion int64, expectedPromptCatalogDigest, updatedAt string) (AgentRoleConfiguration, error) {
	if db == nil {
		return AgentRoleConfiguration{}, fmt.Errorf("database handle is required")
	}
	roleID = strings.TrimSpace(roleID)
	if expectedVersion < 0 {
		return AgentRoleConfiguration{}, invalidAgentConfiguration(fmt.Errorf("role agent configuration version cannot be negative"))
	}
	if err := validateExpectedPromptCatalogDigest(expectedPromptCatalogDigest); err != nil {
		return AgentRoleConfiguration{}, err
	}
	if err := validateRoleGuidance(roleID, ""); err != nil {
		return AgentRoleConfiguration{}, invalidAgentConfiguration(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRoleConfiguration{}, fmt.Errorf("begin role agent guidance reset: %w", err)
	}
	defer tx.Rollback()
	if expectedVersion == 0 {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_role_configuration WHERE role_id = ?)`, roleID).Scan(&exists); err != nil {
			return AgentRoleConfiguration{}, fmt.Errorf("read role agent configuration before reset: %w", err)
		}
		if exists != 0 {
			return AgentRoleConfiguration{}, ErrAgentConfigurationConflict
		}
	} else {
		result, err := tx.ExecContext(ctx, `
UPDATE agent_role_configuration
SET guidance_override = NULL, row_version = row_version + 1, updated_at = ?
WHERE role_id = ? AND row_version = ?`, strings.TrimSpace(updatedAt), roleID, expectedVersion)
		if err != nil {
			return AgentRoleConfiguration{}, fmt.Errorf("reset role agent guidance: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return AgentRoleConfiguration{}, fmt.Errorf("confirm role agent guidance reset: %w", err)
		}
		if changed != 1 {
			return AgentRoleConfiguration{}, ErrAgentConfigurationConflict
		}
	}
	configuration, err := readAgentConfiguration(ctx, tx)
	if err != nil {
		return AgentRoleConfiguration{}, err
	}
	role, ok := agentRoleFromConfiguration(configuration, roleID)
	if !ok {
		return AgentRoleConfiguration{}, fmt.Errorf("reset agent role %q is not available", roleID)
	}
	if err := tx.Commit(); err != nil {
		return AgentRoleConfiguration{}, fmt.Errorf("commit role agent guidance reset: %w", err)
	}
	return role, nil
}

func agentRoleFromConfiguration(configuration AgentConfiguration, roleID string) (AgentRoleConfiguration, bool) {
	for _, role := range configuration.Roles {
		if role.RoleID == roleID {
			return role, true
		}
	}
	return AgentRoleConfiguration{}, false
}

func validateExpectedPromptCatalogDigest(expected string) error {
	expected = strings.TrimSpace(expected)
	if !isLowerHexDigest(expected) {
		return invalidAgentConfiguration(fmt.Errorf("expected prompt catalog digest must be a SHA-256 digest"))
	}
	catalog, err := CanonicalPromptCatalog()
	if err != nil {
		return fmt.Errorf("load canonical prompt catalog: %w", err)
	}
	_, current, err := catalog.Revision()
	if err != nil {
		return err
	}
	if current != expected {
		return ErrAgentPromptCatalogConflict
	}
	return nil
}

func normalizeAgentVoice(voice AgentVoice) AgentVoice {
	voice.Source.Kind = strings.TrimSpace(voice.Source.Kind)
	voice.Source.ThemeID = strings.TrimSpace(voice.Source.ThemeID)
	voice.Source.ThemeDigest = strings.TrimSpace(voice.Source.ThemeDigest)
	return voice
}

func persistPromptCatalogRevisionTx(ctx context.Context, tx *sql.Tx, createdAt string) (string, error) {
	catalog, err := CanonicalPromptCatalog()
	if err != nil {
		return "", fmt.Errorf("load canonical prompt catalog: %w", err)
	}
	raw, digest, err := catalog.Revision()
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO prompt_catalog_revisions (prompt_catalog_digest, catalog_json, created_at) VALUES (?, ?, ?)`, digest, string(raw), strings.TrimSpace(createdAt)); err != nil {
		return "", fmt.Errorf("persist prompt catalog revision: %w", err)
	}
	var stored string
	if err := tx.QueryRowContext(ctx, `SELECT catalog_json FROM prompt_catalog_revisions WHERE prompt_catalog_digest = ?`, digest).Scan(&stored); err != nil {
		return "", fmt.Errorf("verify persisted prompt catalog revision: %w", err)
	}
	if stored != string(raw) {
		return "", fmt.Errorf("prompt catalog digest collision for %q", digest)
	}
	return digest, nil
}

func LoadPromptCatalogRevision(ctx context.Context, db *sql.DB, digest string) (prompts.Catalog, error) {
	if db == nil {
		return prompts.Catalog{}, fmt.Errorf("database handle is required")
	}
	digest = strings.TrimSpace(digest)
	var raw string
	if err := db.QueryRowContext(ctx, `SELECT catalog_json FROM prompt_catalog_revisions WHERE prompt_catalog_digest = ?`, digest).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return prompts.Catalog{}, fmt.Errorf("prompt catalog revision %q is missing", digest)
		}
		return prompts.Catalog{}, fmt.Errorf("load prompt catalog revision %q: %w", digest, err)
	}
	catalog, err := prompts.ParseRevision([]byte(raw), digest, nil)
	if err != nil {
		return prompts.Catalog{}, fmt.Errorf("validate prompt catalog revision %q: %w", digest, err)
	}
	return catalog, nil
}

func CaptureRunAgentConfigurationSnapshotTx(ctx context.Context, tx *sql.Tx, runID, createdAt string) error {
	if tx == nil {
		return fmt.Errorf("transaction is required")
	}
	config, err := readAgentConfiguration(ctx, tx)
	if err != nil {
		return err
	}
	digest, err := persistPromptCatalogRevisionTx(ctx, tx, createdAt)
	if err != nil {
		return err
	}
	snapshot := AgentExecutionConfigurationSnapshot{
		Version:             1,
		PromptCatalogDigest: digest,
		WorkspaceVoice:      config.Workspace.Voice,
		Roles:               make(map[string]AgentRolePromptConfiguration, len(config.Roles)),
	}
	for _, role := range config.Roles {
		snapshot.Roles[role.RoleID] = config.PromptConfigurationForRole(role.RoleID)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode run agent configuration snapshot: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_agent_configuration_snapshots (run_id, snapshot_version, snapshot_json, prompt_catalog_digest, created_at) VALUES (?, 1, ?, ?, ?)`, strings.TrimSpace(runID), string(raw), digest, strings.TrimSpace(createdAt)); err != nil {
		return fmt.Errorf("persist run agent configuration snapshot: %w", err)
	}
	return nil
}

func LoadRunAgentConfigurationSnapshot(ctx context.Context, db *sql.DB, runID string) (AgentExecutionConfigurationSnapshot, error) {
	if db == nil {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("database handle is required")
	}
	var raw, digest string
	err := db.QueryRowContext(ctx, `SELECT snapshot_json, prompt_catalog_digest FROM run_agent_configuration_snapshots WHERE run_id = ?`, strings.TrimSpace(runID)).Scan(&raw, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("run agent configuration snapshot for %q is missing", strings.TrimSpace(runID))
	}
	if err != nil {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("load run agent configuration snapshot: %w", err)
	}
	var snapshot AgentExecutionConfigurationSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("decode run agent configuration snapshot: %w", err)
	}
	if snapshot.Version != 1 || snapshot.Roles == nil || snapshot.PromptCatalogDigest != digest {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("run agent configuration snapshot is unsupported")
	}
	catalog, err := LoadPromptCatalogRevision(ctx, db, digest)
	if err != nil {
		return AgentExecutionConfigurationSnapshot{}, err
	}
	snapshot.PromptCatalog = catalog
	if err := validateAgentVoice(snapshot.WorkspaceVoice); err != nil {
		return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("run agent configuration snapshot voice is invalid: %w", err)
	}
	for roleID, role := range snapshot.Roles {
		if err := validateRoleGuidance(roleID, role.GuidanceText); err != nil {
			return AgentExecutionConfigurationSnapshot{}, err
		}
		if err := validateAgentVoice(role.Voice); err != nil {
			return AgentExecutionConfigurationSnapshot{}, fmt.Errorf("run agent configuration snapshot voice for %q is invalid: %w", roleID, err)
		}
	}
	return snapshot, nil
}

func (s AgentExecutionConfigurationSnapshot) ForRole(roleID string) AgentRolePromptConfiguration {
	if role, ok := s.Roles[strings.TrimSpace(roleID)]; ok {
		return role
	}
	return AgentRolePromptConfiguration{Voice: s.WorkspaceVoice}
}

func (c AgentConfiguration) PromptConfigurationForRole(roleID string) AgentRolePromptConfiguration {
	roleID = strings.TrimSpace(roleID)
	for _, role := range c.Roles {
		if role.RoleID != roleID {
			continue
		}
		voice := c.Workspace.Voice
		if role.VoiceOverride != nil {
			voice = *role.VoiceOverride
		}
		return AgentRolePromptConfiguration{GuidanceText: role.EffectiveGuidance, Voice: voice}
	}
	return AgentRolePromptConfiguration{Voice: c.Workspace.Voice}
}

func CapturePlanningAgentConfigurationSnapshotTx(ctx context.Context, tx *sql.Tx, sessionID, createdAt string) error {
	if tx == nil {
		return fmt.Errorf("transaction is required")
	}
	config, err := readAgentConfiguration(ctx, tx)
	if err != nil {
		return err
	}
	role := config.PromptConfigurationForRole("plan-author")
	digest, err := persistPromptCatalogRevisionTx(ctx, tx, createdAt)
	if err != nil {
		return err
	}
	snapshot, err := json.Marshal(PlanningAgentConfigurationSnapshot{Version: 1, PromptCatalogDigest: digest, Role: role})
	if err != nil {
		return fmt.Errorf("encode planning agent configuration snapshot: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO planning_agent_configuration_snapshots (planning_session_id, snapshot_version, snapshot_json, prompt_catalog_digest, created_at) VALUES (?, 1, ?, ?, ?)`, strings.TrimSpace(sessionID), string(snapshot), digest, strings.TrimSpace(createdAt)); err != nil {
		return fmt.Errorf("persist planning agent configuration snapshot: %w", err)
	}
	return nil
}

func LoadPlanningAgentConfiguration(ctx context.Context, db *sql.DB, sessionID string) (PlanningPromptConfigurationSnapshot, error) {
	if db == nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("database handle is required")
	}
	var raw, digest string
	err := db.QueryRowContext(ctx, `SELECT snapshot_json, prompt_catalog_digest FROM planning_agent_configuration_snapshots WHERE planning_session_id = ?`, strings.TrimSpace(sessionID)).Scan(&raw, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("planning agent configuration snapshot for %q is missing", strings.TrimSpace(sessionID))
	}
	if err != nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("load planning agent configuration snapshot: %w", err)
	}
	var snapshot PlanningAgentConfigurationSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("decode planning agent configuration snapshot: %w", err)
	}
	if snapshot.Version != 1 || snapshot.PromptCatalogDigest != digest {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("planning agent configuration snapshot is unsupported")
	}
	if err := validateRoleGuidance("plan-author", snapshot.Role.GuidanceText); err != nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("planning agent configuration snapshot guidance is invalid: %w", err)
	}
	if err := validateAgentVoice(snapshot.Role.Voice); err != nil {
		return PlanningPromptConfigurationSnapshot{}, fmt.Errorf("planning agent configuration snapshot voice is invalid: %w", err)
	}
	catalog, err := LoadPromptCatalogRevision(ctx, db, digest)
	if err != nil {
		return PlanningPromptConfigurationSnapshot{}, err
	}
	return PlanningPromptConfigurationSnapshot{Role: snapshot.Role, PromptCatalog: catalog}, nil
}

func validateAgentVoice(voice AgentVoice) error {
	if !utf8.ValidString(voice.Instruction) {
		return fmt.Errorf("agent voice must be valid UTF-8")
	}
	switch voice.Source.Kind {
	case AgentVoiceKindNeutral:
		if voice.Source.ThemeID != "" || voice.Source.ThemeDigest != "" || voice.Instruction != "" {
			return fmt.Errorf("neutral agent voice cannot include theme data or custom instructions")
		}
	case AgentVoiceKindTheme:
		if strings.TrimSpace(voice.Source.ThemeID) == "" || len(voice.Source.ThemeDigest) != 64 || strings.TrimSpace(voice.Instruction) == "" {
			return fmt.Errorf("theme agent voice requires a theme revision and communication guidance")
		}
		if !isLowerHexDigest(voice.Source.ThemeDigest) {
			return fmt.Errorf("theme agent voice revision digest is invalid")
		}
		if utf8.RuneCountInString(voice.Instruction) > AgentVoiceTextLimit {
			return fmt.Errorf("agent voice exceeds %d characters", AgentVoiceTextLimit)
		}
	case AgentVoiceKindCustom:
		if voice.Source.ThemeID != "" || voice.Source.ThemeDigest != "" || strings.TrimSpace(voice.Instruction) == "" {
			return fmt.Errorf("custom agent voice requires communication guidance and cannot include theme revision data")
		}
		if !utf8.ValidString(voice.Instruction) {
			return fmt.Errorf("agent voice must be valid UTF-8")
		}
		if utf8.RuneCountInString(voice.Instruction) > AgentVoiceTextLimit {
			return fmt.Errorf("agent voice exceeds %d characters", AgentVoiceTextLimit)
		}
	default:
		return fmt.Errorf("agent voice kind must be neutral-v1, theme, or custom")
	}
	return nil
}

func validateRoleGuidance(roleID, guidance string) error {
	roleID = strings.TrimSpace(roleID)
	validRole := false
	for _, canonical := range AgentRoleIDs() {
		if roleID == canonical {
			validRole = true
			break
		}
	}
	if !validRole {
		return fmt.Errorf("role %q does not support agent guidance", roleID)
	}
	if utf8.RuneCountInString(guidance) > AgentGuidanceLimit {
		return fmt.Errorf("role guidance exceeds %d characters", AgentGuidanceLimit)
	}
	if !utf8.ValidString(guidance) {
		return fmt.Errorf("role guidance must be valid UTF-8")
	}
	return nil
}

func isLowerHexDigest(value string) bool {
	for _, r := range value {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return len(value) == 64
}
