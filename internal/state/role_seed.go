package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const canonicalRoleSeedRetryWindow = 3 * time.Second

type CanonicalRole struct {
	RoleID                         string
	DisplayName                    string
	Description                    string
	CanSpawn                       bool
	AgentModelSelectorVisible      bool
	AgentModelSelectorSectionID    string
	AgentModelSelectorSectionLabel string
	AgentModelSelectorSectionOrder int
	AgentModelSelectorRoleOrder    int
}

var canonicalRoles = []CanonicalRole{
	{
		RoleID:                         "allfather",
		DisplayName:                    "Allfather",
		Description:                    "Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      false,
		AgentModelSelectorSectionID:    "operators",
		AgentModelSelectorSectionLabel: "Operators",
		AgentModelSelectorSectionOrder: 0,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "king",
		DisplayName:                    "King",
		Description:                    "Supreme governance authority that approves plans, requests rework when needed, and closes decrees.",
		CanSpawn:                       true,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "king",
		AgentModelSelectorSectionLabel: "King",
		AgentModelSelectorSectionOrder: 10,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "chancellor",
		DisplayName:                    "Chancellor",
		Description:                    "Chief coordinator that synthesizes advisor input, resolves ambiguity, preserves scope coherence, and delegates clear downstream direction.",
		CanSpawn:                       true,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "chancellor",
		AgentModelSelectorSectionLabel: "Chancellor",
		AgentModelSelectorSectionOrder: 30,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "council-member",
		DisplayName:                    "Council Member",
		Description:                    "Strategic planner that turns decree intent into an execution-ready plan, breaks work into implementation slices, and refines plan deltas during rework.",
		CanSpawn:                       true,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "council-member",
		AgentModelSelectorSectionLabel: "Council Member",
		AgentModelSelectorSectionOrder: 40,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "noble-delegate",
		DisplayName:                    "Noble Delegate",
		Description:                    "Advisor that focuses on functional fit, architecture fit, contract fit, precedent, and long-term system coherence.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "advisors",
		AgentModelSelectorSectionLabel: "Advisors",
		AgentModelSelectorSectionOrder: 20,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "clergy-delegate",
		DisplayName:                    "Clergy Delegate",
		Description:                    "Advisor that focuses on security, trust boundaries, resilience, and blast-radius risk.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "advisors",
		AgentModelSelectorSectionLabel: "Advisors",
		AgentModelSelectorSectionOrder: 20,
		AgentModelSelectorRoleOrder:    20,
	},
	{
		RoleID:                         "burgher-delegate",
		DisplayName:                    "Burgher Delegate",
		Description:                    "Advisor that focuses on performance, scalability, resource efficiency, and maintainability.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "advisors",
		AgentModelSelectorSectionLabel: "Advisors",
		AgentModelSelectorSectionOrder: 20,
		AgentModelSelectorRoleOrder:    30,
	},
	{
		RoleID:                         "peasant-delegate",
		DisplayName:                    "Peasant Delegate",
		Description:                    "Advisor that focuses on usability, operability, support burden, and day-2 practicality.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "advisors",
		AgentModelSelectorSectionLabel: "Advisors",
		AgentModelSelectorSectionOrder: 20,
		AgentModelSelectorRoleOrder:    40,
	},
	{
		RoleID:                         "builder-berserker",
		DisplayName:                    "Berserker",
		Description:                    "Implementation specialist and the only role whose output is treated as implementation deliverable content.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "builder",
		AgentModelSelectorSectionLabel: "Builder",
		AgentModelSelectorSectionOrder: 50,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "reviewer-shieldmaiden",
		DisplayName:                    "Shieldmaiden Reviewer",
		Description:                    "Reviews builder output against the decree and approved plan, checks scope and correctness before validation begins.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "reviewers",
		AgentModelSelectorSectionLabel: "Reviewers",
		AgentModelSelectorSectionOrder: 60,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "intelligence-qa-scout",
		DisplayName:                    "Scout",
		Description:                    "Validates functional behavior, test evidence, and requirement coverage before royal closure.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "validators",
		AgentModelSelectorSectionLabel: "Validators",
		AgentModelSelectorSectionOrder: 70,
		AgentModelSelectorRoleOrder:    10,
	},
	{
		RoleID:                         "intelligence-nightraven",
		DisplayName:                    "Nightraven",
		Description:                    "Validates security posture, security findings, and risk concerns before royal closure.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "validators",
		AgentModelSelectorSectionLabel: "Validators",
		AgentModelSelectorSectionOrder: 70,
		AgentModelSelectorRoleOrder:    20,
	},
	{
		RoleID:                         "intelligence-loki-chaos",
		DisplayName:                    "Loki Servant",
		Description:                    "Resilience validator for chaos, recovery, and failure-mode concerns.",
		CanSpawn:                       false,
		AgentModelSelectorVisible:      true,
		AgentModelSelectorSectionID:    "validators",
		AgentModelSelectorSectionLabel: "Validators",
		AgentModelSelectorSectionOrder: 70,
		AgentModelSelectorRoleOrder:    30,
	},
}

func CanonicalRoleIDs() []string {
	roleIDs := make([]string, 0, len(canonicalRoles))
	for _, role := range canonicalRoles {
		roleIDs = append(roleIDs, role.RoleID)
	}
	return roleIDs
}

func EnsureCanonicalRoles(ctx context.Context, db *sql.DB, now time.Time) error {
	if db == nil {
		return fmt.Errorf("database handle is required")
	}

	var roleTable string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'roles'`).Scan(&roleTable)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check roles table existence: %w", err)
	}

	ts := now.UTC().Format(time.RFC3339)
	for _, role := range canonicalRoles {
		if strings.TrimSpace(role.RoleID) == "" {
			continue
		}
		if err := validateCanonicalRole(role); err != nil {
			return fmt.Errorf("validate canonical role %q: %w", role.RoleID, err)
		}

		canSpawn := 0
		if role.CanSpawn {
			canSpawn = 1
		}
		agentModelSelectorVisible := 0
		if role.AgentModelSelectorVisible {
			agentModelSelectorVisible = 1
		}

		if err := upsertCanonicalRoleWithRetry(ctx, db, role, canSpawn, agentModelSelectorVisible, ts); err != nil {
			return fmt.Errorf("upsert canonical role %q: %w", role.RoleID, err)
		}
	}

	return nil
}

func validateCanonicalRole(role CanonicalRole) error {
	if strings.TrimSpace(role.RoleID) == "" {
		return fmt.Errorf("role_id is required")
	}
	if strings.TrimSpace(role.DisplayName) == "" {
		return fmt.Errorf("display_name is required")
	}
	if role.AgentModelSelectorSectionOrder < 0 {
		return fmt.Errorf("agent_model_selector_section_order must be >= 0")
	}
	if role.AgentModelSelectorRoleOrder < 0 {
		return fmt.Errorf("agent_model_selector_role_order must be >= 0")
	}
	if !role.AgentModelSelectorVisible {
		return nil
	}
	if strings.TrimSpace(role.AgentModelSelectorSectionID) == "" {
		return fmt.Errorf("agent_model_selector_section_id is required when selector visibility is true")
	}
	if strings.TrimSpace(role.AgentModelSelectorSectionLabel) == "" {
		return fmt.Errorf("agent_model_selector_section_label is required when selector visibility is true")
	}
	return nil
}

func upsertCanonicalRoleWithRetry(ctx context.Context, db *sql.DB, role CanonicalRole, canSpawn int, agentModelSelectorVisible int, createdAt string) error {
	deadline := time.Now().Add(canonicalRoleSeedRetryWindow)
	for {
		_, err := db.ExecContext(
			ctx,
			`INSERT INTO roles (role_id, display_name, description, can_spawn, agent_model_selector_visible, agent_model_selector_section_id, agent_model_selector_section_label, agent_model_selector_section_order, agent_model_selector_role_order, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(role_id) DO UPDATE SET
			   display_name = excluded.display_name,
			   description = excluded.description,
			   can_spawn = excluded.can_spawn,
			   agent_model_selector_visible = excluded.agent_model_selector_visible,
			   agent_model_selector_section_id = excluded.agent_model_selector_section_id,
			   agent_model_selector_section_label = excluded.agent_model_selector_section_label,
			   agent_model_selector_section_order = excluded.agent_model_selector_section_order,
			   agent_model_selector_role_order = excluded.agent_model_selector_role_order`,
			role.RoleID,
			role.DisplayName,
			role.Description,
			canSpawn,
			agentModelSelectorVisible,
			role.AgentModelSelectorSectionID,
			role.AgentModelSelectorSectionLabel,
			role.AgentModelSelectorSectionOrder,
			role.AgentModelSelectorRoleOrder,
			createdAt,
		)
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(75 * time.Millisecond):
		}
	}
}

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "sqlite_busy")
}
