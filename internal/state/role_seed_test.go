package state

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
)

func TestEnsureCanonicalRolesSeedsAndUpdates(t *testing.T) {
	t.Parallel()

	db := openRoleSeedTestDB(t)
	applyRoleSeedMigration(t, db)

	if err := EnsureCanonicalRoles(context.Background(), db, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed canonical roles: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM roles`).Scan(&count); err != nil {
		t.Fatalf("count roles: %v", err)
	}
	if count != len(canonicalRoles) {
		t.Fatalf("expected %d roles, got %d", len(canonicalRoles), count)
	}

	if _, err := db.Exec(`UPDATE roles SET display_name = 'Wrong' WHERE role_id = 'governance'`); err != nil {
		t.Fatalf("mutate role: %v", err)
	}
	if _, err := db.Exec(`UPDATE roles SET description = 'Wrong description' WHERE role_id = 'governance'`); err != nil {
		t.Fatalf("mutate role description: %v", err)
	}
	if _, err := db.Exec(`UPDATE roles SET agent_model_selector_section_id = '', agent_model_selector_section_label = '', agent_model_selector_section_order = 0, agent_model_selector_role_order = 0 WHERE role_id = 'governance'`); err != nil {
		t.Fatalf("mutate role model override metadata: %v", err)
	}

	if err := EnsureCanonicalRoles(context.Background(), db, time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("reseed canonical roles: %v", err)
	}

	var display string
	var description string
	if err := db.QueryRow(`SELECT display_name FROM roles WHERE role_id = 'governance'`).Scan(&display); err != nil {
		t.Fatalf("read governance role: %v", err)
	}
	if display != "Governance" {
		t.Fatalf("expected governance display name to be restored, got %q", display)
	}
	if err := db.QueryRow(`SELECT description FROM roles WHERE role_id = 'governance'`).Scan(&description); err != nil {
		t.Fatalf("read governance role description: %v", err)
	}
	if description != "Supreme governance authority that approves plans, requests rework when needed, and closes work orders." {
		t.Fatalf("expected governance description to be restored, got %q", description)
	}

	var groupID, groupLabel string
	var groupOrder, roleOrder, overrideEligible int
	if err := db.QueryRow(`SELECT agent_model_selector_section_id, agent_model_selector_section_label, agent_model_selector_section_order, agent_model_selector_role_order, agent_model_selector_visible FROM roles WHERE role_id = 'governance'`).Scan(&groupID, &groupLabel, &groupOrder, &roleOrder, &overrideEligible); err != nil {
		t.Fatalf("read governance override metadata: %v", err)
	}
	if groupID != "governance" || groupLabel != "Governance" || groupOrder != 10 || roleOrder != 10 || overrideEligible != 1 {
		t.Fatalf("expected governance override metadata restored, got group=%q label=%q group_order=%d role_order=%d eligible=%d", groupID, groupLabel, groupOrder, roleOrder, overrideEligible)
	}

	if err := db.QueryRow(`SELECT display_name FROM roles WHERE role_id = 'risk-advisor'`).Scan(&display); err != nil {
		t.Fatalf("read clergy role: %v", err)
	}
	if display != "Risk Advisor" {
		t.Fatalf("expected clergy display name %q, got %q", "Risk Advisor", display)
	}
}

func TestEnsureCanonicalRolesRetriesBusyLock(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "roles_busy.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	applyRoleSeedMigration(t, db)

	locker, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open lock sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = locker.Close() })

	tx, err := locker.Begin()
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('lock-holder', 'lock', 0, '2026-03-01T12:00:00Z')`); err != nil {
		t.Fatalf("insert lock row: %v", err)
	}

	releaseDone := make(chan struct{})
	go func() {
		time.Sleep(250 * time.Millisecond)
		_ = tx.Rollback()
		close(releaseDone)
	}()

	if err := EnsureCanonicalRoles(context.Background(), db, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("expected role seed to recover from busy lock: %v", err)
	}
	<-releaseDone

	for _, role := range canonicalRoles {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM roles WHERE role_id = ?`, role.RoleID).Scan(&count); err != nil {
			t.Fatalf("count role %s: %v", role.RoleID, err)
		}
		if count != 1 {
			t.Fatalf("expected role %s to be seeded once, got %d", role.RoleID, count)
		}
	}
}

func TestCanonicalRoleDescriptionsMatchReadme(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	readmeDescriptions := make(map[string]string, len(canonicalRoles))
	inCanonicalRolesTable := false
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		switch trimmed {
		case "### Canonical Roles":
			inCanonicalRolesTable = true
			continue
		case "### Managed OpenCode Profiles":
			inCanonicalRolesTable = false
		}
		if !inCanonicalRolesTable || !strings.HasPrefix(trimmed, "|") {
			continue
		}

		cells := strings.Split(trimmed, "|")
		if len(cells) < 4 {
			continue
		}

		roleCell := strings.TrimSpace(cells[1])
		descriptionCell := strings.TrimSpace(cells[2])
		start := strings.Index(roleCell, "`")
		end := strings.LastIndex(roleCell, "`")
		if start == -1 || end <= start {
			continue
		}

		readmeDescriptions[roleCell[start+1:end]] = descriptionCell
	}

	if len(readmeDescriptions) != len(canonicalRoles) {
		t.Fatalf("expected README to define %d canonical role descriptions, got %d", len(canonicalRoles), len(readmeDescriptions))
	}

	for _, role := range canonicalRoles {
		if got := readmeDescriptions[role.RoleID]; got != role.Description {
			t.Fatalf("expected README description for %s to match canonical role seed\nseed:   %q\nreadme: %q", role.RoleID, role.Description, got)
		}
	}
}

func TestIsSQLiteBusyError(t *testing.T) {
	t.Parallel()

	if !IsSQLiteBusy(fmt.Errorf("database is locked (5) (SQLITE_BUSY)")) {
		t.Fatal("expected SQLITE_BUSY marker to be recognized")
	}
	if IsSQLiteBusy(fmt.Errorf("constraint failed")) {
		t.Fatal("did not expect non-busy sqlite error to be classified as busy")
	}
}

func openRoleSeedTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable fk pragma: %v", err)
	}
	return db
}

func applyRoleSeedMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	raw, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}
