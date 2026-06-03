package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var invalidWorkspaceToken = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

type Manager struct {
	db   *sql.DB
	root string
	now  func() time.Time
}

type CreateWorkspaceRequest struct {
	RunID       string
	NodeRunID   string
	WorkspaceID string
	CreatedAt   string
}

type ReleaseWorkspaceRequest struct {
	WorkspaceID string
	ReleasedAt  string
	Cleanup     bool
}

type WorkspaceRecord struct {
	WorkspaceID   string
	RunID         string
	NodeRunID     sql.NullString
	WorkspacePath string
	Status        string
	CreatedAt     string
	ReleasedAt    sql.NullString
}

func NewManager(db *sql.DB, root string) (*Manager, error) {
	if db == nil {
		return nil, fmt.Errorf("workspace manager requires db handle")
	}
	cleanRoot := strings.TrimSpace(root)
	if cleanRoot == "" {
		return nil, fmt.Errorf("workspace root is required")
	}
	if err := os.MkdirAll(cleanRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create workspace root: %w", err)
	}

	return &Manager{db: db, root: filepath.Clean(cleanRoot), now: time.Now}, nil
}

func (m *Manager) CreateWorkspace(ctx context.Context, req CreateWorkspaceRequest) (WorkspaceRecord, error) {
	if strings.TrimSpace(req.RunID) == "" {
		return WorkspaceRecord{}, fmt.Errorf("run id is required")
	}

	workspaceID := strings.TrimSpace(req.WorkspaceID)
	if workspaceID == "" {
		workspaceID = fmt.Sprintf("ws_%s_%d", sanitizeToken(req.RunID), m.now().UTC().UnixNano())
	}
	workspaceID = sanitizeToken(workspaceID)

	createdAt := strings.TrimSpace(req.CreatedAt)
	if createdAt == "" {
		createdAt = m.now().UTC().Format(time.RFC3339)
	}

	nodeRunID := sql.NullString{}
	if trimmed := strings.TrimSpace(req.NodeRunID); trimmed != "" {
		nodeRunID = sql.NullString{String: trimmed, Valid: true}
	}

	workspacePath := filepath.Join(m.root, workspaceID)

	_, err := m.db.ExecContext(ctx,
		`INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at)
		 VALUES (?, ?, ?, ?, 'active', ?, NULL)`,
		workspaceID,
		req.RunID,
		nodeRunID,
		workspacePath,
		createdAt,
	)
	if err != nil {
		return WorkspaceRecord{}, fmt.Errorf("insert workspace record: %w", err)
	}
	if err := os.RemoveAll(workspacePath); err != nil {
		if cleanupErr := m.deleteWorkspaceRecord(ctx, workspaceID); cleanupErr != nil {
			return WorkspaceRecord{}, fmt.Errorf("reset workspace directory: %w (cleanup workspace record: %v)", err, cleanupErr)
		}
		return WorkspaceRecord{}, fmt.Errorf("reset workspace directory: %w", err)
	}
	if err := os.MkdirAll(workspacePath, 0o755); err != nil {
		_ = os.RemoveAll(workspacePath)
		if cleanupErr := m.deleteWorkspaceRecord(ctx, workspaceID); cleanupErr != nil {
			return WorkspaceRecord{}, fmt.Errorf("create workspace directory: %w (cleanup workspace record: %v)", err, cleanupErr)
		}
		return WorkspaceRecord{}, fmt.Errorf("create workspace directory: %w", err)
	}

	return WorkspaceRecord{
		WorkspaceID:   workspaceID,
		RunID:         req.RunID,
		NodeRunID:     nodeRunID,
		WorkspacePath: workspacePath,
		Status:        "active",
		CreatedAt:     createdAt,
	}, nil
}

func (m *Manager) ReleaseWorkspace(ctx context.Context, req ReleaseWorkspaceRequest) error {
	workspaceID := strings.TrimSpace(req.WorkspaceID)
	if workspaceID == "" {
		return fmt.Errorf("workspace id is required")
	}

	releasedAt := strings.TrimSpace(req.ReleasedAt)
	if releasedAt == "" {
		releasedAt = m.now().UTC().Format(time.RFC3339)
	}

	var workspacePath string
	err := m.db.QueryRowContext(ctx,
		`SELECT workspace_path FROM workspaces WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&workspacePath)
	if errorsIsNoRows(err) {
		return fmt.Errorf("workspace not found: %q", workspaceID)
	}
	if err != nil {
		return fmt.Errorf("query workspace record: %w", err)
	}

	if req.Cleanup {
		if rmErr := os.RemoveAll(workspacePath); rmErr != nil {
			return fmt.Errorf("cleanup workspace directory: %w", rmErr)
		}
	}

	_, err = m.db.ExecContext(ctx,
		`UPDATE workspaces SET status = 'released', released_at = ? WHERE workspace_id = ?`,
		releasedAt,
		workspaceID,
	)
	if err != nil {
		return fmt.Errorf("update workspace status: %w", err)
	}

	return nil
}

func (m *Manager) deleteWorkspaceRecord(ctx context.Context, workspaceID string) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM workspaces WHERE workspace_id = ?`, workspaceID)
	if err != nil {
		return fmt.Errorf("delete workspace record %q: %w", workspaceID, err)
	}
	return nil
}

func sanitizeToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "workspace"
	}
	cleaned := invalidWorkspaceToken.ReplaceAllString(trimmed, "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		return "workspace"
	}
	return cleaned
}

func errorsIsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
