package localstack

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/openshell/workspaceowner"
	"time"
)

type workspaceClient = workspaceowner.Client
type workspaceMapping = workspaceowner.Mapping

func initializeMappings(ctx context.Context, db *sql.DB) error {
	_, e := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS local_project_workspaces(project_id TEXT NOT NULL,gateway_id TEXT NOT NULL,database_id TEXT NOT NULL UNIQUE,name TEXT NOT NULL UNIQUE,request_id TEXT NOT NULL UNIQUE,workspace_id TEXT UNIQUE,dispatched_at TEXT,PRIMARY KEY(project_id,gateway_id));
 CREATE TRIGGER IF NOT EXISTS local_workspace_pins BEFORE UPDATE OF project_id,gateway_id,database_id,name,request_id ON local_project_workspaces BEGIN SELECT RAISE(ABORT,'workspace pins immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_workspace_identity BEFORE UPDATE OF workspace_id ON local_project_workspaces WHEN OLD.workspace_id IS NOT NULL AND NEW.workspace_id IS NOT OLD.workspace_id BEGIN SELECT RAISE(ABORT,'workspace identity immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_workspace_dispatch BEFORE UPDATE OF dispatched_at ON local_project_workspaces WHEN OLD.dispatched_at IS NOT NULL AND NEW.dispatched_at IS NOT OLD.dispatched_at BEGIN SELECT RAISE(ABORT,'workspace dispatch immutable'); END;
 CREATE TRIGGER IF NOT EXISTS local_workspace_retained BEFORE DELETE ON local_project_workspaces BEGIN SELECT RAISE(ABORT,'workspace retained'); END;`)
	return e
}
func workspace(ctx context.Context, db *sql.DB, client workspaceClient, project, gateway, databaseID string, now time.Time) (workspaceMapping, error) {
	return workspaceowner.Ensure(ctx, workspaceRepository{db}, client, workspaceowner.Pins{Owner: project, Gateway: gateway, Database: databaseID}, now)
}

type workspaceRepository struct{ db *sql.DB }

func (r workspaceRepository) Reserve(ctx context.Context, p workspaceowner.Pins, proposal workspaceowner.Mapping) (workspaceowner.Mapping, error) {
	var m workspaceMapping
	_, err := r.db.ExecContext(ctx, `INSERT INTO local_project_workspaces(project_id,gateway_id,database_id,name,request_id)VALUES(?,?,?,?,?)ON CONFLICT(project_id,gateway_id)DO NOTHING`, p.Owner, p.Gateway, p.Database, proposal.Name, proposal.RequestID)
	if err != nil {
		return m, err
	}
	err = r.db.QueryRowContext(ctx, `SELECT name,COALESCE(workspace_id,''),request_id,database_id,COALESCE(dispatched_at,'') FROM local_project_workspaces WHERE project_id=? AND gateway_id=?`, p.Owner, p.Gateway).Scan(&m.Name, &m.ID, &m.RequestID, &m.DatabaseID, &m.Dispatched)
	return m, err
}
func (r workspaceRepository) MarkDispatched(ctx context.Context, p workspaceowner.Pins, stamp string) (string, error) {
	if _, err := r.db.ExecContext(ctx, `UPDATE local_project_workspaces SET dispatched_at=? WHERE project_id=? AND gateway_id=? AND dispatched_at IS NULL`, stamp, p.Owner, p.Gateway); err != nil {
		return "", err
	}
	var saved string
	err := r.db.QueryRowContext(ctx, `SELECT dispatched_at FROM local_project_workspaces WHERE project_id=? AND gateway_id=?`, p.Owner, p.Gateway).Scan(&saved)
	return saved, err
}
func (r workspaceRepository) BindIdentity(ctx context.Context, p workspaceowner.Pins, id string) (string, error) {
	if _, err := r.db.ExecContext(ctx, `UPDATE local_project_workspaces SET workspace_id=? WHERE project_id=? AND gateway_id=? AND(workspace_id IS NULL OR workspace_id=?)`, id, p.Owner, p.Gateway, id); err != nil {
		return "", err
	}
	var saved string
	err := r.db.QueryRowContext(ctx, `SELECT workspace_id FROM local_project_workspaces WHERE project_id=? AND gateway_id=?`, p.Owner, p.Gateway).Scan(&saved)
	return saved, err
}
