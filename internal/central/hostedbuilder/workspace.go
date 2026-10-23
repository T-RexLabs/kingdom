package hostedbuilder

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"time"
)

type workspaceClient interface {
	CreateWorkspace(context.Context, *native.CreateWorkspaceRequest, ...grpc.CallOption) (*native.CreateWorkspaceResponse, error)
	GetWorkspace(context.Context, *native.GetWorkspaceRequest, ...grpc.CallOption) (*native.GetWorkspaceResponse, error)
}
type workspaceMapping struct {
	Name, ID, RequestID, DatabaseID string
	Dispatched                      string
}

func initializeMappings(ctx context.Context, db *sql.DB) error {
	_, e := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS hosted_project_workspaces(project_id TEXT NOT NULL REFERENCES central_projects(id),gateway_id TEXT NOT NULL,database_id TEXT NOT NULL UNIQUE,name TEXT NOT NULL UNIQUE,request_id TEXT NOT NULL UNIQUE,workspace_id TEXT UNIQUE,dispatched_at TEXT,PRIMARY KEY(project_id,gateway_id));
 CREATE TRIGGER IF NOT EXISTS hosted_workspace_pins BEFORE UPDATE OF project_id,gateway_id,database_id,name,request_id ON hosted_project_workspaces BEGIN SELECT RAISE(ABORT,'workspace pins immutable'); END;
 CREATE TRIGGER IF NOT EXISTS hosted_workspace_identity BEFORE UPDATE OF workspace_id ON hosted_project_workspaces WHEN OLD.workspace_id IS NOT NULL AND NEW.workspace_id IS NOT OLD.workspace_id BEGIN SELECT RAISE(ABORT,'workspace identity immutable'); END;
 CREATE TRIGGER IF NOT EXISTS hosted_workspace_dispatch BEFORE UPDATE OF dispatched_at ON hosted_project_workspaces WHEN OLD.dispatched_at IS NOT NULL AND NEW.dispatched_at IS NOT OLD.dispatched_at BEGIN SELECT RAISE(ABORT,'workspace dispatch immutable'); END;
 CREATE TRIGGER IF NOT EXISTS hosted_workspace_retained BEFORE DELETE ON hosted_project_workspaces BEGIN SELECT RAISE(ABORT,'workspace retained'); END;`)
	return e
}
func workspace(ctx context.Context, db *sql.DB, client workspaceClient, project, gateway, databaseID string, now time.Time) (workspaceMapping, error) {
	var m workspaceMapping
	// Include a retained random namespace in the name. A recreated project database
	// cannot reuse allocations from an earlier database incarnation.
	request := uuid.NewString()
	hash := sha256.Sum256([]byte(project + "/" + request))
	name := "kp-" + hex.EncodeToString(hash[:8])
	_, e := db.ExecContext(ctx, `INSERT INTO hosted_project_workspaces(project_id,gateway_id,database_id,name,request_id) VALUES(?,?,?,?,?) ON CONFLICT(project_id,gateway_id) DO NOTHING`, project, gateway, databaseID, name, request)
	if e != nil {
		return m, e
	}
	e = db.QueryRowContext(ctx, `SELECT name,COALESCE(workspace_id,''),request_id,database_id,COALESCE(dispatched_at,'') FROM hosted_project_workspaces WHERE project_id=? AND gateway_id=?`, project, gateway).Scan(&m.Name, &m.ID, &m.RequestID, &m.DatabaseID, &m.Dispatched)
	if e != nil {
		return m, e
	}
	if m.DatabaseID != databaseID {
		return m, errors.New("project database identity conflicts with retained workspace")
	}
	if m.ID != "" {
		got, e := client.GetWorkspace(ctx, &native.GetWorkspaceRequest{Name: m.Name})
		if e != nil {
			return m, e
		}
		if got.GetWorkspace().GetMetadata().GetId() != m.ID || got.GetWorkspace().GetMetadata().GetName() != m.Name {
			return m, errors.New("workspace identity changed")
		}
		return m, nil
	}
	if m.Dispatched == "" {
		_, e = db.ExecContext(ctx, `UPDATE hosted_project_workspaces SET dispatched_at=? WHERE project_id=? AND gateway_id=? AND dispatched_at IS NULL`, now.UTC().Format(time.RFC3339Nano), project, gateway)
		if e != nil {
			return m, e
		}
		e = db.QueryRowContext(ctx, `SELECT dispatched_at FROM hosted_project_workspaces WHERE project_id=? AND gateway_id=?`, project, gateway).Scan(&m.Dispatched)
		if e != nil {
			return m, e
		}
	}
	dispatched, e := time.Parse(time.RFC3339Nano, m.Dispatched)
	if e != nil || now.Before(dispatched) || now.Sub(dispatched) >= 23*time.Hour {
		return m, errors.New("workspace creation remains unresolved beyond replay window")
	}
	got, e := client.CreateWorkspace(ctx, &native.CreateWorkspaceRequest{Name: m.Name, RequestId: m.RequestID})
	if e != nil {
		return m, e
	}
	metadata := got.GetWorkspace().GetMetadata()
	if metadata.GetName() != m.Name || metadata.GetId() == "" {
		return m, errors.New("workspace response identity mismatch")
	}
	m.ID = metadata.GetId()
	_, e = db.ExecContext(ctx, `UPDATE hosted_project_workspaces SET workspace_id=? WHERE project_id=? AND gateway_id=? AND (workspace_id IS NULL OR workspace_id=?)`, m.ID, project, gateway, m.ID)
	if e != nil {
		return m, e
	}
	var saved string
	e = db.QueryRowContext(ctx, `SELECT workspace_id FROM hosted_project_workspaces WHERE project_id=? AND gateway_id=?`, project, gateway).Scan(&saved)
	if e != nil {
		return m, e
	}
	if saved != m.ID {
		return m, errors.New("workspace response conflicts with retained identity")
	}
	return m, nil
}
