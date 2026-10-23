package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"kingdom/internal/core/workflow"
	"strings"
)

var ErrWorkflowVersionConflict = errors.New("workflow version already exists with a different definition or graph")

// PublishCompiledWorkflowTx retains the definition and its complete graph in
// one transaction. Exact compiled content is idempotent; versions never mutate.
func PublishCompiledWorkflowTx(ctx context.Context, tx *sql.Tx, id, name, version string, compiled *workflow.CompileResult, createdAt string) (bool, error) {
	if tx == nil || id == "" || name == "" || version == "" || createdAt == "" || compiled == nil {
		return false, fmt.Errorf("compiled workflow identity required")
	}
	sum := sha256.Sum256(compiled.CanonicalJSON)
	if hex.EncodeToString(sum[:]) != compiled.SHA256 {
		return false, fmt.Errorf("compiled workflow checksum mismatch")
	}
	definition, err := workflow.ParseDefinitionJSON(compiled.CanonicalJSON)
	if err != nil {
		return false, err
	}
	if definition.WorkflowID != name {
		return false, fmt.Errorf("compiled workflow name mismatch")
	}
	var existing, raw string
	err = tx.QueryRowContext(ctx, `SELECT workflow_definition_id,definition_json FROM workflow_definitions WHERE workflow_name=? AND workflow_version=?`, name, version).Scan(&existing, &raw)
	if err == nil {
		if raw != string(compiled.CanonicalJSON) {
			return false, ErrWorkflowVersionConflict
		}
		if err = verifyWorkflowGraph(ctx, tx, existing, definition); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_definitions(workflow_definition_id,workflow_name,workflow_version,definition_json,created_at)VALUES(?,?,?,?,?)`, id, name, version, string(compiled.CanonicalJSON), createdAt); err != nil {
		return false, err
	}
	for _, node := range definition.Nodes {
		if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_nodes(workflow_node_id,workflow_definition_id,node_id,required_role,node_class,created_at)VALUES(?,?,?,?,?,?)`, workflowGraphID(id, "node", node.NodeID), id, node.NodeID, node.RequiredRole, node.NodeClass, createdAt); err != nil {
			return false, err
		}
	}
	for _, edge := range definition.Edges {
		on := strings.TrimSpace(edge.On)
		if on == "" {
			on = "success"
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_edges(workflow_edge_id,workflow_definition_id,from_node_id,to_node_id,on_condition,created_at)VALUES(?,?,?,?,?,?)`, workflowGraphID(id, "edge", edge.From+"\x00"+edge.To+"\x00"+on), id, edge.From, edge.To, on, createdAt); err != nil {
			return false, err
		}
	}
	return true, nil
}
func workflowGraphID(definition, kind, key string) string {
	sum := sha256.Sum256([]byte(definition + "\x00" + kind + "\x00" + key))
	return "workflow_" + kind + "_" + hex.EncodeToString(sum[:])
}
func verifyWorkflowGraph(ctx context.Context, tx *sql.Tx, id string, d workflow.Definition) error {
	var nodes, edges int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM workflow_nodes WHERE workflow_definition_id=?),(SELECT count(*) FROM workflow_edges WHERE workflow_definition_id=?)`, id, id).Scan(&nodes, &edges); err != nil {
		return err
	}
	if nodes != len(d.Nodes) || edges != len(d.Edges) {
		return ErrWorkflowVersionConflict
	}
	for _, n := range d.Nodes {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM workflow_nodes WHERE workflow_definition_id=? AND node_id=? AND required_role=? AND node_class=?`, id, n.NodeID, n.RequiredRole, n.NodeClass).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return ErrWorkflowVersionConflict
		}
	}
	for _, e := range d.Edges {
		on := strings.TrimSpace(e.On)
		if on == "" {
			on = "success"
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM workflow_edges WHERE workflow_definition_id=? AND from_node_id=? AND to_node_id=? AND on_condition=?`, id, e.From, e.To, on).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return ErrWorkflowVersionConflict
		}
	}
	return nil
}
