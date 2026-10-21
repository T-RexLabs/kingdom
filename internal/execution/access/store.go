package access

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
)

type Reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// BindProjectTx is deployment configuration for a new project database. It
// cannot turn an existing local database into hosted execution implicitly.
func BindProjectTx(ctx context.Context, tx *sql.Tx, project string) error {
	if !validID(project) {
		return ErrInvalid
	}
	existing, err := Project(ctx, tx)
	if err != nil {
		return err
	}
	if existing != "" {
		if existing == project {
			return nil
		}
		return ErrInvalid
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM workflow_runs)+(SELECT count(*) FROM planning_operations)+(SELECT count(*) FROM provider_preparations)`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("execution project binding requires an empty project")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO execution_access_project(slot,project_id) VALUES(1,?)`, project)
	return err
}

// Project returns an empty ID only for an explicitly unbound local database.
func Project(ctx context.Context, q Reader) (string, error) {
	var id string
	err := q.QueryRowContext(ctx, `SELECT project_id FROM execution_access_project WHERE slot=1`).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !validID(id) {
		return "", ErrInvalid
	}
	return id, nil
}
func requireProject(ctx context.Context, q Reader, id string) error {
	project, err := Project(ctx, q)
	if err != nil {
		return err
	}
	if project == "" || project != id {
		return ErrInvalid
	}
	return nil
}

// CaptureRunTx is called with metadata already checked by the central access
// boundary, in the same transaction that creates the run. No browser-supplied
// actor or project is accepted by that boundary.
func CaptureRunTx(ctx context.Context, tx *sql.Tx, runID string, s RunSnapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := requireProject(ctx, tx, s.ProjectID); err != nil {
		return err
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return capture(ctx, tx, `INSERT INTO execution_access_runs(run_id,snapshot) VALUES(?,?) ON CONFLICT(run_id) DO NOTHING`, `SELECT snapshot FROM execution_access_runs WHERE run_id=?`, runID, string(raw))
}

// CapturePlanningTx shares the operation's enqueue transaction. A new user
// action requires that actor's checked selection; automatic follow-ups copy the
// exact causal operation's snapshot, not a mutable session owner or selection.
func CapturePlanningTx(ctx context.Context, tx *sql.Tx, operationID string, s Snapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := requireProject(ctx, tx, s.ProjectID); err != nil {
		return err
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return capture(ctx, tx, `INSERT INTO execution_access_planning(operation_id,snapshot) VALUES(?,?) ON CONFLICT(operation_id) DO NOTHING`, `SELECT snapshot FROM execution_access_planning WHERE operation_id=?`, operationID, string(raw))
}
func capture(ctx context.Context, tx *sql.Tx, insert, query, id, raw string) error {
	if id == "" {
		return ErrInvalid
	}
	if _, err := tx.ExecContext(ctx, insert, id, raw); err != nil {
		return err
	}
	var saved string
	if err := tx.QueryRowContext(ctx, query, id).Scan(&saved); err != nil {
		return err
	}
	if saved != raw {
		return fmt.Errorf("execution access snapshot conflict")
	}
	return nil
}
func decode(raw string, target any) error {
	if len(raw) > 128<<10 {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalid
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrInvalid
	}
	return nil
}

// CaptureInvocationTx derives the source and role from canonical ownership. It
// must run inside the lease-fenced preparation reservation transaction. Missing
// hosted access is an error; local execution does not acquire central authority.
func CaptureInvocationTx(ctx context.Context, tx *sql.Tx, kind, invocationID string) error {
	project, err := Project(ctx, tx)
	if err != nil {
		return err
	}
	if project == "" {
		return nil
	}
	var source, raw string
	var snapshot Snapshot
	var runID, operationID any
	switch kind {
	case "run":
		var role string
		err = tx.QueryRowContext(ctx, `SELECT a.run_id,n.required_role,s.snapshot FROM provider_invocations i JOIN attempt_executions a ON a.attempt_id=i.attempt_id AND a.run_id=i.run_id AND a.node_run_id=i.node_run_id JOIN node_runs n ON n.node_run_id=a.node_run_id AND n.run_id=a.run_id JOIN execution_access_runs s ON s.run_id=a.run_id WHERE i.provider_invocation_id=?`, invocationID).Scan(&source, &role, &raw)
		if err != nil {
			return err
		}
		var run RunSnapshot
		if err = decode(raw, &run); err != nil {
			return err
		}
		snapshot, err = run.ForRole(role)
		if err != nil {
			return err
		}
		runID = source
	case "planning":
		err = tx.QueryRowContext(ctx, `SELECT o.operation_id,s.snapshot FROM planning_provider_invocations i JOIN planning_operations o ON o.operation_id=i.planning_operation_id AND o.planning_session_id=i.planning_session_id JOIN execution_access_planning s ON s.operation_id=o.operation_id WHERE i.planning_provider_invocation_id=?`, invocationID).Scan(&source, &raw)
		if err != nil {
			return err
		}
		if err = decode(raw, &snapshot); err != nil {
			return err
		}
		operationID = source
	default:
		return ErrInvalid
	}
	if err = snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.ProjectID != project {
		return ErrInvalid
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO execution_access_invocations(owner_kind,invocation_id,run_id,operation_id,snapshot) VALUES(?,?,?,?,?) ON CONFLICT(owner_kind,invocation_id) DO NOTHING`, kind, invocationID, runID, operationID, string(encoded)); err != nil {
		return err
	}
	var saved, savedSource string
	if err = tx.QueryRowContext(ctx, `SELECT snapshot,COALESCE(run_id,operation_id) FROM execution_access_invocations WHERE owner_kind=? AND invocation_id=?`, kind, invocationID).Scan(&saved, &savedSource); err != nil {
		return err
	}
	if saved != string(encoded) || savedSource != source {
		return fmt.Errorf("invocation access snapshot conflict")
	}
	return nil
}

// Invocation returns durable selection facts, not a grant. The caller must
// separately check current canonical lease and central authorization before any
// effects. It must reject request model/variant differences before provisioning.
func Invocation(ctx context.Context, q Reader, kind, id string) (Snapshot, error) {
	var raw string
	if err := q.QueryRowContext(ctx, `SELECT snapshot FROM execution_access_invocations WHERE owner_kind=? AND invocation_id=?`, kind, id).Scan(&raw); err != nil {
		return Snapshot{}, err
	}
	var s Snapshot
	if err := decode(raw, &s); err != nil {
		return Snapshot{}, err
	}
	if err := s.Validate(); err != nil {
		return Snapshot{}, err
	}
	if err := requireProject(ctx, q, s.ProjectID); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}
