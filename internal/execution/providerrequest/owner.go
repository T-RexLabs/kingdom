package providerrequest

import (
	"context"
	"fmt"
	"time"
)

type canonicalOwner struct {
	id                   string
	parent               string
	node                 string
	logicalSession       string
	session              string
	currentSession       string
	state                string
	invocationState      string
	activeInvocation     string
	leaseOwner           string
	leaseVersion         int64
	expiry               string
	settlement           string
	settlementInvocation string
	settlementOutcome    string
	currentOperation     string
	mode                 string
	runStatus            string
}

func loadOwner(ctx context.Context, q Reader, owner Owner) (canonicalOwner, error) {
	var c canonicalOwner
	var err error
	switch owner.Kind {
	case OwnerRun:
		err = q.QueryRowContext(ctx, `SELECT ae.attempt_id,ae.run_id,ae.node_run_id,pi.logical_session_id,pi.provider_session_id,ae.provider_session_id,ae.state,pi.invocation_state,COALESCE(ae.active_provider_invocation_id,''),ae.lease_owner,ae.lease_version,COALESCE(ae.lease_expires_at,''),COALESCE(ae.canonical_settlement_id,''),COALESCE(pcs.provider_invocation_id,''),COALESCE(pcs.outcome,''),ae.execution_mode,wr.run_status FROM provider_invocations pi JOIN attempt_executions ae ON ae.attempt_id=pi.attempt_id AND ae.run_id=pi.run_id AND ae.node_run_id=pi.node_run_id JOIN workflow_runs wr ON wr.run_id=ae.run_id LEFT JOIN provider_completion_settlements pcs ON pcs.settlement_id=ae.canonical_settlement_id WHERE pi.provider_invocation_id=?`, owner.InvocationID).Scan(&c.id, &c.parent, &c.node, &c.logicalSession, &c.session, &c.currentSession, &c.state, &c.invocationState, &c.activeInvocation, &c.leaseOwner, &c.leaseVersion, &c.expiry, &c.settlement, &c.settlementInvocation, &c.settlementOutcome, &c.mode, &c.runStatus)
	case OwnerPlanning:
		err = q.QueryRowContext(ctx, `SELECT po.operation_id,po.planning_session_id,pi.logical_session_id,pi.provider_session_id,COALESCE(po.provider_session_id,''),po.status,pi.invocation_state,COALESCE(po.active_provider_invocation_id,''),po.lease_owner,po.lease_version,COALESCE(po.lease_expires_at,''),COALESCE(po.canonical_settlement_id,''),COALESCE(pcs.planning_provider_invocation_id,''),COALESCE(pcs.outcome,''),CASE WHEN po.type='generate_title' THEN COALESCE(ps.current_title_operation_id,'') ELSE COALESCE(ps.current_operation_id,'') END FROM planning_provider_invocations pi JOIN planning_operations po ON po.operation_id=pi.planning_operation_id AND po.planning_session_id=pi.planning_session_id JOIN planning_sessions ps ON ps.planning_session_id=po.planning_session_id LEFT JOIN planning_provider_completion_settlements pcs ON pcs.settlement_id=po.canonical_settlement_id WHERE pi.planning_provider_invocation_id=?`, owner.InvocationID).Scan(&c.id, &c.parent, &c.logicalSession, &c.session, &c.currentSession, &c.state, &c.invocationState, &c.activeInvocation, &c.leaseOwner, &c.leaseVersion, &c.expiry, &c.settlement, &c.settlementInvocation, &c.settlementOutcome, &c.currentOperation)
	default:
		return c, fmt.Errorf("unknown provider owner kind")
	}
	return c, err
}
func (c canonicalOwner) requireActive(b Binding, registering bool, now time.Time) error {
	validState := c.state == "running" || (b.Owner.Kind == OwnerRun && (c.state == "starting" || (registering && c.state == "queued")))
	if !validState || c.invocationState != "open" || c.activeInvocation != b.Owner.InvocationID || c.session != b.Identity.SessionID || c.currentSession != b.Identity.SessionID || c.leaseOwner != b.Lease.Owner || c.leaseVersion != b.Lease.Version || (b.LogicalSessionID != "" && b.LogicalSessionID != c.logicalSession) || (b.Owner.Kind == OwnerPlanning && c.currentOperation != c.id) || (b.Owner.Kind == OwnerRun && c.mode != "provider_backed") {
		return fmt.Errorf("canonical provider owner or lease conflict")
	}
	if !now.IsZero() {
		// Only work admission requires a running workflow. Existing exact receipts,
		// cancellation and completed handoffs may drain after the run terminalizes.
		if b.Owner.Kind == OwnerRun && c.runStatus != "running" {
			return fmt.Errorf("terminal workflow run cannot admit provider work")
		}
		expiry, err := time.Parse(time.RFC3339Nano, c.expiry)
		if err != nil || !now.Before(expiry) {
			return fmt.Errorf("canonical provider lease expired")
		}
	}
	return nil
}
func requireSessionReusable(ctx context.Context, q Reader, b Binding, current canonicalOwner) error {
	// A session already referenced by another unbound invocation has no shared
	// request evidence. It cannot be adopted as a fresh managed conversation.
	var unbound int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM (
 SELECT pi.provider_invocation_id FROM provider_invocations pi
 LEFT JOIN provider_requests pr ON pr.run_invocation_id=pi.provider_invocation_id
 WHERE pi.provider_session_id=? AND pr.binding_id IS NULL AND NOT (?='run' AND pi.provider_invocation_id=?)
 UNION ALL
 SELECT pi.planning_provider_invocation_id FROM planning_provider_invocations pi
 LEFT JOIN provider_requests pr ON pr.planning_invocation_id=pi.planning_provider_invocation_id
 WHERE pi.provider_session_id=? AND pr.binding_id IS NULL AND NOT (?='planning' AND pi.planning_provider_invocation_id=?)
 )`, b.Identity.SessionID, b.Owner.Kind, b.Owner.InvocationID, b.Identity.SessionID, b.Owner.Kind, b.Owner.InvocationID).Scan(&unbound); err != nil {
		return err
	}
	if unbound != 0 {
		return fmt.Errorf("provider session has unbound canonical invocation history")
	}
	rows, err := q.QueryContext(ctx, `SELECT binding_id FROM provider_requests WHERE provider_session_id=? AND binding_id<>?`, b.Identity.SessionID, b.ID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		old, err := Get(ctx, q, id)
		if err != nil {
			return err
		}
		previous, err := loadOwner(ctx, q, old.Owner)
		if err != nil {
			return err
		}
		if old.Identity.InstanceID != b.Identity.InstanceID || old.Identity.Generation != b.Identity.Generation || old.Identity.Location != b.Identity.Location || old.Owner.Kind != b.Owner.Kind || previous.parent != current.parent || old.LogicalSessionID != b.LogicalSessionID {
			return fmt.Errorf("provider session belongs to a different conversation or generation")
		}
		terminalState := "committed_success"
		if old.Owner.Kind == OwnerPlanning {
			terminalState = "succeeded"
		}
		if old.TerminalReference == nil || old.DispatchIssuedAt == "" || previous.state != terminalState || previous.settlement == "" || previous.settlementInvocation != old.Owner.InvocationID || previous.settlementOutcome != "success" || previous.invocationState != "settled_success" {
			return fmt.Errorf("provider session predecessor lacks exact successful settlement")
		}
		if err := RequireResourcesClosedTx(ctx, q, id); err != nil {
			return err
		}
	}
	return nil
}

// guardLineage is shared by completion and resource readback. The guardian
// capability is ingested elsewhere; these checks consume only canonical facts.
func guardLineage(b Binding, owner canonicalOwner) (string, []any) {
	kind := "work_order_attempt"
	if b.Owner.Kind == OwnerPlanning {
		kind = "planning_operation"
	}
	clause := `g.owner_kind=? AND g.owner_id=? AND g.provider_invocation_id=? AND g.lease_owner=? AND g.lease_version=?`
	args := []any{kind, owner.id, b.Owner.InvocationID, b.Lease.Owner, b.Lease.Version}
	if b.Owner.Kind == OwnerRun {
		clause += ` AND g.attempt_id=? AND g.run_id=? AND g.node_run_id=? AND g.planning_operation_id='' AND g.planning_session_id=''`
		args = append(args, owner.id, owner.parent, owner.node)
	} else {
		clause += ` AND g.planning_operation_id=? AND g.planning_session_id=? AND g.attempt_id='' AND g.run_id='' AND g.node_run_id=''`
		args = append(args, owner.id, owner.parent)
	}
	return clause, args
}
