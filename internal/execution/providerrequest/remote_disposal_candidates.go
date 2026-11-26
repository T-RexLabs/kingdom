package providerrequest

import (
	"context"
	"fmt"
	"math"
)

// RemoteDisposalCandidate is scheduling metadata, never deletion authority.
// The provider revalidates exact resource and settlement proofs before disposal.
type RemoteDisposalCandidate struct {
	Allocation       int64
	Owner            Owner
	RunID, AttemptID string
}

const disposalCandidatesSQL = `SELECT r.allocation,r.owner_kind,r.invocation_id,COALESCE(ri.run_id,''),COALESCE(ri.attempt_id,'')
FROM remote_runtimes r
JOIN provider_preparations p ON p.owner_kind=r.owner_kind AND p.invocation_id=r.invocation_id AND p.lease_owner=r.lease_owner AND p.lease_version=r.lease_version
LEFT JOIN provider_invocations ri ON r.owner_kind='run' AND ri.provider_invocation_id=r.invocation_id
LEFT JOIN planning_provider_invocations pi ON r.owner_kind='planning' AND pi.planning_provider_invocation_id=r.invocation_id
LEFT JOIN provider_requests b ON (r.owner_kind='run' AND b.run_invocation_id=r.invocation_id) OR (r.owner_kind='planning' AND b.planning_invocation_id=r.invocation_id)
WHERE r.allocation>? AND r.allocation<=? AND r.admission_closed=1 AND p.admission_closed=1
AND NOT EXISTS(SELECT 1 FROM remote_runtime_deletions d WHERE d.allocation=r.allocation)
AND NOT EXISTS(SELECT 1 FROM remote_runtimes_never_created n WHERE n.allocation=r.allocation)
AND ((b.binding_id IS NULL AND COALESCE(ri.invocation_state,pi.invocation_state)='issued') OR
(b.binding_id=r.binding_id AND b.execution_backend='openshell' AND b.remote_allocation=r.allocation AND b.lease_owner=r.lease_owner AND b.lease_version=r.lease_version AND COALESCE(b.admission_closed_at,'')<>'' AND COALESCE(ri.invocation_state,pi.invocation_state) IN ('settled_success','settled_failed','settled_cancelled','invalidated','superseded')))
ORDER BY r.allocation LIMIT ?`

func ListRemoteDisposalCandidates(ctx context.Context, q Reader, after int64, limit int) ([]RemoteDisposalCandidate, error) {
	return ListRemoteDisposalCandidatesThrough(ctx, q, after, math.MaxInt64, limit)
}

// ListRemoteDisposalCandidatesThrough bounds a fair scan to a retained high-water mark.
func ListRemoteDisposalCandidatesThrough(ctx context.Context, q Reader, after, through int64, limit int) ([]RemoteDisposalCandidate, error) {
	if after < 0 || through < after || limit < 1 || limit > 256 {
		return nil, fmt.Errorf("invalid remote disposal page")
	}
	rows, e := q.QueryContext(ctx, disposalCandidatesSQL, after, through, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []RemoteDisposalCandidate
	for rows.Next() {
		var c RemoteDisposalCandidate
		if e = rows.Scan(&c.Allocation, &c.Owner.Kind, &c.Owner.InvocationID, &c.RunID, &c.AttemptID); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func RemoteDisposalStillEligible(ctx context.Context, q Reader, c RemoteDisposalCandidate) (bool, error) {
	if c.Allocation < 1 {
		return false, fmt.Errorf("invalid remote allocation")
	}
	found, e := ListRemoteDisposalCandidates(ctx, q, c.Allocation-1, 1)
	if e != nil {
		return false, e
	}
	return len(found) == 1 && found[0] == c, nil
}
