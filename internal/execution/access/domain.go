package access

import (
	"context"
	"database/sql"
)

// ProjectDomain returns the immutable deployment domain. Unbound host-local
// execution has no access domain and receives no managed credential authority.
func ProjectDomain(ctx context.Context, q Reader) (Domain, string, error) {
	project, err := Project(ctx, q)
	if err != nil || project == "" {
		return "", "", err
	}
	var owner string
	err = q.QueryRowContext(ctx, `SELECT owner_id FROM execution_access_local_owner WHERE slot=1 AND domain='local_owner'`).Scan(&owner)
	if err == sql.ErrNoRows {
		return DomainHosted, "", nil
	}
	if err != nil {
		return "", "", err
	}
	if !validID(owner) {
		return "", "", ErrInvalid
	}
	return DomainLocalOwner, owner, nil
}

func requireSnapshotDomain(ctx context.Context, q Reader, s Snapshot) error {
	domain, owner, err := ProjectDomain(ctx, q)
	if err != nil {
		return err
	}
	if domain != s.AuthorityDomain() || domain == DomainLocalOwner && owner != s.ActorID {
		return ErrInvalid
	}
	return nil
}
