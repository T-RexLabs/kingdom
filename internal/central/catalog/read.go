package catalog

import (
	"context"
	"database/sql"
)

type Membership struct {
	User User
	Role Role
}

func (s *Store) Memberships(ctx context.Context, actor, project string) ([]Membership, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := authorize(ctx, tx, actor, project, ManageProject); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT u.id,u.display_name,u.team_owner,m.role FROM central_memberships m JOIN central_users u ON u.id=m.user_id WHERE m.project_id=? AND m.revoked=0 AND u.disabled=0 ORDER BY u.id`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Membership{}
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.User.ID, &m.User.DisplayName, &m.User.TeamOwner, &m.Role); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

// Project reads the caller's current role without inferring project authority
// from team ownership, including after an idempotent project creation replay.
func (s *Store) Project(ctx context.Context, actor, id string) (Project, error) {
	var p Project
	err := s.db.QueryRowContext(ctx, `SELECT p.id,p.display_name,m.role FROM central_projects p JOIN central_memberships m ON m.project_id=p.id JOIN central_users u ON u.id=m.user_id WHERE p.id=? AND m.user_id=? AND m.revoked=0 AND u.disabled=0`, id, actor).Scan(&p.ID, &p.DisplayName, &p.Role)
	if err == sql.ErrNoRows {
		return Project{}, ErrDenied
	}
	return p, err
}

// DirectoryUser is an administrative directory entry, visible only to a current
// team owner. Project members cannot enumerate the team. Identity references
// must never be included in project memberships or ordinary session metadata.
type DirectoryUser struct {
	User       User
	Identities []Identity
}

func (s *Store) UserDirectory(ctx context.Context, actor string) ([]DirectoryUser, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = requireTeamOwner(ctx, tx, actor); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT u.id,u.display_name,u.team_owner,i.issuer,i.subject FROM central_users u JOIN central_identities i ON i.user_id=u.id WHERE u.disabled=0 ORDER BY u.id,i.issuer,i.subject`)
	if err != nil {
		return nil, err
	}
	result := []DirectoryUser{}
	for rows.Next() {
		var user User
		var identity Identity
		if err = rows.Scan(&user.ID, &user.DisplayName, &user.TeamOwner, &identity.Issuer, &identity.Subject); err != nil {
			rows.Close()
			return nil, err
		}
		if len(result) == 0 || result[len(result)-1].User.ID != user.ID {
			result = append(result, DirectoryUser{User: user, Identities: []Identity{}})
		}
		result[len(result)-1].Identities = append(result[len(result)-1].Identities, identity)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return result, tx.Commit()
}
