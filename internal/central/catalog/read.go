package catalog

import (
	"context"
	"database/sql"
)

type Membership struct {
	User User
	Role Role
}

// Users is an administrative directory. Project members cannot enumerate the team.
func (s *Store) Users(ctx context.Context, actor string) ([]User, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := requireTeamOwner(ctx, tx, actor); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,display_name,team_owner FROM central_users WHERE disabled=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []User{}
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.DisplayName, &u.TeamOwner); err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit()
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
