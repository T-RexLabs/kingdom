// Package catalog owns hosted identities and project access. Workflow state
// remains in each project's database. Callers must verify external identity
// assertions before passing an Identity to this package.
package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"kingdom/internal/central/mutation"
)

var (
	ErrDenied  = errors.New("access denied")
	ErrInvalid = errors.New("invalid catalog input")
)

type Identity struct{ Issuer, Subject string }
type User struct {
	ID, DisplayName string
	TeamOwner       bool
}
type Project struct{ ID, DisplayName string }
type Role string

const (
	Member Role = "member"
	Owner  Role = "owner"
)

type Permission string

const (
	UseProject    Permission = "use"
	ManageProject Permission = "manage"
)

type Config struct{ InitialOwners []Identity }
type Store struct {
	db            *sql.DB
	initialOwners map[Identity]bool
}

// New initializes a separate central catalog. The caller owns the database
// lifecycle and must configure SQLite busy handling for concurrent requests.
func New(ctx context.Context, db *sql.DB, cfg Config) (*Store, error) {
	if db == nil {
		return nil, ErrInvalid
	}
	s := &Store{db: db, initialOwners: make(map[Identity]bool)}
	for _, id := range cfg.InitialOwners {
		if !validIdentity(id) {
			return nil, ErrInvalid
		}
		s.initialOwners[id] = true
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, statement := range schema {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return nil, fmt.Errorf("initialize central catalog: %w", err)
		}
	}
	if err := mutation.Initialize(ctx, tx); err != nil {
		return nil, err
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM central_schema WHERE slot=1`).Scan(&version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported central catalog schema version %d", version)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s, nil
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS central_schema (slot INTEGER PRIMARY KEY CHECK(slot=1), version INTEGER NOT NULL)`,
	`INSERT INTO central_schema(slot,version) VALUES(1,1) ON CONFLICT(slot) DO NOTHING`,
	`CREATE TABLE IF NOT EXISTS central_users (id TEXT PRIMARY KEY, display_name TEXT NOT NULL, team_owner INTEGER NOT NULL CHECK(team_owner IN (0,1)), disabled INTEGER NOT NULL DEFAULT 0 CHECK(disabled IN (0,1)))`,
	`CREATE TABLE IF NOT EXISTS central_identities (issuer TEXT NOT NULL, subject TEXT NOT NULL, user_id TEXT NOT NULL REFERENCES central_users(id), PRIMARY KEY(issuer,subject))`,
	`CREATE TRIGGER IF NOT EXISTS central_identity_immutable BEFORE UPDATE ON central_identities BEGIN SELECT RAISE(ABORT, 'identity mapping is immutable'); END`,
	`CREATE TRIGGER IF NOT EXISTS central_identity_retained BEFORE DELETE ON central_identities BEGIN SELECT RAISE(ABORT, 'identity mapping must be retained'); END`,
	`CREATE TABLE IF NOT EXISTS central_bootstrap (slot INTEGER PRIMARY KEY CHECK(slot=1), user_id TEXT NOT NULL UNIQUE REFERENCES central_users(id))`,
	`CREATE TABLE IF NOT EXISTS central_projects (id TEXT PRIMARY KEY, display_name TEXT NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS central_memberships (project_id TEXT NOT NULL REFERENCES central_projects(id), user_id TEXT NOT NULL REFERENCES central_users(id), role TEXT NOT NULL CHECK(role IN ('owner','member')), revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)), PRIMARY KEY(project_id,user_id))`,
}

func validText(v string) bool {
	return v != "" && strings.TrimSpace(v) == v && len(v) <= 512 && !strings.ContainsAny(v, "\x00\r\n")
}
func validIdentity(id Identity) bool { return validText(id.Issuer) && validText(id.Subject) }

// BootstrapOwner admits exactly one explicitly allowlisted identity. It does
// not grant ownership to the first arbitrary login. The singleton claim and
// identity insertion are committed together, including across Store instances.
func (s *Store) BootstrapOwner(ctx context.Context, identity Identity, displayName string) (User, error) {
	if !validIdentity(identity) || !validText(displayName) {
		return User{}, ErrInvalid
	}
	if !s.initialOwners[identity] {
		return User{}, ErrDenied
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback()
	var ownerID string
	err = tx.QueryRowContext(ctx, `SELECT user_id FROM central_bootstrap WHERE slot=1`).Scan(&ownerID)
	if err == nil {
		user, err := resolve(ctx, tx, identity)
		if err != nil || user.ID != ownerID || !user.TeamOwner {
			return User{}, ErrDenied
		}
		return user, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return User{}, err
	}
	user, err := insertUser(ctx, tx, identity, displayName, true)
	if err != nil {
		return User{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_bootstrap(slot,user_id) VALUES(1,?)`, user.ID); err != nil {
		return User{}, err
	}
	return user, tx.Commit()
}

// Reader permits authorization lookups in the caller's transaction.
type Reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type queryer = Reader

func resolve(ctx context.Context, q queryer, id Identity) (User, error) {
	var u User
	err := q.QueryRowContext(ctx, `SELECT u.id,u.display_name,u.team_owner FROM central_users u JOIN central_identities i ON i.user_id=u.id WHERE i.issuer=? AND i.subject=? AND u.disabled=0`, id.Issuer, id.Subject).Scan(&u.ID, &u.DisplayName, &u.TeamOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrDenied
	}
	return u, err
}
func (s *Store) ResolveIdentity(ctx context.Context, id Identity) (User, error) {
	if !validIdentity(id) {
		return User{}, ErrDenied
	}
	return resolve(ctx, s.db, id)
}

// ActiveUser resolves only a currently enabled account. Session issuance and
// authorization rechecks must not accept a cached user after disablement.
func (s *Store) ActiveUser(ctx context.Context, id string) (User, error) {
	return LookupUser(ctx, s.db, id)
}

// LookupUser reads current account authority through the supplied transaction
// or database. It must be called inside the transaction that changes secrets.
func LookupUser(ctx context.Context, reader Reader, id string) (User, error) {
	var u User
	err := reader.QueryRowContext(ctx, `SELECT id,display_name,team_owner FROM central_users WHERE id=? AND disabled=0`, id).Scan(&u.ID, &u.DisplayName, &u.TeamOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrDenied
	}
	return u, err
}

func insertUser(ctx context.Context, tx *sql.Tx, identity Identity, displayName string, owner bool) (User, error) {
	u := User{ID: uuid.NewString(), DisplayName: displayName, TeamOwner: owner}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_users(id,display_name,team_owner) VALUES(?,?,?)`, u.ID, u.DisplayName, u.TeamOwner); err != nil {
		return User{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_identities(issuer,subject,user_id) VALUES(?,?,?)`, identity.Issuer, identity.Subject, u.ID); err != nil {
		return User{}, err
	}
	return u, nil
}

func requireTeamOwner(ctx context.Context, tx *sql.Tx, actor string) error {
	var yes bool
	err := tx.QueryRowContext(ctx, `SELECT team_owner FROM central_users WHERE id=? AND disabled=0`, actor).Scan(&yes)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !yes {
		return ErrDenied
	}
	return err
}

// CreateUser preauthorizes an immutable identity. It never promotes an existing
// user or links identities based on display names or email addresses.
func (s *Store) CreateUser(ctx context.Context, actor string, identity Identity, displayName string) (User, error) {
	return s.createUser(ctx, actor, identity, displayName, "")
}
func (s *Store) CreateUserRequest(ctx context.Context, actor string, identity Identity, displayName, key string) (User, error) {
	if !mutation.ValidKey(key) {
		return User{}, mutation.ErrInvalid
	}
	return s.createUser(ctx, actor, identity, displayName, key)
}
func (s *Store) createUser(ctx context.Context, actor string, identity Identity, displayName, key string) (User, error) {
	if !validIdentity(identity) || !validText(displayName) {
		return User{}, ErrInvalid
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback()
	if err := requireTeamOwner(ctx, tx, actor); err != nil {
		return User{}, err
	}
	fingerprint, _ := mutation.Fingerprint(struct {
		Identity Identity
		Name     string
	}{identity, displayName})
	if result, found, err := mutation.Replay[User](ctx, tx, actor, key, "user.create", "", fingerprint); err != nil || found {
		return result, err
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT user_id FROM central_identities WHERE issuer=? AND subject=?`, identity.Issuer, identity.Subject).Scan(&existing)
	if err == nil {
		u, err := resolve(ctx, tx, identity)
		if err != nil {
			return User{}, err
		}
		if err := mutation.Record(ctx, tx, actor, key, "user.create", "", fingerprint, u); err != nil {
			return User{}, err
		}
		return u, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return User{}, err
	}
	u, err := insertUser(ctx, tx, identity, displayName, false)
	if err != nil {
		return User{}, err
	}
	if err := mutation.Record(ctx, tx, actor, key, "user.create", "", fingerprint, u); err != nil {
		return User{}, err
	}
	return u, tx.Commit()
}

func (s *Store) CreateProject(ctx context.Context, actor, displayName string) (Project, error) {
	return s.createProject(ctx, actor, displayName, "")
}
func (s *Store) CreateProjectRequest(ctx context.Context, actor, displayName, key string) (Project, error) {
	if !mutation.ValidKey(key) {
		return Project{}, mutation.ErrInvalid
	}
	return s.createProject(ctx, actor, displayName, key)
}
func (s *Store) createProject(ctx context.Context, actor, displayName, key string) (Project, error) {
	if !validText(displayName) {
		return Project{}, ErrInvalid
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback()
	if err := requireTeamOwner(ctx, tx, actor); err != nil {
		return Project{}, err
	}
	fingerprint, _ := mutation.Fingerprint(displayName)
	if result, found, err := mutation.Replay[Project](ctx, tx, actor, key, "project.create", "", fingerprint); err != nil || found {
		return result, err
	}
	p := Project{ID: uuid.NewString(), DisplayName: displayName}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_projects(id,display_name) VALUES(?,?)`, p.ID, p.DisplayName); err != nil {
		return Project{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_memberships(project_id,user_id,role) VALUES(?,?,'owner')`, p.ID, actor); err != nil {
		return Project{}, err
	}
	if err := mutation.Record(ctx, tx, actor, key, "project.create", "", fingerprint, p); err != nil {
		return Project{}, err
	}
	return p, tx.Commit()
}

func authorize(ctx context.Context, q queryer, actor, project string, permission Permission) error {
	if permission != UseProject && permission != ManageProject {
		return ErrDenied
	}
	var role Role
	err := q.QueryRowContext(ctx, `SELECT m.role FROM central_memberships m JOIN central_users u ON u.id=m.user_id JOIN central_projects p ON p.id=m.project_id WHERE m.project_id=? AND m.user_id=? AND m.revoked=0 AND u.disabled=0`, project, actor).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDenied
	}
	if err != nil {
		return err
	}
	if permission == ManageProject && role != Owner {
		return ErrDenied
	}
	return nil
}
func (s *Store) Authorize(ctx context.Context, actor, project string, permission Permission) error {
	return authorize(ctx, s.db, actor, project, permission)
}

// AuthorizeTx checks current project authority in the caller's catalog
// transaction, so credential access and membership use one consistent view.
func AuthorizeTx(ctx context.Context, tx *sql.Tx, actor, project string, permission Permission) error {
	if tx == nil {
		return ErrDenied
	}
	return authorize(ctx, tx, actor, project, permission)
}

// SetMembership changes only project authority. No project role can grant
// team ownership, and authorization is checked in the mutation transaction.
func (s *Store) SetMembership(ctx context.Context, actor, project, user string, role Role) error {
	return s.setMembership(ctx, actor, project, user, role, "")
}
func (s *Store) SetMembershipRequest(ctx context.Context, actor, project, user string, role Role, key string) error {
	if !mutation.ValidKey(key) {
		return mutation.ErrInvalid
	}
	return s.setMembership(ctx, actor, project, user, role, key)
}
func (s *Store) setMembership(ctx context.Context, actor, project, user string, role Role, key string) error {
	if role != Member && role != Owner {
		return ErrInvalid
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := authorize(ctx, tx, actor, project, ManageProject); err != nil {
		return err
	}
	fingerprint, _ := mutation.Fingerprint(struct {
		User string
		Role Role
	}{user, role})
	if _, found, err := mutation.Replay[struct{}](ctx, tx, actor, key, "membership.set", project, fingerprint); err != nil || found {
		return err
	}
	if role != Owner {
		if err := retainProjectOwner(ctx, tx, project, user); err != nil {
			return err
		}
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM central_users WHERE id=? AND disabled=0`, user).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return ErrDenied
	} else if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO central_memberships(project_id,user_id,role) VALUES(?,?,?) ON CONFLICT(project_id,user_id) DO UPDATE SET role=excluded.role,revoked=0`, project, user, role)
	if err != nil {
		return err
	}
	if err := mutation.Record(ctx, tx, actor, key, "membership.set", project, fingerprint, struct{}{}); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) RevokeMembership(ctx context.Context, actor, project, user string) error {
	return s.revokeMembership(ctx, actor, project, user, "")
}
func (s *Store) RevokeMembershipRequest(ctx context.Context, actor, project, user string, key string) error {
	if !mutation.ValidKey(key) {
		return mutation.ErrInvalid
	}
	return s.revokeMembership(ctx, actor, project, user, key)
}
func (s *Store) revokeMembership(ctx context.Context, actor, project, user string, key string) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := authorize(ctx, tx, actor, project, ManageProject); err != nil {
		return err
	}
	fingerprint, _ := mutation.Fingerprint(user)
	if _, found, err := mutation.Replay[struct{}](ctx, tx, actor, key, "membership.revoke", project, fingerprint); err != nil || found {
		return err
	}
	if err := retainProjectOwner(ctx, tx, project, user); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE central_memberships SET revoked=1 WHERE project_id=? AND user_id=?`, project, user); err != nil {
		return err
	}
	if err := mutation.Record(ctx, tx, actor, key, "membership.revoke", project, fingerprint, struct{}{}); err != nil {
		return err
	}
	return tx.Commit()
}

func retainProjectOwner(ctx context.Context, tx *sql.Tx, project, user string) error {
	var lastOwner bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM central_memberships WHERE project_id=? AND user_id=? AND role='owner' AND revoked=0) AND NOT EXISTS(SELECT 1 FROM central_memberships m JOIN central_users u ON u.id=m.user_id WHERE m.project_id=? AND m.user_id<>? AND m.role='owner' AND m.revoked=0 AND u.disabled=0)`, project, user, project, user).Scan(&lastOwner)
	if err != nil {
		return err
	}
	if lastOwner {
		return ErrDenied
	}
	return nil
}

func (s *Store) Projects(ctx context.Context, actor string) ([]Project, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.id,p.display_name FROM central_projects p JOIN central_memberships m ON m.project_id=p.id JOIN central_users u ON u.id=m.user_id WHERE m.user_id=? AND m.revoked=0 AND u.disabled=0 ORDER BY p.id`, actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	projects := []Project{}
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.ID, &p.DisplayName); err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

// Reserve SQLite's writer before reading authority. A concurrent revocation
// therefore precedes or follows this whole operation, never its authorization.
func (s *Store) beginWrite(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE central_bootstrap SET slot=slot WHERE slot=1`); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}
