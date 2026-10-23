package personal

import (
	"context"
	"database/sql"
	"encoding/json"

	"kingdom/internal/central/capability"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/mutation"
)

// Portal combines the two provider-specific sign-in flows behind owner-bound
// routes. It never turns a personal subscription into a shared team credential.
type Portal struct {
	Copilot     *SignIns
	ChatGPT     *Handoffs
	Accounts    *Service
	Connections *connections.Store
	Registry    *capability.Registry
}

func (p *Portal) Providers() []string {
	out := []string{}
	if p.Copilot != nil {
		out = append(out, "github-copilot")
	}
	if p.ChatGPT != nil {
		out = append(out, "openai")
	}
	return out
}
func (p *Portal) Begin(ctx context.Context, owner, session, provider, label, key string) (SignIn, error) {
	if p == nil || p.Accounts == nil {
		return SignIn{}, ErrDenied
	}
	if provider == "github-copilot" && p.Copilot != nil {
		if err := p.Copilot.authorize(ctx, owner, session); err != nil {
			return SignIn{}, err
		}
	} else if provider == "openai" && p.ChatGPT != nil {
		if p.ChatGPT.config.CheckSession(ctx, owner, session) != nil {
			return SignIn{}, ErrDenied
		}
	} else {
		return SignIn{}, ErrDenied
	}
	ctx, done, err := p.Accounts.enter(ctx)
	if err != nil {
		return SignIn{}, err
	}
	defer done()
	if err = reservePortalBegin(ctx, p.Accounts.db, owner, session, provider, label, key); err != nil {
		return SignIn{}, err
	}
	if provider == "github-copilot" && p.Copilot != nil {
		return p.Copilot.Begin(ctx, owner, session, label, key)
	}
	if provider == "openai" && p.ChatGPT != nil {
		return p.ChatGPT.Begin(ctx, owner, session, label, key)
	}
	return SignIn{}, ErrDenied
}

func initializePortalRequests(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_personal_begin_requests(owner_id TEXT NOT NULL,request_key TEXT NOT NULL,session_id TEXT NOT NULL,provider TEXT NOT NULL,label TEXT NOT NULL,PRIMARY KEY(owner_id,request_key));
CREATE TRIGGER IF NOT EXISTS central_personal_begin_immutable BEFORE UPDATE ON central_personal_begin_requests BEGIN SELECT RAISE(ABORT,'personal sign-in request is immutable');END;
CREATE TRIGGER IF NOT EXISTS central_personal_begin_retained BEFORE DELETE ON central_personal_begin_requests BEGIN SELECT RAISE(ABORT,'personal sign-in request is retained');END;`)
	return err
}
func reservePortalBegin(ctx context.Context, db *sql.DB, owner, session, provider, label, key string) error {
	if !mutation.ValidKey(key) || !safe(label, 256) {
		return ErrDenied
	}
	_, err := db.ExecContext(ctx, `INSERT INTO central_personal_begin_requests(owner_id,request_key,session_id,provider,label) VALUES(?,?,?,?,?) ON CONFLICT(owner_id,request_key) DO NOTHING`, owner, key, session, provider, label)
	if err != nil {
		return err
	}
	var savedSession, savedProvider, savedLabel string
	if err = db.QueryRowContext(ctx, `SELECT session_id,provider,label FROM central_personal_begin_requests WHERE owner_id=? AND request_key=?`, owner, key).Scan(&savedSession, &savedProvider, &savedLabel); err != nil {
		return err
	}
	if savedSession != session || savedProvider != provider || savedLabel != label {
		return mutation.ErrConflict
	}
	return nil
}
func (p *Portal) Get(ctx context.Context, owner, session, id string) (SignIn, error) {
	if p.Copilot != nil {
		if v, err := p.Copilot.Get(ctx, owner, session, id); err == nil {
			return v, nil
		}
	}
	if p.ChatGPT != nil {
		return p.ChatGPT.Get(ctx, owner, session, id)
	}
	return SignIn{}, ErrDenied
}
func (p *Portal) Cancel(ctx context.Context, owner, session, id string) error {
	if p.Copilot != nil {
		if _, err := p.Copilot.Get(ctx, owner, session, id); err == nil {
			return p.Copilot.Cancel(ctx, owner, session, id)
		}
	}
	if p.ChatGPT != nil {
		return p.ChatGPT.Cancel(ctx, owner, session, id)
	}
	return ErrDenied
}
func (p *Portal) Disconnect(ctx context.Context, owner, id, key string) (bool, error) {
	if p == nil || p.Connections == nil || p.Registry == nil || p.Accounts == nil {
		return false, ErrDenied
	}
	ctx, done, err := p.Accounts.enter(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	items, err := p.Connections.List(ctx, owner)
	if err != nil {
		return false, err
	}
	var current connections.Metadata
	for _, m := range items {
		if m.ID == id && m.OwnerID == owner && m.Scope == connections.Personal {
			current = m
			break
		}
	}
	if current.ID == "" {
		return false, ErrDenied
	}
	m, ref, err := p.Connections.ResolveAccountForDisconnect(ctx, owner, id, current.Revision)
	if err != nil {
		return false, ErrDenied
	}
	if err = p.Connections.DisableRequest(ctx, owner, id, key); err != nil {
		return false, err
	}
	if err = p.Registry.RevokeConnection(ctx, id, 0); err != nil {
		return false, err
	}
	confirmed, err := p.Accounts.Disconnect(ctx, owner, m.Definition, ref)
	// The durable connection fence and joined model admissions already prevent
	// use. Do not claim an upstream revocation that the provider did not confirm.
	return confirmed && err == nil, nil
}
func (p *Portal) PrepareHandoff(ctx context.Context, code string) (HandoffTarget, error) {
	if p.ChatGPT == nil {
		return HandoffTarget{}, ErrDenied
	}
	return p.ChatGPT.Prepare(ctx, code)
}
func (p *Portal) CompleteHandoff(ctx context.Context, code string, payload json.RawMessage) (SignIn, error) {
	if p.ChatGPT == nil {
		return SignIn{}, ErrDenied
	}
	return p.ChatGPT.Complete(ctx, code, payload)
}
