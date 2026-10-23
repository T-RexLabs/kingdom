package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/google/uuid"
	"kingdom/internal/central/mutation"
)

// AccountReference identifies one authorization held by the trusted personal
// account service. Refresh rotates its tokens without changing this revision.
// It contains no tokens and is never part of public connection metadata.
type AccountReference struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
}

type accountPayload struct {
	Version   int              `json:"version"`
	Reference AccountReference `json:"account"`
}

func (r AccountReference) valid() bool {
	id, err := uuid.Parse(r.ID)
	return err == nil && id != uuid.Nil && id.String() == r.ID && r.Revision > 0
}

// CreateAccountRequest accepts only a reference already verified by the trusted
// account service for this actor and provider. The reference is encrypted with
// this immutable connection revision; the connection can only be personal.
func (s *Store) CreateAccountRequest(ctx context.Context, actor string, d Definition, ref AccountReference, key string) (Metadata, error) {
	if !mutation.ValidKey(key) {
		return Metadata{}, mutation.ErrInvalid
	}
	if !ref.valid() {
		return Metadata{}, ErrInvalid
	}
	encoded, err := json.Marshal(accountPayload{1, ref})
	if err != nil {
		return Metadata{}, ErrInvalid
	}
	return s.create(ctx, actor, Personal, d, encoded, key, true)
}

// ReviseAccountRequest retains the old reference and creates an exact successor.
// It cannot change an API-key connection or create a team account connection.
func (s *Store) ReviseAccountRequest(ctx context.Context, actor, id string, expected int64, d Definition, ref AccountReference, key string) (Metadata, error) {
	if !mutation.ValidKey(key) {
		return Metadata{}, mutation.ErrInvalid
	}
	if !ref.valid() {
		return Metadata{}, ErrInvalid
	}
	encoded, err := json.Marshal(accountPayload{1, ref})
	if err != nil {
		return Metadata{}, ErrInvalid
	}
	return s.revise(ctx, actor, id, expected, d, encoded, key, true)
}

// ResolveAccountForProject checks membership and personal ownership in the same
// catalog transaction. The account service must then check this exact reference
// and its current authorization before resolving any credential outside a worker.
func (s *Store) ResolveAccountForProject(ctx context.Context, actor, project, id string, revision int64) (Metadata, AccountReference, error) {
	if strings.TrimSpace(project) == "" {
		return Metadata{}, AccountReference{}, ErrInvalid
	}
	return s.resolveAccount(ctx, actor, project, id, revision, false)
}

// ResolveAccount returns a usable personal account reference to its active
// owner. It does not grant use in a project without a separate membership check.
func (s *Store) ResolveAccount(ctx context.Context, actor, id string, revision int64) (Metadata, AccountReference, error) {
	return s.resolveAccount(ctx, actor, "", id, revision, false)
}

// ResolveAccountForDisconnect retains owner-only access after the connection is
// disabled so an interrupted revocation can be recovered. This method grants no
// credential or execution authority and must only be used to disconnect.
func (s *Store) ResolveAccountForDisconnect(ctx context.Context, actor, id string, revision int64) (Metadata, AccountReference, error) {
	return s.resolveAccount(ctx, actor, "", id, revision, true)
}

func (s *Store) resolveAccount(ctx context.Context, actor, project, id string, revision int64, allowDisabled bool) (Metadata, AccountReference, error) {
	m, secret, err := s.resolve(ctx, actor, project, id, revision, resolutionPolicy{account: true, allowDisabled: allowDisabled})
	if err != nil {
		return Metadata{}, AccountReference{}, err
	}
	defer clear(secret.value)
	decoder := json.NewDecoder(bytes.NewReader(secret.value))
	decoder.DisallowUnknownFields()
	var payload accountPayload
	if decoder.Decode(&payload) != nil || payload.Version != 1 || !payload.Reference.valid() {
		return Metadata{}, AccountReference{}, ErrUnavailable
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return Metadata{}, AccountReference{}, ErrUnavailable
	}
	return m, payload.Reference, nil
}
