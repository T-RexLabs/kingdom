package providerrequest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/remote"
)

type RemoteCapabilityCertificate struct {
	Version       int
	RegistryID    string
	CertificateID string
	Purpose       string
	Binding       struct {
		Preparation Preparation
		Runtime     remote.Reference
		Access      access.Snapshot
		Request     *providerexecution.RequestIdentity
	}
	Capabilities []struct {
		ID      string
		Request *providerexecution.RequestIdentity
	}
}

// ValidateRemoteCapabilityClosure checks historical project facts only. It does
// not prove registry drain and cannot create a certificate or store one.
func ValidateRemoteCapabilityClosure(ctx context.Context, q Reader, raw []byte) (RemoteCapabilityCertificate, string, error) {
	var c RemoteCapabilityCertificate
	if len(raw) == 0 || len(raw) > 1<<20 {
		return c, "", remote.ErrConflict
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF {
		return c, "", remote.ErrConflict
	}
	canonical, _ := json.Marshal(c)
	if !bytes.Equal(canonical, raw) || c.Version != 1 || (c.Purpose != "model" && c.Purpose != "plugin") || c.Binding.Request != nil {
		return c, "", remote.ErrConflict
	}
	for _, id := range []string{c.RegistryID, c.CertificateID} {
		parsed, e := uuid.Parse(id)
		if e != nil || parsed == uuid.Nil || parsed.String() != id {
			return c, "", remote.ErrConflict
		}
	}
	var registry string
	if err := q.QueryRowContext(ctx, `SELECT registry_id FROM remote_capability_registry WHERE singleton=1`).Scan(&registry); err != nil {
		return c, "", err
	}
	if registry != c.RegistryID {
		return c, "", remote.ErrConflict
	}
	p := c.Binding.Preparation
	saved, err := access.Invocation(ctx, q, string(p.Owner.Kind), p.Owner.InvocationID)
	if err != nil {
		return c, "", err
	}
	if saved != c.Binding.Access {
		return c, "", remote.ErrConflict
	}
	current, err := loadOwner(ctx, q, p.Owner)
	if err != nil {
		return c, "", err
	}
	if current.id != p.ResourceID {
		return c, "", remote.ErrConflict
	}
	var lease Lease
	var fenced bool
	if err = q.QueryRowContext(ctx, `SELECT lease_owner,lease_version,admission_closed FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, p.Owner.Kind, p.Owner.InvocationID).Scan(&lease.Owner, &lease.Version, &fenced); err != nil {
		return c, "", err
	}
	if lease != p.Lease || !fenced {
		return c, "", remote.ErrConflict
	}
	r, err := remote.ReadRuntime(ctx, q, c.Binding.Runtime.Allocation)
	if err != nil {
		return c, "", err
	}
	if r.Reference != c.Binding.Runtime || !r.AdmissionClosed || r.Owner.Kind != string(p.Owner.Kind) || r.Owner.InvocationID != p.Owner.InvocationID || r.Owner.LeaseOwner != p.Lease.Owner || r.Owner.LeaseVersion != p.Lease.Version {
		return c, "", remote.ErrConflict
	}
	bound, found, err := ForOwner(ctx, q, p.Owner)
	if err != nil {
		return c, "", err
	}
	if found && (bound.Backend != providerexecution.BackendOpenShell || bound.RemoteAllocation != r.Reference.Allocation || bound.ID != r.Owner.BindingID) {
		return c, "", remote.ErrConflict
	}
	previous := ""
	for _, cap := range c.Capabilities {
		parsed, e := uuid.Parse(cap.ID)
		if e != nil || parsed == uuid.Nil || parsed.String() != cap.ID || cap.ID <= previous {
			return c, "", remote.ErrConflict
		}
		previous = cap.ID
		if cap.Request != nil {
			if cap.Request.Validate() != nil || cap.Request.InstanceID != r.Reference.MainInstanceID {
				return c, "", remote.ErrConflict
			}
			// Before binding, activation remains retained in the trusted registry proof.
			// It grants no prompt dispatch and must not be discarded during abandonment.
			if found && *cap.Request != bound.Identity {
				return c, "", remote.ErrConflict
			}
		}
	}
	sum := sha256.Sum256(raw)
	return c, hex.EncodeToString(sum[:]), nil
}

func RequireRemoteCapabilitiesClosed(ctx context.Context, q Reader, owner Owner, allocation int64) error {
	for _, purpose := range []string{"model", "plugin"} {
		var raw []byte
		var digest, registry, id string
		err := q.QueryRowContext(ctx, `SELECT certificate,certificate_sha256,registry_id,certificate_id FROM remote_capability_closures WHERE allocation=? AND purpose=?`, allocation, purpose).Scan(&raw, &digest, &registry, &id)
		if err != nil {
			return openResources(OpenUnproven, "remote capability closure is not retained")
		}
		c, actual, err := ValidateRemoteCapabilityClosure(ctx, q, raw)
		if err != nil {
			return err
		}
		if actual != digest || c.RegistryID != registry || c.CertificateID != id || c.Purpose != purpose || c.Binding.Runtime.Allocation != allocation || c.Binding.Preparation.Owner != owner {
			return fmt.Errorf("remote capability closure conflicts with canonical owner")
		}
	}
	return nil
}
