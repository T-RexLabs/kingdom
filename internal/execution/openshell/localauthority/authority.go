// Package localauthority joins canonical execution facts with an explicit
// OS-local credential owner. It does not grant central account membership.
package localauthority

import (
	"context"
	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

type Canonical interface {
	ValidateRemoteCapability(context.Context, providerrequest.Preparation, remote.Reference, access.Snapshot, *providerexecution.RequestIdentity, providerrequest.RemoteCapabilityStage) error
}

// Revisions checks current ownership and the exact retained revision. It must
// not search host OpenCode accounts, substitute another revision, or return secrets.
type Revisions interface {
	RequireLocalRevision(context.Context, access.Snapshot) error
}
type Authority struct {
	canonical      Canonical
	revisions      Revisions
	owner, project string
}

func New(c Canonical, r Revisions, owner, project string) (*Authority, error) {
	if c == nil || r == nil || !validID(owner) || !validID(project) {
		return nil, capability.ErrDenied
	}
	return &Authority{canonical: c, revisions: r, owner: owner, project: project}, nil
}

type Model struct{ Authority *Authority }
type Plugin struct{ Authority *Authority }

func (a Model) Check(ctx context.Context, b capability.Binding, stage modelproxy.Stage) error {
	if stage != modelproxy.StagePrepare && stage != modelproxy.StageActivate && stage != modelproxy.StageActive {
		return capability.ErrDenied
	}
	return a.Authority.check(ctx, b, providerrequest.RemoteCapabilityStage(stage))
}
func (a Plugin) Check(ctx context.Context, b capability.Binding, stage pluginauthority.Stage) error {
	if stage != pluginauthority.Prepare && stage != pluginauthority.Activate && stage != pluginauthority.Active && stage != pluginauthority.Receipt {
		return capability.ErrDenied
	}
	return a.Authority.check(ctx, b, providerrequest.RemoteCapabilityStage(stage))
}
func (a *Authority) check(ctx context.Context, b capability.Binding, stage providerrequest.RemoteCapabilityStage) error {
	if a == nil || b.Access.Validate() != nil || b.Access.AuthorityDomain() != access.DomainLocalOwner || b.Access.ActorID != a.owner || b.Access.ProjectID != a.project {
		return capability.ErrDenied
	}
	if a.canonical.ValidateRemoteCapability(ctx, b.Preparation, b.Runtime, b.Access, b.Request, stage) != nil {
		return capability.ErrDenied
	}
	if stage == providerrequest.CapabilityReceipt {
		return nil
	}
	if a.revisions.RequireLocalRevision(ctx, b.Access) != nil {
		return capability.ErrDenied
	}
	return nil
}
func validID(raw string) bool {
	id, err := uuid.Parse(raw)
	return err == nil && id != uuid.Nil && id.String() == raw
}
