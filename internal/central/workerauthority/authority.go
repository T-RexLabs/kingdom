// Package workerauthority joins project execution facts and current central
// access. Neither a saved connection reference nor an opaque token is authority.
package workerauthority

import (
	"context"

	"kingdom/internal/central/capability"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
)

type Authority struct {
	project     *providerrequest.Authority
	connections *connections.Store
}

func New(project *providerrequest.Authority, connections *connections.Store) (*Authority, error) {
	if project == nil || connections == nil {
		return nil, capability.ErrDenied
	}
	return &Authority{project: project, connections: connections}, nil
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
	if a == nil || a.project == nil || a.connections == nil || b.Access.AuthorityDomain() != access.DomainHosted {
		return capability.ErrDenied
	}
	if err := a.project.ValidateRemoteCapability(ctx, b.Preparation, b.Runtime, b.Access, b.Request, stage); err != nil {
		return capability.ErrDenied
	}
	// Late receipts resolve admitted effects after access loss; they cannot
	// dispatch, create credentials, or start another tool.
	if stage == providerrequest.CapabilityReceipt {
		return nil
	}
	c := b.Access.Selection.Connection
	m, err := a.connections.PinForProject(ctx, b.Access.ActorID, b.Access.ProjectID, c.ID, c.Revision)
	if err != nil || m.ID != c.ID || m.Revision != c.Revision || m.OwnerID != c.OwnerID || string(m.Scope) != c.Scope || m.Definition.Provider != c.Provider || m.Disabled {
		return capability.ErrDenied
	}
	return nil
}
