package localauthority

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"testing"
)

type canonicalFake struct {
	calls int
	err   error
}

func (f *canonicalFake) ValidateRemoteCapability(context.Context, providerrequest.Preparation, remote.Reference, access.Snapshot, *providerexecution.RequestIdentity, providerrequest.RemoteCapabilityStage) error {
	f.calls++
	return f.err
}

type revisionsFake struct {
	calls int
	err   error
}

func (f *revisionsFake) RequireLocalRevision(context.Context, access.Snapshot) error {
	f.calls++
	return f.err
}
func TestLocalAuthorityNeverInheritsHostedOrOtherOwner(t *testing.T) {
	owner, project := uuid.NewString(), uuid.NewString()
	c, r := &canonicalFake{}, &revisionsFake{}
	a, err := New(c, r, owner, project)
	if err != nil {
		t.Fatal(err)
	}
	b := capability.Binding{Access: access.Snapshot{Version: 2, Domain: access.DomainLocalOwner, ActorID: owner, ProjectID: project, Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), Revision: 3, OwnerID: owner, Scope: "local_owner", Provider: "openai"}}}}
	if err := (Plugin{a}).Check(context.Background(), b, pluginauthority.Active); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*access.Snapshot){func(s *access.Snapshot) { s.Version = 1; s.Domain = ""; s.Selection.Connection.Scope = "personal" }, func(s *access.Snapshot) { s.ActorID = uuid.NewString(); s.Selection.Connection.OwnerID = s.ActorID }, func(s *access.Snapshot) { s.ProjectID = uuid.NewString() }} {
		changed := b
		alter(&changed.Access)
		if (Plugin{a}).Check(context.Background(), changed, pluginauthority.Active) == nil {
			t.Fatal("foreign authority accepted")
		}
	}
	if c.calls != 1 || r.calls != 1 {
		t.Fatal("foreign scope reached canonical or credential store")
	}
	r.err = errors.New("revoked")
	if (Plugin{a}).Check(context.Background(), b, pluginauthority.Active) == nil {
		t.Fatal("revoked revision admitted")
	}
	before := r.calls
	if err := (Plugin{a}).Check(context.Background(), b, pluginauthority.Receipt); err != nil {
		t.Fatal(err)
	}
	if r.calls != before {
		t.Fatal("historical receipt requires current credential")
	}
	c.err = errors.New("wrong canonical identity")
	if (Plugin{a}).Check(context.Background(), b, pluginauthority.Receipt) == nil {
		t.Fatal("receipt bypassed canonical scope")
	}
}
