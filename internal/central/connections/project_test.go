package connections

import (
	"context"
	"errors"
	"testing"

	"kingdom/internal/central/catalog"
)

func TestProjectCredentialPinRequiresCurrentMembershipAndExactRevision(t *testing.T) {
	s, db, owner, member := setup(t)
	ctx := context.Background()
	c, err := catalog.New(ctx, db, catalog.Config{})
	if err != nil {
		t.Fatal(err)
	}
	project, err := c.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	personal, err := s.Create(ctx, member.ID, Personal, definition(), []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PinForProject(ctx, member.ID, project.ID, personal.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("nonmember pin: %v", err)
	}
	if err := c.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	pin, err := s.PinForProject(ctx, member.ID, project.ID, personal.ID, 1)
	if err != nil || pin != personal {
		t.Fatalf("pin: %+v %v", pin, err)
	}
	if _, err := s.PinForProject(ctx, owner.ID, project.ID, personal.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("project owner read personal: %v", err)
	}
	if _, err := s.PinForProject(ctx, member.ID, project.ID, personal.ID, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unpinned selection: %v", err)
	}
	if _, err := s.Revise(ctx, member.ID, personal.ID, 1, definition(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	_, secret, err := s.ResolveForProject(ctx, member.ID, project.ID, pin.ID, pin.Revision)
	if err != nil || string(secret.Bytes()) != "first" {
		t.Fatalf("revision changed under invocation: %v", err)
	}
	if err := c.RevokeMembership(ctx, owner.ID, project.ID, member.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ResolveForProject(ctx, member.ID, project.ID, pin.ID, pin.Revision); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked member resolve: %v", err)
	}
}

func TestProjectCredentialPinDoesNotDecryptAndDisableBlocksAllRevisions(t *testing.T) {
	s, db, owner, member := setup(t)
	ctx := context.Background()
	c, err := catalog.New(ctx, db, catalog.Config{})
	if err != nil {
		t.Fatal(err)
	}
	project, err := c.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	team, err := s.Create(ctx, owner.ID, Team, definition(), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	// Pinning metadata must not depend on opening the credential ciphertext.
	s.aead = nil
	if _, err := s.PinForProject(ctx, member.ID, project.ID, team.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Disable(ctx, owner.ID, team.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PinForProject(ctx, member.ID, project.ID, team.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled pin: %v", err)
	}
	if _, _, err := s.ResolveForProject(ctx, member.ID, project.ID, team.ID, 1); !errors.Is(err, ErrDenied) {
		t.Fatalf("disabled resolve: %v", err)
	}
}
