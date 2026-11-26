package access

import (
	"encoding/json"
	"testing"
)

func TestLocalOwnerSnapshotHasSeparateAuthorityDomain(t *testing.T) {
	const raw = `{"version":2,"domain":"local_owner","actor_id":"11111111-1111-4111-8111-111111111111","project_id":"22222222-2222-4222-8222-222222222222","selection":{"model":"openai/model","connection":{"id":"33333333-3333-4333-8333-333333333333","revision":1,"owner_id":"11111111-1111-4111-8111-111111111111","scope":"local_owner","provider":"openai"}}}`
	var s Snapshot
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("explicit local ownership rejected: %v", err)
	}
	for _, scope := range []string{"personal", "team"} {
		altered := s
		altered.Selection.Connection.Scope = scope
		if altered.Validate() == nil {
			t.Fatal("local domain accepted central credential", scope)
		}
	}
	s.Selection.Connection.OwnerID = s.ProjectID
	if s.Validate() == nil {
		t.Fatal("local owner mismatch accepted")
	}
}

func TestLocalSnapshotVersionAndRoleCannotLoseDomain(t *testing.T) {
	owner := "11111111-1111-4111-8111-111111111111"
	s := Snapshot{Version: 2, Domain: DomainLocalOwner, ActorID: owner, ProjectID: "22222222-2222-4222-8222-222222222222", Selection: Selection{Model: "openai/model", Connection: Connection{ID: "33333333-3333-4333-8333-333333333333", Revision: 1, OwnerID: owner, Scope: "local_owner", Provider: "openai"}}}
	for _, change := range []func(*Snapshot){func(s *Snapshot) { s.Version = 1 }, func(s *Snapshot) { s.Domain = DomainHosted }, func(s *Snapshot) { s.Domain = "" }} {
		other := s
		change(&other)
		if other.Validate() == nil {
			t.Fatal("ambiguous local authority accepted")
		}
	}
	run := RunSnapshot{Version: s.Version, Domain: s.Domain, ActorID: s.ActorID, ProjectID: s.ProjectID, Global: s.Selection}
	got, err := run.ForRole("builder")
	if err != nil || got != s {
		t.Fatal("role lost local ownership", got, err)
	}
}
