package access

import (
	"github.com/google/uuid"
	"testing"
)

func TestSelectionRejectsAmbiguousProviderAndText(t *testing.T) {
	actor := uuid.NewString()
	valid := Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: Selection{Model: "openai/model", Connection: Connection{ID: uuid.NewString(), OwnerID: actor, Scope: "personal", Revision: 1, Provider: "openai"}}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Snapshot){
		func(s *Snapshot) { s.Selection.Model = "anthropic/model" },
		func(s *Snapshot) { s.Selection.Model = "openai/" },
		func(s *Snapshot) { s.Selection.Model = "openai/model\t" },
		func(s *Snapshot) { s.Selection.Model = "openai/\xff" },
		func(s *Snapshot) {
			s.Selection.Connection.Provider = "openai/alias"
			s.Selection.Model = "openai/alias/model"
		},
		func(s *Snapshot) { s.Selection.Connection.Revision = 0 },
		func(s *Snapshot) { s.Selection.Connection.OwnerID = uuid.NewString() },
		func(s *Snapshot) { s.Selection.ThinkingEffort = "medium\x00" },
	} {
		altered := valid
		change(&altered)
		if err := altered.Validate(); err == nil {
			t.Fatalf("invalid selection accepted: %+v", altered)
		}
	}
}
