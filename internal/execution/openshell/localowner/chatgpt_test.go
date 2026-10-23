package localowner

import (
	"context"
	"github.com/google/uuid"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	"kingdom/internal/subscription/chatgpt"
	"net/http"
	"testing"
)

type subscriptionFixture struct {
	reg   chatgpt.Registration
	calls int
}

func (f *subscriptionFixture) Registration(context.Context, string, string) (chatgpt.Registration, error) {
	return f.reg, nil
}
func (f *subscriptionFixture) Models(context.Context, string, string, http.RoundTripper) ([]chatgpt.Model, chatgpt.Registration, error) {
	f.calls++
	return []chatgpt.Model{{Slug: "model", Name: "Account Model", Visibility: "list", Reasoning: []chatgpt.ReasoningLevel{{Effort: "medium"}}}}, f.reg, nil
}
func (f *subscriptionFixture) Access(context.Context, string, string) (chatgpt.Token, chatgpt.Registration, error) {
	return chatgpt.Token{}, f.reg, nil
}
func TestChatGPTSelectionUsesCurrentOwnerAndRegistration(t *testing.T) {
	ctx := context.Background()
	owner, project, id := uuid.NewString(), uuid.NewString(), uuid.NewString()
	f := &subscriptionFixture{reg: chatgpt.Registration{ID: id, State: "ready", Revision: 1}}
	m, e := NewChatGPT(f, owner, project, id, nil)
	if e != nil {
		t.Fatal(e)
	}
	selection, e := m.ResolveLocal(ctx, "openai/model", "medium")
	if e != nil {
		t.Fatal(e)
	}
	snapshot := access.Snapshot{Version: 2, Domain: access.DomainLocalOwner, ActorID: owner, ProjectID: project, Selection: selection}
	route, e := m.Pin(ctx, modelproxy.Binding{Access: snapshot})
	if e != nil || route.Protocol != "openai_chatgpt" || route.Endpoint != "https://api.openai.com/v1" {
		t.Fatalf("route: %+v %v", route, e)
	}
	if _, e = m.ResolveLocal(ctx, "openai/foreign", ""); e == nil {
		t.Fatal("foreign model accepted")
	}
	if _, e = m.ResolveLocal(ctx, "openai/model", "ultra"); e == nil {
		t.Fatal("undiscovered effort accepted")
	}
	for _, change := range []func(*access.Snapshot){func(s *access.Snapshot) { s.ActorID = uuid.NewString() }, func(s *access.Snapshot) { s.ProjectID = uuid.NewString() }, func(s *access.Snapshot) { s.Domain = access.DomainHosted; s.Selection.Connection.Scope = "personal" }, func(s *access.Snapshot) { s.Selection.Connection.ID = uuid.NewString() }} {
		foreign := snapshot
		change(&foreign)
		if m.RequireLocalRevision(ctx, foreign) == nil {
			t.Fatal("foreign authority accepted")
		}
	}
	f.reg.Revision = 2
	if m.RequireLocalRevision(ctx, snapshot) == nil {
		t.Fatal("reauth preserved stale authority")
	}
	if f.calls != 2 {
		t.Fatal("catalog not refreshed on reauth")
	}
	f.reg.State = "signed_out"
	if _, e = m.List(ctx, false); e == nil {
		t.Fatal("disconnected catalog usable")
	}
}
