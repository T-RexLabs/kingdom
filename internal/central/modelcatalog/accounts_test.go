package modelcatalog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
)

type accountDiscoveryFunc func(context.Context, string, connections.Definition, connections.AccountReference) ([]AccountModel, error)

func (f accountDiscoveryFunc) Discover(ctx context.Context, owner string, d connections.Definition, ref connections.AccountReference) ([]AccountModel, error) {
	return f(ctx, owner, d, ref)
}

func TestAccountDiscoveryPinsPrivateRoutesWithoutValidationIO(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ref := connections.AccountReference{ID: uuid.NewString(), Revision: 3}
	definition := connections.Definition{Label: "Personal Account", Provider: "openai", AuthKind: "openai_oauth"}
	m, err := f.vault.CreateAccountRequest(ctx, f.member.ID, definition, ref, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.service.accounts = accountDiscoveryFunc(func(_ context.Context, owner string, d connections.Definition, r connections.AccountReference) ([]AccountModel, error) {
		calls++
		if owner != f.member.ID || d != definition || r != ref {
			t.Fatal("account identity changed")
		}
		return []AccountModel{{Model: Model{ID: "account-model", Name: "Account Model", Efforts: []string{"medium"}}, Route: modelproxy.Route{Protocol: "openai_chatgpt", Endpoint: "https://api.openai.com/v1", Model: "account-model"}}}, nil
	})
	if _, err = f.service.Refresh(ctx, f.owner.ID, f.project.ID, m.ID, m.Revision); err == nil || calls != 0 {
		t.Fatal("another user discovered personal models")
	}
	v, err := f.service.Refresh(ctx, f.member.ID, f.project.ID, m.ID, m.Revision)
	if err != nil || len(v.Models) != 1 || calls != 1 {
		t.Fatal(v, err, calls)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "api.openai.com") || strings.Contains(string(raw), ref.ID) {
		t.Fatal("private account route leaked")
	}
	if err = f.service.Validate(ctx, m, "openai/account-model", "medium"); err != nil {
		t.Fatal(err)
	}
	if err = f.service.Validate(ctx, m, "openai/account-model", "high"); err == nil {
		t.Fatal("guessed reasoning effort accepted")
	}
	route, err := f.service.AccountRoute(m, "openai/account-model")
	if err != nil || route.Protocol != "openai_chatgpt" || calls != 1 {
		t.Fatal(route, err, calls)
	}
	if _, err = f.service.AccountRoute(m, "other/account-model"); err == nil {
		t.Fatal("foreign provider accepted")
	}
	if _, err = f.service.AccountRoute(m, "openai/unknown"); err == nil {
		t.Fatal("undiscovered model accepted")
	}
	changed := m
	changed.Revision++
	if _, err = f.service.AccountRoute(changed, "openai/account-model"); err == nil {
		t.Fatal("route reused by another revision")
	}
}

func TestAccountDiscoveryRejectsMismatchedAndDuplicateModels(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "mismatch", true: "duplicate"}[duplicate], func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			m, err := f.vault.CreateAccountRequest(ctx, f.member.ID, connections.Definition{Label: "Account", Provider: "openai", AuthKind: "openai_oauth"}, connections.AccountReference{ID: uuid.NewString(), Revision: 1}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			item := AccountModel{Model: Model{ID: "model", Name: "Model"}, Route: modelproxy.Route{Protocol: "openai_chatgpt", Endpoint: "https://api.openai.com/v1", Model: "different"}}
			items := []AccountModel{item}
			if duplicate {
				item.Route.Model = "model"
				items = []AccountModel{item, item}
			}
			f.service.accounts = accountDiscoveryFunc(func(context.Context, string, connections.Definition, connections.AccountReference) ([]AccountModel, error) {
				return items, nil
			})
			if _, err = f.service.Refresh(ctx, f.member.ID, f.project.ID, m.ID, m.Revision); err == nil {
				t.Fatal("invalid catalog accepted")
			}
			if err = f.service.Validate(ctx, m, "openai/model", ""); err == nil {
				t.Fatal("failed discovery cached")
			}
		})
	}
}
