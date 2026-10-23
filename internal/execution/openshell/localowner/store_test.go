package localowner

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	_ "modernc.org/sqlite"
	"strings"
	"testing"
)

func TestLocalOwnerRetainsEncryptedRevisionAndRejectsForeignAuthority(t *testing.T) {
	ctx := context.Background()
	db, e := sql.Open("sqlite", ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	owner := "00000000-0000-4000-8000-000000000001"
	project := "00000000-0000-4000-8000-000000000002"
	id := "00000000-0000-4000-8000-000000000003"
	key := bytes.Repeat([]byte{7}, 32)
	s, e := New(ctx, db, key, owner, project)
	if e != nil {
		t.Fatal(e)
	}
	d := Definition{Provider: "openai", Endpoint: "https://api.openai.com/v1", Models: []Model{{ID: "model", Efforts: []string{"medium"}}}}
	secret := []byte("test-only-explicit-secret")
	rev, e := s.Put(ctx, id, d, secret, false)
	if e != nil || rev != 1 {
		t.Fatal(rev, e)
	}
	rev, e = s.Put(ctx, id, d, secret, false)
	if e != nil || rev != 1 {
		t.Fatal("idempotent startup changed revision", rev, e)
	}
	selected, e := s.Selection(ctx, id, "openai/model", "medium")
	if e != nil {
		t.Fatal(e)
	}
	a := access.Snapshot{Version: 2, Domain: access.DomainLocalOwner, ActorID: owner, ProjectID: project, Selection: selected}
	b := modelproxy.Binding{Access: a}
	route, e := s.Pin(ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	credential, e := s.Resolve(ctx, b, route)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(fmt.Sprintf("%v %#v", credential, credential), string(secret)) {
		t.Fatal("credential formatting leaked secret")
	}
	if _, e = json.Marshal(credential); e == nil {
		t.Fatal("credential serialized")
	}
	var ciphertext []byte
	var definition string
	if e = db.QueryRow(`SELECT ciphertext FROM local_model_secrets`).Scan(&ciphertext); e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(ciphertext, secret) {
		t.Fatal("plaintext retained")
	}
	if e = db.QueryRow(`SELECT definition FROM local_model_revisions`).Scan(&definition); e != nil || strings.Contains(definition, string(secret)) {
		t.Fatal("metadata contains secret", e)
	}
	for _, mutate := range []func(*access.Snapshot){func(a *access.Snapshot) { a.ActorID = project }, func(a *access.Snapshot) { a.ProjectID = owner }, func(a *access.Snapshot) { a.Domain = access.DomainHosted }, func(a *access.Snapshot) { a.Selection.Connection.Revision++ }} {
		bad := a
		mutate(&bad)
		if _, e = s.Pin(ctx, modelproxy.Binding{Access: bad}); e == nil {
			t.Fatal("foreign authority accepted")
		}
	}
	route.Endpoint = "https://other.invalid/v1"
	if _, e = s.Resolve(ctx, b, route); e == nil {
		t.Fatal("caller endpoint accepted")
	}
	rev, e = s.Put(ctx, id, d, []byte("rotated-explicit-key"), false)
	if e != nil || rev != 2 {
		t.Fatal(rev, e)
	}
	if e = s.RequireLocalRevision(ctx, a); e == nil {
		t.Fatal("old revision remained current")
	}
	if _, e = New(ctx, db, key, project, project); e == nil {
		t.Fatal("owner changed")
	}
	reopened, e := New(ctx, db, key, owner, project)
	if e != nil {
		t.Fatal(e)
	}
	if got, e := reopened.Selection(ctx, id, "openai/model", "medium"); e != nil || got.Connection.Revision != 2 {
		t.Fatal(got, e)
	}
	if _, e = s.Put(ctx, id, d, []byte("rotated-explicit-key"), true); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Selection(ctx, id, "openai/model", "medium"); e == nil {
		t.Fatal("disabled credential accepted")
	}
}
func TestLocalOwnerRejectsUnsafeEndpointAndUnknownModel(t *testing.T) {
	for _, endpoint := range []string{"http://localhost/v1", "https://a/v1?secret=x", "https://user:secret@a/v1", "https://a/v1#fragment", "https://a/other"} {
		if validate(Definition{Provider: "openai", Endpoint: endpoint, Models: []Model{{ID: "model"}}}) == nil {
			t.Fatal(endpoint)
		}
	}
	d := Definition{Provider: "openai", Endpoint: "https://api.openai.com/v1", Models: []Model{{ID: "model", Efforts: []string{"medium"}}}}
	if supports(d, "openai/other", "") || supports(d, "openai/model", "unknown") {
		t.Fatal("unconfigured selection accepted")
	}
}
