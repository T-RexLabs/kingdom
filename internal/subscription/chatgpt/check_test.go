package chatgpt

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestCheckRegistrationIsReadOnlyAndRejectsWrongAuthority(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vault.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{7}, 32)
	v, err := NewVault(ctx, db, key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO chatgpt_registrations(id,owner_id,client_id,subject,revision,token_version,state) VALUES('registration','owner','oaiapp_fixture','subject',1,1,'ready')`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if err = CheckRegistration(ctx, ro, key, "owner", "registration"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key       []byte
		owner, id string
	}{{bytes.Repeat([]byte{8}, 32), "owner", "registration"}, {key, "foreign", "registration"}, {key, "owner", "missing"}} {
		if CheckRegistration(ctx, ro, test.key, test.owner, test.id) == nil {
			t.Fatal("invalid registration admitted")
		}
	}
	_ = v
	rw, _ := sql.Open("sqlite", path)
	defer rw.Close()
	for _, state := range []string{"pending", "exchanging", "refreshing", "reauth", "revoking", "signed_out"} {
		_, err = rw.Exec(`UPDATE chatgpt_registrations SET state=?`, state)
		if err != nil {
			t.Fatal(err)
		}
		if CheckRegistration(ctx, ro, key, "owner", "registration") == nil {
			t.Fatal("non-ready registration admitted", state)
		}
	}
	_, err = rw.Exec(`UPDATE chatgpt_registrations SET state='ready';INSERT INTO chatgpt_exports(registration_id,revision,transfer_id,target_host,state,payload) VALUES('registration',1,'transfer','foreign-host','pending',x'01')`)
	if err != nil {
		t.Fatal(err)
	}
	if CheckRegistration(ctx, ro, key, "owner", "registration") == nil {
		t.Fatal("fenced registration admitted")
	}
	empty, _ := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "empty.db")+"?mode=rwc")
	defer empty.Close()
	if CheckRegistration(ctx, empty, key, "owner", "registration") == nil {
		t.Fatal("missing schema admitted")
	}
	var count int
	_ = empty.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&count)
	if count != 0 {
		t.Fatal("check created schema")
	}
}
