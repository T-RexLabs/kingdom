package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
)

func TestAccessProviderAdmissionUsesConfiguredIdentity(t *testing.T) {
	f := setup(t)
	checkStatus(t, f.call("GET", BasePath+"/users/providers", "", "", false), 403)
	providers := f.call("GET", BasePath+"/users/providers", "", "", true)
	checkStatus(t, providers, 200)
	var choices []struct {
		ID             string `json:"id"`
		AccountIDLabel string `json:"account_id_label"`
	}
	if json.Unmarshal(providers.Body.Bytes(), &choices) != nil || len(choices) != 1 || choices[0].ID != "github" || choices[0].AccountIDLabel != "GitHub User ID" {
		t.Fatal("configured provider choices differ")
	}
	for _, body := range []string{
		`{"issuer":"https://github.com","subject":"42","display_name":"New"}`,
		`{"provider_id":"unconfigured","account_id":"42","display_name":"New"}`,
		`{"provider_id":"github","account_id":"alice","display_name":"New"}`,
		`{"provider_id":"github","account_id":"0042","display_name":"New"}`,
	} {
		checkStatus(t, f.call("POST", BasePath+"/users", body, uuid.NewString(), true), 400)
	}
	body := `{"provider_id":"github","account_id":"42","display_name":"New"}`
	checkStatus(t, f.call("POST", BasePath+"/users", body, uuid.NewString(), false), 403)
	key := uuid.NewString()
	first := f.call("POST", BasePath+"/users", body, key, true)
	checkStatus(t, first, 201)
	again := f.call("POST", BasePath+"/users", body, key, true)
	checkStatus(t, again, 201)
	if first.Body.String() != again.Body.String() {
		t.Fatal("admission replay changed identity")
	}
	user, err := f.catalog.ResolveIdentity(context.Background(), catalog.Identity{Issuer: "https://github.com", Subject: "42"})
	if err != nil || user.TeamOwner || user.DisplayName != "New" {
		t.Fatal("configured identity not preauthorized", err)
	}
	if _, err = f.catalog.ResolveIdentity(context.Background(), catalog.Identity{Issuer: "https://issuer.example", Subject: "42"}); err == nil {
		t.Fatal("browser selected another issuer")
	}
}

func TestProjectSummaryReportsCurrentProjectRole(t *testing.T) {
	f := setup(t)
	read := func(owner bool, want catalog.Role) {
		t.Helper()
		response := f.call("GET", BasePath+"/projects", "", "", owner)
		checkStatus(t, response, 200)
		var items []struct {
			ID   string       `json:"id"`
			Role catalog.Role `json:"role"`
		}
		if json.Unmarshal(response.Body.Bytes(), &items) != nil {
			t.Fatal("invalid project summary")
		}
		for _, item := range items {
			if item.ID == f.project.ID {
				if item.Role != want {
					t.Fatalf("current role %q want%q", item.Role, want)
				}
				return
			}
		}
		t.Fatal("project absent")
	}
	read(true, catalog.Owner)
	read(false, catalog.Member)
	if err := f.catalog.SetMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID, catalog.Owner); err != nil {
		t.Fatal(err)
	}
	read(false, catalog.Owner)
	checkStatus(t, f.call("GET", BasePath+"/users", "", "", false), 403)
	checkStatus(t, f.call("GET", BasePath+"/projects/"+f.project.ID+"/memberships", "", "", false), 200)
	if err := f.catalog.SetMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	read(false, catalog.Member)
}

func TestDirectoryAccountReferencesRemainTeamOwnerOnly(t *testing.T) {
	f := setup(t)
	first := f.call("POST", BasePath+"/users", `{"provider_id":"github","account_id":"42","display_name":"Duplicate"}`, uuid.NewString(), true)
	checkStatus(t, first, 201)
	if strings.Contains(first.Body.String(), "accounts") {
		t.Fatal("admission response disclosed directory account references")
	}
	directory := f.call("GET", BasePath+"/users", "", "", true)
	checkStatus(t, directory, 200)
	if !strings.Contains(directory.Body.String(), `"provider_id":"github","account_id":"42"`) || strings.Contains(directory.Body.String(), "https://github.com") {
		t.Fatal("directory references differ or issuer escaped")
	}
	if err := f.catalog.SetMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID, catalog.Owner); err != nil {
		t.Fatal(err)
	}
	checkStatus(t, f.call("GET", BasePath+"/users", "", "", false), 403)
	members := f.call("GET", BasePath+"/projects/"+f.project.ID+"/memberships", "", "", false)
	checkStatus(t, members, 200)
	if strings.Contains(members.Body.String(), "accounts") || strings.Contains(members.Body.String(), "account_id") || strings.Contains(members.Body.String(), "issuer") {
		t.Fatal("membership response disclosed directory identity")
	}
}

func TestProjectCreationReplayCannotRestoreOldProjectRole(t *testing.T) {
	f := setup(t)
	key := uuid.NewString()
	body := `{"display_name":"Role Replay"}`
	first := f.call("POST", BasePath+"/projects", body, key, true)
	checkStatus(t, first, 201)
	var project projectDTO
	if err := json.Unmarshal(first.Body.Bytes(), &project); err != nil {
		t.Fatal(err)
	}
	if err := f.catalog.SetMembership(context.Background(), f.owner.ID, project.ID, f.member.ID, catalog.Owner); err != nil {
		t.Fatal(err)
	}
	if err := f.catalog.SetMembership(context.Background(), f.member.ID, project.ID, f.owner.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	replay := f.call("POST", BasePath+"/projects", body, key, true)
	checkStatus(t, replay, 201)
	if err := json.Unmarshal(replay.Body.Bytes(), &project); err != nil || project.Role != catalog.Member {
		t.Fatal("creation replay restored old role", err)
	}
	if err := f.sessions.RevokeMembership(context.Background(), f.member.ID, project.ID, f.owner.ID); err != nil {
		t.Fatal(err)
	}
	checkStatus(t, f.call("POST", BasePath+"/projects", body, key, true), 403)
}

func TestProjectOwnerWithKnownIDKeepsExistingGrantAuthority(t *testing.T) {
	f := setup(t)
	if err := f.catalog.SetMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID, catalog.Owner); err != nil {
		t.Fatal(err)
	}
	third, err := f.catalog.CreateUser(context.Background(), f.owner.ID, catalog.Identity{Issuer: "https://github.com", Subject: "321"}, "Third")
	if err != nil {
		t.Fatal(err)
	}
	checkStatus(t, f.call("PUT", BasePath+"/projects/"+f.project.ID+"/memberships/"+third.ID, `{"role":"member"}`, uuid.NewString(), false), 204)
	checkStatus(t, f.call("GET", BasePath+"/users", "", "", false), 403)
	checkStatus(t, f.call("GET", BasePath+"/users/providers", "", "", false), 403)
}
