package repositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/publishedresult"
)

func TestPublicationLostReplyUsesOnlyExactReconciliation(t *testing.T) {
	for _, visible := range []bool{false, true} {
		t.Run(map[bool]string{false: "still-uncertain", true: "reconciled"}[visible], func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "catalog.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
			authority, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
			if err != nil {
				t.Fatal(err)
			}
			owner, err := authority.BootstrapOwner(ctx, identity, "Owner")
			if err != nil {
				t.Fatal(err)
			}
			project, err := authority.CreateProject(ctx, owner.ID, "Project")
			if err != nil {
				t.Fatal(err)
			}
			client := fixture(t)
			grant := Grant{ProjectID: project.ID, InstallationID: 2, RepositoryID: 3, Publish: true}
			client.grants[project.ID] = grant
			service, err := New(ctx, db, authority, client, unusedSource{}, resultFixture{})
			if err != nil {
				t.Fatal(err)
			}
			service.results = unchangedResult{}
			client.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
				t.Fatal("unchanged result contacted GitHub")
				return nil, nil
			})
			if result, err := service.Publish(ctx, owner.ID, project.ID, uuid.NewString(), PublishRequest{RunID: "unchanged", BaseBranch: "main", Title: "No Change"}); err != nil || result.State != "unchanged" {
				t.Fatalf("no changes: %+v %v", result, err)
			}
			service.results = resultFixture{}
			key := uuid.NewString()
			request := PublishRequest{RunID: "run", BaseBranch: "main", Title: "Result"}
			record, _, err := service.publication(ctx, owner.ID, project.ID, key, request, &publishSpec{Request: request, Grant: grant, BaseCommit: strings.Repeat("b", 40), Created: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			commit := strings.Repeat("a", 40)
			if _, err = service.updatePublication(ctx, owner.ID, project.ID, record.ID, commit, "", 0); err != nil {
				t.Fatal(err)
			}
			branch := "kingdom/" + record.ID
			marker := "<!-- kingdom-publication:" + record.ID + " -->"
			posts := 0
			var pr pullResponse
			pr.Number = 42
			pr.Body = marker
			pr.Head.Ref = branch
			pr.Head.SHA = commit
			pr.Head.Repo.ID = 3
			pr.Base.Ref = "main"
			pr.Base.Repo.ID = 3
			client.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/app/installations/2/access_tokens":
					var body struct {
						Permissions map[string]string `json:"permissions"`
					}
					json.NewDecoder(r.Body).Decode(&body)
					return response(201, map[string]any{"token": "private", "expires_at": time.Now().Add(time.Hour), "permissions": body.Permissions, "repositories": []any{map[string]any{"id": 3}}}), nil
				case "/repositories/3":
					return response(200, map[string]any{"id": 3, "name": "repo", "owner": map[string]any{"login": "owner"}}), nil
				case "/repos/owner/repo/git/ref/heads/" + branch:
					return response(200, map[string]any{"ref": "refs/heads/" + branch, "object": map[string]string{"sha": commit, "type": "commit"}}), nil
				case "/repos/owner/repo/pulls":
					if r.Method == http.MethodPost {
						posts++
						return response(503, map[string]string{"secret": "private"}), nil
					}
					if r.URL.Query().Get("head") != "owner:"+branch || r.URL.Query().Get("base") != "main" || r.URL.Query().Get("state") != "all" {
						t.Fatal("broad PR lookup")
					}
					if posts > 0 && visible {
						return response(200, []pullResponse{pr}), nil
					}
					return response(200, []pullResponse{}), nil
				default:
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
					return nil, nil
				}
			})
			if _, err = service.Publish(ctx, owner.ID, project.ID, key, request); err == nil || posts != 1 {
				t.Fatal("lost reply not retained", err, posts)
			}
			service, err = New(ctx, db, authority, client, unusedSource{}, resultFixture{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.Publish(ctx, owner.ID, project.ID, key, request)
			if err != nil || posts != 1 {
				t.Fatal("retried PR write", err, posts)
			}
			if visible {
				if result.State != "published" || result.PullRequest != 42 || result.URL != "https://github.com/owner/repo/pull/42" {
					t.Fatalf("reconcile %+v", result)
				}
			} else if result.State != "pending" || result.PullRequest != 0 {
				t.Fatal("invented PR absence")
			}
			replacement, err := authority.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "replacement"}, "Replacement")
			if err != nil {
				t.Fatal(err)
			}
			if err = authority.SetMembership(ctx, owner.ID, project.ID, replacement.ID, catalog.Owner); err != nil {
				t.Fatal(err)
			}
			if err = authority.RevokeMembership(ctx, replacement.ID, project.ID, owner.ID); err != nil {
				t.Fatal(err)
			}
			client.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
				t.Fatal("revoked actor contacted GitHub")
				return nil, nil
			})
			if _, err = service.Reconcile(ctx, owner.ID, project.ID, record.ID); err == nil {
				t.Fatal("revoked owner reconciled publication")
			}
			if _, err = service.Publications(ctx, owner.ID, project.ID, "run"); err == nil {
				t.Fatal("revoked owner listed publications")
			}

		})
	}
}
func TestPublicationRejectsForeignPullAndBranch(t *testing.T) {
	c := fixture(t)
	a := access{token: "token", grant: Grant{RepositoryID: 3}}
	a.repo.Owner.Login = "owner"
	a.repo.Name = "repo"
	c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		return response(200, map[string]any{"ref": "refs/heads/kingdom/foreign", "object": map[string]string{"sha": strings.Repeat("a", 40), "type": "commit"}}), nil
	})
	if err := c.ensureBranch(context.Background(), a, "kingdom/owned", strings.Repeat("a", 40)); err == nil {
		t.Fatal("foreign branch accepted")
	}
	var pull pullResponse
	pull.Number = 1
	pull.Body = "marker"
	pull.Head.Ref = "owned"
	pull.Head.SHA = strings.Repeat("a", 40)
	pull.Head.Repo.ID = 4
	pull.Base.Ref = "main"
	pull.Base.Repo.ID = 3
	if pull.matches(a, "owned", "main", pull.Head.SHA, "marker") {
		t.Fatal("foreign repository PR accepted")
	}
}

type unchangedResult struct{ resultFixture }

func (unchangedResult) Resolve(_ context.Context, _, _, run string) (publishedresult.ResultSnapshot, error) {
	return publishedresult.ResultSnapshot{RunID: run, SnapshotID: "snapshot", SourceSnapshotID: "source", ManifestSHA256: strings.Repeat("a", 64), Files: []publishedresult.ResultFile{}, Changes: []publishedresult.ResultChange{}}, nil
}
