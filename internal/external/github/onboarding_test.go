package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSetupClientConvertsManifestAtDeterministicEndpoint(t *testing.T) {
	pem := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/app-manifests/manifest-code/conversions" || r.Header.Get("X-GitHub-Api-Version") != APIVersion {
			t.Fatalf("unexpected conversion request %s %s headers=%v", r.Method, r.URL.String(), r.Header)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1, "client_id": "Iv1", "slug": "kingdom-issues", "name": "Editable App Name", "client_secret": "transient-client-secret", "pem": string(pem), "webhook_secret": "discard-me", "permissions": map[string]string{"metadata": "read", "issues": "read"}, "events": []string{}})
	}))
	defer server.Close()
	client, err := NewSetupClient(SetupClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	converted, err := client.ConvertManifest(context.Background(), "manifest-code")
	if err != nil {
		t.Fatal(err)
	}
	defer zeroBytes(converted.PrivateKey)
	defer zeroBytes(converted.ClientSecret)
	if converted.AppID != "1" || converted.ClientID != "Iv1" || converted.AppSlug != "kingdom-issues" || converted.Name != "Editable App Name" || string(converted.ClientSecret) != "transient-client-secret" {
		t.Fatalf("conversion=%#v", converted)
	}
}

func TestSetupClientProvesPaginatedUserInstallationAndRejectsSpoofing(t *testing.T) {
	expected := SetupInstallation{InstallationID: "2", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues"}
	valid := func() map[string]any {
		return map[string]any{"id": 2, "app_id": 1, "client_id": "Iv1", "app_slug": "kingdom-issues", "target_id": 10, "account": map[string]any{"id": 10, "login": "octocat"}, "permissions": map[string]string{"metadata": "read", "issues": "read"}, "events": []string{}, "suspended_at": nil}
	}
	for _, tc := range []struct {
		name       string
		mutateUser func(map[string]any)
		mutate     func(map[string]any)
		missing    bool
		paginate   bool
		wantErr    bool
	}{
		{name: "valid paginated", paginate: true},
		{name: "wrong app", mutate: func(v map[string]any) { v["app_id"] = 99 }, wantErr: true},
		{name: "wrong user visibility", missing: true, wantErr: true},
		{name: "wrong account", mutate: func(v map[string]any) { v["target_id"] = 11 }, wantErr: true},
		{name: "suspended", mutate: func(v map[string]any) { v["suspended_at"] = "2026-08-15T12:00:00Z" }, wantErr: true},
		{name: "extra permission", mutate: func(v map[string]any) {
			v["permissions"] = map[string]string{"metadata": "read", "issues": "read", "contents": "read"}
		}, wantErr: true},
		{name: "invalid authorizing user", mutateUser: func(v map[string]any) { v["id"] = 0 }, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer user-token" {
					t.Fatalf("missing user token: %v", r.Header)
				}
				switch r.URL.Path {
				case "/user":
					user := map[string]any{"id": 50, "login": "authorizing-user"}
					if tc.mutateUser != nil {
						tc.mutateUser(user)
					}
					_ = json.NewEncoder(w).Encode(user)
				case "/user/installations":
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					if tc.paginate && page == 1 {
						items := make([]map[string]any, 100)
						for i := range items {
							items[i] = map[string]any{"id": 1000 + i}
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 101, "installations": items})
						return
					}
					items := []map[string]any{}
					if !tc.missing {
						installation := valid()
						if tc.mutate != nil {
							tc.mutate(installation)
						}
						items = append(items, installation)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(items), "installations": items})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, _ := NewSetupClient(SetupClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true})
			proved, err := client.ProveUserInstallation(context.Background(), []byte("user-token"), expected)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProveUserInstallation()=%#v err=%v wantErr=%t", proved, err, tc.wantErr)
			}
			if err == nil && (proved.AccountID != "10" || proved.AccountLogin != "octocat") {
				t.Fatalf("proved identity=%#v", proved)
			}
		})
	}
}

func TestSetupClientIndependentlyValidatesAppKeyInstallationAndRepositories(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Fatalf("missing authorization for %s", r.URL.Path)
		}
		switch r.URL.Path {
		case "/app":
			_, _ = w.Write([]byte(`{"id":1,"client_id":"Iv1","slug":"kingdom-issues","permissions":{"metadata":"read","issues":"read"},"events":[]}`))
		case "/app/installations/2":
			_, _ = w.Write([]byte(`{"id":2,"app_id":1,"client_id":"Iv1","app_slug":"kingdom-issues","target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"},"events":[],"suspended_at":null}`))
		case "/app/installations/2/access_tokens":
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"installation-token","expires_at":%q,"permissions":{"metadata":"read","issues":"read"}}`, now.Add(time.Hour).Format(time.RFC3339))
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"total_count":1,"repositories":[{"owner":{"id":10}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, _ := NewSetupClient(SetupClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
	expected := SetupInstallation{InstallationID: "2", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues", AccountID: "10", AccountLogin: "octocat"}
	if err := client.ValidatePrivateKeyInstallation(context.Background(), testPrivateKey(t), expected); err != nil {
		t.Fatal(err)
	}
}
