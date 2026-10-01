package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/theme"
)

// An unknown-length upload must stop before validation can accumulate its
// base64 payload. This exercises the real middleware order, not just decoding.
func TestThemeUploadBudgetPrecedesValidation(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	const limit = int64(theme.MaxPackageBytes*4/3 + theme.MaxManifestBytes*6 + 1<<20)
	for _, path := range []string{"/themes/import", "/themes/validate", "/themes/validate-archive", "/themes/test-theme/source"} {
		t.Run(path, func(t *testing.T) {
			payload := &themeBudgetReader{remaining: limit + 1<<20}
			method := http.MethodPost
			if strings.HasSuffix(path, "/source") {
				method = http.MethodPut
			}
			req := httptest.NewRequest(method, "/api/v1"+path, io.MultiReader(strings.NewReader(`{"archive":"`), payload))
			req.ContentLength = -1
			req.RemoteAddr = "127.0.0.1:12345"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+apiTestOperatorCredential)
			req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, path, time.Now()))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != http.StatusRequestEntityTooLarge || payload.read > limit+1 {
				t.Fatalf("status=%d read=%d limit=%d body=%s", res.Code, payload.read, limit, res.Body.String())
			}
		})
	}
}

type themeBudgetReader struct{ remaining, read int64 }

func (r *themeBudgetReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.remaining)
	for i := range p[:n] {
		p[i] = 'A'
	}
	r.remaining -= n
	r.read += n
	return int(n), nil
}

func TestThemeLifecycleKeepsPreviewPrivateAndRevisionsPinned(t *testing.T) {
	store, err := theme.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ThemeStore: store})
	request := func(method, path, key string, body any, status int) []byte {
		t.Helper()
		var fields map[string]any
		if body != nil {
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
		}
		res := performJSONRequest(t, handler, requestSpec{method: method, path: "/api/v1" + path, idempotencyKey: key, body: fields})
		if res.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, res.Code, res.Body.String())
		}
		return res.Body.Bytes()
	}
	source := `{"schemaVersion":1,"resolverVersion":1,"id":"test-theme","version":"1.0.0","name":"Test Theme","base":{"id":"obsidian-court","version":"1.1.0"}}`
	draft := gen.ThemeDraftRequest{Source: source, Assets: map[string][]byte{}}
	var preview theme.Package
	if err := json.Unmarshal(request("POST", "/themes/validate", "validate-preview", draft, 200), &preview); err != nil {
		t.Fatal(err)
	}
	var catalog theme.Catalog
	if err := json.Unmarshal(request("GET", "/themes", "", nil, 200), &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Items) != 5 || catalog.WorkspaceID == "" {
		t.Fatalf("preview persisted or missing workspace: %#v", catalog)
	}
	var saved gen.ThemeSource
	if err := json.Unmarshal(request("PUT", "/themes/test-theme/source", "save-source", gen.ThemeSaveRequest{Source: source, Assets: map[string][]byte{}, ExpectedSourceDigest: ""}, 200), &saved); err != nil {
		t.Fatal(err)
	}
	request("PUT", "/themes/test-theme/source", "stale-save", gen.ThemeSaveRequest{Source: strings.Replace(source, "Test Theme", "Changed Theme", 1), Assets: map[string][]byte{}, ExpectedSourceDigest: "wrong"}, 409)
	request("POST", "/themes/test-theme/publish", "publish-theme", gen.PublishThemeJSONBody{ExpectedSourceDigest: saved.SourceDigest}, 201)
	revisionURL := "/themes/test-theme/revisions/" + preview.ResolvedDigest
	request("GET", revisionURL, "", nil, 200)
	var original gen.ThemeSource
	if err := json.Unmarshal(request("GET", "/themes/test-theme/source?revision="+preview.ResolvedDigest, "", nil, 200), &original); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(original.Source, `"base"`) || strings.Contains(original.Source, `"tokens"`) {
		t.Fatalf("revision source was flattened instead of retaining author intent: %s", original.Source)
	}
	request("GET", revisionURL+"/assets/undeclared", "", nil, 404)
	request("DELETE", "/themes/test-theme", "remove-theme", nil, http.StatusNoContent)
	request("GET", revisionURL, "", nil, 404)
	request("GET", "/themes/test-theme/source", "", nil, 200)
	request("DELETE", "/themes/test-theme/source", "stale-source-removal", gen.PublishThemeJSONBody{ExpectedSourceDigest: "wrong"}, 409)
	request("DELETE", "/themes/test-theme/source", "remove-source", gen.PublishThemeJSONBody{ExpectedSourceDigest: saved.SourceDigest}, 204)
	request("GET", "/themes/test-theme/source", "", nil, 404)
}

func TestThemeArchiveValidationDoesNotPublishOrSaveSource(t *testing.T) {
	store, err := theme.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ThemeStore: store})
	source := []byte(`{"schemaVersion":1,"resolverVersion":1,"id":"private-preview","version":"1.0.0","name":"Private Preview","base":{"id":"obsidian-court","version":"1.1.0"}}`)
	pkg, err := store.Resolver().Resolve(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := store.Resolver().ExportZIP(pkg)
	if err != nil {
		t.Fatal(err)
	}
	res := performJSONRequest(t, handler, requestSpec{method: "POST", path: "/api/v1/themes/validate-archive", idempotencyKey: "validate-archive", body: map[string]any{"archive": archive}})
	if res.Code != http.StatusOK {
		t.Fatalf("validate: %d %s", res.Code, res.Body.String())
	}
	var preview theme.Package
	if err := json.Unmarshal(res.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if preview.Manifest.ID != pkg.Manifest.ID || preview.ResolvedDigest != pkg.ResolvedDigest {
		t.Fatalf("wrong preview identity: %#v", preview)
	}
	catalog, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Items) != 5 || len(catalog.Sources) != 0 {
		t.Fatalf("validation wrote shared state: %#v", catalog)
	}
	if _, err := store.ReadSource(pkg.Manifest.ID); !errors.Is(err, theme.ErrNotFound) {
		t.Fatalf("validation saved source: %v", err)
	}
	invalid := performJSONRequest(t, handler, requestSpec{method: "POST", path: "/api/v1/themes/validate-archive", idempotencyKey: "invalid-archive", body: map[string]any{"archive": []byte("not a zip")}})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid archive: %d %s", invalid.Code, invalid.Body.String())
	}
}

func TestThemeStylingWarningsDoNotBlockPreviewSaveOrPublish(t *testing.T) {
	store, err := theme.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ThemeStore: store})
	const source = `{"schemaVersion":1,"resolverVersion":1,"id":"experimental-theme","version":"1.0.0","name":"Experimental Theme","base":{"id":"obsidian-court","version":"1.1.0"},"tokens":{"text.primary":"#0d1110"},"surfaces":{"radius":128.5,"borderWidth":0},"typography":{"bodyWeight":250.5,"headingWeight":900}}`
	request := func(method, path string, body map[string]any, status int) []byte {
		t.Helper()
		res := performJSONRequest(t, handler, requestSpec{method: method, path: "/api/v1" + path, idempotencyKey: method + path, body: body})
		if res.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, res.Code, res.Body.String())
		}
		return res.Body.Bytes()
	}
	assertPackage := func(raw []byte) theme.Package {
		t.Helper()
		var p theme.Package
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		if len(p.Warnings) == 0 || p.Warnings[0].Code != "contrast" {
			t.Fatalf("contrast warnings missing from API package: %s", raw)
		}
		if p.Manifest.Surfaces.BorderWidth == nil || *p.Manifest.Surfaces.BorderWidth != 0 || p.Manifest.Surfaces.Radius == nil || *p.Manifest.Surfaces.Radius != 128.5 || p.Manifest.Typography.BodyWeight != 250.5 {
			t.Fatalf("authored numeric styles did not survive serialization: %s", raw)
		}
		return p
	}
	preview := assertPackage(request("POST", "/themes/validate", map[string]any{"source": source, "assets": map[string][]byte{}}, http.StatusOK))
	var saved gen.ThemeSource
	if err := json.Unmarshal(request("PUT", "/themes/experimental-theme/source", map[string]any{"source": source, "assets": map[string][]byte{}, "expected_source_digest": ""}, http.StatusOK), &saved); err != nil {
		t.Fatal(err)
	}
	published := assertPackage(request("POST", "/themes/experimental-theme/publish", map[string]any{"expected_source_digest": saved.SourceDigest}, http.StatusCreated))
	assertPackage(request("GET", "/themes/experimental-theme/revisions/"+published.ResolvedDigest, nil, http.StatusOK))
	if published.ResolvedDigest != preview.ResolvedDigest {
		t.Fatal("preview and published theme differ")
	}
}
