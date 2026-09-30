package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestOpenAPIRejectsLargeForbiddenPropertiesWithBoundedAllocations(t *testing.T) {
	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatal(err)
	}
	const size = (17 << 20) + 1
	for _, tc := range []struct{ name, prefix, suffix string }{
		{"string value", `{"work_order_title":"test","work_order_text":"valid","unexpected":"`, `"}`},
		{"object value", `{"work_order_title":"test","work_order_text":"valid","unexpected":{"nested":"`, `"}}`},
		{"property name", `{"work_order_title":"test","work_order_text":"valid","work_order_te`, `":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := io.MultiReader(strings.NewReader(tc.prefix), io.LimitReader(requestTextRepeatReader('x'), size), strings.NewReader(tc.suffix))
			req := httptest.NewRequest(http.MethodPost, "/work-orders", body)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "forbidden-property", time.Now()))
			req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
			called := false
			handler := validator(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			response := httptest.NewRecorder()
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			handler.ServeHTTP(response, req)
			runtime.ReadMemStats(&after)
			if response.Code != http.StatusBadRequest || called {
				t.Fatalf("status=%d called=%t", response.Code, called)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
				t.Fatalf("forbidden %s materialized: allocated=%d response_bytes=%d", tc.name, allocated, response.Body.Len())
			}
			t.Logf("allocated=%d response_bytes=%d", after.TotalAlloc-before.TotalAlloc, response.Body.Len())
			if response.Body.Len() > 4096 {
				t.Fatalf("error echoed forbidden input: %d bytes", response.Body.Len())
			}
		})
	}
}

type forbiddenValueProbe struct{ reads int }

func (p *forbiddenValueProbe) Read([]byte) (int, error) { p.reads++; return 0, io.ErrUnexpectedEOF }

func TestStreamingJSONRejectsForbiddenPropertyBeforeReadingValue(t *testing.T) {
	for _, tc := range []struct{ schema, prefix string }{
		{`{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}`, `{"unexpected":`},
		{`{"allOf":[{"type":"object","additionalProperties":false,"properties":{"payload":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}}}]}`, `{"payload":{"unexpected":`},
		{`{"oneOf":[{"type":"object","additionalProperties":false,"properties":{"payload":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}}},{"type":"object","additionalProperties":false,"properties":{"other":{"type":"string"}}}]}`, `{"payload":{"unexpected":`},
		{`{"anyOf":[{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"}}},{"type":"object","additionalProperties":false,"properties":{"b":{"type":"string"}}}]}`, `{"unexpected":`},
		{`{"type":"object","additionalProperties":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}}`, `{"dynamic":{"unexpected":`},
	} {
		schema := propertyTestSchema(t, tc.schema)
		probe := &forbiddenValueProbe{}
		parser := &schemaJSONReader{reader: bufio.NewReader(io.MultiReader(strings.NewReader(tc.prefix), probe))}
		if _, err := parser.value(schema, true); err == nil {
			t.Fatalf("forbidden property passed: %s", tc.schema)
		}
		if probe.reads != 0 {
			t.Fatalf("forbidden value read %d times: %s", probe.reads, tc.schema)
		}
	}
}

func propertyTestSchema(t *testing.T, raw string) *openapi3.Schema {
	t.Helper()
	var schema openapi3.Schema
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		t.Fatal(err)
	}
	return &schema
}

func parsePropertyTest(schema *openapi3.Schema, raw string) (any, error) {
	parser := &schemaJSONReader{reader: bufio.NewReader(strings.NewReader(raw))}
	value, err := parser.value(schema, true)
	if err == nil {
		if _, tailErr := parser.nonspace(); tailErr != io.EOF {
			err = fmt.Errorf("trailing JSON data")
		}
	}
	if err == nil {
		err = schema.VisitJSON(value, openapi3.VisitAsRequest())
	}
	return value, err
}

func TestStreamingPropertiesPreserveCompositionAndValueConstraints(t *testing.T) {
	for _, tc := range []struct {
		name, schema string
		bodies       []string
	}{
		{"allOf intersection", `{"allOf":[{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"},"common":{"type":"string"}}},{"type":"object","additionalProperties":false,"properties":{"b":{"type":"string"},"common":{"type":"string"}}}]}`, []string{`{}`, `{"common":"x"}`, `{"a":"x"}`, `{"b":"x"}`, `{"common":3}`}},
		{"allOf cannot extend closed siblings", `{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"}},"allOf":[{"properties":{"b":{"type":"string"}}}]}`, []string{`{"a":"x"}`, `{"b":"x"}`, `{"a":2}`}},
		{"oneOf discriminator and nested shapes", `{"oneOf":[{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["a"]},"payload":{"type":"object","additionalProperties":false,"required":["alpha"],"properties":{"alpha":{"type":"string","pattern":"^a+$"}}}}},{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["b"]},"payload":{"type":"object","additionalProperties":false,"required":["beta"],"properties":{"beta":{"type":"integer","minimum":1}}}}}]}`, []string{`{"kind":"a","payload":{"alpha":"aaa"}}`, `{"kind":"b","payload":{"beta":1}}`, `{"kind":"a","payload":{"beta":1}}`, `{"kind":"b","payload":{"beta":0}}`, `{"kind":"a","payload":{"alpha":"` + strings.Repeat("a", 8192) + `b"}}`, `{"kind":"a","payload":{"unknown":"x"}}`}},
		{"oneOf identical child restrictions", `{"oneOf":[{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["a"]},"payload":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}}},{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["b"]},"payload":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}}}}]}`, []string{`{"kind":"a","payload":{"text":"valid"}}`, `{"kind":"b","payload":{"text":"valid"}}`, `{"kind":"c","payload":{"text":"invalid"}}`}},
		{"anyOf open branch", `{"anyOf":[{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"}}},{"type":"object","additionalProperties":true}]}`, []string{`{"dynamic":"allowed"}`, `{"a":3}`, `{"dynamic":{"deep":true}}`}},
		{"typed additional properties", `{"type":"object","additionalProperties":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string","pattern":"^ok$"}}}}`, []string{`{"dynamic":{"text":"ok"}}`, `{"dynamic":{"text":"no"}}`, `{"dynamic":{"unknown":"no"}}`}},
		{"not does not close the parent", `{"type":"object","not":{"additionalProperties":false,"properties":{"closed":{"type":"string"}}}}`, []string{`{"dynamic":"valid"}`, `{"closed":"invalid"}`, `{"closed":1}`}},
		{"enum and pattern", `{"type":"object","additionalProperties":false,"properties":{"mode":{"type":"string","enum":["safe"]},"text":{"type":"string","pattern":"^a+$"}}}`, []string{`{"mode":"safe","text":"aaa"}`, `{"mode":"unsafe"}`, `{"text":"` + strings.Repeat("a", 8192) + `b"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := propertyTestSchema(t, tc.schema)
			for _, body := range tc.bodies {
				var exact any
				decoder := json.NewDecoder(strings.NewReader(body))
				decoder.UseNumber()
				if err := decoder.Decode(&exact); err != nil {
					t.Fatal(err)
				}
				want := schema.VisitJSON(exact, openapi3.VisitAsRequest())
				_, got := parsePropertyTest(schema, body)
				if (want == nil) != (got == nil) {
					t.Fatalf("streaming validation differs from canonical schema: want=%v got=%v", want, got)
				}
			}
		})
	}
}

func TestStreamingPropertyNamesValidateExactDecodedNamesAndTails(t *testing.T) {
	schema := propertyTestSchema(t, `{"type":"object","additionalProperties":false,"properties":{"":{"type":"string"},"text":{"type":"string"},"text_more":{"type":"string"},"é😀":{"type":"string"},"�":{"type":"string"}}}`)
	longName := strings.Repeat("k", 8192)
	schema.Properties[longName] = &openapi3.SchemaRef{Value: openapi3.NewStringSchema()}
	for _, body := range []string{`{"":"ok"}`, `{"\u0074ext":"ok","text_more":"ok"}`, `{"é\ud83d\ude00":"ok"}`, `{"\ud800":"ok"}`, `{"` + longName + `":"ok"}`} {
		if _, err := parsePropertyTest(schema, body); err != nil {
			t.Fatalf("valid property name rejected: %v", err)
		}
	}
	for _, body := range []string{`{"text":"a","\u0074ext":"b"}`, `{"text`, `{"text\q":"bad"}`, `{"é\ud83d\ude00":"bad\q"}`, `{"text":"` + strings.Repeat("x", 8192) + `\q"}`, `{"text":"ok"} trailing`, "{\"text\xff\":\"bad\"}"} {
		if _, err := parsePropertyTest(schema, body); err == nil {
			t.Fatal("duplicate property or malformed tail was accepted")
		}
	}
}

func TestOpenAPISecurityValidationDoesNotMaterializeValidLargeBody(t *testing.T) {
	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatal(err)
	}
	const size = (17 << 20) + 1
	prefix, suffix := `{"work_order_title":"test","work_order_text":"`, `"}`
	req := httptest.NewRequest(http.MethodPost, "/work-orders", io.MultiReader(strings.NewReader(prefix), io.LimitReader(requestTextRepeatReader('x'), size), strings.NewReader(suffix)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "valid-large-property", time.Now()))
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	var read int64
	handler := validator(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Value(requestSpoolKey{}).(*requestSpool); !ok {
			t.Fatal("validated request lost disk spool")
		}
		var err error
		read, err = io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	runtime.ReadMemStats(&after)
	if response.Code != http.StatusNoContent || read != int64(len(prefix)+size+len(suffix)) {
		t.Fatalf("status=%d read=%d", response.Code, read)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("security validation materialized body: allocated=%d", allocated)
	}
}

func TestRequestAuthorityAndHeaderRejectionPrecedeBodySpooling(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	bootstrap := httptest.NewRequest(http.MethodPost, "http://localhost/api/v1/auth/session", nil)
	bootstrap.RemoteAddr = "127.0.0.1:49152"
	bootstrap.Header.Set("Origin", "http://localhost")
	bootstrap.Header.Set(browserBootstrapHeader, "1")
	bootResponse := httptest.NewRecorder()
	handler.ServeHTTP(bootResponse, bootstrap)
	cookies := bootResponse.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("bootstrap status=%d cookies=%d", bootResponse.Code, len(cookies))
	}
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	for _, tc := range []struct {
		name   string
		status int
		setup  func(*http.Request)
	}{
		{"unauthenticated", http.StatusUnauthorized, func(*http.Request) {}},
		{"invalid bearer", http.StatusUnauthorized, func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid") }},
		{"insecure remote bearer", http.StatusForbidden, func(r *http.Request) { authenticateAPITestRequest(r); r.RemoteAddr = "203.0.113.7:49152" }},
		{"invalid CSRF", http.StatusForbidden, func(r *http.Request) {
			r.AddCookie(cookies[0])
			r.Header.Set("Origin", "http://localhost")
			r.Header.Set("X-CSRF-Token", "invalid")
		}},
		{"missing idempotency header", http.StatusBadRequest, func(r *http.Request) { authenticateAPITestRequest(r); r.Header.Del(idempotencyKeyHeader) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &forbiddenValueProbe{}
			req := httptest.NewRequest(http.MethodPost, "http://localhost/api/v1/work-orders", probe)
			req.RemoteAddr = "127.0.0.1:49152"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "body-auth-order", time.Now()))
			tc.setup(req)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != tc.status || probe.reads != 0 {
				t.Fatalf("status=%d want=%d body_reads=%d", response.Code, tc.status, probe.reads)
			}
			files, err := os.ReadDir(dir)
			if err != nil || len(files) != 0 {
				t.Fatalf("rejected request created staging: %v %v", files, err)
			}
		})
	}
}

func FuzzStreamingPropertyValidationMatchesSchema(f *testing.F) {
	schemaJSON := []string{
		`{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"},"text_more":{"type":"string"},"é😀":{"type":"string"},"�":{"type":"string"}}}`,
		`{"oneOf":[{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["a"]},"payload":{"type":"object","additionalProperties":false,"properties":{"alpha":{"type":"string","pattern":"^a+$"}}}}},{"type":"object","additionalProperties":false,"required":["kind","payload"],"properties":{"kind":{"enum":["b"]},"payload":{"type":"object","additionalProperties":false,"properties":{"beta":{"type":"string"}}}}}]}`,
		`{"allOf":[{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"},"a":{"type":"string"}}},{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"},"b":{"type":"string"}}}]}`,
		`{"anyOf":[{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string"}}},{"type":"object","additionalProperties":true}]}`,
		`{"type":"object","additionalProperties":{"type":"object","additionalProperties":false,"properties":{"text":{"type":"string","pattern":"^ok$"}}}}`,
	}
	schemas := make([]*openapi3.Schema, len(schemaJSON))
	for i, raw := range schemaJSON {
		var schema openapi3.Schema
		if err := json.Unmarshal([]byte(raw), &schema); err != nil {
			f.Fatal(err)
		}
		schemas[i] = &schema
	}
	for i := range schemas {
		for _, name := range []string{"text", "text_more", "unknown", "alpha", "beta", "é😀", "�", ""} {
			f.Add(name, "aaa", uint8(i))
		}
	}
	f.Fuzz(func(t *testing.T, name, value string, variant uint8) {
		index := int(variant) % len(schemas)
		var body any = map[string]any{name: value}
		if index == 1 {
			kind := "a"
			if variant%2 == 0 {
				kind = "b"
			}
			body = map[string]any{"kind": kind, "payload": body}
		}
		if index == 4 {
			body = map[string]any{"dynamic": body}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		// JSON marshaling normalizes invalid Go UTF-8. Compare both validators
		// against those same wire bytes, never against the pre-encoding map.
		var exact any
		if err := json.Unmarshal(raw, &exact); err != nil {
			t.Fatal(err)
		}
		want := schemas[index].VisitJSON(exact, openapi3.VisitAsRequest())
		_, got := parsePropertyTest(schemas[index], string(raw))
		if (want == nil) != (got == nil) {
			t.Fatalf("schema=%d streaming validity differs: want=%v got=%v", index, want, got)
		}
	})
}
