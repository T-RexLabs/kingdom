package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/getkin/kin-openapi/openapi3"
	"kingdom/internal/state"
)

type requestTextRepeatReader byte

func (r requestTextRepeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

func requestTextSchema(t *testing.T) *openapi3.Schema {
	t.Helper()
	var schema openapi3.Schema
	if err := json.Unmarshal([]byte(`{"type":"object","additionalProperties":false,"required":["title","content"],"properties":{"title":{"type":"string"},"content":{"type":"string","minLength":1},"nested":{"type":"object","properties":{"text":{"type":"string","nullable":true}}}}}`), &schema); err != nil {
		t.Fatal(err)
	}
	schema.Not = &openapi3.SchemaRef{Value: &openapi3.Schema{Required: []string{"missing_a", "missing_b"}}}
	return &schema
}

func TestCreateDecreeStreamsTextAndReplaysCompactResponse(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	const size = (17 << 20) + 1
	key := testIdempotencyKey(t, "streamed-decree", time.Now())
	request := func() *httptest.ResponseRecorder {
		body := io.MultiReader(strings.NewReader(`{"decree_title":"Streamed","decree_text":"`), io.LimitReader(requestTextRepeatReader('x'), size), strings.NewReader(`\n\ud83d\ude00"}`))
		req := httptest.NewRequest(http.MethodPost, "/api/v1/decrees", body)
		authenticateAPITestRequest(req)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	first := request()
	if first.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", first.Code, first.Body.String())
	}
	if first.Body.Len() > 1024 {
		t.Fatalf("mutation response echoed large text: %d bytes", first.Body.Len())
	}
	var result struct {
		DecreeID string `json:"decree_id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	expected := sha256.New()
	io.CopyN(expected, requestTextRepeatReader('x'), size)
	io.WriteString(expected, "\n😀")
	actual := sha256.New()
	if err := state.CopyDecreeText(context.Background(), db, result.DecreeID, actual); err != nil || !bytes.Equal(expected.Sum(nil), actual.Sum(nil)) {
		t.Fatalf("stored decree text changed: %v", err)
	}
	get := httptest.NewRequest(http.MethodGet, "/api/v1/decrees/"+result.DecreeID, nil)
	authenticateAPITestRequest(get)
	loaded := httptest.NewRecorder()
	handler.ServeHTTP(loaded, get)
	if loaded.Code != http.StatusOK {
		t.Fatalf("get decree status=%d body=%s", loaded.Code, loaded.Body.String())
	}
	var detail struct {
		Text string `json:"decree_text"`
	}
	if err := json.Unmarshal(loaded.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	actual.Reset()
	io.WriteString(actual, detail.Text)
	if !bytes.Equal(expected.Sum(nil), actual.Sum(nil)) {
		t.Fatal("GET decree read the empty inline column or changed streamed UTF-8 text")
	}
	replay := request()
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay=%d %s", replay.Code, replay.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM decree_text_refs WHERE decree_id=?`, result.DecreeID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("publication count=%d err=%v", count, err)
	}
}

func TestGetDecreeRejectsCorruptChunkAuthorityBeforeWritingSuccess(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	created := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", body: map[string]any{"decree_title": "Corruption", "decree_text": "canonical text"}, idempotencyKey: testIdempotencyKey(t, "corrupt-decree-get", time.Now())})
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d %s", created.Code, created.Body.String())
	}
	var value struct {
		ID string `json:"decree_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TRIGGER request_content_chunks_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE request_content_chunks SET content=X'626164' WHERE content_id=(SELECT content_id FROM decree_text_refs WHERE decree_id=?)`, value.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE decrees SET decree_text='misleading inline fallback' WHERE decree_id=?`, value.ID); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/decrees/"+value.ID, nil)
	authenticateAPITestRequest(request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "misleading inline fallback") || strings.Contains(response.Body.String(), `"decree_text"`) {
		t.Fatalf("corrupt decree response=%d %s", response.Code, response.Body.String())
	}
}

func TestDecodeRequestWithTextUsesBoundedMemoryAndExactDecodedBytes(t *testing.T) {
	const count = (17 << 20) + 1
	suffix := "\n😀�end"
	expectedHash := sha256.New()
	io.CopyN(expectedHash, requestTextRepeatReader('x'), count)
	io.WriteString(expectedHash, suffix)
	schema := requestTextSchema(t)
	body := io.MultiReader(strings.NewReader(`{"title":"exact metadata","content":"`), io.LimitReader(requestTextRepeatReader('x'), count), strings.NewReader(`\n\ud83d\ude00\ud800end","nested":{"text":null}}`))
	req := httptest.NewRequest(http.MethodPost, "/test", body)
	req.Header.Set("Content-Type", "application/json")
	var path string
	handler := spoolRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contract := &openapi3.RequestBody{Required: true, Content: openapi3.Content{"application/json": &openapi3.MediaType{Schema: &openapi3.SchemaRef{Value: schema}}}}
		if err := validateSpooledJSON(r, contract); err != nil {
			t.Fatal(err)
		}
		var metadata struct {
			Title   string `json:"title"`
			Content string `json:"content"`
			Nested  struct {
				Text *string `json:"text"`
			} `json:"nested"`
		}
		fields, err := DecodeRequestWithText(r, &metadata, "/content", "/nested/text")
		if err != nil {
			t.Fatal(err)
		}
		defer fields.Close()
		if metadata.Title != "exact metadata" || metadata.Content != "" || metadata.Nested.Text != nil {
			t.Fatalf("metadata=%+v", metadata)
		}
		if len(fields) != 1 {
			t.Fatalf("optional null field captured: %#v", fields)
		}
		text := fields["/content"]
		path = text.file.Name()
		if text.SizeBytes != count+int64(len(suffix)) || text.SHA256 != hex.EncodeToString(expectedHash.Sum(nil)) {
			t.Fatalf("text descriptor: %+v", text)
		}
		for i := 0; i < 2; i++ {
			reader, err := text.Open()
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			_, err = io.Copy(h, reader)
			reader.Close()
			if err != nil || !bytes.Equal(h.Sum(nil), expectedHash.Sum(nil)) {
				t.Fatalf("replay %d: %v", i, err)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	runtime.ReadMemStats(&after)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d", response.Code)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("17 MiB text materialized in memory: allocated=%d", allocated)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("decoded text spool leaked: %v", err)
	}
}

func TestStreamingJSONRejectsInvalidTailAfterCompaction(t *testing.T) {
	schema := openapi3.NewStringSchema()
	for _, tail := range []string{`\q"`, `\u+123"`, `\u12z4"`, "\xff\"", "\x01\"", `\u123`, ""} {
		parser := &schemaJSONReader{reader: bufio.NewReader(io.MultiReader(strings.NewReader(`"`), io.LimitReader(requestTextRepeatReader('x'), 8192), strings.NewReader(tail)))}
		if _, err := parser.value(schema, true); err == nil {
			t.Fatalf("invalid tail %q passed after diagnostic prefix was discarded", tail)
		}
	}
	for _, raw := range []string{`{"content":"a","\u0063ontent":"b"}`, `{"nested":{"text":"a","text":"b"}}`} {
		parser := &schemaJSONReader{reader: bufio.NewReader(strings.NewReader(raw))}
		if _, err := parser.value(requestTextSchema(t), true); err == nil {
			t.Fatalf("duplicate property accepted: %s", raw)
		}
	}
}

func FuzzStreamingJSONStringMatchesDecodedBytes(f *testing.F) {
	for _, text := range []string{"", "plain", "quote\" slash\\", "\x00\b\f\n\r\t", "é中😀�", "invalid UTF-8: \xff", strings.Repeat("x", 8192) + "\n😀"} {
		f.Add(text)
	}
	f.Fuzz(func(t *testing.T, input string) {
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var expected string
		if err := json.Unmarshal(raw, &expected); err != nil {
			t.Fatal(err)
		}
		parser := &schemaJSONReader{reader: bufio.NewReader(bytes.NewReader(raw[1:]))}
		var output bytes.Buffer
		preview, length, err := parser.readString(true, &output)
		if err != nil || output.String() != expected || length != uint64(utf8.RuneCountInString(expected)) {
			t.Fatalf("decoded text differs: length=%d err=%v", length, err)
		}
		if length <= 4096 && preview != expected || length > 4096 && preview != "" {
			t.Fatal("diagnostic prefix is inconsistent")
		}
		if _, err := parser.reader.ReadByte(); err != io.EOF {
			t.Fatalf("string parser did not consume exact input: %v", err)
		}
	})
}

func TestRequestTextTrimmedPreservesPlannerWhitespaceSemantics(t *testing.T) {
	for _, input := range []string{"", " \n\t\u2003", "\t\u2003body\u00a0\n", "first\n second", "\u0085😀\u3000"} {
		file, err := os.CreateTemp(t.TempDir(), "text-*")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, input); err != nil {
			file.Close()
			t.Fatal(err)
		}
		field := &RequestText{file: file, SizeBytes: int64(len(input))}
		view, err := field.Trimmed(context.Background())
		if err != nil {
			file.Close()
			t.Fatal(err)
		}
		reader, err := view.Open()
		if err != nil {
			file.Close()
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		reader.Close()
		file.Close()
		want := strings.TrimSpace(input)
		hash := sha256.Sum256([]byte(want))
		if err != nil || string(body) != want || view.SizeBytes != int64(len(want)) || view.SHA256 != hex.EncodeToString(hash[:]) {
			t.Fatalf("input=%q body=%q descriptor=%+v err=%v", input, body, view, err)
		}
	}
}

type authBodyProbe struct {
	reads int
	t     *testing.T
}

func (b *authBodyProbe) Read(p []byte) (int, error) {
	b.reads++
	if len(p) > 1 {
		b.t.Fatalf("auth read requested %d bytes", len(p))
	}
	p[0] = 'x'
	return 1, nil
}

func TestAuthBootstrapProbesOneByteOnlyAfterOriginAuthority(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	for _, validOrigin := range []bool{false, true} {
		body := &authBodyProbe{t: t}
		req := httptest.NewRequest(http.MethodPost, "http://localhost/api/v1/auth/session", body)
		req.ContentLength = -1
		req.RemoteAddr = "127.0.0.1:49152"
		req.Header.Set(browserBootstrapHeader, "1")
		if validOrigin {
			req.Header.Set("Origin", "http://localhost")
		} else {
			req.Header.Set("Origin", "https://outside.example")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if validOrigin {
			if response.Code != 400 || body.reads != 1 {
				t.Fatalf("valid origin status=%d reads=%d", response.Code, body.reads)
			}
		} else if response.Code != 403 || body.reads != 0 {
			t.Fatalf("invalid origin status=%d reads=%d", response.Code, body.reads)
		}
		if len(response.Result().Cookies()) != 0 {
			t.Fatal("nonempty or unauthorized body created a session")
		}
	}
}
