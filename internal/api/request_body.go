package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/getkin/kin-openapi/openapi3"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/securefs"
)

type requestSpoolKey struct{}
type requestSpool struct {
	file   *os.File
	size   int64
	schema *openapi3.Schema
}

// spoolRequestBody gives validation, hashing and handlers independent readers of
// exactly the wire bytes. It owns the private file for the entire handler call.
func spoolRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Bootstrap has an empty-body contract and probes only one byte after
		// checking its loopback/origin authority; do not spool a rejected body.
		if r.Method == http.MethodPost && r.URL.Path == "/auth/session" {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := r.Context().Value(requestSpoolKey{}).(*requestSpool); ok {
			next.ServeHTTP(w, r)
			return
		}
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		file, err := os.CreateTemp("", "kingdom-request-*")
		if err != nil {
			writeCatalogError(w, 500, errorcatalog.CodeInternalError, "create request spool failed")
			return
		}
		defer os.Remove(file.Name())
		defer file.Close()
		size, err := securefs.Copy(r.Context(), file, r.Body)
		_ = r.Body.Close()
		if err != nil {
			writeCatalogError(w, 400, errorcatalog.CodePolicyViolation, "read request body")
			return
		}
		spool := &requestSpool{file: file, size: size}
		r = r.WithContext(context.WithValue(r.Context(), requestSpoolKey{}, spool))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(io.NewSectionReader(file, 0, size)), nil }
		r.Body, _ = r.GetBody()
		r.ContentLength = size
		next.ServeHTTP(w, r)
	})
}

// RequestText is a validated, decoded UTF-8 JSON string. Its bytes are private
// request-scoped staging, never durable authority. Open returns an independent
// replay reader; all readers must finish before RequestTextFields.Close.
type RequestText struct {
	file      *os.File
	offset    int64
	SizeBytes int64
	SHA256    string
}

func (t *RequestText) Open() (io.ReadCloser, error) {
	if t == nil || t.file == nil {
		return nil, fmt.Errorf("request text is unavailable")
	}
	if _, err := t.file.Stat(); err != nil {
		return nil, err
	}
	return io.NopCloser(io.NewSectionReader(t.file, t.offset, t.SizeBytes)), nil
}

// Trimmed provides strings.TrimSpace semantics without materializing content.
// It shares the original field's request lifetime and leaves its bytes/hash
// unchanged. Planner ingestion can persist this view using its own size/hash.
func (t *RequestText) Trimmed(ctx context.Context) (*RequestText, error) {
	reader, err := t.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	buffer := bufio.NewReader(reader)
	var offset, start, end int64
	found := false
	for count := 0; ; count++ {
		if count%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		r, size, err := buffer.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("invalid UTF-8 in staged request text")
		}
		if !unicode.IsSpace(r) {
			if !found {
				start = offset
				found = true
			}
			end = offset + int64(size)
		}
		offset += int64(size)
	}
	view := &RequestText{file: t.file, offset: t.offset + start, SizeBytes: end - start}
	hash := sha256.New()
	if _, err := securefs.Copy(ctx, hash, io.NewSectionReader(view.file, view.offset, view.SizeBytes)); err != nil {
		return nil, err
	}
	view.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return view, nil
}

type RequestTextFields map[string]*RequestText

func (fields RequestTextFields) Close() error {
	var first error
	for _, text := range fields {
		if text == nil || text.file == nil {
			continue
		}
		err := text.file.Close()
		removeErr := os.Remove(text.file.Name())
		if first == nil {
			if err != nil {
				first = err
			} else {
				first = removeErr
			}
		}
		text.file = nil
	}
	return first
}

// DecodeRequestWithText decodes strict typed metadata while staging designated
// JSON-pointer string fields incrementally. Those fields are omitted from dst:
// callers must consume the returned readers, never the generated string fields.
// Missing optional fields are absent from the returned map. Schema validation,
// duplicate keys, every escape, UTF-8, and trailing input are checked before use.
func DecodeRequestWithText(r *http.Request, dst any, pointers ...string) (RequestTextFields, error) {
	spool, ok := r.Context().Value(requestSpoolKey{}).(*requestSpool)
	if !ok || spool.schema == nil {
		return nil, fmt.Errorf("validated request spool is required")
	}
	fields := make(RequestTextFields, len(pointers))
	for _, pointer := range pointers {
		if !strings.HasPrefix(pointer, "/") {
			return nil, fmt.Errorf("request text field must be a JSON pointer")
		}
		fields[pointer] = nil
	}
	parser := &schemaJSONReader{reader: bufio.NewReader(io.NewSectionReader(spool.file, 0, spool.size)), textFields: fields, exactMetadata: true, ctx: r.Context()}
	value, err := parser.value(spool.schema, true)
	if err == nil {
		if _, e := parser.nonspace(); e != io.EOF {
			err = fmt.Errorf("unexpected data after JSON value")
		}
	}
	if err == nil {
		err = spool.schema.VisitJSON(value, openapi3.VisitAsRequest())
	}
	if err == nil {
		for pointer, text := range fields {
			if text == nil {
				delete(fields, pointer)
				continue
			}
			omitRequestText(value, strings.Split(strings.TrimPrefix(pointer, "/"), "/"))
		}
		var raw []byte
		raw, err = json.Marshal(value)
		if err == nil {
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			decoder.DisallowUnknownFields()
			err = decoder.Decode(dst)
		}
	}
	if err != nil {
		_ = fields.Close()
		return nil, err
	}
	return fields, nil
}

func omitRequestText(value any, path []string) {
	if len(path) == 0 {
		return
	}
	key := strings.ReplaceAll(strings.ReplaceAll(path[0], "~1", "/"), "~0", "~")
	if object, ok := value.(map[string]any); ok {
		if len(path) == 1 {
			delete(object, key)
		} else {
			omitRequestText(object[key], path[1:])
		}
	} else if array, ok := value.([]any); ok {
		index, err := strconv.Atoi(key)
		if err != nil || index < 0 || index >= len(array) {
			return
		}
		if len(path) == 1 {
			array[index] = ""
		} else {
			omitRequestText(array[index], path[1:])
		}
	}
}

func (p *schemaJSONReader) stageString() (string, uint64, error) {
	file, err := os.CreateTemp("", "kingdom-request-text-*")
	if err != nil {
		return "", 0, err
	}
	text := &RequestText{file: file}
	p.textFields[p.path] = text
	hash := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hash))
	value, length, err := p.readString(true, writer)
	if err == nil {
		err = writer.Flush()
	}
	if err != nil {
		return "", 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return "", 0, err
	}
	text.SizeBytes = info.Size()
	text.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return value, length, nil
}
