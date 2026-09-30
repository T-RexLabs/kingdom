package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	middleware "github.com/oapi-codegen/nethttp-middleware"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
)

func NewOpenAPIRequestValidator() (func(http.Handler) http.Handler, error) {
	swagger, err := loadOpenAPISpec()
	if err != nil {
		return nil, fmt.Errorf("load OpenAPI spec: %w", err)
	}

	swagger.Servers = nil

	validatorOpts := &middleware.Options{
		Options: openapi3filter.Options{
			// kin-openapi's body validator calls ReadAll and re-encodes defaults.
			// Validate the immutable spool below instead, without rewriting bytes
			// before idempotency hashes them.
			ExcludeRequestBody: true,
			AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
				if input == nil || input.SecurityScheme == nil || input.RequestValidationInput == nil || input.RequestValidationInput.Request == nil {
					return fmt.Errorf("missing authentication context")
				}

				if _, ok := authenticatedPrincipal(ctx); !ok {
					return fmt.Errorf("missing authenticated principal")
				}

				return nil
			},
		},
		ErrorHandlerWithOpts: func(ctx context.Context, err error, w http.ResponseWriter, r *http.Request, opts middleware.ErrorHandlerOpts) {
			statusCode := opts.StatusCode
			code := validationErrorCodeForStatus(statusCode)
			correlationID := "openapi-validator"
			if route := opts.MatchedRoute; route != nil && route.Route != nil {
				correlationID = "openapi-validator:" + route.Route.Path
			}

			writeCanonicalError(
				w,
				statusCode,
				code,
				err.Error(),
				correlationID,
				map[string]any{"path": r.URL.Path, "method": r.Method},
			)
		},
		DoNotValidateServers: true,
	}

	router, err := gorillamux.NewRouter(swagger)
	if err != nil {
		return nil, err
	}
	validateHeaders := middleware.OapiRequestValidatorWithOptions(swagger, validatorOpts)
	return func(next http.Handler) http.Handler {
		validateBody := spoolRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route, _, err := router.FindRoute(r)
			if err == nil && route.Operation.RequestBody != nil {
				err = validateSpooledJSON(r, route.Operation.RequestBody.Value)
			}
			if err != nil {
				correlationID := "openapi-validator"
				if route != nil {
					correlationID += ":" + route.Path
				}
				writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), correlationID, map[string]any{"path": r.URL.Path, "method": r.Method})
				return
			}
			next.ServeHTTP(w, r)
		}))
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// kin-openapi also ReadAlls bodies while checking security, even
			// with ExcludeRequestBody. Our authentication callback consults only
			// the already-verified principal, so validate request metadata with
			// a bodyless clone. Only after those checks may the real body reach
			// the private spool. Preserve any parameter defaults on the clone.
			headersOnly := r.Clone(r.Context())
			headersOnly.Body = http.NoBody
			headersOnly.GetBody = nil
			validateHeaders(http.HandlerFunc(func(w http.ResponseWriter, validated *http.Request) {
				validated.Body = r.Body
				validated.GetBody = r.GetBody
				validateBody.ServeHTTP(w, validated)
			})).ServeHTTP(w, headersOnly)
		})
	}, nil
}

func validateSpooledJSON(r *http.Request, body *openapi3.RequestBody) error {
	spool, ok := r.Context().Value(requestSpoolKey{}).(*requestSpool)
	if !ok || spool.size == 0 {
		if body.Required {
			return fmt.Errorf("request body is required")
		}
		return nil
	}
	media := body.Content.Get(r.Header.Get("Content-Type"))
	if media == nil {
		return fmt.Errorf("unexpected request Content-Type")
	}
	if media.Schema == nil {
		return nil
	}
	parser := &schemaJSONReader{reader: bufio.NewReader(io.NewSectionReader(spool.file, 0, spool.size)), ctx: r.Context()}
	value, err := parser.value(media.Schema.Value, true)
	if err != nil {
		return err
	}
	if _, err := parser.nonspace(); err != io.EOF {
		return fmt.Errorf("unexpected data after JSON value")
	}
	if err := media.Schema.Value.VisitJSON(value, openapi3.VisitAsRequest()); err != nil {
		return err
	}
	spool.schema = media.Schema.Value
	return nil
}

// schemaJSONReader retains the typed structure, but consumes plain content
// strings incrementally. Length-only strings are represented by a schema-valid
// witness after checking their actual Unicode length. Constrained strings and
// values under equality/composition constraints retain their exact values.
// The original request remains in the spool for typed handler decoding/hash.
type schemaJSONReader struct {
	ctx           context.Context
	reader        *bufio.Reader
	depth         int
	path          string
	textFields    RequestTextFields
	exactMetadata bool
}

func (p *schemaJSONReader) nonspace() (byte, error) {
	for {
		c, err := p.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if c != ' ' && c != '\n' && c != '\r' && c != '\t' {
			return c, nil
		}
	}
}

func (p *schemaJSONReader) value(schema *openapi3.Schema, compress bool) (any, error) {
	if p.ctx != nil {
		if err := p.ctx.Err(); err != nil {
			return nil, err
		}
	}
	// Match encoding/json's syntax nesting guard; this is independent of body
	// size and prevents malformed control structures from exhausting the stack.
	if p.depth >= 10000 {
		return nil, fmt.Errorf("JSON exceeds maximum nesting depth")
	}
	p.depth++
	defer func() { p.depth-- }()
	c, err := p.nonspace()
	if err != nil {
		return nil, err
	}
	_, selectedText := p.textFields[p.path]
	if selectedText && c != '"' && !(c == 'n' && schema != nil && schema.Nullable) {
		return nil, fmt.Errorf("request text field %q must be a string", p.path)
	}
	if schema == nil || len(schema.Enum) > 0 || len(schema.OneOf)+len(schema.AnyOf)+len(schema.AllOf) > 0 || schema.Not != nil && !requiredOnlyConstraint(schema.Not.Value) || schema.UniqueItems {
		compress = false
	}
	switch c {
	case '"':
		plain := compress && schema.Type.Is("string") && schema.Format == "" && schema.Pattern == ""
		compact := plain && !p.exactMetadata
		var text string
		var length uint64
		if selectedText {
			if !plain {
				return nil, fmt.Errorf("request text field %q has non-streamable schema constraints", p.path)
			}
			compact = true
			text, length, err = p.stageString()
		} else {
			text, length, err = p.stringValue(compact)
		}
		if err != nil {
			return nil, err
		}
		if plain {
			if length < schema.MinLength || (schema.MaxLength != nil && length > *schema.MaxLength) {
				return nil, fmt.Errorf("JSON string length violates schema")
			}
			if compact && length > 4096 {
				return strings.Repeat("x", int(schema.MinLength)), nil
			}
		}
		return text, nil
	case '{':
		keys := requestObjectKeys(schema, make(map[*openapi3.Schema]bool))
		object := make(map[string]any)
		c, err = p.nonspace()
		if err != nil {
			return nil, err
		}
		if c == '}' {
			return object, nil
		}
		_ = p.reader.UnreadByte()
		for {
			c, err = p.nonspace()
			if err != nil || c != '"' {
				return nil, fmt.Errorf("expected JSON object key")
			}
			key, err := p.propertyName(keys)
			if err != nil {
				return nil, err
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON property %q", key)
			}
			c, err = p.nonspace()
			if err != nil || c != ':' {
				return nil, fmt.Errorf("expected JSON colon")
			}
			child := requestPropertySchema(schema, key, make(map[*openapi3.Schema]bool))
			parent := p.path
			p.path += "/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
			value, err := p.value(child, compress)
			p.path = parent
			if err != nil {
				return nil, err
			}
			object[key] = value
			c, err = p.nonspace()
			if err != nil {
				return nil, err
			}
			if c == '}' {
				return object, nil
			}
			if c != ',' {
				return nil, fmt.Errorf("expected JSON object separator")
			}
		}
	case '[':
		array := make([]any, 0)
		var child *openapi3.Schema
		if schema != nil && schema.Items != nil {
			child = schema.Items.Value
		}
		c, err = p.nonspace()
		if err != nil {
			return nil, err
		}
		if c == ']' {
			return array, nil
		}
		_ = p.reader.UnreadByte()
		for {
			parent := p.path
			p.path += "/" + strconv.Itoa(len(array))
			value, err := p.value(child, compress)
			p.path = parent
			if err != nil {
				return nil, err
			}
			array = append(array, value)
			c, err = p.nonspace()
			if err != nil {
				return nil, err
			}
			if c == ']' {
				return array, nil
			}
			if c != ',' {
				return nil, fmt.Errorf("expected JSON array separator")
			}
		}
	default:
		var token strings.Builder
		token.WriteByte(c)
		for {
			c, err = p.reader.ReadByte()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if strings.ContainsRune(" \t\r\n,]}", rune(c)) {
				_ = p.reader.UnreadByte()
				break
			}
			token.WriteByte(c)
		}
		var value any
		decoder := json.NewDecoder(strings.NewReader(token.String()))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if _, ok := value.(string); ok {
			return nil, fmt.Errorf("invalid JSON token")
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return nil, fmt.Errorf("invalid JSON token")
		}
		return value, nil
	}
}

// A finite key set is a necessary condition, not a replacement for VisitJSON.
// Local/allOf restrictions intersect; alternatives union only when every branch
// is closed. nil means unrestricted (including cycles we cannot prove closed).
// In particular, properties from sibling allOf branches do not extend a closed
// object's own allowed set, and negated schemas do not contribute allowed keys.
func requestObjectKeys(schema *openapi3.Schema, visiting map[*openapi3.Schema]bool) map[string]struct{} {
	if schema == nil || visiting[schema] {
		return nil
	}
	visiting[schema] = true
	defer delete(visiting, schema)
	var keys map[string]struct{}
	if schema.AdditionalProperties.Has != nil && !*schema.AdditionalProperties.Has {
		keys = make(map[string]struct{}, len(schema.Properties))
		for key := range schema.Properties {
			keys[key] = struct{}{}
		}
	}
	intersect := func(other map[string]struct{}) {
		if keys == nil {
			keys = other
			return
		}
		if other != nil {
			for key := range keys {
				if _, ok := other[key]; !ok {
					delete(keys, key)
				}
			}
		}
	}
	for _, ref := range schema.AllOf {
		intersect(requestObjectKeys(ref.Value, visiting))
	}
	for _, alternatives := range []openapi3.SchemaRefs{schema.AnyOf, schema.OneOf} {
		if len(alternatives) == 0 {
			continue
		}
		union := make(map[string]struct{})
		for _, ref := range alternatives {
			branch := requestObjectKeys(ref.Value, visiting)
			if branch == nil {
				union = nil
				break
			}
			for key := range branch {
				union[key] = struct{}{}
			}
		}
		intersect(union)
	}
	return keys
}

type requestPropertyMatcher struct {
	candidates []string
	offset     int
}

func (m *requestPropertyMatcher) Write(raw []byte) (int, error) {
	remaining := m.candidates[:0]
	for _, name := range m.candidates {
		if len(name)-m.offset >= len(raw) && name[m.offset:m.offset+len(raw)] == string(raw) {
			remaining = append(remaining, name)
		}
	}
	m.candidates = remaining
	if len(remaining) == 0 {
		return 0, fmt.Errorf("JSON property is not allowed by the request schema")
	}
	m.offset += len(raw)
	return len(raw), nil
}

func (p *schemaJSONReader) propertyName(keys map[string]struct{}) (string, error) {
	if keys == nil {
		key, _, err := p.stringValue(false)
		return key, err
	}
	matcher := &requestPropertyMatcher{candidates: make([]string, 0, len(keys))}
	for key := range keys {
		matcher.candidates = append(matcher.candidates, key)
	}
	// Reuse full escape/UTF-8 validation. Even a schema-declared name longer
	// than the diagnostic prefix is returned exactly from the schema, not from
	// a truncated input prefix. Impossible names fail before their value is read.
	if _, _, err := p.readString(true, matcher); err != nil {
		return "", err
	}
	for _, key := range matcher.candidates {
		if len(key) == matcher.offset {
			return key, nil
		}
	}
	return "", fmt.Errorf("JSON property is not allowed by the request schema")
}

// Carry necessary child constraints through compositions so a permitted parent
// property cannot hide a closed nested object behind a nil child schema. Child
// alternatives use anyOf, even for parent oneOf: two branches can constrain the
// same child identically while differing elsewhere. The original parent still
// performs full oneOf, pattern, enum, and other value validation in VisitJSON.
func requestPropertySchema(schema *openapi3.Schema, key string, visiting map[*openapi3.Schema]bool) *openapi3.Schema {
	if schema == nil || visiting[schema] {
		return nil
	}
	visiting[schema] = true
	defer delete(visiting, schema)
	var constraints openapi3.SchemaRefs
	if ref := schema.Properties[key]; ref != nil {
		constraints = append(constraints, ref)
	} else if ref := schema.AdditionalProperties.Schema; ref != nil {
		constraints = append(constraints, ref)
	}
	for _, ref := range schema.AllOf {
		if child := requestPropertySchema(ref.Value, key, visiting); child != nil {
			constraints = append(constraints, &openapi3.SchemaRef{Value: child})
		}
	}
	for _, alternatives := range []openapi3.SchemaRefs{schema.AnyOf, schema.OneOf} {
		var choices openapi3.SchemaRefs
		for _, ref := range alternatives {
			if keys := requestObjectKeys(ref.Value, make(map[*openapi3.Schema]bool)); keys != nil {
				if _, allowed := keys[key]; !allowed {
					continue
				}
			}
			child := requestPropertySchema(ref.Value, key, visiting)
			if child == nil {
				choices = nil
				break
			}
			choices = append(choices, &openapi3.SchemaRef{Value: child})
		}
		if len(choices) > 0 {
			constraints = append(constraints, &openapi3.SchemaRef{Value: &openapi3.Schema{AnyOf: choices}})
		}
	}
	if len(constraints) == 0 {
		return nil
	}
	if len(constraints) == 1 {
		return constraints[0].Value
	}
	return &openapi3.Schema{AllOf: constraints}
}

// A negated required-set (the work order provenance exclusivity rule) observes only
// property presence. Compacting string values cannot change its result. Other
// negations remain value-sensitive and keep exact values conservatively.
func requiredOnlyConstraint(schema *openapi3.Schema) bool {
	if schema == nil || len(schema.Required) == 0 {
		return false
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return false
	}
	var properties map[string]json.RawMessage
	if err := json.Unmarshal(raw, &properties); err != nil {
		return false
	}
	return len(properties) == 1 && properties["required"] != nil
}

func (p *schemaJSONReader) stringValue(discard bool) (string, uint64, error) {
	return p.readString(discard, nil)
}

// readString validates the entire string, even after its bounded diagnostic
// prefix has been discarded. JSON escapes are decoded incrementally, including
// surrogate pairs; unpaired surrogates match encoding/json's U+FFFD semantics.
func (p *schemaJSONReader) readString(compact bool, dst io.Writer) (string, uint64, error) {
	var text strings.Builder
	var length uint64
	omitted := false
	var high rune
	var encoded [utf8.UTFMax]byte
	emit := func(r rune) error {
		if length%4096 == 0 && p.ctx != nil {
			if err := p.ctx.Err(); err != nil {
				return err
			}
		}
		n := utf8.EncodeRune(encoded[:], r)
		if dst != nil {
			written, err := dst.Write(encoded[:n])
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrShortWrite
			}
		}
		length++
		if compact && length > 4096 {
			if !omitted {
				text.Reset()
				omitted = true
			}
		} else {
			text.Write(encoded[:n])
		}
		return nil
	}
	flushHigh := func() error {
		if high != 0 {
			high = 0
			return emit(utf8.RuneError)
		}
		return nil
	}
	for {
		c, err := p.reader.ReadByte()
		if err != nil {
			return "", 0, fmt.Errorf("unterminated JSON string: %w", err)
		}
		if c == '"' {
			if err := flushHigh(); err != nil {
				return "", 0, err
			}
			return text.String(), length, nil
		}
		if c < 0x20 {
			return "", 0, fmt.Errorf("invalid control byte in JSON string")
		}
		var decoded rune
		if c == '\\' {
			escape, err := p.reader.ReadByte()
			if err != nil {
				return "", 0, err
			}
			if escape == 'u' {
				var raw [4]byte
				if _, err := io.ReadFull(p.reader, raw[:]); err != nil {
					return "", 0, err
				}
				for _, digit := range raw {
					if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F') {
						return "", 0, fmt.Errorf("invalid JSON Unicode escape")
					}
				}
				code, err := strconv.ParseUint(string(raw[:]), 16, 16)
				if err != nil {
					return "", 0, fmt.Errorf("invalid JSON Unicode escape")
				}
				if high != 0 && code >= 0xdc00 && code <= 0xdfff {
					decoded = utf16.DecodeRune(high, rune(code))
					high = 0
					if err := emit(decoded); err != nil {
						return "", 0, err
					}
					continue
				}
				if err := flushHigh(); err != nil {
					return "", 0, err
				}
				if code >= 0xd800 && code <= 0xdbff {
					high = rune(code)
					continue
				}
				decoded = rune(code)
				if code >= 0xdc00 && code <= 0xdfff {
					decoded = utf8.RuneError
				}
				if err := emit(decoded); err != nil {
					return "", 0, err
				}
				continue
			}
			switch escape {
			case '"', '\\', '/':
				decoded = rune(escape)
			case 'b':
				decoded = '\b'
			case 'f':
				decoded = '\f'
			case 'n':
				decoded = '\n'
			case 'r':
				decoded = '\r'
			case 't':
				decoded = '\t'
			default:
				return "", 0, fmt.Errorf("invalid JSON escape")
			}
		} else if c >= utf8.RuneSelf {
			_ = p.reader.UnreadByte()
			r, size, err := p.reader.ReadRune()
			if err != nil {
				return "", 0, err
			}
			if r == utf8.RuneError && size == 1 {
				return "", 0, fmt.Errorf("invalid UTF-8 in JSON string")
			}
			decoded = r
		} else {
			decoded = rune(c)
		}
		if err := flushHigh(); err != nil {
			return "", 0, err
		}
		if err := emit(decoded); err != nil {
			return "", 0, err
		}
	}
}

func loadOpenAPISpec() (*openapi3.T, error) {
	return gen.GetSwagger()
}

func validationErrorCodeForStatus(statusCode int) errorcatalog.Code {
	switch {
	case statusCode >= http.StatusInternalServerError:
		return errorcatalog.CodeInternalError
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return errorcatalog.CodePolicyViolation
	case statusCode == http.StatusNotFound:
		return errorcatalog.CodeDependencyBlocked
	default:
		return errorcatalog.CodePolicyViolation
	}
}
