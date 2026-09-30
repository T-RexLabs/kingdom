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
		return validateHeaders(spoolRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		})))
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
			key, _, err := p.stringValue(false)
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
			var child *openapi3.Schema
			if schema != nil {
				if ref := schema.Properties[key]; ref != nil {
					child = ref.Value
				} else if schema.AdditionalProperties.Schema != nil {
					child = schema.AdditionalProperties.Schema.Value
				}
			}
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

// A negated required-set (the decree provenance exclusivity rule) observes only
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
