package opencode

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// eventProjection follows the typed wire schema using encoding/json tokens.
// Unknown objects are skipped incrementally; diagnostic values have a bounded
// preview. JSON syntax/number/escape validation remains the standard decoder's
// responsibility. The schema is derived from the wire type, not duplicated.
type eventProjection struct {
	fields            map[string]*eventProjection
	diagnostic, loose bool
	control           bool
}

type boundedJSONTokens struct {
	*json.Decoder
	depth int
}

func (d *boundedJSONTokens) Token() (json.Token, error) {
	token, err := d.Decoder.Token()
	if err != nil {
		return token, err
	}
	if delimiter, ok := token.(json.Delim); ok {
		if delimiter == '{' || delimiter == '[' {
			d.depth++
		} else {
			d.depth--
		}
		if d.depth > 10000 || d.depth < 0 {
			return nil, fmt.Errorf("provider JSON exceeds standard nesting depth")
		}
	}
	return token, nil
}

var runEventProjection = func() *eventProjection {
	p := projectionForType(reflect.TypeFor[opencodeRunEventWire]())
	errorFields := map[string]*eventProjection{}
	for _, key := range []string{"name", "code", "category", "status", "statusCode", "message", "text", "retry_at", "retry_after_ms"} {
		errorFields[key] = &eventProjection{diagnostic: true, control: true}
	}
	data := &eventProjection{fields: errorFields, loose: true}
	top := map[string]*eventProjection{"data": data}
	for key, value := range errorFields {
		top[key] = value
	}
	p.fields["error"] = &eventProjection{fields: top, loose: true, control: true}
	return p
}()

func projectionForType(t reflect.Type) *eventProjection {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeFor[json.RawMessage]() {
		return &eventProjection{diagnostic: true}
	}
	if t.Kind() != reflect.Struct {
		return &eventProjection{}
	}
	p := &eventProjection{fields: map[string]*eventProjection{}}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			p.fields[name] = projectionForType(field.Type)
		}
	}
	return p
}

func projectEventValue(d *boundedJSONTokens, p *eventProjection, numberMarker string) (any, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	if text, ok := token.(string); ok && numberMarker != "" && text == numberMarker && (!p.diagnostic || p.control) {
		return nil, fmt.Errorf("numeric provider control field is not compact")
	}
	if p.diagnostic {
		budget := 16 * 1024
		value, err := previewJSONToken(d, token, &budget)
		if err != nil {
			return nil, err
		}
		if budget < 0 {
			return "Full value is preserved in the provider stream diagnostic.", nil
		}
		return value, nil
	}
	if token == nil {
		return nil, nil
	}
	if token != json.Delim('{') {
		if _, container := token.(json.Delim); container {
			return nil, fmt.Errorf("invalid provider field shape")
		}
		if p.fields != nil && !p.loose {
			return nil, fmt.Errorf("provider field must be an object")
		}
		return token, nil
	}
	if p.fields == nil {
		return nil, fmt.Errorf("provider field must be a scalar")
	}
	result := make(map[string]any, len(p.fields))
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("invalid provider object key")
		}
		child := p.fields[name]
		if child == nil {
			for canonical, candidate := range p.fields {
				if strings.EqualFold(canonical, name) {
					name = canonical
					child = candidate
					break
				}
			}
		}
		if child == nil {
			token, err := d.Token()
			if err != nil {
				return nil, err
			}
			if err := skipJSONToken(d, token); err != nil {
				return nil, err
			}
			continue
		}
		if _, duplicate := result[name]; duplicate {
			return nil, fmt.Errorf("duplicate provider control field %q", name)
		}
		value, err := projectEventValue(d, child, numberMarker)
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	_, err = d.Token()
	return result, err
}

func skipJSONToken(d *boundedJSONTokens, token json.Token) error {
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return fmt.Errorf("unexpected JSON delimiter")
	}
	depth := 1
	for depth > 0 {
		token, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			if delim == '{' || delim == '[' {
				depth++
			} else {
				depth--
			}
		}
	}
	return nil
}

func previewJSONToken(d *boundedJSONTokens, token json.Token, budget *int) (any, error) {
	*budget -= 16
	if text, ok := token.(string); ok {
		*budget -= len(text)
	}
	if *budget < 0 {
		return nil, skipJSONToken(d, token)
	}
	switch token {
	case json.Delim('{'):
		result := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("invalid JSON key")
			}
			*budget -= len(name) + 16
			next, err := d.Token()
			if err != nil {
				return nil, err
			}
			value, err := previewJSONToken(d, next, budget)
			if err != nil {
				return nil, err
			}
			if *budget >= 0 || value != nil {
				result[name] = value
			}
		}
		_, err := d.Token()
		return result, err
	case json.Delim('['):
		var result []any
		for d.More() {
			next, err := d.Token()
			if err != nil {
				return nil, err
			}
			value, err := previewJSONToken(d, next, budget)
			if err != nil {
				return nil, err
			}
			if *budget >= 0 || value != nil {
				result = append(result, value)
			}
		}
		_, err := d.Token()
		return result, err
	default:
		return token, nil
	}
}
