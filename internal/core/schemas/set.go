package schemas

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Descriptor struct {
	Name     string
	FileName string
	Version  string
}

var Required = []Descriptor{
	{Name: "workflow", FileName: "workflow.schema.json", Version: "v1"},
	{Name: "event_v1", FileName: "event_v1.schema.json", Version: "v1"},
	{Name: "retry_policy", FileName: "retry_policy.schema.json", Version: "v1"},
	{Name: "worker_output", FileName: "worker_output.schema.json", Version: "v1"},
}

type Set struct {
	schemas map[string]*jsonschema.Schema
}

func Load(schemaDir string) (*Set, error) {
	compiler := jsonschema.NewCompiler()

	schemas := make(map[string]*jsonschema.Schema, len(Required))
	for _, descriptor := range Required {
		schemaPath := filepath.Join(schemaDir, descriptor.FileName)
		absoluteSchemaPath, err := filepath.Abs(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("resolve schema path %q: %w", schemaPath, err)
		}
		schemaURL := "file://" + filepath.ToSlash(schemaPath)
		schemaURL = "file://" + filepath.ToSlash(absoluteSchemaPath)

		schema, err := compiler.Compile(schemaURL)
		if err != nil {
			return nil, fmt.Errorf("compile schema %q (%s): %w", descriptor.Name, descriptor.FileName, err)
		}

		schemas[descriptor.Name] = schema
	}

	return &Set{schemas: schemas}, nil
}

func (s *Set) ValidateJSONBytes(schemaName string, raw []byte) error {
	schema, ok := s.schemas[schemaName]
	if !ok {
		return fmt.Errorf("schema %q is not loaded", schemaName)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode JSON for %q: %w", schemaName, err)
	}

	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode JSON for %q: multiple top-level values", schemaName)
	}

	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("validate %q: %w", schemaName, err)
	}

	return nil
}
