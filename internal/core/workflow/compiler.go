// Package workflow compiles and schedules work order workflows.
//
// It parses YAML definitions into canonical JSON, validates them against the
// shipped schema and runtime invariants, computes immutable workflow checksums,
// and provides the DAG scheduling helpers that the control plane uses to decide
// which nodes may advance next.
package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"gopkg.in/yaml.v3"

	"kingdom/internal/core/schemas"
)

const (
	defaultMaxExecutionReviewCycles  = 2
	defaultMaxValidationReworkCycles = 2
)

type CompileStage string

const (
	CompileStageDecodeYAML     CompileStage = "decode_yaml"
	CompileStageCanonicalize   CompileStage = "canonicalize"
	CompileStageSchemaValidate CompileStage = "schema_validate"
	CompileStageGraphValidate  CompileStage = "graph_validate"
)

type CompileError struct {
	Stage CompileStage
	Err   error
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("compile workflow (%s): %v", e.Stage, e.Err)
}

func (e *CompileError) Unwrap() error {
	return e.Err
}

type CompileResult struct {
	Definition    Definition
	CanonicalJSON []byte
	SHA256        string
	Graph         *Graph
}

type Compiler struct {
	schemaSet *schemas.Set
	parser    *Parser
}

func NewCompiler(schemaSet *schemas.Set, parser *Parser) *Compiler {
	return &Compiler{
		schemaSet: schemaSet,
		parser:    parser,
	}
}

func (c *Compiler) CompileYAML(raw []byte) (*CompileResult, error) {
	definition, err := decodeWorkflowYAML(raw)
	if err != nil {
		return nil, &CompileError{Stage: CompileStageDecodeYAML, Err: err}
	}

	normalizeDefinition(&definition)

	canonicalJSON, err := json.Marshal(definition)
	if err != nil {
		return nil, &CompileError{Stage: CompileStageCanonicalize, Err: err}
	}

	if err := c.schemaSet.ValidateJSONBytes("workflow", canonicalJSON); err != nil {
		return nil, &CompileError{Stage: CompileStageSchemaValidate, Err: err}
	}

	graph, err := c.parser.ParseDefinition(definition)
	if err != nil {
		return nil, &CompileError{Stage: CompileStageGraphValidate, Err: err}
	}

	checksum := sha256.Sum256(canonicalJSON)

	return &CompileResult{
		Definition:    definition,
		CanonicalJSON: canonicalJSON,
		SHA256:        hex.EncodeToString(checksum[:]),
		Graph:         graph,
	}, nil
}

func decodeWorkflowYAML(raw []byte) (Definition, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	var definition Definition
	if err := decoder.Decode(&definition); err != nil {
		return Definition{}, fmt.Errorf("decode workflow yaml: %w", err)
	}

	if err := decoder.Decode(new(struct{})); err != io.EOF {
		if err == nil {
			return Definition{}, fmt.Errorf("decode workflow yaml: multiple top-level documents")
		}
		return Definition{}, fmt.Errorf("decode workflow yaml: %w", err)
	}

	return definition, nil
}

func normalizeDefinition(definition *Definition) {
	if definition.Metadata.MaxExecutionReviewCycles == nil && definitionHasNodeClass(definition.Nodes, "execution_review") {
		definition.Metadata.MaxExecutionReviewCycles = refInt(defaultMaxExecutionReviewCycles)
	}
	if definition.Metadata.MaxValidationReworkCycles == nil && definitionHasNodeClass(definition.Nodes, "validation_gates") {
		definition.Metadata.MaxValidationReworkCycles = refInt(defaultMaxValidationReworkCycles)
	}

	for i := range definition.Edges {
		if definition.Edges[i].On == "" {
			definition.Edges[i].On = "success"
		}
	}

	sort.Slice(definition.Nodes, func(i, j int) bool {
		return definition.Nodes[i].NodeID < definition.Nodes[j].NodeID
	})

	sort.Slice(definition.Edges, func(i, j int) bool {
		if definition.Edges[i].From != definition.Edges[j].From {
			return definition.Edges[i].From < definition.Edges[j].From
		}
		if definition.Edges[i].To != definition.Edges[j].To {
			return definition.Edges[i].To < definition.Edges[j].To
		}
		return definition.Edges[i].On < definition.Edges[j].On
	})
}

func refInt(value int) *int {
	return &value
}

func definitionHasNodeClass(nodes []Node, nodeClass string) bool {
	for _, node := range nodes {
		if node.NodeClass == nodeClass {
			return true
		}
	}
	return false
}

// CompileJSON uses the same canonical compiler as YAML publication and startup.
func (c *Compiler) CompileJSON(expectedID string, raw []byte) (*CompileResult, error) {
	definition, err := ParseDefinitionJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode workflow definition_json: %w", err)
	}
	if definition.WorkflowID == "" || definition.WorkflowID != expectedID {
		return nil, fmt.Errorf("workflow_id mismatch between request and definition_json")
	}
	encoded, err := yaml.Marshal(definition)
	if err != nil {
		return nil, err
	}
	return c.CompileYAML(encoded)
}
