package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"kingdom/internal/core/schemas"
)

func TestCompilerDeterministicCanonicalOutput(t *testing.T) {
	t.Parallel()

	compiler := newTestCompiler(t)
	input := []byte(`version: v1
workflow_id: royal-decree-v1
name: Royal Decree
nodes:
  - node_id: plan_drafting
    required_role: council-member
    node_class: plan_drafting
  - node_id: decree_intake
    required_role: king
    node_class: decree_intake
  - node_id: royal_closure
    required_role: king
    node_class: royal_closure
edges:
  - from: plan_drafting
    to: royal_closure
    on: approved
  - from: decree_intake
    to: plan_drafting
`)

	first, err := compiler.CompileYAML(input)
	if err != nil {
		t.Fatalf("first compile failed: %v", err)
	}

	second, err := compiler.CompileYAML(input)
	if err != nil {
		t.Fatalf("second compile failed: %v", err)
	}

	if string(first.CanonicalJSON) != string(second.CanonicalJSON) {
		t.Fatalf("canonical output changed between runs\nfirst:  %s\nsecond: %s", first.CanonicalJSON, second.CanonicalJSON)
	}

	if first.SHA256 != second.SHA256 {
		t.Fatalf("canonical hash changed between runs: %s != %s", first.SHA256, second.SHA256)
	}

	if first.Definition.Metadata.MaxExecutionReviewCycles != nil {
		t.Fatalf("expected max_execution_review_cycles to be omitted without review nodes, got %#v", first.Definition.Metadata.MaxExecutionReviewCycles)
	}
	if first.Definition.Metadata.MaxValidationReworkCycles != nil {
		t.Fatalf("expected max_validation_rework_cycles to be omitted without validation nodes, got %#v", first.Definition.Metadata.MaxValidationReworkCycles)
	}

	if first.Definition.Edges[0].On == "" {
		t.Fatal("expected edge condition defaulting")
	}
}

func TestCompilerAppliesReviewAndValidationDefaultsWhenNodesExist(t *testing.T) {
	t.Parallel()

	compiler := newTestCompiler(t)
	input := []byte(`version: v1
workflow_id: royal-decree-v1
nodes:
  - node_id: delegated_execution_berserker
    required_role: builder-berserker
    node_class: delegated_execution
  - node_id: execution_review_shieldmaiden
    required_role: reviewer-shieldmaiden
    node_class: execution_review
  - node_id: validation_gates_qa_scout
    required_role: intelligence-qa-scout
    node_class: validation_gates
edges:
  - from: delegated_execution_berserker
    to: execution_review_shieldmaiden
    on: success
  - from: execution_review_shieldmaiden
    to: validation_gates_qa_scout
    on: success
`)

	result, err := compiler.CompileYAML(input)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if result.Definition.Metadata.MaxExecutionReviewCycles == nil || *result.Definition.Metadata.MaxExecutionReviewCycles != 2 {
		t.Fatalf("expected normalized max_execution_review_cycles=2, got %#v", result.Definition.Metadata.MaxExecutionReviewCycles)
	}
	if result.Definition.Metadata.MaxValidationReworkCycles == nil || *result.Definition.Metadata.MaxValidationReworkCycles != 2 {
		t.Fatalf("expected normalized max_validation_rework_cycles=2, got %#v", result.Definition.Metadata.MaxValidationReworkCycles)
	}
}

func TestCompilerEquivalentInputsYieldSameHash(t *testing.T) {
	t.Parallel()

	compiler := newTestCompiler(t)
	firstYAML := []byte(`version: v1
workflow_id: royal-decree-v1
nodes:
  - node_id: decree_intake
    required_role: king
    node_class: decree_intake
  - node_id: plan_drafting
    required_role: council-member
    node_class: plan_drafting
edges:
  - from: decree_intake
    to: plan_drafting
    on: success
`)

	secondYAML := []byte(`workflow_id: royal-decree-v1
version: v1
edges:
  - to: plan_drafting
    from: decree_intake
    on: success
nodes:
  - required_role: council-member
    node_class: plan_drafting
    node_id: plan_drafting
  - node_class: decree_intake
    node_id: decree_intake
    required_role: king
`)

	first, err := compiler.CompileYAML(firstYAML)
	if err != nil {
		t.Fatalf("first compile failed: %v", err)
	}

	second, err := compiler.CompileYAML(secondYAML)
	if err != nil {
		t.Fatalf("second compile failed: %v", err)
	}

	if first.SHA256 != second.SHA256 {
		t.Fatalf("expected equivalent YAML inputs to produce same hash, got %s vs %s", first.SHA256, second.SHA256)
	}
}

func TestCompilerRejectsInvalidWorkflowsPrePublish(t *testing.T) {
	t.Parallel()

	compiler := newTestCompiler(t)

	t.Run("unknown_field_in_yaml", func(t *testing.T) {
		t.Parallel()

		_, err := compiler.CompileYAML([]byte(`version: v1
workflow_id: royal-decree-v1
nodes:
  - node_id: decree_intake
    required_role: king
    node_class: decree_intake
bad_field: true
`))
		assertCompileStage(t, err, CompileStageDecodeYAML)
	})

	t.Run("schema_violation_missing_nodes", func(t *testing.T) {
		t.Parallel()

		_, err := compiler.CompileYAML([]byte(`version: v1
workflow_id: royal-decree-v1
edges: []
`))
		assertCompileStage(t, err, CompileStageSchemaValidate)
	})

	t.Run("graph_violation_cycle", func(t *testing.T) {
		t.Parallel()

		_, err := compiler.CompileYAML([]byte(`version: v1
workflow_id: royal-decree-v1
nodes:
  - node_id: a
    required_role: king
    node_class: decree_intake
  - node_id: b
    required_role: king
    node_class: plan_drafting
edges:
  - from: a
    to: b
    on: success
  - from: b
    to: a
    on: success
`))
		assertCompileStage(t, err, CompileStageGraphValidate)
	})
}

func TestCanonicalNodeClassesMatchWorkflowSchema(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join(resolveSchemaDir(t), "workflow.schema.json"))
	if err != nil {
		t.Fatalf("read workflow schema: %v", err)
	}

	var schema struct {
		Defs struct {
			Node struct {
				Properties struct {
					NodeClass struct {
						Enum []string `json:"enum"`
					} `json:"node_class"`
				} `json:"properties"`
			} `json:"node"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("decode workflow schema: %v", err)
	}

	if !reflect.DeepEqual(CanonicalNodeClasses(), schema.Defs.Node.Properties.NodeClass.Enum) {
		t.Fatalf("canonical node classes drifted from workflow schema\ncode:   %#v\nschema: %#v", CanonicalNodeClasses(), schema.Defs.Node.Properties.NodeClass.Enum)
	}
}

func newTestCompiler(t *testing.T) *Compiler {
	t.Helper()

	schemaDir := resolveSchemaDir(t)
	schemaSet, err := schemas.Load(schemaDir)
	if err != nil {
		t.Fatalf("load schema set: %v", err)
	}

	parser := NewParser([]string{
		"king",
		"chancellor",
		"council-member",
		"noble-delegate",
		"clergy-delegate",
		"burgher-delegate",
		"peasant-delegate",
		"builder-berserker",
		"reviewer-shieldmaiden",
		"intelligence-qa-scout",
		"intelligence-nightraven",
		"intelligence-loki-chaos",
	})

	return NewCompiler(schemaSet, parser)
}

func resolveSchemaDir(t *testing.T) string {
	t.Helper()

	candidates := []string{
		filepath.Join("contracts", "schemas"),
		filepath.Join("..", "..", "..", "contracts", "schemas"),
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate
		}
	}

	t.Fatalf("unable to resolve contracts schema directory from candidates: %v", candidates)
	return ""
}

func assertCompileStage(t *testing.T, err error, stage CompileStage) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected compile failure at stage %q", stage)
	}

	var compileErr *CompileError
	if !errors.As(err, &compileErr) {
		t.Fatalf("expected CompileError, got %T: %v", err, err)
	}

	if compileErr.Stage != stage {
		t.Fatalf("expected compile stage %q, got %q", stage, compileErr.Stage)
	}
}

func TestCompileErrorFormattingAndUnwrap(t *testing.T) {
	t.Parallel()

	root := fmt.Errorf("decode failed")
	compileErr := &CompileError{Stage: CompileStageDecodeYAML, Err: root}

	if compileErr.Error() != "compile workflow (decode_yaml): decode failed" {
		t.Fatalf("unexpected compile error message: %q", compileErr.Error())
	}

	if !errors.Is(compileErr, root) {
		t.Fatal("expected compile error to unwrap root cause")
	}
}
