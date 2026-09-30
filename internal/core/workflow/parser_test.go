package workflow

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseJSON(t *testing.T) {
	t.Parallel()

	parser := NewParser([]string{"governance"})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		definition := Definition{
			Version:    "v1",
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
			},
		}

		raw, err := json.Marshal(definition)
		if err != nil {
			t.Fatalf("marshal definition: %v", err)
		}

		graph, err := parser.ParseJSON(raw)
		if err != nil {
			t.Fatalf("parse json: %v", err)
		}
		if graph.WorkflowID != "work-order-v1" {
			t.Fatalf("unexpected workflow id: %s", graph.WorkflowID)
		}
	})

	t.Run("decode_error", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseJSON([]byte("{"))
		if err == nil {
			t.Fatal("expected parse error")
		}
	})

	t.Run("decode_unknown_field_error", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseJSON([]byte(`{"version":"v1","workflow_id":"work-order-v1","nodes":[{"node_id":"work_intake","required_role":"governance","node_class":"work_intake","extra":true}],"edges":[]}`))
		if err == nil {
			t.Fatal("expected parse error")
		}
	})

	t.Run("decode_multiple_values_error", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseJSON([]byte(`{"version":"v1","workflow_id":"work-order-v1","nodes":[{"node_id":"work_intake","required_role":"governance","node_class":"work_intake"}],"edges":[]} {}`))
		if err == nil {
			t.Fatal("expected parse error")
		}
	})
}

func TestValidationErrorMessageFallback(t *testing.T) {
	t.Parallel()

	err := (&ValidationError{}).Error()
	if err != "workflow validation failed" {
		t.Fatalf("unexpected fallback message: %q", err)
	}
}

func TestParseDefinitionValidRoyalWorkOrderV1(t *testing.T) {
	t.Parallel()

	parser := NewParser([]string{
		"governance",
		"architecture-advisor",
		"coordinator",
		"plan-author",
		"builder",
		"qa-validator",
	})

	graph, err := parser.ParseDefinition(Definition{
		Version:    "v1",
		WorkflowID: "work-order-v1",
		Nodes: []Node{
			{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
			{NodeID: "advisor_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
			{NodeID: "advisory_synthesis", RequiredRole: "coordinator", NodeClass: "advisory_synthesis"},
			{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
			{NodeID: "plan_decision", RequiredRole: "governance", NodeClass: "plan_decision"},
			{NodeID: "execution_kickoff", RequiredRole: "plan-author", NodeClass: "execution_kickoff"},
			{NodeID: "delegated_execution", RequiredRole: "builder", NodeClass: "delegated_execution"},
			{NodeID: "validation_gates", RequiredRole: "qa-validator", NodeClass: "validation_gates"},
			{NodeID: "completion", RequiredRole: "governance", NodeClass: "completion"},
		},
		Edges: []Edge{
			{From: "work_intake", To: "advisor_consultation", On: "success"},
			{From: "advisor_consultation", To: "advisory_synthesis", On: "success"},
			{From: "advisory_synthesis", To: "plan_drafting", On: "success"},
			{From: "plan_drafting", To: "plan_decision", On: "success"},
			{From: "plan_decision", To: "execution_kickoff", On: "approved"},
			{From: "execution_kickoff", To: "delegated_execution", On: "success"},
			{From: "delegated_execution", To: "validation_gates", On: "success"},
			{From: "validation_gates", To: "completion", On: "success"},
		},
	})
	if err != nil {
		t.Fatalf("expected valid workflow, got error: %v", err)
	}

	if graph.WorkflowID != "work-order-v1" {
		t.Fatalf("unexpected workflow id: %s", graph.WorkflowID)
	}
	if len(graph.Nodes) != 9 {
		t.Fatalf("unexpected node count: %d", len(graph.Nodes))
	}
	if len(graph.Topological) != len(graph.Nodes) {
		t.Fatalf("topological order length mismatch: %d vs %d", len(graph.Topological), len(graph.Nodes))
	}
}

func TestParseDefinitionInvalidDiagnostics(t *testing.T) {
	t.Parallel()

	parser := NewParser([]string{"governance"})

	t.Run("duplicate_node_id", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
				{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
			},
		})
		assertValidationCode(t, err, CodeDuplicateNodeID)
	})

	t.Run("unknown_node_reference", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
			},
			Edges: []Edge{{From: "work_intake", To: "missing_node", On: "success"}},
		})
		assertValidationCode(t, err, CodeUnknownNodeRef)
	})

	t.Run("invalid_required_role", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "work_intake", RequiredRole: "invalid-role", NodeClass: "work_intake"},
			},
		})
		assertValidationCode(t, err, CodeInvalidRequiredRole)
	})

	t.Run("invalid_node_class", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "totally_unknown_class"},
			},
		})
		assertValidationCode(t, err, CodeInvalidNodeClass)
	})

	t.Run("cycle_detected", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "work-order-v1",
			Nodes: []Node{
				{NodeID: "a", RequiredRole: "governance", NodeClass: "work_intake"},
				{NodeID: "b", RequiredRole: "governance", NodeClass: "work_intake"},
			},
			Edges: []Edge{
				{From: "a", To: "b", On: "success"},
				{From: "b", To: "a", On: "success"},
			},
		})
		assertValidationCode(t, err, CodeCycleDetected)
	})
}

func assertValidationCode(t *testing.T, err error, code string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected validation error with code %q", code)
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}

	for _, diag := range validationErr.Diagnostics {
		if diag.Code == code {
			return
		}
	}

	t.Fatalf("expected validation code %q, got diagnostics %#v", code, validationErr.Diagnostics)
}
