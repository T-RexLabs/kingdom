package workflow

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseJSON(t *testing.T) {
	t.Parallel()

	parser := NewParser([]string{"king"})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		definition := Definition{
			Version:    "v1",
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
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
		if graph.WorkflowID != "royal-decree-v1" {
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

		_, err := parser.ParseJSON([]byte(`{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"decree_intake","required_role":"king","node_class":"decree_intake","extra":true}],"edges":[]}`))
		if err == nil {
			t.Fatal("expected parse error")
		}
	})

	t.Run("decode_multiple_values_error", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseJSON([]byte(`{"version":"v1","workflow_id":"royal-decree-v1","nodes":[{"node_id":"decree_intake","required_role":"king","node_class":"decree_intake"}],"edges":[]} {}`))
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

func TestParseDefinitionValidRoyalDecreeV1(t *testing.T) {
	t.Parallel()

	parser := NewParser([]string{
		"king",
		"noble-delegate",
		"chancellor",
		"council-member",
		"builder-berserker",
		"intelligence-qa-scout",
	})

	graph, err := parser.ParseDefinition(Definition{
		Version:    "v1",
		WorkflowID: "royal-decree-v1",
		Nodes: []Node{
			{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
			{NodeID: "advisor_consultation", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
			{NodeID: "advisory_synthesis", RequiredRole: "chancellor", NodeClass: "advisory_synthesis"},
			{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: "plan_drafting"},
			{NodeID: "king_plan_decision", RequiredRole: "king", NodeClass: "king_plan_decision"},
			{NodeID: "execution_kickoff", RequiredRole: "council-member", NodeClass: "execution_kickoff"},
			{NodeID: "delegated_execution", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
			{NodeID: "validation_gates", RequiredRole: "intelligence-qa-scout", NodeClass: "validation_gates"},
			{NodeID: "royal_closure", RequiredRole: "king", NodeClass: "royal_closure"},
		},
		Edges: []Edge{
			{From: "decree_intake", To: "advisor_consultation", On: "success"},
			{From: "advisor_consultation", To: "advisory_synthesis", On: "success"},
			{From: "advisory_synthesis", To: "plan_drafting", On: "success"},
			{From: "plan_drafting", To: "king_plan_decision", On: "success"},
			{From: "king_plan_decision", To: "execution_kickoff", On: "approved"},
			{From: "execution_kickoff", To: "delegated_execution", On: "success"},
			{From: "delegated_execution", To: "validation_gates", On: "success"},
			{From: "validation_gates", To: "royal_closure", On: "success"},
		},
	})
	if err != nil {
		t.Fatalf("expected valid workflow, got error: %v", err)
	}

	if graph.WorkflowID != "royal-decree-v1" {
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

	parser := NewParser([]string{"king"})

	t.Run("duplicate_node_id", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
				{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
			},
		})
		assertValidationCode(t, err, CodeDuplicateNodeID)
	})

	t.Run("unknown_node_reference", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
			},
			Edges: []Edge{{From: "decree_intake", To: "missing_node", On: "success"}},
		})
		assertValidationCode(t, err, CodeUnknownNodeRef)
	})

	t.Run("invalid_required_role", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "decree_intake", RequiredRole: "invalid-role", NodeClass: "decree_intake"},
			},
		})
		assertValidationCode(t, err, CodeInvalidRequiredRole)
	})

	t.Run("invalid_node_class", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "totally_unknown_class"},
			},
		})
		assertValidationCode(t, err, CodeInvalidNodeClass)
	})

	t.Run("cycle_detected", func(t *testing.T) {
		t.Parallel()

		_, err := parser.ParseDefinition(Definition{
			WorkflowID: "royal-decree-v1",
			Nodes: []Node{
				{NodeID: "a", RequiredRole: "king", NodeClass: "decree_intake"},
				{NodeID: "b", RequiredRole: "king", NodeClass: "plan_drafting"},
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
