package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

const (
	CodeDuplicateNodeID     = "duplicate_node_id"
	CodeUnknownNodeRef      = "unknown_node_reference"
	CodeInvalidRequiredRole = "invalid_required_role"
	CodeInvalidNodeRolePair = "invalid_node_role_pair"
	CodeDuplicateEdge       = "duplicate_edge"
	CodeCycleDetected       = "cycle_detected"
)

type Definition struct {
	Version    string   `json:"version" yaml:"version"`
	WorkflowID string   `json:"workflow_id" yaml:"workflow_id"`
	Name       string   `json:"name,omitempty" yaml:"name,omitempty"`
	Nodes      []Node   `json:"nodes" yaml:"nodes"`
	Edges      []Edge   `json:"edges" yaml:"edges"`
	Metadata   Metadata `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type Metadata struct {
	MaxExecutionReviewCycles  *int `json:"max_execution_review_cycles,omitempty" yaml:"max_execution_review_cycles,omitempty"`
	MaxValidationReworkCycles *int `json:"max_validation_rework_cycles,omitempty" yaml:"max_validation_rework_cycles,omitempty"`
}

type Node struct {
	NodeID       string `json:"node_id" yaml:"node_id"`
	RequiredRole string `json:"required_role" yaml:"required_role"`
	NodeClass    string `json:"node_class" yaml:"node_class"`
}

type Edge struct {
	From string `json:"from" yaml:"from"`
	To   string `json:"to" yaml:"to"`
	On   string `json:"on,omitempty" yaml:"on,omitempty"`
}

type Graph struct {
	WorkflowID  string
	Nodes       map[string]Node
	Outgoing    map[string][]Edge
	Incoming    map[string]int
	Topological []string
}

type Diagnostic struct {
	Code    string
	Message string
	Field   string
}

type ValidationError struct {
	Diagnostics []Diagnostic
}

func (e *ValidationError) Error() string {
	if len(e.Diagnostics) == 0 {
		return "workflow validation failed"
	}
	return fmt.Sprintf("workflow validation failed: %s (%s)", e.Diagnostics[0].Message, e.Diagnostics[0].Code)
}

type Parser struct {
	allowedRoles map[string]struct{}
}

func NewParser(allowedRoles []string) *Parser {
	set := make(map[string]struct{}, len(allowedRoles))
	for _, role := range allowedRoles {
		set[role] = struct{}{}
	}

	return &Parser{allowedRoles: set}
}

func (p *Parser) ParseJSON(raw []byte) (*Graph, error) {
	def, err := ParseDefinitionJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode workflow definition: %w", err)
	}
	return p.ParseDefinition(def)
}

func ParseDefinitionJSON(raw []byte) (Definition, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var def Definition
	if err := decoder.Decode(&def); err != nil {
		return Definition{}, err
	}

	if err := decoder.Decode(new(struct{})); err != io.EOF {
		if err == nil {
			return Definition{}, fmt.Errorf("multiple JSON values are not allowed")
		}
		return Definition{}, err
	}

	return def, nil
}

func (p *Parser) ParseDefinition(def Definition) (*Graph, error) {
	diagnostics := make([]Diagnostic, 0)
	nodes := make(map[string]Node, len(def.Nodes))
	outgoing := make(map[string][]Edge, len(def.Nodes))
	incoming := make(map[string]int, len(def.Nodes))
	edgeSeen := make(map[string]struct{}, len(def.Edges))

	for idx, node := range def.Nodes {
		if _, exists := nodes[node.NodeID]; exists {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeDuplicateNodeID,
				Message: fmt.Sprintf("node_id %q is duplicated", node.NodeID),
				Field:   fmt.Sprintf("nodes[%d].node_id", idx),
			})
			continue
		}
		nodes[node.NodeID] = node
		incoming[node.NodeID] = 0

		if len(p.allowedRoles) > 0 {
			if _, ok := p.allowedRoles[node.RequiredRole]; !ok {
				diagnostics = append(diagnostics, Diagnostic{
					Code:    CodeInvalidRequiredRole,
					Message: fmt.Sprintf("required_role %q is not allowed", node.RequiredRole),
					Field:   fmt.Sprintf("nodes[%d].required_role", idx),
				})
			}
		}

		if !IsCanonicalNodeClass(node.NodeClass) {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeInvalidNodeClass,
				Message: fmt.Sprintf("node_class %q is not allowed", node.NodeClass),
				Field:   fmt.Sprintf("nodes[%d].node_class", idx),
			})
			continue
		}

		if _, err := BehaviorForNodeContract(node.NodeClass, node.RequiredRole); err != nil {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeInvalidNodeRolePair,
				Message: err.Error(),
				Field:   fmt.Sprintf("nodes[%d].required_role", idx),
			})
		}
	}

	for idx, edge := range def.Edges {
		if _, ok := nodes[edge.From]; !ok {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeUnknownNodeRef,
				Message: fmt.Sprintf("edge.from references unknown node %q", edge.From),
				Field:   fmt.Sprintf("edges[%d].from", idx),
			})
			continue
		}
		if _, ok := nodes[edge.To]; !ok {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeUnknownNodeRef,
				Message: fmt.Sprintf("edge.to references unknown node %q", edge.To),
				Field:   fmt.Sprintf("edges[%d].to", idx),
			})
			continue
		}

		edgeKey := fmt.Sprintf("%s|%s|%s", edge.From, edge.To, edge.On)
		if _, exists := edgeSeen[edgeKey]; exists {
			diagnostics = append(diagnostics, Diagnostic{
				Code:    CodeDuplicateEdge,
				Message: fmt.Sprintf("duplicate edge %q -> %q on %q", edge.From, edge.To, edge.On),
				Field:   fmt.Sprintf("edges[%d]", idx),
			})
			continue
		}
		edgeSeen[edgeKey] = struct{}{}

		outgoing[edge.From] = append(outgoing[edge.From], edge)
		incoming[edge.To]++
	}

	if len(diagnostics) > 0 {
		return nil, &ValidationError{Diagnostics: diagnostics}
	}

	topological, hasCycle := topoSort(nodes, outgoing, incoming)
	if hasCycle {
		return nil, &ValidationError{Diagnostics: []Diagnostic{{
			Code:    CodeCycleDetected,
			Message: "workflow graph contains a cycle",
			Field:   "edges",
		}}}
	}

	return &Graph{
		WorkflowID:  def.WorkflowID,
		Nodes:       nodes,
		Outgoing:    outgoing,
		Incoming:    incoming,
		Topological: topological,
	}, nil
}

func topoSort(nodes map[string]Node, outgoing map[string][]Edge, incoming map[string]int) ([]string, bool) {
	mutableIncoming := make(map[string]int, len(incoming))
	for key, value := range incoming {
		mutableIncoming[key] = value
	}

	queue := make([]string, 0)
	for nodeID, count := range mutableIncoming {
		if count == 0 {
			queue = append(queue, nodeID)
		}
	}
	sort.Strings(queue)

	ordered := make([]string, 0, len(nodes))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		ordered = append(ordered, current)

		nextEdges := outgoing[current]
		sort.Slice(nextEdges, func(i, j int) bool {
			if nextEdges[i].To == nextEdges[j].To {
				return nextEdges[i].On < nextEdges[j].On
			}
			return nextEdges[i].To < nextEdges[j].To
		})

		for _, edge := range nextEdges {
			mutableIncoming[edge.To]--
			if mutableIncoming[edge.To] == 0 {
				queue = append(queue, edge.To)
				sort.Strings(queue)
			}
		}
	}

	return ordered, len(ordered) != len(nodes)
}
