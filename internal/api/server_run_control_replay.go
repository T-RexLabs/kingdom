package api

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"kingdom/internal/core/workflow"
)

type replayTargetPlan struct {
	DisplayAnchorNodeID string
	AnchorNodeIDs       []string
	TargetNodeIDs       []string
}

func (s *Server) manualRerunPlan(ctx context.Context, runID, requestedNodeID string) (replayTargetPlan, error) {
	graph, err := s.workflowGraphForRun(ctx, runID)
	if err != nil {
		return replayTargetPlan{}, err
	}
	trimmedRequestedNodeID := strings.TrimSpace(requestedNodeID)
	node, ok := graph.Nodes[trimmedRequestedNodeID]
	if !ok {
		return replayTargetPlan{}, fmt.Errorf("requested rerun node %q not found in workflow graph", trimmedRequestedNodeID)
	}
	anchorNodeIDs := []string{node.NodeID}
	if node.NodeClass == workflow.NodeClassCompletion {
		anchorNodeIDs = replayAnchorNodeIDsForClass(graph, workflow.NodeClassPlanDrafting)
		if len(anchorNodeIDs) == 0 {
			return replayTargetPlan{}, fmt.Errorf("rerun from completion requires a %s node", workflow.NodeClassPlanDrafting)
		}
	}
	targetNodeIDs := replayTargetNodeIDs(graph, anchorNodeIDs, false)
	if len(targetNodeIDs) == 0 {
		return replayTargetPlan{}, fmt.Errorf("no replay targets resolved from %q", trimmedRequestedNodeID)
	}
	return replayTargetPlan{
		DisplayAnchorNodeID: anchorNodeIDs[0],
		AnchorNodeIDs:       anchorNodeIDs,
		TargetNodeIDs:       targetNodeIDs,
	}, nil
}

func (s *Server) finalReworkPlan(ctx context.Context, runID string) (replayTargetPlan, error) {
	graph, err := s.workflowGraphForRun(ctx, runID)
	if err != nil {
		return replayTargetPlan{}, err
	}
	anchorNodeIDs, err := s.planDraftingNodeIDsForGraph(graph)
	if err != nil {
		return replayTargetPlan{}, err
	}
	targetNodeIDs := replayTargetNodeIDs(graph, anchorNodeIDs, true)
	if len(targetNodeIDs) == 0 {
		return replayTargetPlan{}, fmt.Errorf("final rework resolved no replay targets")
	}
	return replayTargetPlan{
		DisplayAnchorNodeID: anchorNodeIDs[0],
		AnchorNodeIDs:       anchorNodeIDs,
		TargetNodeIDs:       targetNodeIDs,
	}, nil
}

func (s *Server) planDraftingNodeIDsForRun(ctx context.Context, runID string) ([]string, error) {
	graph, err := s.workflowGraphForRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return s.planDraftingNodeIDsForGraph(graph)
}

func (s *Server) planDraftingNodeIDsForGraph(graph *workflow.Graph) ([]string, error) {
	anchorNodeIDs := replayAnchorNodeIDsForClass(graph, workflow.NodeClassPlanDrafting)
	if len(anchorNodeIDs) == 0 {
		return nil, fmt.Errorf("run workflow requires a %s node", workflow.NodeClassPlanDrafting)
	}
	return anchorNodeIDs, nil
}

func (s *Server) workflowGraphForRun(ctx context.Context, runID string) (*workflow.Graph, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run id is required")
	}
	var definitionJSON string
	err := s.db.QueryRowContext(ctx, `
SELECT wd.definition_json
FROM workflow_runs wr
JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ?`, trimmedRunID).Scan(&definitionJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("workflow definition not found for run %q", trimmedRunID)
		}
		return nil, fmt.Errorf("load workflow definition for run %q: %w", trimmedRunID, err)
	}
	definition, err := workflow.ParseDefinitionJSON([]byte(definitionJSON))
	if err != nil {
		return nil, fmt.Errorf("parse workflow definition for run %q: %w", trimmedRunID, err)
	}
	graph, err := workflow.NewParser(nil).ParseDefinition(definition)
	if err != nil {
		return nil, fmt.Errorf("build workflow graph for run %q: %w", trimmedRunID, err)
	}
	return graph, nil
}

func replayAnchorNodeIDsForClass(graph *workflow.Graph, nodeClass string) []string {
	if graph == nil {
		return nil
	}
	trimmedNodeClass := strings.TrimSpace(nodeClass)
	anchorNodeIDs := make([]string, 0)
	for _, nodeID := range graph.Topological {
		node, ok := graph.Nodes[nodeID]
		if !ok || strings.TrimSpace(node.NodeClass) != trimmedNodeClass {
			continue
		}
		anchorNodeIDs = append(anchorNodeIDs, node.NodeID)
	}
	return anchorNodeIDs
}

func replayTargetNodeIDs(graph *workflow.Graph, anchorNodeIDs []string, excludeNeedsReworkEdges bool) []string {
	if graph == nil || len(anchorNodeIDs) == 0 {
		return nil
	}
	reachable := make(map[string]struct{}, len(anchorNodeIDs))
	queue := make([]string, 0, len(anchorNodeIDs))
	for _, nodeID := range anchorNodeIDs {
		trimmedNodeID := strings.TrimSpace(nodeID)
		if trimmedNodeID == "" {
			continue
		}
		if _, ok := graph.Nodes[trimmedNodeID]; !ok {
			continue
		}
		if _, seen := reachable[trimmedNodeID]; seen {
			continue
		}
		reachable[trimmedNodeID] = struct{}{}
		queue = append(queue, trimmedNodeID)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, edge := range graph.Outgoing[current] {
			if excludeNeedsReworkEdges && strings.EqualFold(strings.TrimSpace(edge.On), "needs_rework") {
				continue
			}
			if _, seen := reachable[edge.To]; seen {
				continue
			}
			reachable[edge.To] = struct{}{}
			queue = append(queue, edge.To)
		}
	}
	targetNodeIDs := make([]string, 0, len(reachable))
	for _, nodeID := range graph.Topological {
		if _, ok := reachable[nodeID]; ok {
			targetNodeIDs = append(targetNodeIDs, nodeID)
		}
	}
	return targetNodeIDs
}
