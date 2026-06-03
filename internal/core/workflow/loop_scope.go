package workflow

import (
	"fmt"
	"sort"
	"strings"
)

type ExecutionLoopScopePlan struct {
	ScopeID             string
	NodeClass           string
	ParticipantNodeIDs  []string
	ReplayAnchorNodeIDs []string
}

func ResolveExecutionLoopScopePlan(graph *Graph, nodeID string) (ExecutionLoopScopePlan, bool, error) {
	if graph == nil {
		return ExecutionLoopScopePlan{}, false, fmt.Errorf("workflow graph is required")
	}
	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedNodeID == "" {
		return ExecutionLoopScopePlan{}, false, fmt.Errorf("node id is required")
	}
	node, ok := graph.Nodes[trimmedNodeID]
	if !ok {
		return ExecutionLoopScopePlan{}, false, fmt.Errorf("node %q not found in workflow graph", trimmedNodeID)
	}
	classID := ResolveNodeClass(node.NodeID, node.NodeClass)
	plan := ExecutionLoopScopePlan{NodeClass: classID}
	switch classID {
	case NodeClassExecutionReview:
		scopeID, err := reviewLoopScopeID(graph, trimmedNodeID)
		if err != nil {
			return ExecutionLoopScopePlan{}, false, err
		}
		participants, err := reviewParticipantNodeIDsForScope(graph, scopeID)
		if err != nil {
			return ExecutionLoopScopePlan{}, false, err
		}
		plan.ScopeID = scopeID
		plan.ParticipantNodeIDs = participants
	case NodeClassValidationGates:
		scopeID, err := validationLoopScopeID(graph, trimmedNodeID)
		if err != nil {
			return ExecutionLoopScopePlan{}, false, err
		}
		participants, err := validationParticipantNodeIDsForScope(graph, scopeID)
		if err != nil {
			return ExecutionLoopScopePlan{}, false, err
		}
		plan.ScopeID = scopeID
		plan.ParticipantNodeIDs = participants
	default:
		return ExecutionLoopScopePlan{}, false, nil
	}
	anchors, err := replayAnchorNodeIDsForParticipants(graph, plan.ParticipantNodeIDs)
	if err != nil {
		return ExecutionLoopScopePlan{}, false, err
	}
	if len(anchors) == 0 {
		anchors = append([]string(nil), plan.ParticipantNodeIDs...)
	}
	plan.ReplayAnchorNodeIDs = anchors
	return plan, true, nil
}

func reviewLoopScopeID(graph *Graph, nodeID string) (string, error) {
	executionAncestors, err := nearestAncestorNodeIDsByClass(graph, nodeID, NodeClassDelegatedExecution, true)
	if err != nil {
		return "", err
	}
	if len(executionAncestors) > 0 {
		return "execution_review:execution:" + strings.Join(executionAncestors, "+"), nil
	}
	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedNodeID == "" {
		return "", fmt.Errorf("node id is required")
	}
	return "execution_review:node:" + trimmedNodeID, nil
}

func reviewParticipantNodeIDsForScope(graph *Graph, scopeID string) ([]string, error) {
	if graph == nil {
		return nil, fmt.Errorf("workflow graph is required")
	}
	participants := make([]string, 0)
	for _, nodeID := range graph.Topological {
		node, ok := graph.Nodes[nodeID]
		if !ok || ResolveNodeClass(node.NodeID, node.NodeClass) != NodeClassExecutionReview {
			continue
		}
		nodeScopeID, err := reviewLoopScopeID(graph, nodeID)
		if err != nil {
			return nil, err
		}
		if nodeScopeID == scopeID {
			participants = append(participants, nodeID)
		}
	}
	return participants, nil
}

func SliceNodeIDsBetween(graph *Graph, anchorNodeIDs, targetNodeIDs []string, excludeNeedsReworkEdges bool) []string {
	if graph == nil || len(anchorNodeIDs) == 0 || len(targetNodeIDs) == 0 {
		return nil
	}
	forward := reachableNodeSetForward(graph, anchorNodeIDs, excludeNeedsReworkEdges)
	backward := reachableNodeSetBackward(graph, targetNodeIDs, excludeNeedsReworkEdges)
	if len(forward) == 0 || len(backward) == 0 {
		return nil
	}
	nodeIDs := make([]string, 0)
	for _, nodeID := range graph.Topological {
		if _, ok := forward[nodeID]; !ok {
			continue
		}
		if _, ok := backward[nodeID]; !ok {
			continue
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs
}

func validationLoopScopeID(graph *Graph, nodeID string) (string, error) {
	reviewAncestors, err := nearestAncestorNodeIDsByClass(graph, nodeID, NodeClassExecutionReview, true)
	if err != nil {
		return "", err
	}
	if len(reviewAncestors) > 0 {
		return "validation:review:" + strings.Join(reviewAncestors, "+"), nil
	}
	executionAncestors, err := nearestAncestorNodeIDsByClass(graph, nodeID, NodeClassDelegatedExecution, true)
	if err != nil {
		return "", err
	}
	if len(executionAncestors) > 0 {
		return "validation:execution:" + strings.Join(executionAncestors, "+"), nil
	}
	trimmedNodeID := strings.TrimSpace(nodeID)
	if trimmedNodeID == "" {
		return "", fmt.Errorf("node id is required")
	}
	return "validation:node:" + trimmedNodeID, nil
}

func validationParticipantNodeIDsForScope(graph *Graph, scopeID string) ([]string, error) {
	if graph == nil {
		return nil, fmt.Errorf("workflow graph is required")
	}
	participants := make([]string, 0)
	for _, nodeID := range graph.Topological {
		node, ok := graph.Nodes[nodeID]
		if !ok || ResolveNodeClass(node.NodeID, node.NodeClass) != NodeClassValidationGates {
			continue
		}
		nodeScopeID, err := validationLoopScopeID(graph, nodeID)
		if err != nil {
			return nil, err
		}
		if nodeScopeID == scopeID {
			participants = append(participants, nodeID)
		}
	}
	return participants, nil
}

func replayAnchorNodeIDsForParticipants(graph *Graph, participantNodeIDs []string) ([]string, error) {
	if graph == nil {
		return nil, fmt.Errorf("workflow graph is required")
	}
	anchorSet := make(map[string]struct{})
	for _, participantNodeID := range participantNodeIDs {
		ancestors, err := nearestAncestorNodeIDsByClass(graph, participantNodeID, NodeClassDelegatedExecution, true)
		if err != nil {
			return nil, err
		}
		for _, ancestorNodeID := range ancestors {
			anchorSet[ancestorNodeID] = struct{}{}
		}
	}
	anchors := make([]string, 0, len(anchorSet))
	for _, nodeID := range graph.Topological {
		if _, ok := anchorSet[nodeID]; ok {
			anchors = append(anchors, nodeID)
		}
	}
	return anchors, nil
}

func nearestAncestorNodeIDsByClass(graph *Graph, nodeID, classID string, excludeNeedsReworkEdges bool) ([]string, error) {
	if graph == nil {
		return nil, fmt.Errorf("workflow graph is required")
	}
	trimmedNodeID := strings.TrimSpace(nodeID)
	trimmedClassID := strings.TrimSpace(classID)
	if trimmedNodeID == "" || trimmedClassID == "" {
		return nil, fmt.Errorf("node id and class id are required")
	}
	if _, ok := graph.Nodes[trimmedNodeID]; !ok {
		return nil, fmt.Errorf("node %q not found in workflow graph", trimmedNodeID)
	}
	incoming := buildIncomingEdgeIndex(graph)
	type queueEntry struct {
		nodeID   string
		distance int
	}
	queue := []queueEntry{{nodeID: trimmedNodeID, distance: 0}}
	visited := map[string]int{trimmedNodeID: 0}
	bestDistance := -1
	ancestorSet := make(map[string]struct{})
	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]
		if bestDistance >= 0 && entry.distance > bestDistance {
			break
		}
		for _, edge := range incoming[entry.nodeID] {
			if excludeNeedsReworkEdges && strings.EqualFold(strings.TrimSpace(edge.On), "needs_rework") {
				continue
			}
			ancestorNode, ok := graph.Nodes[edge.From]
			if !ok {
				continue
			}
			distance := entry.distance + 1
			if ResolveNodeClass(ancestorNode.NodeID, ancestorNode.NodeClass) == trimmedClassID {
				bestDistance = distance
				ancestorSet[ancestorNode.NodeID] = struct{}{}
				continue
			}
			if bestDistance >= 0 && distance >= bestDistance {
				continue
			}
			if priorDistance, seen := visited[ancestorNode.NodeID]; seen && priorDistance <= distance {
				continue
			}
			visited[ancestorNode.NodeID] = distance
			queue = append(queue, queueEntry{nodeID: ancestorNode.NodeID, distance: distance})
		}
	}
	ancestors := make([]string, 0, len(ancestorSet))
	for nodeID := range ancestorSet {
		ancestors = append(ancestors, nodeID)
	}
	sort.Strings(ancestors)
	return ancestors, nil
}

func buildIncomingEdgeIndex(graph *Graph) map[string][]Edge {
	incoming := make(map[string][]Edge, len(graph.Nodes))
	for fromNodeID, edges := range graph.Outgoing {
		for _, edge := range edges {
			incoming[edge.To] = append(incoming[edge.To], Edge{From: fromNodeID, To: edge.To, On: edge.On})
		}
	}
	return incoming
}

func reachableNodeSetForward(graph *Graph, startNodeIDs []string, excludeNeedsReworkEdges bool) map[string]struct{} {
	reachable := make(map[string]struct{})
	queue := make([]string, 0, len(startNodeIDs))
	for _, rawNodeID := range startNodeIDs {
		nodeID := strings.TrimSpace(rawNodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := graph.Nodes[nodeID]; !ok {
			continue
		}
		if _, seen := reachable[nodeID]; seen {
			continue
		}
		reachable[nodeID] = struct{}{}
		queue = append(queue, nodeID)
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
	return reachable
}

func reachableNodeSetBackward(graph *Graph, startNodeIDs []string, excludeNeedsReworkEdges bool) map[string]struct{} {
	incoming := buildIncomingEdgeIndex(graph)
	reachable := make(map[string]struct{})
	queue := make([]string, 0, len(startNodeIDs))
	for _, rawNodeID := range startNodeIDs {
		nodeID := strings.TrimSpace(rawNodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := graph.Nodes[nodeID]; !ok {
			continue
		}
		if _, seen := reachable[nodeID]; seen {
			continue
		}
		reachable[nodeID] = struct{}{}
		queue = append(queue, nodeID)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, edge := range incoming[current] {
			if excludeNeedsReworkEdges && strings.EqualFold(strings.TrimSpace(edge.On), "needs_rework") {
				continue
			}
			if _, seen := reachable[edge.From]; seen {
				continue
			}
			reachable[edge.From] = struct{}{}
			queue = append(queue, edge.From)
		}
	}
	return reachable
}
