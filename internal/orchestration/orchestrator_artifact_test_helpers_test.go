package orchestration

import (
	"context"
	"sort"
	"strings"
)

// Small fixture adapters keep filesystem projection tests concise. Runtime
// publication uses replayable canonical metadata sources, never these slices.
func artifactProjectionSlice(artifacts []artifactOutputRecord) artifactProjectionSource {
	artifacts = append([]artifactOutputRecord(nil), artifacts...)
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].ArtifactID < artifacts[j].ArtifactID })
	return func(ctx context.Context, visit func(artifactOutputRecord) error) error {
		for _, artifact := range artifacts {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(artifact); err != nil {
				return err
			}
		}
		return nil
	}
}

func publicationAttemptID(artifacts []artifactOutputRecord) string {
	if len(artifacts) == 0 {
		return ""
	}
	prefix := strings.TrimSpace(artifacts[0].ArtifactID)
	idx := strings.LastIndex(prefix, "_artifact_")
	if idx <= 0 {
		return ""
	}
	return prefix[:idx]
}

func (o *orchestrationLoop) publishArtifactsToProjectRoot(ctx context.Context, workspacePath, runID string, artifacts []artifactOutputRecord) error {
	if len(artifacts) == 0 {
		return nil
	}
	return o.publishArtifactProjection(ctx, workspacePath, nil, runID, o.publishMode, publicationAttemptID(artifacts), artifactProjectionSlice(artifacts))
}

func (o *orchestrationLoop) publishArtifactsDirectWithTracking(ctx context.Context, workspacePath string, inline map[string][]byte, runID, targetRoot, artifactRoot string, artifacts []artifactOutputRecord) error {
	return o.publishArtifactSourceDirect(ctx, workspacePath, inline, runID, publicationAttemptID(artifacts), targetRoot, artifactRoot, artifactProjectionSlice(artifacts))
}
