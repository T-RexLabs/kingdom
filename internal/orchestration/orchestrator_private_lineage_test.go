package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"kingdom/internal/providerlimits"
	"kingdom/internal/runtimefs"
)

func TestInspectionInputLineageSurvivesSnapshotCompaction(t *testing.T) {
	loop, run, source := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	const at = "2026-09-30T12:00:00Z"
	parent := "input"
	var latest runtimefs.SnapshotRecord
	for n := 1; n <= providerlimits.SnapshotLineageDepth; n++ {
		id := fmt.Sprintf("builder-result-%d", n)
		var err error
		latest, err = loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: id, RunID: run.RunID, ParentSnapshotID: parent, SnapshotKind: "attempt_result", RootPath: source, CreatedAt: at})
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, loop.db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_builder_%d','run_1','builder',%d,1,'succeeded','governance','agent_governance',1)`, n+1, n+1))
		mustExec(t, loop.db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,base_snapshot_id,result_snapshot_id,created_at,updated_at) VALUES ('run_1:builder:%d','run_1','nr_builder_%d','committed_success','provider_backed','server_response','%s','%s','%s','%s')`, n+1, n+1, parent, id, at, at))
		parent = id
	}
	if latest.ParentSnapshotID != "" {
		t.Fatalf("fixture did not cross a physical manifest compaction boundary: %+v", latest)
	}
	// The join receives both a previous inspection's pinned input and the newer
	// builder result. Compaction of storage overlays cannot make them divergent.
	mustExec(t, loop.db, `UPDATE node_runs SET node_status='succeeded' WHERE node_id='review'`)
	mustExec(t, loop.db, `UPDATE attempt_executions SET state='committed_success',base_snapshot_id='input' WHERE attempt_id='run_1:review:1'`)
	mustExec(t, loop.db, `INSERT INTO workflow_edges (workflow_edge_id,workflow_definition_id,from_node_id,to_node_id,on_condition,created_at) VALUES ('builder_to_qa','wfdef_1','builder','qa','success','2026-09-30T12:00:00Z')`)
	got, err := loop.inspectionInputSnapshot(ctx, run, nodeDefinition{NodeID: "qa", NodeClass: "validation_gates"})
	if err != nil || got != latest.SnapshotID {
		t.Fatalf("inspection input across compaction = %q, want %q; %v", got, latest.SnapshotID, err)
	}
	if contains, err := loop.snapshotContainsAncestor(ctx, "unrelated-run", latest.SnapshotID, "input"); err != nil || contains {
		t.Fatalf("lineage crossed run boundary: contains=%v err=%v", contains, err)
	}
	rows, err := loop.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+snapshotCausalLineageQuery, latest.SnapshotID, run.RunID, run.RunID, "input")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	indexedLookup := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		t.Log(detail)
		if strings.Contains(detail, "USING COVERING INDEX idx_attempt_executions_snapshot_lineage (run_id=? AND result_snapshot_id=?)") {
			indexedLookup = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !indexedLookup {
		t.Fatal("causal lineage does not use an indexed result-snapshot lookup")
	}
}

func TestInspectionRejectsDivergentInputLineage(t *testing.T) {
	loop, run, source := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	if _, err := loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "independent-input", RunID: run.RunID, SnapshotKind: "baseline", RootPath: source, CreatedAt: "2026-09-30T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	mustExec(t, loop.db, `UPDATE node_runs SET node_status='succeeded' WHERE node_id='review'`)
	mustExec(t, loop.db, `UPDATE attempt_executions SET state='committed_success',base_snapshot_id='independent-input' WHERE attempt_id='run_1:review:1'`)
	mustExec(t, loop.db, `INSERT INTO workflow_edges (workflow_edge_id,workflow_definition_id,from_node_id,to_node_id,on_condition,created_at) VALUES ('builder_to_qa','wfdef_1','builder','qa','success','2026-09-30T12:00:00Z')`)
	if _, err := loop.inspectionInputSnapshot(ctx, run, nodeDefinition{NodeID: "qa", NodeClass: "validation_gates"}); err == nil || !strings.Contains(err.Error(), "divergent snapshot lineage") {
		t.Fatalf("unrelated same-content input was accepted: %v", err)
	}
}

func TestStandaloneInspectionWaitsForTrustedTargetInput(t *testing.T) {
	loop, run, source := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	mustExec(t, loop.db, `DELETE FROM workflow_edges`)
	if _, err := loop.fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: run.RunID, TargetMode: "direct", ExecutionRoot: source, SourceRoot: source, PublishRoot: source, TrustedSnapshotID: "input", State: "attempt_dirty", CreatedAt: "2026-09-30T12:00:00Z", UpdatedAt: "2026-09-30T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	review := nodeDefinition{NodeID: "review", NodeClass: "execution_review"}
	if _, err := loop.inspectionInputSnapshot(ctx, run, review); !errors.Is(err, errWorkspaceBusy) {
		t.Fatalf("standalone inspection accepted a dirty target: %v", err)
	}
	if err := loop.fsStore.UpdateExecutionTargetState(ctx, run.RunID, "trusted", "input", "", "2026-09-30T12:01:00Z"); err != nil {
		t.Fatal(err)
	}
	if got, err := loop.inspectionInputSnapshot(ctx, run, review); err != nil || got != "input" {
		t.Fatalf("trusted fallback input = %q, %v", got, err)
	}
}
