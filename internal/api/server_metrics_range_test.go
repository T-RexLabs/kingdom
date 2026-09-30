package api

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
)

func TestAnalyticsCalendarRangeBoundsEveryAggregate(t *testing.T) {
	t.Parallel()
	handler, db := newAPIServerForTest(t)
	for _, role := range []string{"builder", "reviewer"} {
		mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, spawn_depth, created_at, updated_at) VALUES (?, ?, 'idle', 0, '2024-04-01T00:00:00Z', '2024-04-01T00:00:00Z')`, "calendar-"+role, role)
	}
	stamps := []string{"2024-04-01T10:00:00.1Z", "2024-04-01T12:00:00.25+02:00", "2024-04-01T11:00:00.5Z", "2024-04-01T12:00:00.75Z", "2024-04-01T12:00:00.9Z"}
	runIDs := make([]string, len(stamps))
	for i, stamp := range stamps {
		runID := seedMetricsRun(t, handler, fmt.Sprintf("calendar-%d", i), "Calendar range")
		runIDs[i] = runID
		status := "failed"
		if i == 1 {
			status = "succeeded"
		}
		mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = ?, created_at = ?, updated_at = ? WHERE run_id = ?`, status, stamp, stamp, runID)
		var definition string
		if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&definition); err != nil {
			t.Fatal(err)
		}
		// Incidence is scoped by the run's creation time, not the review timestamp.
		for _, class := range []string{"execution_review", "validation_gates"} {
			id := fmt.Sprintf("%s-%d", class, i)
			mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (?, ?, ?, 'reviewer', ?, ?)`, id, definition, id, class, stamp)
			failed := "failed"
			if (class == "execution_review" && i == 2) || (class == "validation_gates" && i == 1) {
				failed = "succeeded"
			}
			mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id) VALUES (?, ?, ?, 1, 1, ?, 'reviewer', 'calendar-reviewer')`, id, runID, id, failed)
		}
	}
	// Resource timestamps are deliberately attached to an out-of-window run.
	// Selecting an interval must scope each resource itself, not its parent run.
	var definition string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runIDs[0]).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	for i, stamp := range stamps {
		id := fmt.Sprintf("calendar-node-%d", i)
		started, _ := time.Parse(time.RFC3339Nano, stamp)
		duration := time.Duration(i+1) * time.Minute
		mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (?, ?, ?, 'builder', 'delegated_execution', ?)`, id, definition, id, stamp)
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES (?, ?, ?, 1, 1, 'succeeded', 'builder', 'calendar-builder', ?, ?)`, id, runIDs[0], id, stamp, started.Add(duration).Format(time.RFC3339Nano))
		mustExecSQL(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES (?, ?, ?, ?, 'builder', ?, 'none', 10, 2, 15, 1, 3, 4, ?)`, id, runIDs[0], id, id, fmt.Sprintf("model-%d", i), stamp)
		mustExecSQL(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (?, ?, ?, ?, 'deliverable', 'file.txt', 'text/plain', 'file.txt', 'hash', 100, 'builder', ?)`, id, runIDs[0], id, id, stamp)
	}
	query := url.Values{"start_at": {"2024-04-01T12:00:00.25+02:00"}, "end_at": {"2024-04-01T12:00:00.75Z"}}.Encode()
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/analytics/snapshot?" + query})
	if response.Code != http.StatusOK {
		t.Fatalf("snapshot status %d: %s", response.Code, response.Body.String())
	}
	var snapshot gen.AnalyticsSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Overview.TotalRuns != 2 || snapshot.Overview.SucceededRuns != 1 || snapshot.Overview.FailedRuns != 1 || snapshot.Overview.SuccessRate != 50 || snapshot.Overview.ReviewReworkRate != 50 || snapshot.Overview.ValidationReworkRate != 50 {
		t.Fatalf("out-of-range runs leaked into overview: %+v", snapshot.Overview)
	}
	if len(snapshot.RunAnalytics.RecentRuns) != 2 {
		t.Fatalf("recent runs: %+v", snapshot.RunAnalytics.RecentRuns)
	}
	outcomes := 0
	for _, bucket := range snapshot.RunAnalytics.OutcomeSeries {
		outcomes += bucket.Succeeded + bucket.Failed + bucket.Cancelled
	}
	if outcomes != 2 {
		t.Fatalf("outcome count = %d", outcomes)
	}
	if len(snapshot.RunAnalytics.DurationByRole) != 1 || math.Abs(float64(snapshot.RunAnalytics.DurationByRole[0].AvgDurationSeconds)-150) > .01 {
		t.Fatalf("duration: %+v", snapshot.RunAnalytics.DurationByRole)
	}
	var bounds map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &bounds); err != nil {
		t.Fatal(err)
	}
	if bounds["window_start"] != "2024-04-01T10:00:00.25Z" || bounds["window_end"] != "2024-04-01T12:00:00.75Z" || snapshot.WindowMinutes != 121 || snapshot.SnapshotAt.Year() == 2024 {
		t.Fatalf("incorrect returned interval: %v", bounds)
	}
	for _, selected := range []bool{false, true} {
		path := "/api/v1/analytics/execution-detail?" + query
		count := 2
		if selected {
			path += "&run_id=" + runIDs[0]
			count = 5
		}
		response = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if response.Code != http.StatusOK {
			t.Fatalf("detail status %d: %s", response.Code, response.Body.String())
		}
		var detail gen.AnalyticsExecutionDetailSnapshot
		if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}
		d := detail.ExecutionDetail
		if d.Nodes.TotalExecuted != count || d.Artifacts.TotalCount != count || d.Artifacts.TotalSizeBytes != count*100 || d.Tokens.Input != count*10 || d.Tokens.Output != count*2 || d.Tokens.Reasoning != count || d.Tokens.CacheRead != count*3 || d.Tokens.CacheWrite != count*4 || d.Tokens.ProviderTotalTokens != count*15 {
			t.Fatalf("selected=%v detail=%+v", selected, d)
		}
		if len(d.Tokens.ByRole) != 1 || len(d.Tokens.ByRole[0].ByModel) != count {
			t.Fatalf("role/model breakdown contains out-of-window data: %+v", d.Tokens.ByRole)
		}
	}
}

func TestAnalyticsCalendarRangeValidation(t *testing.T) {
	t.Parallel()
	handler, _ := newAPIServerForTest(t)
	for _, endpoint := range []string{"snapshot", "execution-detail"} {
		for _, query := range []string{
			"start_at=2024-01-01T00:00:00Z", "end_at=2024-01-01T00:00:00Z",
			"start_at=bad&end_at=2024-01-01T00:00:00Z", "start_at=2024-01-01&end_at=2024-01-02",
			"start_at=2024-01-01T00:00:00Z&end_at=2024-01-01T00:00:00Z",
			"start_at=2024-01-02T00:00:00Z&end_at=2024-01-01T00:00:00Z",
			"start_at=2024-01-01T00:00:00Z&end_at=2024-01-02T00:00:00Z&window_minutes=60",
			"start_at=&end_at=2024-01-02T00:00:00Z",
		} {
			t.Run(endpoint+"/"+query, func(t *testing.T) {
				response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/analytics/" + endpoint + "?" + query})
				if response.Code != http.StatusBadRequest {
					t.Fatalf("expected 400, got %d: %s", response.Code, response.Body.String())
				}
			})
		}
		for _, query := range []string{
			"start_at=2040-01-01T00:00:00Z&end_at=2040-01-02T00:00:00Z",
			"start_at=2024-01-01T00:00:00Z&end_at=2025-01-01T00:00:00Z",
			"start_at=1900-01-01T00:00:00Z&end_at=9999-01-01T00:00:00Z",
		} {
			response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/analytics/" + endpoint + "?" + query})
			if response.Code != http.StatusOK {
				t.Fatalf("valid future or long calendar intervals should be usable: %d %s", response.Code, response.Body.String())
			}
			if query == "start_at=1900-01-01T00:00:00Z&end_at=9999-01-01T00:00:00Z" {
				var payload struct {
					WindowMinutes int64 `json:"window_minutes"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
				end := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
				if want := (end.Unix() - start.Unix()) / 60; payload.WindowMinutes != want {
					t.Fatalf("long calendar duration=%d, want %d", payload.WindowMinutes, want)
				}
			}
		}
	}
}
