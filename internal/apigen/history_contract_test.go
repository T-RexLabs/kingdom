package apigen

import "testing"

func TestHistoryCollectionsExposeContinuation(t *testing.T) {
	spec, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/planning-sessions", "/runs/{id}/mailbox/messages", "/runs/{id}/plan-reviews",
		"/planning-sessions/{id}/context/{snapshotId}/entries",
	} {
		operation := spec.Paths.Value(path).Get
		parameters := map[string]bool{}
		for _, parameter := range operation.Parameters {
			parameters[parameter.Value.Name] = true
		}
		if !parameters["cursor"] || !parameters["limit"] {
			t.Fatalf("%s lacks paging parameters", path)
		}
		response := operation.Responses.Status(200).Value.Content["application/json"].Schema.Value
		if response.Properties["next_cursor"] == nil || response.Properties["items"] == nil {
			t.Fatalf("%s lacks a continuation envelope", path)
		}
	}
	summary := spec.Components.Schemas["PlanningContextSnapshot"].Value
	if summary.Properties["included_files"] != nil || summary.Properties["excluded_files"] != nil {
		t.Fatal("context summary contains unbounded entry arrays")
	}
}

func TestContentCeilingsRemovedButInstallationLimitsPreserved(t *testing.T) {
	spec, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range [][2]string{
		{"ExternalComment", "retained_bytes"}, {"ExternalSnapshotResponse", "retained_comment_count"},
		{"ExternalSnapshotResponse", "retained_comment_bytes"}, {"ArtifactResponse", "size_bytes"},
	} {
		if spec.Components.Schemas[field[0]].Value.Properties[field[1]].Value.Max != nil {
			t.Fatalf("obsolete ceiling on %s.%s", field[0], field[1])
		}
	}
	for _, field := range [][2]string{{"IssuePreview", "comments"}, {"RunArtifactsResponse", "items"}} {
		if spec.Components.Schemas[field[0]].Value.Properties[field[1]].Value.MaxItems != nil {
			t.Fatalf("obsolete collection ceiling on %s.%s", field[0], field[1])
		}
	}
	for _, field := range [][2]string{{"NormalizedIssue", "title"}, {"NormalizedIssue", "body"}} {
		if spec.Components.Schemas[field[0]].Value.Properties[field[1]].Value.MaxLength != nil {
			t.Fatalf("obsolete text ceiling on %s.%s", field[0], field[1])
		}
	}
	for _, field := range []struct {
		schema, property string
		limit            uint64
	}{
		{"Connection", "installations", 500}, {"SetConnectionInstallationsRequest", "installation_ids", 500},
		{"FinalizeGitHubDeviceFlowRequest", "installation_ids", 500},
		{"GitHubDeviceFlowStatus", "candidates", 1000},
	} {
		limit := spec.Components.Schemas[field.schema].Value.Properties[field.property].Value.MaxItems
		if limit == nil || *limit != field.limit {
			t.Fatalf("installation limit changed on %s.%s", field.schema, field.property)
		}
	}
	candidates := spec.Paths.Value("/connections/{id}/installation-candidates").Get.Responses.Status(200).Value.Content["application/json"].Schema.Value.Properties["items"].Value
	if candidates.MaxItems == nil || *candidates.MaxItems != 1000 {
		t.Fatal("installation candidate response limit changed")
	}
	for _, schemaName := range []string{"CreateRunRequest", "RunSummary"} {
		schema := spec.Components.Schemas[schemaName].Value
		for _, property := range []string{"max_execution_review_cycles", "max_validation_rework_cycles"} {
			field := schema.Properties[property].Value
			if field.Min == nil || *field.Min != 0 {
				t.Fatalf("%s.%s must preserve explicit zero loop caps", schemaName, property)
			}
		}
		if schema.Properties["run_until_all_approve"] == nil || schema.Properties["autonomous_approvals"] == nil {
			t.Fatalf("%s lost governance controls", schemaName)
		}
	}
	if spec.Paths.Value("/decrees").Post.Responses.Status(413) != nil {
		t.Fatal("obsolete decree request-body ceiling response")
	}
}
