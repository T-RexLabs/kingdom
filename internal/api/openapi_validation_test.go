package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	gen "kingdom/internal/apigen"
)

func TestStreamingJSONValidationPreservesTypedConstraints(t *testing.T) {
	var schema openapi3.Schema
	if err := json.Unmarshal([]byte(`{"type":"object","additionalProperties":false,"required":["text","count"],"properties":{"text":{"type":"string","minLength":1,"maxLength":3},"count":{"type":"integer","minimum":1},"mode":{"type":"string","enum":["safe"]},"pattern":{"type":"string","pattern":"^a+$"},"unique":{"type":"array","uniqueItems":true,"items":{"type":"string"}}}}`), &schema); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"text":"abc","count":1,"mode":"safe","pattern":"aaa","unique":["a","b"]}`, true},
		{`{"text":"\ud83d\ude00","count":2}`, true},
		{`{"text":"é中","count":1}`, true},
		{`{"text":"�","count":1}`, true},
		{`{"text":"abcd","count":1}`, false},
		{`{"text":"","count":1}`, false},
		{`{"text":1,"count":1}`, false},
		{`{"text":"a","count":1.2}`, false},
		{`{"text":"a","count":0}`, false},
		{`{"text":"a","count":1,"mode":"unsafe"}`, false},
		{`{"text":"a","count":1,"pattern":"b"}`, false},
		{`{"text":"a","count":1,"unique":["a","a"]}`, false},
		{`{"text":"a","count":1,"unknown":true}`, false},
		{`{"text":"a"}`, false},
		{`{"text":"a","text":"b","count":1}`, false},
		{`{"text":"\uZZZZ","count":1}`, false},
		{`{"text":"\q","count":1}`, false},
		{`{"text":"a","count":1,}`, false},
		{`{"text":"a","count":1} {}`, false},
		{`{"text":"a","count":1e}`, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			parser := &schemaJSONReader{reader: bufio.NewReader(strings.NewReader(tc.body))}
			value, err := parser.value(&schema, true)
			if err == nil {
				if _, e := parser.nonspace(); e != io.EOF {
					err = fmt.Errorf("trailing JSON")
				}
			}
			if err == nil {
				err = schema.VisitJSON(value, openapi3.VisitAsRequest())
			}
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
		})
	}
}

func TestNewOpenAPIRequestValidator(t *testing.T) {
	t.Parallel()

	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatalf("expected validator constructor to succeed: %v", err)
	}
	if validator == nil {
		t.Fatal("expected validator middleware function")
	}
}

func TestLoadOpenAPISpecIgnoresWorkingDirectoryFiles(t *testing.T) {
	tempDir := t.TempDir()
	contractsDir := filepath.Join(tempDir, "contracts", "openapi")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatalf("create contracts directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "openapi.yaml"), []byte("openapi: not valid"), 0o644); err != nil {
		t.Fatalf("write invalid openapi spec: %v", err)
	}

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if chdirErr := os.Chdir(originalWD); chdirErr != nil {
			t.Fatalf("restore working directory: %v", chdirErr)
		}
	})

	swagger, err := loadOpenAPISpec()
	if err != nil {
		t.Fatalf("expected generated spec to load regardless of cwd files: %v", err)
	}
	if swagger == nil {
		t.Fatal("expected generated swagger document")
	}
	if swagger.Components.Schemas["CreateRunRequest"] == nil {
		t.Fatal("expected generated swagger schema content")
	}
}

func TestOpenAPISpecRequiresModelSelectionOnCreateRun(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	schemaRef := swagger.Components.Schemas["CreateRunRequest"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("expected CreateRunRequest schema")
	}
	if !containsRequiredField(schemaRef.Value.Required, "model_selection") {
		t.Fatalf("expected CreateRunRequest to require model_selection, got %v", schemaRef.Value.Required)
	}
	if !containsRequiredField(schemaRef.Value.Required, "publish_mode") {
		t.Fatalf("expected CreateRunRequest to require publish_mode, got %v", schemaRef.Value.Required)
	}
	if containsRequiredField(schemaRef.Value.Required, "workflow_id") {
		t.Fatalf("expected CreateRunRequest to stop requiring workflow_id, got %v", schemaRef.Value.Required)
	}
	if _, ok := schemaRef.Value.Properties["skip_reviews_and_validations"]; !ok {
		t.Fatalf("expected CreateRunRequest to expose skip_reviews_and_validations")
	}
}

func TestOpenAPISpecRequiresGlobalModelSelectionEntry(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	schemaRef := swagger.Components.Schemas["RunModelSelection"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("expected RunModelSelection schema")
	}
	if !containsRequiredField(schemaRef.Value.Required, "global") {
		t.Fatalf("expected RunModelSelection to require global, got %v", schemaRef.Value.Required)
	}
}

func TestOpenAPISpecPlacesPlannerResumeHeaderOnAcpEventsEndpoint(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	getOperation := mustGetOperation(t, swagger, "/acp/runs/{runId}")
	if operationHasParameterRef(getOperation, "#/components/parameters/LastEventId") {
		t.Fatal("expected GetAcpRun to omit Last-Event-ID")
	}

	eventsOperation := mustGetOperation(t, swagger, "/acp/runs/{runId}/events")
	if !operationHasParameterRef(eventsOperation, "#/components/parameters/LastEventId") {
		t.Fatal("expected streamAcpRunEvents to accept Last-Event-ID")
	}

	streamParamsType := reflect.TypeOf(gen.StreamAcpRunEventsParams{})
	if _, ok := streamParamsType.FieldByName("LastEventID"); !ok {
		t.Fatal("expected StreamAcpRunEventsParams to include LastEventID")
	}
}

func TestOpenAPISpecExcludesExecutablePreviewRoutes(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	for _, path := range []string{"/artifacts/{id}/preview-session", "/preview/{token}", "/preview/{token}/{assetPath}"} {
		if swagger.Paths.Find(path) != nil {
			t.Fatalf("executable preview route %s remained in the spec", path)
		}
	}
	for _, parameter := range []string{"PreviewToken", "PreviewAssetPath"} {
		if swagger.Components.Parameters[parameter] != nil {
			t.Fatalf("executable preview parameter %s remained in the spec", parameter)
		}
	}
}

func TestOpenAPIAuthenticatedMutationsExposeCSRFHeader(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	for path, item := range swagger.Paths.Map() {
		for method, operation := range item.Operations() {
			if path == "/auth/session" || !isMutatingMethod(strings.ToUpper(method)) {
				continue
			}
			if !operationHasParameterRef(operation, "#/components/parameters/CSRFToken") {
				t.Errorf("%s %s does not expose the optional browser CSRF header", strings.ToUpper(method), path)
			}
		}
	}
}

func TestOpenAPIGitHubCallbacksArePublicAndFlowMutationsRequireIdempotency(t *testing.T) {
	t.Parallel()
	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/connections/github/manifest-flows/manifest-callback",
		"/connections/github/manifest-flows/setup-callback",
		"/connections/github/manifest-flows/oauth-callback",
	} {
		operation := mustGetOperation(t, swagger, path)
		if operation.Security == nil || len(*operation.Security) != 0 {
			t.Fatalf("GET %s must override top-level security with []", path)
		}
	}
	for _, path := range []string{
		"/connections/github/manifest-flows",
		"/connections/github/manifest-flows/{id}/conversion",
		"/connections/github/manifest-flows/{id}/installation",
		"/connections/github/manifest-flows/{id}/authorization",
		"/connections/github/manifest-flows/{id}/completion",
		"/connections/github/device-flows",
		"/connections/github/device-flows/{id}/poll",
		"/connections/github/device-flows/{id}/candidates",
		"/connections/github/device-flows/{id}/finalize",
	} {
		item := swagger.Paths.Find(path)
		if item == nil || item.Post == nil {
			t.Fatalf("missing POST %s", path)
		}
		if !operationHasParameterRef(item.Post, "#/components/parameters/IdempotencyKey") {
			t.Fatalf("setup POST %s must require idempotency", path)
		}
	}
	deviceItem := swagger.Paths.Find("/connections/github/device-flows/{id}")
	if deviceItem == nil || deviceItem.Delete == nil || !operationHasParameterRef(deviceItem.Delete, "#/components/parameters/IdempotencyKey") {
		t.Fatal("device-flow discard must be declared and require idempotency")
	}
}

func TestOpenAPIDeviceFlowStatusIncludesSafeStartingReservation(t *testing.T) {
	t.Parallel()
	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	schemaRef := swagger.Components.Schemas["GitHubDeviceFlowStatus"]
	if schemaRef == nil || schemaRef.Value == nil {
		t.Fatal("missing GitHubDeviceFlowStatus schema")
	}
	stage := schemaRef.Value.Properties["stage"]
	if stage == nil || stage.Value == nil {
		t.Fatal("missing device flow stage schema")
	}
	if !slices.Contains(stage.Value.Enum, any("starting_authorization")) {
		t.Fatalf("device flow stages=%v", stage.Value.Enum)
	}
	interval := schemaRef.Value.Properties["interval_seconds"]
	if interval == nil || interval.Value == nil || interval.Value.Min == nil || *interval.Value.Min != 0 {
		t.Fatalf("starting reservation interval schema=%#v", interval)
	}
}

func TestOpenAPISecuredOperationsDeclareCanonicalValidationAndAuthErrors(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	missingValidation := make([]string, 0)
	missingAuthentication := make([]string, 0)
	for _, operation := range listOperations(swagger) {
		if operation.isPublic {
			continue
		}
		responses := operation.operation.Responses
		if responses == nil || responses.Status(400) == nil {
			missingValidation = append(missingValidation, operation.label())
		}
		if responses == nil || responses.Status(401) == nil {
			missingAuthentication = append(missingAuthentication, operation.label())
		}
	}

	if len(missingValidation) > 0 {
		sort.Strings(missingValidation)
		t.Fatalf("expected secured operations to declare 400 canonical error responses, missing: %s", strings.Join(missingValidation, ", "))
	}
	if len(missingAuthentication) > 0 {
		sort.Strings(missingAuthentication)
		t.Fatalf("expected secured operations to declare 401 canonical error responses, missing: %s", strings.Join(missingAuthentication, ", "))
	}
}

func TestOpenAPISpecKeepsTypedDecreeBody(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}
	path := swagger.Paths.Find("/decrees")
	if path == nil || path.Post == nil {
		t.Fatal("expected POST /decrees operation")
	}
	operation := path.Post
	if operation.RequestBody == nil || operation.RequestBody.Value == nil || !operation.RequestBody.Value.Required {
		t.Fatal("decree request body must remain required")
	}
	content := operation.RequestBody.Value.Content.Get("application/json")
	if content == nil || content.Schema == nil || content.Schema.Value == nil {
		t.Fatal("decree JSON schema is missing")
	}
	if err := content.Schema.Value.VisitJSON(map[string]any{"decree_title": "test", "decree_text": 42}); err == nil {
		t.Fatal("decree text must remain a typed string")
	}
}

func TestOpenAPISpecIncludesCanonicalExamplesForCoreSchemas(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}

	for _, schemaName := range []string{
		"ErrorEnvelope",
		"CreateDecreeRequest",
		"CreateDecreeResult",
		"PlanningSessionDetail",
		"AcpRun",
		"CreateRunRequest",
		"RunSummary",
		"RunDetailSummary",
		"RunModelSelection",
		"WorkspaceCapabilities",
	} {
		schemaRef := swagger.Components.Schemas[schemaName]
		if schemaRef == nil || schemaRef.Value == nil {
			t.Fatalf("expected %s schema", schemaName)
		}
		if schemaRef.Value.Example == nil {
			t.Fatalf("expected %s schema to define an example", schemaName)
		}
	}
}

func TestOpenAPIErrorEnvelopeDefinesOptionalRetryAt(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}
	envelope := swagger.Components.Schemas["ErrorEnvelope"]
	if envelope == nil || envelope.Value == nil {
		t.Fatal("expected ErrorEnvelope schema")
	}
	errorProperty := envelope.Value.Properties["error"]
	if errorProperty == nil || errorProperty.Value == nil {
		t.Fatal("expected ErrorEnvelope.error schema")
	}
	retryAt := errorProperty.Value.Properties["retry_at"]
	if retryAt == nil || retryAt.Value == nil || retryAt.Value.Format != "date-time" {
		t.Fatalf("expected retry_at date-time property, got %#v", retryAt)
	}
	if containsRequiredField(errorProperty.Value.Required, "retry_at") {
		t.Fatal("retry_at must remain optional")
	}
}

func TestOpenAPIExternalSourceStatusMatchesPersistedEnum(t *testing.T) {
	t.Parallel()

	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatalf("load swagger: %v", err)
	}
	status := swagger.Components.Schemas["ExternalSourceStatus"]
	if status == nil || status.Value == nil {
		t.Fatal("expected reusable ExternalSourceStatus schema")
	}
	want := []any{"never_checked", "unchanged", "changed", "inaccessible_or_missing", "deleted", "rate_limited", "connection_disabled", "service_unavailable", "revoked", "permission_mismatch", "timeout", "malformed_response", "oversized_response"}
	if !reflect.DeepEqual(status.Value.Enum, want) {
		t.Fatalf("ExternalSourceStatus enum=%v, want %v", status.Value.Enum, want)
	}
	for _, schemaName := range []string{"ExternalLink", "ExternalProvenanceSummary"} {
		property := swagger.Components.Schemas[schemaName].Value.Properties["status"]
		if property == nil || property.Ref != "#/components/schemas/ExternalSourceStatus" {
			t.Fatalf("%s.status does not reuse ExternalSourceStatus: %#v", schemaName, property)
		}
	}
}

func TestOpenAPIPlanningSourceAttachHasExactMutationContract(t *testing.T) {
	t.Parallel()
	swagger, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	item := swagger.Paths.Find("/planning-sessions/{id}/external-sources")
	if item == nil || item.Post == nil {
		t.Fatal("expected planning source attach POST operation")
	}
	operation := item.Post
	if operation.OperationID != "AttachPlanningSessionExternalSource" {
		t.Fatalf("attach operation id=%q", operation.OperationID)
	}
	for _, ref := range []string{"#/components/parameters/PlanningSessionId", "#/components/parameters/IdempotencyKey", "#/components/parameters/CSRFToken"} {
		if !operationHasParameterRef(operation, ref) {
			t.Fatalf("planning source attach is missing %s", ref)
		}
	}
	if operation.RequestBody == nil || operation.RequestBody.Value == nil || operation.RequestBody.Value.Content.Get("application/json") == nil || operation.RequestBody.Value.Content.Get("application/json").Schema == nil || operation.RequestBody.Value.Content.Get("application/json").Schema.Ref != "#/components/schemas/ExternalIntake" {
		t.Fatalf("planning source attach request body is not ExternalIntake: %#v", operation.RequestBody)
	}
	created := operation.Responses.Status(http.StatusCreated)
	if created == nil || created.Value == nil || created.Value.Content.Get("application/json") == nil || created.Value.Content.Get("application/json").Schema == nil || created.Value.Content.Get("application/json").Schema.Ref != "#/components/schemas/ExternalProvenanceSummary" {
		t.Fatalf("planning source attach response is not ExternalProvenanceSummary: %#v", created)
	}
}

func containsRequiredField(required []string, field string) bool {
	for _, candidate := range required {
		if candidate == field {
			return true
		}
	}
	return false
}

func mustGetOperation(t *testing.T, swagger *openapi3.T, path string) *openapi3.Operation {
	t.Helper()

	pathItem := swagger.Paths.Find(path)
	if pathItem == nil || pathItem.Get == nil {
		t.Fatalf("expected GET %s in the spec", path)
	}
	return pathItem.Get
}

func operationHasParameterRef(operation *openapi3.Operation, ref string) bool {
	for _, parameter := range operation.Parameters {
		if parameter != nil && parameter.Ref == ref {
			return true
		}
	}
	return false
}

type specOperation struct {
	path      string
	method    string
	operation *openapi3.Operation
	isPublic  bool
}

func (o specOperation) label() string {
	return fmt.Sprintf("%s %s", o.method, o.path)
}

func listOperations(swagger *openapi3.T) []specOperation {
	if swagger == nil || swagger.Paths == nil {
		return nil
	}

	operations := make([]specOperation, 0, swagger.Paths.Len())
	for path, item := range swagger.Paths.Map() {
		if item == nil {
			continue
		}
		appendOperation := func(method string, operation *openapi3.Operation) {
			if operation == nil {
				return
			}
			isPublic := operation.Security != nil && len(*operation.Security) == 0
			operations = append(operations, specOperation{
				path:      path,
				method:    method,
				operation: operation,
				isPublic:  isPublic,
			})
		}
		appendOperation("DELETE", item.Delete)
		appendOperation("GET", item.Get)
		appendOperation("PATCH", item.Patch)
		appendOperation("POST", item.Post)
		appendOperation("PUT", item.Put)
	}

	return operations
}
