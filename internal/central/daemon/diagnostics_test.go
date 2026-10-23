package daemon

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/central/modelproxy"
)

func TestModelFailureOperatorOutputIsBoundedAndAtomic(t *testing.T) {
	var output bytes.Buffer
	report := modelFailureLogger(synchronizedOutput(&output))
	project := uuid.NewString()
	secret := "private-upstream-secret"
	report(secret, modelproxy.Failure{Phase: modelproxy.PhaseUpstreamStatus, UpstreamStatus: 429})
	report(project, modelproxy.Failure{Phase: modelproxy.FailurePhase(secret)})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseTransport, TransportClass: modelproxy.TransportClass(secret)})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseUpstreamStatus, UpstreamStatus: 200})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseUpstreamStatus, UpstreamStatus: 999})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseResponseContract, UpstreamStatus: 200, ContractReason: modelproxy.ContractReason(secret)})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseResponseContract, UpstreamStatus: 200, ContractReason: modelproxy.ContractContentEncoding, ContentEncoding: modelproxy.ContentEncoding(secret)})
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseResponseContract, UpstreamStatus: 200, ContractReason: modelproxy.ContractMediaType, MediaType: modelproxy.MediaType(secret)})
	if output.Len() != 0 {
		t.Fatal("unbounded diagnostic admitted")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report(project, modelproxy.Failure{Phase: modelproxy.PhaseUpstreamStatus, UpstreamStatus: 429})
		}()
	}
	wg.Wait()
	if strings.Contains(output.String(), secret) {
		t.Fatal("secret reached operator output")
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 32 {
		t.Fatal("lost or interleaved diagnostic", len(lines))
	}
	for _, line := range lines {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil || len(event) != 4 || event["event"] != "model_proxy_failure" || event["project_id"] != project || event["phase"] != "upstream_status" || event["upstream_status"] != float64(429) {
			t.Fatal("unexpected diagnostic schema", line)
		}
	}
	modelFailureLogger(nil)(project, modelproxy.Failure{Phase: modelproxy.PhaseTransport, TransportClass: modelproxy.TransportTimeout})
}

func TestModelContractDiagnosticHasOnlyBoundedValues(t *testing.T) {
	var output bytes.Buffer
	report := modelFailureLogger(synchronizedOutput(&output))
	project := uuid.NewString()
	report(project, modelproxy.Failure{Phase: modelproxy.PhaseResponseContract, UpstreamStatus: 200, ContractReason: modelproxy.ContractContentEncoding, ContentEncoding: modelproxy.EncodingGzip})
	var event map[string]any
	if json.Unmarshal(output.Bytes(), &event) != nil || len(event) != 6 || event["contract_reason"] != "content_encoding" || event["content_encoding"] != "gzip" {
		t.Fatal("unexpected contract diagnostic", output.String())
	}
}

func TestModelMediaDiagnosticHasOnlyBoundedCategory(t *testing.T) {
	var output bytes.Buffer
	report := modelFailureLogger(synchronizedOutput(&output))
	report(uuid.NewString(), modelproxy.Failure{Phase: modelproxy.PhaseResponseContract, UpstreamStatus: 200, ContractReason: modelproxy.ContractMediaType, MediaType: modelproxy.MediaHTML})
	var event map[string]any
	if json.Unmarshal(output.Bytes(), &event) != nil || len(event) != 6 || event["contract_reason"] != "media_type" || event["media_type"] != "html" {
		t.Fatal("unexpected media diagnostic", output.String())
	}
}
