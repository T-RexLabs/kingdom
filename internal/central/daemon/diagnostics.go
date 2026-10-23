package daemon

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/google/uuid"
	"kingdom/internal/central/modelproxy"
)

type operatorOutput struct {
	mu     sync.Mutex
	writer io.Writer
}

func (o *operatorOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writer.Write(p)
}
func synchronizedOutput(out io.Writer) io.Writer {
	if out == nil {
		return nil
	}
	return &operatorOutput{writer: out}
}

// The fixed record is the complete operator contract. No upstream data or
// freeform error is accepted, including through explicit enum conversions.
func modelFailureLogger(out io.Writer) func(string, modelproxy.Failure) {
	return func(projectID string, f modelproxy.Failure) {
		id, err := uuid.Parse(projectID)
		if out == nil || err != nil || id == uuid.Nil || id.String() != projectID || !f.Valid() {
			return
		}
		record := struct {
			Event     string `json:"event"`
			ProjectID string `json:"project_id"`
			modelproxy.Failure
		}{Event: "model_proxy_failure", ProjectID: projectID, Failure: f}
		// Marshal before one atomic Write so a record cannot interleave with startup
		// output or another project's diagnostic. Logging never changes admission.
		data, err := json.Marshal(record)
		if err == nil {
			_, _ = out.Write(append(data, '\n'))
		}
	}
}
