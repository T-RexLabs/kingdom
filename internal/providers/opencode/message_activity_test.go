package opencode

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestProviderTextMessageActivityPreservesExactBytesAndIdentity(t *testing.T) {
	text := "\n  " + strings.Repeat("界😀 response\n", 20000) + "final\n"
	encoded, _ := json.Marshal(text)
	wire := `{"type":"text","part":{"type":"text","id":"message-part","text":` + string(encoded) + `,"time":{"end":1781275082480}}}` + "\n"
	observed := false
	err := decodeOpencodeRunEventStream(strings.NewReader(wire), nil, func(event opencodeRunEvent) error {
		emitActivityObservation(ExecuteRequest{InvocationID: "inv-message", OnActivity: func(item ExecuteActivityObservation) {
			observed = true
			if item.Kind != "message" || item.ProviderPartID != "message-part" || item.InvocationID != "inv-message" {
				t.Fatalf("message identity lost: %+v", item)
			}
			content := item.ContentSources()["message"]
			if content.Availability != "available" || content.Open == nil {
				t.Fatalf("message bytes missing: %+v", content)
			}
			reader, err := content.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			actual, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(actual) != text {
				t.Fatalf("message changed: got %d bytes, want %d", len(actual), len(text))
			}
			if item.Reasoning.Normalized().Availability != "not_provided" {
				t.Fatal("ordinary text was mislabeled reasoning")
			}
		}}, event)
		return nil
	}, func(_ string, err error) { t.Fatal(err) })
	if err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("provider text was silently excluded from activity")
	}
}
