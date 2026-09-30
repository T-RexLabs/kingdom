package executionoutput

import (
	"io"
	"strings"
	"testing"
)

func TestStreamingVerdictChecksEntireUnboundedResponse(t *testing.T) {
	for _, test := range []struct {
		suffix string
		valid  bool
	}{{"", true}, {"\nVERDICT: PASS", false}, {"\nVERDICT: REWORK", false}} {
		reader := io.MultiReader(strings.NewReader("\u2003\n VERDICT:"), strings.NewReader(strings.Repeat(" ", 5*1024*1024)), strings.NewReader("PASS\u2003\n"), strings.NewReader(strings.Repeat("x", 9*1024*1024)), strings.NewReader(test.suffix))
		result, err := ValidateReader("review", "execution_review", reader, "{}")
		if (err == nil) != test.valid {
			t.Fatalf("suffix=%q err=%v", test.suffix, err)
		}
		if err == nil && (result.CanonicalText != "" || result.StructuredPayloadJSON != `{"verdict":"PASS"}`) {
			t.Fatalf("streaming result=%+v", result)
		}
	}
}
