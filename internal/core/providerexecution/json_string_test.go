package providerexecution

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

type shortJSONReader struct {
	io.Reader
	n int
}

func (r shortJSONReader) Read(p []byte) (int, error) { return r.Reader.Read(p[:min(len(p), r.n)]) }
func TestWriteJSONStringPreservesSplitUTF8AndEscapes(t *testing.T) {
	for _, width := range []int{1, 3, 64 * 1024} {
		for _, offset := range []int{1, 2, 3} {
			text := strings.Repeat("x", ContentChunkBytes-offset) + "😀界\u2028\u2029 <>&\"\\\n\t"
			var output bytes.Buffer
			if err := WriteJSONString(context.Background(), &output, shortJSONReader{strings.NewReader(text), width}); err != nil {
				t.Fatal(err)
			}
			want, _ := json.Marshal(text)
			if !bytes.Equal(output.Bytes(), want) {
				t.Fatalf("JSON string changed at width=%d offset=%d", width, offset)
			}
		}
	}
}
