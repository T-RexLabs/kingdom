package providerexecution

import (
	"context"
	"encoding/json"
	"io"
	"unicode/utf8"
)

// WriteJSONString streams one JSON string without splitting UTF-8 sequences
// between independently escaped fragments. It matches encoding/json escaping,
// including HTML characters and U+2028/U+2029.
func WriteJSONString(ctx context.Context, w io.Writer, r io.Reader) error {
	if _, err := io.WriteString(w, `"`); err != nil {
		return err
	}
	buffer := make([]byte, ContentChunkBytes+utf8.UTFMax)
	carry := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := r.Read(buffer[carry:ContentChunkBytes])
		n += carry
		end := n
		if readErr == nil && n > 0 {
			start := n - 1
			for start > 0 && !utf8.RuneStart(buffer[start]) {
				start--
			}
			if !utf8.FullRune(buffer[start:n]) {
				end = start
			}
		}
		if end > 0 {
			raw, err := json.Marshal(string(buffer[:end]))
			if err != nil {
				return err
			}
			if _, err := w.Write(raw[1 : len(raw)-1]); err != nil {
				return err
			}
		}
		carry = copy(buffer, buffer[end:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	_, err := io.WriteString(w, `"`)
	return err
}
