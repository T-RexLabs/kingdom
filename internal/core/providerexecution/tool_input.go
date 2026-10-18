package providerexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

// CanonicalToolInput normalizes one provider tool input object while preserving
// numbers exactly, so durable admissions and projected tools compare equal.
func CanonicalToolInput(raw json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return "", fmt.Errorf("provider tool input must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", fmt.Errorf("provider tool input must contain one JSON value")
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func ToolInputDigest(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
