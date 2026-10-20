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

// ToolInputDigests digests each top-level argument of a tool input. The server
// projects the arguments the model sent, while a tool only sees the arguments
// its schema keeps, so admissions and projections are compared per argument.
func ToolInputDigests(raw json.RawMessage) (map[string]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var arguments map[string]any
	if err := decoder.Decode(&arguments); err != nil || arguments == nil {
		return nil, fmt.Errorf("provider tool input must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("provider tool input must contain one JSON value")
	}
	digests := make(map[string]string, len(arguments))
	for name, value := range arguments {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(encoded)
		digests[name] = hex.EncodeToString(sum[:])
	}
	return digests, nil
}

// ToolInputsAgree reports whether an admitted input and a projected input
// describe the same call: every argument they share is identical and, unless
// the admitted input is empty, they share at least one.
func ToolInputsAgree(admitted, projected map[string]string) bool {
	shared := 0
	for name, digest := range admitted {
		if other, ok := projected[name]; ok {
			if other != digest {
				return false
			}
			shared++
		}
	}
	return shared > 0 || len(admitted) == 0
}
