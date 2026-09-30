package processguardian

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type wireSpec struct {
	Version       string  `json:"version"`
	GuardID       string  `json:"guard_id"`
	Capability    string  `json:"capability"`
	RuntimeRoot   string  `json:"runtime_root"`
	Durable       bool    `json:"durable"`
	CancelGraceMS int64   `json:"cancel_grace_ms"`
	CancelForceMS int64   `json:"cancel_force_ms"`
	Command       Command `json:"command"`
}

type wireStatus struct {
	Type      string    `json:"type"`
	Ownership Ownership `json:"ownership,omitempty"`
	Result    Result    `json:"result,omitempty"`
	Error     string    `json:"error,omitempty"`
}

func newRandomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func writeFrame(w io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > maxWireBytes {
		return fmt.Errorf("guardian frame length %d is invalid", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readFrame(r io.Reader, value any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > maxWireBytes {
		return fmt.Errorf("guardian frame length %d is invalid", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.More() {
		return fmt.Errorf("guardian frame contains trailing JSON")
	}
	return nil
}

func readControl(control io.Reader) (byte, error) {
	var value [1]byte
	_, err := io.ReadFull(control, value[:])
	return value[0], err
}

func validateWireSpec(spec wireSpec) error {
	if spec.Version != "v1" {
		return fmt.Errorf("unsupported guardian wire version %q", spec.Version)
	}
	if len(spec.GuardID) != 32 || len(spec.Capability) != 64 {
		return fmt.Errorf("invalid guardian identity")
	}
	if strings.TrimSpace(spec.Command.Path) == "" {
		return fmt.Errorf("guardian command path is required")
	}
	if spec.CancelGraceMS <= 0 || spec.CancelForceMS <= 0 {
		return fmt.Errorf("guardian cancellation periods must be positive")
	}
	return nil
}
