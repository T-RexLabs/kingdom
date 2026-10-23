// Package publishedresult defines the bounded, immutable result exposed to
// trusted publication adapters. It contains no host paths or credentials.
package publishedresult

import (
	"context"
	"errors"
	"io"
)

var ErrUnavailable = errors.New("published result unavailable")

const MaxFiles = 32768
const MaxFileBytes int64 = 64 << 20
const MaxTotalBytes int64 = 1 << 30

type ResultFile struct {
	Path   string `json:"path"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type ResultSnapshot struct {
	RunID          string       `json:"run_id"`
	SnapshotID     string       `json:"snapshot_id"`
	ManifestSHA256 string       `json:"manifest_sha256"`
	Files          []ResultFile `json:"files"`
}

// Reader checks canonical publication eligibility on every access. Consumers
// must retain opened bytes privately until their full size and digest verify.
type Reader interface {
	Resolve(context.Context, string) (ResultSnapshot, error)
	Open(context.Context, string, string, string) (io.ReadCloser, error)
}
