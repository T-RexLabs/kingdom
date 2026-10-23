package modelproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"mime"
	"net"
	"strings"
)

// FailurePhase identifies a fixed model proxy failure boundary.
type FailurePhase string

const (
	PhaseTransport        FailurePhase = "transport"
	PhaseUpstreamStatus   FailurePhase = "upstream_status"
	PhaseResponseContract FailurePhase = "response_contract"
)

type TransportClass string

const (
	TransportTimeout  TransportClass = "timeout"
	TransportTLS      TransportClass = "tls"
	TransportNetwork  TransportClass = "network"
	TransportCanceled TransportClass = "canceled"
	TransportDenied   TransportClass = "denied"
)

type ContractReason string

const (
	ContractMediaType       ContractReason = "media_type"
	ContractContentEncoding ContractReason = "content_encoding"
	ContractSizeLimit       ContractReason = "size_limit"
)

type MediaType string

const (
	MediaMissing     MediaType = "missing"
	MediaInvalid     MediaType = "invalid"
	MediaJSON        MediaType = "json"
	MediaSSE         MediaType = "sse"
	MediaHTML        MediaType = "html"
	MediaPlain       MediaType = "plain"
	MediaOctetStream MediaType = "octet_stream"
	MediaNDJSON      MediaType = "ndjson"
	MediaProblemJSON MediaType = "problem_json"
	MediaOther       MediaType = "other"
)

type ContentEncoding string

const (
	EncodingGzip     ContentEncoding = "gzip"
	EncodingBrotli   ContentEncoding = "br"
	EncodingDeflate  ContentEncoding = "deflate"
	EncodingZstd     ContentEncoding = "zstd"
	EncodingIdentity ContentEncoding = "identity"
	EncodingOther    ContentEncoding = "other"
)

// Failure excludes errors, bodies, headers, endpoints, and credential material.
type Failure struct {
	Phase           FailurePhase    `json:"phase"`
	UpstreamStatus  int             `json:"upstream_status,omitempty"`
	TransportClass  TransportClass  `json:"transport_class,omitempty"`
	ContractReason  ContractReason  `json:"contract_reason,omitempty"`
	ContentEncoding ContentEncoding `json:"content_encoding,omitempty"`
	MediaType       MediaType       `json:"media_type,omitempty"`
}

// Valid also bounds data at an operator sink, including explicit enum casts.
func (f Failure) Valid() bool {
	switch f.Phase {
	case PhaseTransport:
		if f.UpstreamStatus != 0 || f.ContractReason != "" || f.ContentEncoding != "" || f.MediaType != "" {
			return false
		}
		switch f.TransportClass {
		case TransportTimeout, TransportTLS, TransportNetwork, TransportCanceled, TransportDenied:
			return true
		}
	case PhaseUpstreamStatus:
		return f.TransportClass == "" && f.ContractReason == "" && f.ContentEncoding == "" && f.MediaType == "" && f.UpstreamStatus >= 100 && f.UpstreamStatus <= 599 && (f.UpstreamStatus < 200 || f.UpstreamStatus >= 300)
	case PhaseResponseContract:
		if f.TransportClass != "" || f.UpstreamStatus < 200 || f.UpstreamStatus > 299 {
			return false
		}
		switch f.ContractReason {
		case ContractMediaType:
			if f.ContentEncoding != "" {
				return false
			}
			switch f.MediaType {
			case MediaMissing, MediaInvalid, MediaJSON, MediaSSE, MediaHTML, MediaPlain, MediaOctetStream, MediaNDJSON, MediaProblemJSON, MediaOther:
				return true
			}
		case ContractSizeLimit:
			return f.ContentEncoding == "" && f.MediaType == ""
		case ContractContentEncoding:
			if f.MediaType != "" {
				return false
			}
			switch f.ContentEncoding {
			case EncodingGzip, EncodingBrotli, EncodingDeflate, EncodingZstd, EncodingIdentity, EncodingOther:
				return true
			}
		}
	}
	return false
}

func classifyTransport(err error) TransportClass {
	if errors.Is(err, context.DeadlineExceeded) {
		return TransportTimeout
	}
	if errors.Is(err, context.Canceled) {
		return TransportCanceled
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return TransportTimeout
	}
	var verification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var invalidCertificate x509.CertificateInvalidError
	var hostname x509.HostnameError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	if errors.As(err, &verification) || errors.As(err, &unknownAuthority) || errors.As(err, &invalidCertificate) || errors.As(err, &hostname) || errors.As(err, &record) || errors.As(err, &alert) {
		return TransportTLS
	}
	if errors.Is(err, ErrDenied) || errors.Is(err, ErrInvalid) {
		return TransportDenied
	}
	return TransportNetwork
}

func (b *Broker) reportFailure(f Failure) {
	if b.cfg.OnFailure != nil && f.Valid() {
		b.cfg.OnFailure(f)
	}
}

func classifyEncoding(value string) ContentEncoding {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "gzip":
		return EncodingGzip
	case "br":
		return EncodingBrotli
	case "deflate":
		return EncodingDeflate
	case "zstd":
		return EncodingZstd
	case "identity":
		return EncodingIdentity
	default:
		return EncodingOther
	}
}

func classifyMediaType(value string) MediaType {
	if strings.TrimSpace(value) == "" {
		return MediaMissing
	}
	media, _, err := mime.ParseMediaType(value)
	if err != nil {
		return MediaInvalid
	}
	switch media {
	case "application/json":
		return MediaJSON
	case "text/event-stream":
		return MediaSSE
	case "text/html":
		return MediaHTML
	case "text/plain":
		return MediaPlain
	case "application/octet-stream":
		return MediaOctetStream
	case "application/x-ndjson", "application/ndjson":
		return MediaNDJSON
	case "application/problem+json":
		return MediaProblemJSON
	default:
		return MediaOther
	}
}
