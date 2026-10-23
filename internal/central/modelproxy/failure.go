package modelproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
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

// Failure excludes errors, bodies, headers, endpoints, and credential material.
type Failure struct {
	Phase          FailurePhase   `json:"phase"`
	UpstreamStatus int            `json:"upstream_status,omitempty"`
	TransportClass TransportClass `json:"transport_class,omitempty"`
}

// Valid also bounds data at an operator sink, including explicit enum casts.
func (f Failure) Valid() bool {
	switch f.Phase {
	case PhaseTransport:
		if f.UpstreamStatus != 0 {
			return false
		}
		switch f.TransportClass {
		case TransportTimeout, TransportTLS, TransportNetwork, TransportCanceled, TransportDenied:
			return true
		}
	case PhaseUpstreamStatus:
		return f.TransportClass == "" && f.UpstreamStatus >= 100 && f.UpstreamStatus <= 599 && (f.UpstreamStatus < 200 || f.UpstreamStatus >= 300)
	case PhaseResponseContract:
		return f.TransportClass == "" && f.UpstreamStatus >= 200 && f.UpstreamStatus <= 299
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
