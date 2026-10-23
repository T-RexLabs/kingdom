// Package modelproxy owns invocation-scoped inference capabilities. It does not
// activate a listener or authorize provider tools or workspace publication.
package modelproxy

import (
	"context"
	"crypto/x509"
	"errors"
	"net/netip"
	"time"

	"kingdom/internal/central/capability"
)

var ErrDenied = errors.New("model access denied")
var ErrInvalid = errors.New("invalid model proxy configuration")
var ErrOpen = errors.New("model access closure remains unresolved")

type Stage string

const (
	StagePrepare  Stage = "prepare"
	StageActivate Stage = "activate"
	StageActive   Stage = "active"
)

type Binding = capability.Binding
type Authorizer interface {
	Check(context.Context, Binding, Stage) error
}

// Route is selected by trusted metadata resolution, never an HTTP client.
type Route = capability.Route

const (
	ProtocolOpenAI          = capability.ProtocolOpenAI
	ProtocolOpenAIResponses = capability.ProtocolOpenAIResponses
	ProtocolOpenAIChat      = capability.ProtocolOpenAIChat
	ProtocolOpenAIChatGPT   = capability.ProtocolOpenAIChatGPT
	ProtocolAnthropic       = capability.ProtocolAnthropic
)

type Resolver interface {
	Pin(context.Context, Binding) (Route, error)
	Resolve(context.Context, Binding, Route) (Credential, error)
}
type Credential struct {
	token   string
	copilot bool
}

func (c Credential) String() string               { return "[credential]" }
func (c Credential) GoString() string             { return "[credential]" }
func (c Credential) MarshalJSON() ([]byte, error) { return nil, ErrDenied }
func NewBearerCredential(token string) (Credential, error) {
	if len(token) == 0 || len(token) > 16384 {
		return Credential{}, ErrDenied
	}
	for _, r := range token {
		if r <= 32 || r >= 127 {
			return Credential{}, ErrDenied
		}
	}
	return Credential{token: token}, nil
}

// NewCopilotCredential binds the fixed Copilot request profile to a trusted
// credential resolution. Neither callers nor model bodies provide headers.
func NewCopilotCredential(token string) (Credential, error) {
	credential, err := NewBearerCredential(token)
	if err != nil {
		return Credential{}, err
	}
	credential.copilot = true
	return credential, nil
}

type Lease struct {
	capability.Lease
	BaseURL string
	// Protocol is pinned by the trusted resolver before capability issuance.
	Protocol string
}

// OriginPolicy permits one exact HTTPS origin. Non-public addresses require an
// explicit operator address range; DNS never grants that exception implicitly.
type OriginPolicy struct {
	Origin                  string
	AllowedPrivateAddresses []netip.Prefix
}
type Config struct {
	Registry         *capability.Registry
	PublicBaseURL    string
	Authorizer       Authorizer
	Resolver         Resolver
	Origins          []OriginPolicy
	TLSRoots         *x509.CertPool
	RequestTimeout   time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
}
