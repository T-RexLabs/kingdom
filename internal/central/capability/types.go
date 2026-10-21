// Package capability retains worker model/plugin capabilities. It is an
// admission registry, not canonical execution or membership authorization.
package capability

import (
	"context"
	"errors"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"sync"
	"time"
)

var ErrDenied = errors.New("worker capability denied")
var ErrInvalid = errors.New("invalid capability registry configuration")
var ErrOpen = errors.New("worker capability closure remains unresolved")

type Purpose string

const (
	Model  Purpose = "model"
	Plugin Purpose = "plugin"
)

type Mode int

const (
	ModeNew Mode = iota
	ModeReceipt
)

type Binding struct {
	Preparation providerrequest.Preparation
	Runtime     remote.Reference
	Access      access.Snapshot
	Request     *providerexecution.RequestIdentity
}
type Route struct{ Protocol, Endpoint, Model string }

// Pins are trusted, non-secret metadata. Workload requests cannot issue or alter them.
type Pins struct {
	Purpose Purpose
	Binding Binding
	Route   Route
}
type Config struct {
	DatabasePath string
	Lifetime     time.Duration
}
type Lease struct {
	ID        string
	ExpiresAt time.Time
	token     string
}

func (l Lease) Token() string    { return l.token }
func (l Lease) String() string   { return "[worker capability]" }
func (l Lease) GoString() string { return "[worker capability]" }

type Admission struct {
	Context context.Context
	Pins    Pins
	Request providerexecution.RequestIdentity
	once    sync.Once
	finish  func()
}

func (a *Admission) Done() {
	if a != nil {
		a.once.Do(a.finish)
	}
}
