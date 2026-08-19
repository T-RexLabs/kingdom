package api

import (
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	githubexternal "kingdom/internal/external/github"
)

const deviceFlowMaxRetained = 32

var (
	errDeviceFlowNotFound = errors.New("github device flow not found")
	errDeviceFlowStage    = errors.New("github device flow is not in the required stage")
	errDeviceFlowPaced    = errors.New("github device flow poll is too early")
	errDeviceFlowCapacity = errors.New("too many active github device flows")
)

type deviceFlowStage string

const (
	deviceStageAwaitingAuthorization deviceFlowStage = "awaiting_authorization"
	deviceStageDiscovering           deviceFlowStage = "discovering_installations"
	deviceStageSelecting             deviceFlowStage = "selecting_installation"
	deviceStageNeedsInstallation     deviceFlowStage = "needs_installation"
	deviceStageFinalizing            deviceFlowStage = "finalizing"
	deviceStageCompleted             deviceFlowStage = "completed"
	deviceStageDenied                deviceFlowStage = "access_denied"
	deviceStageExpired               deviceFlowStage = "expired"
	deviceStageFailed                deviceFlowStage = "failed"
)

type githubDeviceFlow struct {
	ID, SessionID, Origin, DeviceCode, UserCode, VerificationURI string
	ReconnectConnectionID                                        string
	Stage                                                        deviceFlowStage
	CreatedAt, ExpiresAt, RetryAt, TerminalAt                    time.Time
	Interval                                                     time.Duration
	Tokens                                                       githubexternal.UserTokens
	Candidates                                                   []githubexternal.InstallationCandidate
	ConnectionID, Failure                                        string
	InFlight                                                     bool
}

type deviceFlowStatus struct {
	FlowID          string                                 `json:"flow_id"`
	Stage           deviceFlowStage                        `json:"stage"`
	UserCode        string                                 `json:"user_code,omitempty"`
	VerificationURI string                                 `json:"verification_uri,omitempty"`
	ExpiresAt       time.Time                              `json:"expires_at"`
	IntervalSeconds int                                    `json:"interval_seconds"`
	RetryAt         *time.Time                             `json:"retry_at,omitempty"`
	InstallURL      string                                 `json:"install_url,omitempty"`
	Candidates      []githubexternal.InstallationCandidate `json:"candidates"`
	ConnectionID    string                                 `json:"connection_id,omitempty"`
	Failure         string                                 `json:"failure,omitempty"`
}

type deviceFlowManager struct {
	mu          sync.Mutex
	now         func() time.Time
	maxRetained int
	flows       map[string]*githubDeviceFlow
}

func newDeviceFlowManager(now func() time.Time) *deviceFlowManager {
	if now == nil {
		now = time.Now
	}
	return &deviceFlowManager{now: now, maxRetained: deviceFlowMaxRetained, flows: map[string]*githubDeviceFlow{}}
}

func (m *deviceFlowManager) Create(sessionID, origin, reconnectID string, sessionExpiresAt time.Time, authorization githubexternal.DeviceAuthorization) (*githubDeviceFlow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	if len(m.flows) >= m.maxRetained && !m.evictOldestTerminalLocked() {
		return nil, errDeviceFlowCapacity
	}
	id, err := randomToken(24)
	if err != nil {
		return nil, err
	}
	now := m.now().UTC()
	if sessionExpiresAt.IsZero() || !sessionExpiresAt.After(now) {
		return nil, errDeviceFlowNotFound
	}
	expiresAt := authorization.ExpiresAt
	if sessionExpiresAt.Before(expiresAt) {
		expiresAt = sessionExpiresAt
	}
	flow := &githubDeviceFlow{ID: id, SessionID: sessionID, Origin: origin, DeviceCode: authorization.DeviceCode, UserCode: authorization.UserCode, VerificationURI: authorization.VerificationURI, ReconnectConnectionID: strings.TrimSpace(reconnectID), Stage: deviceStageAwaitingAuthorization, CreatedAt: now, ExpiresAt: expiresAt, RetryAt: now.Add(authorization.Interval), Interval: authorization.Interval}
	m.flows[id] = flow
	copy := *flow
	return &copy, nil
}

type devicePollLease struct {
	FlowID, DeviceCode string
	ExpiresAt          time.Time
}

func (m *deviceFlowManager) BeginPoll(id, sessionID, origin string) (devicePollLease, *time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.Stage != deviceStageAwaitingAuthorization {
		return devicePollLease{}, nil, errDeviceFlowStage
	}
	now := m.now().UTC()
	if now.Before(flow.RetryAt) {
		retry := flow.RetryAt
		return devicePollLease{}, &retry, errDeviceFlowPaced
	}
	flow.Stage = deviceStageDiscovering
	flow.InFlight = true
	return devicePollLease{FlowID: flow.ID, DeviceCode: flow.DeviceCode, ExpiresAt: flow.ExpiresAt}, nil, nil
}

func (m *deviceFlowManager) CompletePoll(id string, result githubexternal.DevicePollResult, candidates []githubexternal.InstallationCandidate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[id]
	if flow == nil || flow.Stage != deviceStageDiscovering {
		return errDeviceFlowStage
	}
	flow.InFlight = false
	now := m.now().UTC()
	switch result.Status {
	case githubexternal.DevicePollPending:
		flow.Stage = deviceStageAwaitingAuthorization
		flow.RetryAt = now.Add(flow.Interval)
	case githubexternal.DevicePollSlowDown:
		flow.Interval += 5 * time.Second
		if flow.Interval > time.Minute {
			flow.Interval = time.Minute
		}
		flow.Stage = deviceStageAwaitingAuthorization
		flow.RetryAt = now.Add(flow.Interval)
	case githubexternal.DevicePollAuthorized:
		flow.DeviceCode, flow.UserCode = "", ""
		flow.Tokens = result.Tokens
		flow.Candidates = append([]githubexternal.InstallationCandidate(nil), candidates...)
		if len(candidates) == 0 {
			flow.Stage = deviceStageNeedsInstallation
		} else {
			flow.Stage = deviceStageSelecting
		}
	case githubexternal.DevicePollDenied:
		flow.Stage = deviceStageDenied
	case githubexternal.DevicePollExpired:
		flow.Stage = deviceStageExpired
	case githubexternal.DevicePollDisabled:
		flow.Stage, flow.Failure = deviceStageFailed, "GitHub device authorization is not enabled for this App."
	default:
		flow.Stage, flow.Failure = deviceStageFailed, "GitHub authorization returned an invalid result."
	}
	if flow.terminal() {
		flow.TerminalAt = now
		flow.clearSecrets()
		m.pruneLocked()
	}
	return nil
}

type deviceCandidateLease struct {
	AccessToken string
	ExpiresAt   time.Time
}

func (m *deviceFlowManager) BeginCandidateRefresh(id, sessionID, origin string) (deviceCandidateLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.Stage != deviceStageNeedsInstallation && flow.Stage != deviceStageSelecting || flow.Tokens.AccessToken == "" {
		return deviceCandidateLease{}, errDeviceFlowStage
	}
	flow.Stage = deviceStageDiscovering
	flow.InFlight = true
	return deviceCandidateLease{AccessToken: flow.Tokens.AccessToken, ExpiresAt: flow.ExpiresAt}, nil
}

func (m *deviceFlowManager) CompleteCandidateRefresh(id string, candidates []githubexternal.InstallationCandidate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[id]
	if flow == nil || flow.Stage != deviceStageDiscovering || flow.Tokens.AccessToken == "" {
		return errDeviceFlowStage
	}
	flow.InFlight = false
	flow.Candidates = append([]githubexternal.InstallationCandidate(nil), candidates...)
	if len(candidates) == 0 {
		flow.Stage = deviceStageNeedsInstallation
	} else {
		flow.Stage = deviceStageSelecting
	}
	return nil
}

type deviceFinalizeLease struct {
	FlowID, ReconnectConnectionID string
	Tokens                        githubexternal.UserTokens
	ExpiresAt                     time.Time
}

// BeginFinalize consumes the flow for one connection covering every selected
// installation. The anti-spoof guard is that each requested id appears in the
// freshly fetched candidate list; the display name is the operator's label for
// the connection, so it is no longer matched against a candidate.
func (m *deviceFlowManager) BeginFinalize(id, sessionID, origin string, installationIDs []string, fresh []githubexternal.InstallationCandidate) (deviceFinalizeLease, []githubexternal.InstallationCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil {
		return deviceFinalizeLease{}, nil, errDeviceFlowStage
	}
	if flow.Stage != deviceStageSelecting || !flow.InFlight || flow.Tokens.AccessToken == "" || len(installationIDs) == 0 {
		return deviceFinalizeLease{}, nil, errDeviceFlowStage
	}
	byID := make(map[string]githubexternal.InstallationCandidate, len(fresh))
	for _, candidate := range fresh {
		byID[candidate.InstallationID] = candidate
	}
	selected := make([]githubexternal.InstallationCandidate, 0, len(installationIDs))
	seen := make(map[string]struct{}, len(installationIDs))
	for _, installationID := range installationIDs {
		candidate, ok := byID[installationID]
		if !ok {
			flow.InFlight = false
			return deviceFinalizeLease{}, nil, errDeviceFlowStage
		}
		if _, duplicate := seen[installationID]; duplicate {
			continue
		}
		seen[installationID] = struct{}{}
		selected = append(selected, candidate)
	}
	lease := deviceFinalizeLease{FlowID: flow.ID, ReconnectConnectionID: flow.ReconnectConnectionID, Tokens: flow.Tokens, ExpiresAt: flow.ExpiresAt}
	flow.Tokens = githubexternal.UserTokens{}
	flow.Candidates = nil
	flow.Stage = deviceStageFinalizing
	return lease, selected, nil
}

func (m *deviceFlowManager) AccessTokenForFinalize(id, sessionID, origin string) (deviceCandidateLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.Stage != deviceStageSelecting || flow.InFlight || flow.Tokens.AccessToken == "" {
		return deviceCandidateLease{}, errDeviceFlowStage
	}
	flow.InFlight = true
	return deviceCandidateLease{AccessToken: flow.Tokens.AccessToken, ExpiresAt: flow.ExpiresAt}, nil
}

func (m *deviceFlowManager) EndFinalizeVerification(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if flow := m.flows[id]; flow != nil && flow.Stage == deviceStageSelecting {
		flow.InFlight = false
	}
}

func (m *deviceFlowManager) Finalize(id, connectionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[id]
	if flow == nil || flow.Stage != deviceStageFinalizing {
		return errDeviceFlowStage
	}
	flow.ConnectionID, flow.Stage = connectionID, deviceStageCompleted
	flow.InFlight = false
	flow.TerminalAt = m.now().UTC()
	flow.clearSecrets()
	m.pruneLocked()
	return nil
}

func (m *deviceFlowManager) Fail(id, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if flow := m.flows[id]; flow != nil && flow.Stage != deviceStageCompleted {
		flow.InFlight = false
		flow.Failure, flow.Stage = strings.TrimSpace(message), deviceStageFailed
		flow.TerminalAt = m.now().UTC()
		flow.clearSecrets()
		m.pruneLocked()
	}
}

func (m *deviceFlowManager) Status(id, sessionID, origin, slug string) (deviceFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil {
		return deviceFlowStatus{}, err
	}
	return statusFromDeviceFlow(flow, slug), nil
}

func statusFromDeviceFlow(flow *githubDeviceFlow, slug string) deviceFlowStatus {
	status := deviceFlowStatus{FlowID: flow.ID, Stage: flow.Stage, ExpiresAt: flow.ExpiresAt, IntervalSeconds: int(flow.Interval / time.Second), Candidates: append([]githubexternal.InstallationCandidate(nil), flow.Candidates...), ConnectionID: flow.ConnectionID, Failure: flow.Failure}
	if flow.Stage == deviceStageAwaitingAuthorization {
		status.UserCode, status.VerificationURI = flow.UserCode, flow.VerificationURI
		retry := flow.RetryAt
		status.RetryAt = &retry
	}
	// Installing on a further account stays reachable while choosing among existing
	// installations, not only when the operator has none at all.
	if flow.Stage == deviceStageNeedsInstallation || flow.Stage == deviceStageSelecting {
		status.InstallURL = githubexternal.GitHubWebOrigin + "/apps/" + url.PathEscape(slug) + "/installations/new"
	}
	return status
}

func (m *deviceFlowManager) boundLocked(id, sessionID, origin string) (*githubDeviceFlow, error) {
	flow := m.flows[id]
	if flow == nil || flow.SessionID != sessionID || flow.Origin != origin || !flow.ExpiresAt.After(m.now().UTC()) {
		return nil, errDeviceFlowNotFound
	}
	return flow, nil
}

func (m *deviceFlowManager) pruneLocked() {
	now := m.now().UTC()
	for id, flow := range m.flows {
		if flow.InFlight {
			continue
		}
		if flow.ExpiresAt.After(now) {
			continue
		}
		flow.clearSecrets()
		delete(m.flows, id)
	}
	for len(m.flows) > m.maxRetained {
		if !m.evictOldestTerminalLocked() {
			return
		}
	}
}

func (f *githubDeviceFlow) terminal() bool {
	return f.Stage == deviceStageCompleted || f.Stage == deviceStageDenied || f.Stage == deviceStageExpired || f.Stage == deviceStageFailed
}

func (f *githubDeviceFlow) clearSecrets() {
	f.DeviceCode, f.UserCode = "", ""
	f.Tokens = githubexternal.UserTokens{}
	f.Candidates = nil
}

func (m *deviceFlowManager) evictOldestTerminalLocked() bool {
	var oldest *githubDeviceFlow
	for _, flow := range m.flows {
		if !flow.terminal() {
			continue
		}
		if oldest == nil || deviceTerminalLess(flow, oldest) {
			oldest = flow
		}
	}
	if oldest == nil {
		return false
	}
	oldest.clearSecrets()
	delete(m.flows, oldest.ID)
	return true
}

func deviceTerminalLess(left, right *githubDeviceFlow) bool {
	leftAt, rightAt := left.TerminalAt, right.TerminalAt
	if leftAt.IsZero() {
		leftAt = left.CreatedAt
	}
	if rightAt.IsZero() {
		rightAt = right.CreatedAt
	}
	if !leftAt.Equal(rightAt) {
		return leftAt.Before(rightAt)
	}
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	return left.ID < right.ID
}
