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
	errDeviceFlowNotFound       = errors.New("github device flow not found")
	errDeviceFlowStage          = errors.New("github device flow is not in the required stage")
	errDeviceFlowPaced          = errors.New("github device flow poll is too early")
	errDeviceFlowCapacity       = errors.New("too many active github device flows")
	errDeviceFlowIntentConflict = errors.New("another github device flow intent is active for this browser binding")
	errDeviceFlowInFlight       = errors.New("github device flow work is in flight")
)

type deviceFlowStage string

const (
	deviceStageStarting              deviceFlowStage = "starting_authorization"
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
	LeaseGeneration                                              string
	LeaseKind                                                    deviceOperationKind
}

type deviceOperationKind string

const (
	deviceOperationStart                deviceOperationKind = "start"
	deviceOperationPoll                 deviceOperationKind = "poll"
	deviceOperationCandidateRefresh     deviceOperationKind = "candidate_refresh"
	deviceOperationFinalizeVerification deviceOperationKind = "finalize_verification"
	deviceOperationFinalize             deviceOperationKind = "finalize"
)

type deviceOperationLease struct {
	FlowID, Generation string
	Kind               deviceOperationKind
	ExpiresAt          time.Time
}

type deviceFlowStatus struct {
	FlowID                string                                 `json:"flow_id"`
	Stage                 deviceFlowStage                        `json:"stage"`
	UserCode              string                                 `json:"user_code,omitempty"`
	VerificationURI       string                                 `json:"verification_uri,omitempty"`
	ReconnectConnectionID string                                 `json:"reconnect_connection_id,omitempty"`
	ExpiresAt             time.Time                              `json:"expires_at"`
	IntervalSeconds       int                                    `json:"interval_seconds"`
	RetryAt               *time.Time                             `json:"retry_at,omitempty"`
	InstallURL            string                                 `json:"install_url,omitempty"`
	Candidates            []githubexternal.InstallationCandidate `json:"candidates"`
	ConnectionID          string                                 `json:"connection_id,omitempty"`
	Failure               string                                 `json:"failure,omitempty"`
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

// ReserveStart claims capacity and the browser binding before provider
// authorization begins. Repeating the same intent resumes the existing flow;
// a different intent cannot compete for the same browser session and origin.
func (m *deviceFlowManager) ReserveStart(sessionID, origin, reconnectID string, sessionExpiresAt time.Time) (*githubDeviceFlow, deviceOperationLease, bool, error) {
	sessionID, origin, reconnectID = strings.TrimSpace(sessionID), strings.TrimSpace(origin), strings.TrimSpace(reconnectID)
	if sessionID == "" || origin == "" {
		return nil, deviceOperationLease{}, false, errDeviceFlowNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	for _, flow := range m.flows {
		if flow.SessionID != sessionID || flow.Origin != origin || flow.terminal() {
			continue
		}
		if flow.ReconnectConnectionID != reconnectID {
			return nil, deviceOperationLease{}, false, errDeviceFlowIntentConflict
		}
		if flow.LeaseGeneration != "" && flow.LeaseKind == deviceOperationStart {
			return nil, deviceOperationLease{}, false, errDeviceFlowInFlight
		}
		copy := *flow
		copy.Candidates = append([]githubexternal.InstallationCandidate(nil), flow.Candidates...)
		return &copy, deviceOperationLease{}, false, nil
	}
	if len(m.flows) >= m.maxRetained && !m.evictOldestTerminalLocked() {
		return nil, deviceOperationLease{}, false, errDeviceFlowCapacity
	}
	id, err := randomToken(24)
	if err != nil {
		return nil, deviceOperationLease{}, false, err
	}
	generation, err := randomToken(24)
	if err != nil {
		return nil, deviceOperationLease{}, false, err
	}
	now := m.now().UTC()
	if sessionExpiresAt.IsZero() || !sessionExpiresAt.After(now) {
		return nil, deviceOperationLease{}, false, errDeviceFlowNotFound
	}
	flow := &githubDeviceFlow{ID: id, SessionID: sessionID, Origin: origin, ReconnectConnectionID: reconnectID, Stage: deviceStageStarting, CreatedAt: now, ExpiresAt: sessionExpiresAt, LeaseGeneration: generation, LeaseKind: deviceOperationStart}
	m.flows[id] = flow
	copy := *flow
	return &copy, deviceOperationLease{FlowID: id, Generation: generation, Kind: deviceOperationStart, ExpiresAt: flow.ExpiresAt}, true, nil
}

func (m *deviceFlowManager) CompleteStart(lease deviceOperationLease, authorization githubexternal.DeviceAuthorization) (*githubDeviceFlow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[lease.FlowID]
	if flow == nil || flow.Stage != deviceStageStarting || !flow.owns(lease, deviceOperationStart) {
		return nil, errDeviceFlowStage
	}
	now := m.now().UTC()
	expiresAt := authorization.ExpiresAt
	if flow.ExpiresAt.Before(expiresAt) {
		expiresAt = flow.ExpiresAt
	}
	if !expiresAt.After(now) {
		m.removeFlowLocked(flow.ID, flow)
		return nil, errDeviceFlowStage
	}
	flow.DeviceCode, flow.UserCode, flow.VerificationURI = authorization.DeviceCode, authorization.UserCode, authorization.VerificationURI
	flow.ExpiresAt, flow.Interval, flow.RetryAt = expiresAt, authorization.Interval, now.Add(authorization.Interval)
	flow.Stage = deviceStageAwaitingAuthorization
	flow.clearLease()
	copy := *flow
	return &copy, nil
}

func (m *deviceFlowManager) AbortStart(lease deviceOperationLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if flow := m.flows[lease.FlowID]; flow != nil && flow.Stage == deviceStageStarting && flow.owns(lease, deviceOperationStart) {
		m.removeFlowLocked(lease.FlowID, flow)
	}
}

type devicePollLease struct {
	deviceOperationLease
	DeviceCode string
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
	lease, err := m.beginLocked(flow, deviceOperationPoll)
	if err != nil {
		return devicePollLease{}, nil, err
	}
	return devicePollLease{deviceOperationLease: lease, DeviceCode: flow.DeviceCode}, nil, nil
}

func (m *deviceFlowManager) CompletePoll(lease devicePollLease, result githubexternal.DevicePollResult, candidates []githubexternal.InstallationCandidate, slug string) (deviceFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[lease.FlowID]
	if flow == nil || flow.Stage != deviceStageAwaitingAuthorization || !flow.owns(lease.deviceOperationLease, deviceOperationPoll) {
		return deviceFlowStatus{}, errDeviceFlowStage
	}
	flow.clearLease()
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
		status := statusFromDeviceFlow(flow, slug)
		m.pruneLocked()
		return status, nil
	}
	return statusFromDeviceFlow(flow, slug), nil
}

type deviceCandidateLease struct {
	deviceOperationLease
	AccessToken string
}

func (m *deviceFlowManager) BeginCandidateRefresh(id, sessionID, origin string) (deviceCandidateLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.Stage != deviceStageNeedsInstallation && flow.Stage != deviceStageSelecting || flow.Tokens.AccessToken == "" {
		return deviceCandidateLease{}, errDeviceFlowStage
	}
	if flow.LeaseGeneration != "" {
		return deviceCandidateLease{}, errDeviceFlowInFlight
	}
	lease, err := m.beginLocked(flow, deviceOperationCandidateRefresh)
	if err != nil {
		return deviceCandidateLease{}, err
	}
	return deviceCandidateLease{deviceOperationLease: lease, AccessToken: flow.Tokens.AccessToken}, nil
}

func (m *deviceFlowManager) CompleteCandidateRefresh(lease deviceCandidateLease, candidates []githubexternal.InstallationCandidate, slug string) (deviceFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[lease.FlowID]
	if flow == nil || flow.Stage != deviceStageNeedsInstallation && flow.Stage != deviceStageSelecting || flow.Tokens.AccessToken == "" || !flow.owns(lease.deviceOperationLease, deviceOperationCandidateRefresh) {
		return deviceFlowStatus{}, errDeviceFlowStage
	}
	flow.clearLease()
	flow.Candidates = append([]githubexternal.InstallationCandidate(nil), candidates...)
	if len(candidates) == 0 {
		flow.Stage = deviceStageNeedsInstallation
	} else {
		flow.Stage = deviceStageSelecting
	}
	return statusFromDeviceFlow(flow, slug), nil
}

type deviceFinalizeLease struct {
	deviceOperationLease
	ReconnectConnectionID string
	Tokens                githubexternal.UserTokens
}

// CompleteFinalizeVerification consumes the verified token for one connection
// covering every selected installation. Each requested id must appear in the
// freshly fetched candidate list.
func (m *deviceFlowManager) CompleteFinalizeVerification(verification deviceCandidateLease, installationIDs []string, fresh []githubexternal.InstallationCandidate) (deviceFinalizeLease, []githubexternal.InstallationCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[verification.FlowID]
	if flow == nil || flow.Stage != deviceStageSelecting || flow.Tokens.AccessToken == "" || !flow.owns(verification.deviceOperationLease, deviceOperationFinalizeVerification) {
		return deviceFinalizeLease{}, nil, errDeviceFlowStage
	}
	if len(installationIDs) == 0 {
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
			flow.clearLease()
			return deviceFinalizeLease{}, nil, errDeviceFlowStage
		}
		if _, duplicate := seen[installationID]; duplicate {
			continue
		}
		seen[installationID] = struct{}{}
		selected = append(selected, candidate)
	}
	lease, err := m.replaceLeaseLocked(flow, verification.deviceOperationLease, deviceOperationFinalizeVerification, deviceOperationFinalize)
	if err != nil {
		return deviceFinalizeLease{}, nil, err
	}
	finalize := deviceFinalizeLease{deviceOperationLease: lease, ReconnectConnectionID: flow.ReconnectConnectionID, Tokens: flow.Tokens}
	flow.Tokens = githubexternal.UserTokens{}
	flow.Candidates = nil
	flow.Stage = deviceStageFinalizing
	return finalize, selected, nil
}

func (m *deviceFlowManager) BeginFinalizeVerification(id, sessionID, origin string) (deviceCandidateLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.Stage != deviceStageSelecting || flow.LeaseGeneration != "" || flow.Tokens.AccessToken == "" {
		return deviceCandidateLease{}, errDeviceFlowStage
	}
	lease, err := m.beginLocked(flow, deviceOperationFinalizeVerification)
	if err != nil {
		return deviceCandidateLease{}, err
	}
	return deviceCandidateLease{deviceOperationLease: lease, AccessToken: flow.Tokens.AccessToken}, nil
}

func (m *deviceFlowManager) EndFinalizeVerification(lease deviceCandidateLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if flow := m.flows[lease.FlowID]; flow != nil && flow.Stage == deviceStageSelecting && flow.owns(lease.deviceOperationLease, deviceOperationFinalizeVerification) {
		flow.clearLease()
	}
}

func (m *deviceFlowManager) Finalize(lease deviceFinalizeLease, connectionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[lease.FlowID]
	if flow == nil || flow.Stage != deviceStageFinalizing || !flow.owns(lease.deviceOperationLease, deviceOperationFinalize) {
		return errDeviceFlowStage
	}
	flow.ConnectionID, flow.Stage = connectionID, deviceStageCompleted
	flow.clearLease()
	flow.TerminalAt = m.now().UTC()
	flow.clearSecrets()
	m.pruneLocked()
	return nil
}

func (m *deviceFlowManager) Fail(lease deviceOperationLease, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if flow := m.flows[lease.FlowID]; flow != nil && flow.Stage != deviceStageCompleted && flow.owns(lease, lease.Kind) {
		flow.clearLease()
		flow.Failure, flow.Stage = strings.TrimSpace(message), deviceStageFailed
		flow.TerminalAt = m.now().UTC()
		flow.clearSecrets()
		m.pruneLocked()
		return nil
	}
	return errDeviceFlowStage
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

func (m *deviceFlowManager) Current(sessionID, origin, slug string) (deviceFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	var selected *githubDeviceFlow
	for _, flow := range m.flows {
		if flow.SessionID != sessionID || flow.Origin != origin || flow.terminal() {
			continue
		}
		if selected == nil || flow.CreatedAt.After(selected.CreatedAt) {
			selected = flow
		}
	}
	if selected == nil {
		return deviceFlowStatus{}, errDeviceFlowNotFound
	}
	return statusFromDeviceFlow(selected, slug), nil
}

func (m *deviceFlowManager) ReconnectIntent(id, sessionID, origin string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.terminal() {
		return "", errDeviceFlowStage
	}
	return flow.ReconnectConnectionID, nil
}

func (m *deviceFlowManager) Discard(id, sessionID, origin string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundLocked(id, sessionID, origin)
	if err != nil || flow.terminal() {
		return errDeviceFlowNotFound
	}
	if flow.LeaseGeneration != "" {
		return errDeviceFlowInFlight
	}
	m.removeFlowLocked(id, flow)
	return nil
}

func statusFromDeviceFlow(flow *githubDeviceFlow, slug string) deviceFlowStatus {
	candidates := make([]githubexternal.InstallationCandidate, len(flow.Candidates))
	copy(candidates, flow.Candidates)
	status := deviceFlowStatus{FlowID: flow.ID, Stage: flow.Stage, ReconnectConnectionID: flow.ReconnectConnectionID, ExpiresAt: flow.ExpiresAt, IntervalSeconds: int(flow.Interval / time.Second), Candidates: candidates, ConnectionID: flow.ConnectionID, Failure: flow.Failure}
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
		if flow.LeaseGeneration != "" {
			continue
		}
		if flow.ExpiresAt.After(now) {
			continue
		}
		m.removeFlowLocked(id, flow)
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

func (m *deviceFlowManager) beginLocked(flow *githubDeviceFlow, kind deviceOperationKind) (deviceOperationLease, error) {
	if flow.LeaseGeneration != "" {
		return deviceOperationLease{}, errDeviceFlowInFlight
	}
	generation, err := randomToken(24)
	if err != nil {
		return deviceOperationLease{}, err
	}
	flow.LeaseGeneration, flow.LeaseKind = generation, kind
	return deviceOperationLease{FlowID: flow.ID, Generation: generation, Kind: kind, ExpiresAt: flow.ExpiresAt}, nil
}

func (m *deviceFlowManager) replaceLeaseLocked(flow *githubDeviceFlow, current deviceOperationLease, currentKind, nextKind deviceOperationKind) (deviceOperationLease, error) {
	if !flow.owns(current, currentKind) {
		return deviceOperationLease{}, errDeviceFlowStage
	}
	generation, err := randomToken(24)
	if err != nil {
		return deviceOperationLease{}, err
	}
	flow.LeaseGeneration, flow.LeaseKind = generation, nextKind
	return deviceOperationLease{FlowID: flow.ID, Generation: generation, Kind: nextKind, ExpiresAt: flow.ExpiresAt}, nil
}

func (f *githubDeviceFlow) owns(lease deviceOperationLease, kind deviceOperationKind) bool {
	return lease.FlowID == f.ID && lease.Generation != "" && lease.Generation == f.LeaseGeneration && lease.Kind == kind && f.LeaseKind == kind
}

func (f *githubDeviceFlow) clearLease() {
	f.LeaseGeneration, f.LeaseKind = "", ""
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
	m.removeFlowLocked(oldest.ID, oldest)
	return true
}

func (m *deviceFlowManager) removeFlowLocked(id string, flow *githubDeviceFlow) {
	flow.clearSecrets()
	delete(m.flows, id)
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
