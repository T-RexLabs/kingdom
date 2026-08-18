package api

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	githubexternal "kingdom/internal/external/github"
)

const (
	manifestFlowTTL         = time.Hour
	manifestFlowMaxRetained = 32
)

var (
	errManifestFlowNotFound = errors.New("github manifest flow not found")
	errManifestFlowStage    = errors.New("github manifest flow is not in the required stage")
	errManifestFlowCapacity = errors.New("too many active github manifest flows")
)

type manifestFlowStage string

const (
	manifestStageAwaitingManifest     manifestFlowStage = "awaiting_app_creation"
	manifestStageManifestCodeReceived manifestFlowStage = "app_created"
	manifestStageConverting           manifestFlowStage = "converting_app"
	manifestStageAwaitingInstallation manifestFlowStage = "awaiting_installation"
	manifestStageInstallationClaimed  manifestFlowStage = "installation_received"
	manifestStageAwaitingOAuth        manifestFlowStage = "awaiting_authorization"
	manifestStageOAuthCodeReceived    manifestFlowStage = "authorization_received"
	manifestStageFinalizing           manifestFlowStage = "validating_connection"
	manifestStageFinalized            manifestFlowStage = "completed"
	manifestStageFailed               manifestFlowStage = "failed"
)

type manifestStateKind string

const (
	manifestStateRegistration manifestStateKind = "registration"
	manifestStateSetup        manifestStateKind = "setup"
	manifestStateOAuth        manifestStateKind = "oauth"
)

type manifestStateRef struct {
	FlowID string
	Kind   manifestStateKind
}

type githubManifestFlow struct {
	ID, SessionID, Origin                           string
	Stage                                           manifestFlowStage
	CreatedAt, ExpiresAt, TerminalAt                time.Time
	ManifestState, SetupState, OAuthState           string
	ManifestCode, OAuthCode, OAuthVerifier          []byte
	PrivateKey, ClientSecret                        []byte
	AppID, ClientID, AppSlug, AppName               string
	ClaimedInstallationID                           string
	InstallationAccountID, InstallationAccountLogin string
	ConnectionID                                    string
	Failure                                         string
}

type manifestFlowStatus struct {
	FlowID       string            `json:"flow_id"`
	Stage        manifestFlowStage `json:"stage"`
	ExpiresAt    time.Time         `json:"expires_at"`
	AppName      string            `json:"app_name,omitempty"`
	AppSlug      string            `json:"app_slug,omitempty"`
	ConnectionID string            `json:"connection_id,omitempty"`
	Failure      string            `json:"failure,omitempty"`
}

type manifestFlowManager struct {
	mu          sync.Mutex
	now         func() time.Time
	maxRetained int
	flows       map[string]*githubManifestFlow
	states      map[string]manifestStateRef
}

func newManifestFlowManager(now func() time.Time) *manifestFlowManager {
	if now == nil {
		now = time.Now
	}
	return &manifestFlowManager{now: now, maxRetained: manifestFlowMaxRetained, flows: make(map[string]*githubManifestFlow), states: make(map[string]manifestStateRef)}
}

func (m *manifestFlowManager) Create(sessionID, origin string, sessionExpiresAt time.Time) (*githubManifestFlow, error) {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(origin) == "" {
		return nil, errManifestFlowNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	if len(m.flows) >= m.maxRetained && !m.evictOldestTerminalLocked() {
		return nil, errManifestFlowCapacity
	}
	flowID, err := randomToken(24)
	if err != nil {
		return nil, err
	}
	manifestState, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	setupState, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	now := m.now().UTC()
	expiresAt := now.Add(manifestFlowTTL)
	if sessionExpiresAt.IsZero() || !sessionExpiresAt.After(now) {
		return nil, errManifestFlowNotFound
	}
	if sessionExpiresAt.Before(expiresAt) {
		expiresAt = sessionExpiresAt
	}
	flow := &githubManifestFlow{ID: flowID, SessionID: sessionID, Origin: origin, Stage: manifestStageAwaitingManifest, CreatedAt: now, ExpiresAt: expiresAt, ManifestState: manifestState, SetupState: setupState}
	m.flows[flow.ID] = flow
	m.states[manifestState] = manifestStateRef{FlowID: flow.ID, Kind: manifestStateRegistration}
	m.states[setupState] = manifestStateRef{FlowID: flow.ID, Kind: manifestStateSetup}
	copy := *flow
	return &copy, nil
}

func (m *manifestFlowManager) Current(sessionID, origin string) (manifestFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	var selected *githubManifestFlow
	for _, flow := range m.flows {
		if flow.SessionID == sessionID && flow.Origin == origin && (selected == nil || flow.CreatedAt.After(selected.CreatedAt)) {
			selected = flow
		}
	}
	if selected == nil {
		return manifestFlowStatus{}, errManifestFlowNotFound
	}
	return statusFromManifestFlow(selected), nil
}

func (m *manifestFlowManager) Status(flowID, sessionID, origin string) (manifestFlowStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundFlowLocked(flowID, sessionID, origin)
	if err != nil {
		return manifestFlowStatus{}, err
	}
	return statusFromManifestFlow(flow), nil
}

func statusFromManifestFlow(flow *githubManifestFlow) manifestFlowStatus {
	return manifestFlowStatus{FlowID: flow.ID, Stage: flow.Stage, ExpiresAt: flow.ExpiresAt, AppName: flow.AppName, AppSlug: flow.AppSlug, ConnectionID: flow.ConnectionID, Failure: flow.Failure}
}

func (m *manifestFlowManager) RecordManifestCode(state, code string) (*githubManifestFlow, error) {
	if !manifestCodePatternAPI.MatchString(code) {
		return nil, errManifestFlowNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.consumeStateLocked(state, manifestStateRegistration, manifestStageAwaitingManifest)
	if err != nil {
		return nil, err
	}
	flow.ManifestCode = []byte(code)
	flow.Stage = manifestStageManifestCodeReceived
	copy := *flow
	return &copy, nil
}

type manifestConversionLease struct {
	Code      []byte
	ExpiresAt time.Time
}

func (m *manifestFlowManager) BeginConversion(flowID, sessionID, origin string) (manifestConversionLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundFlowLocked(flowID, sessionID, origin)
	if err != nil || flow.Stage != manifestStageManifestCodeReceived || len(flow.ManifestCode) == 0 {
		return manifestConversionLease{}, errManifestFlowStage
	}
	code := append([]byte(nil), flow.ManifestCode...)
	zeroSecret(flow.ManifestCode)
	flow.ManifestCode = nil
	flow.Stage = manifestStageConverting
	return manifestConversionLease{Code: code, ExpiresAt: flow.ExpiresAt}, nil
}

func (m *manifestFlowManager) CompleteConversion(flowID string, converted githubexternal.ManifestConversion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[flowID]
	if flow == nil || flow.Stage != manifestStageConverting || !flow.ExpiresAt.After(m.now().UTC()) {
		zeroSecret(converted.PrivateKey)
		zeroSecret(converted.ClientSecret)
		return errManifestFlowStage
	}
	flow.AppID, flow.ClientID, flow.AppSlug, flow.AppName = converted.AppID, converted.ClientID, converted.AppSlug, converted.Name
	flow.PrivateKey, flow.ClientSecret = converted.PrivateKey, converted.ClientSecret
	flow.Stage = manifestStageAwaitingInstallation
	return nil
}

func (m *manifestFlowManager) InstallationURL(flowID, sessionID, origin string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundFlowLocked(flowID, sessionID, origin)
	if err != nil || flow.Stage != manifestStageAwaitingInstallation || !appSlugPatternAPI.MatchString(flow.AppSlug) {
		return "", errManifestFlowStage
	}
	return githubexternal.GitHubWebOrigin + "/apps/" + url.PathEscape(flow.AppSlug) + "/installations/new?state=" + url.QueryEscape(flow.SetupState), nil
}

func (m *manifestFlowManager) RecordInstallation(state, installationID, setupAction string) (*githubManifestFlow, error) {
	if setupAction != "install" || !numericIDPattern.MatchString(installationID) {
		return nil, errManifestFlowNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.consumeStateLocked(state, manifestStateSetup, manifestStageAwaitingInstallation)
	if err != nil {
		return nil, err
	}
	flow.ClaimedInstallationID = installationID
	flow.Stage = manifestStageInstallationClaimed
	copy := *flow
	return &copy, nil
}

type manifestOAuthRequest struct {
	ClientID, State, Challenge string
	ExpiresAt                  time.Time
}

func (m *manifestFlowManager) OAuthRequest(flowID, sessionID, origin string) (manifestOAuthRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundFlowLocked(flowID, sessionID, origin)
	if err != nil || (flow.Stage != manifestStageInstallationClaimed && flow.Stage != manifestStageAwaitingOAuth) {
		return manifestOAuthRequest{}, errManifestFlowStage
	}
	if flow.Stage == manifestStageInstallationClaimed {
		state, tokenErr := randomToken(32)
		if tokenErr != nil {
			return manifestOAuthRequest{}, tokenErr
		}
		verifier, tokenErr := randomToken(32)
		if tokenErr != nil {
			return manifestOAuthRequest{}, tokenErr
		}
		flow.OAuthState, flow.OAuthVerifier = state, []byte(verifier)
		m.states[state] = manifestStateRef{FlowID: flow.ID, Kind: manifestStateOAuth}
		flow.Stage = manifestStageAwaitingOAuth
	}
	digest := sha256.Sum256(flow.OAuthVerifier)
	return manifestOAuthRequest{ClientID: flow.ClientID, State: flow.OAuthState, Challenge: base64.RawURLEncoding.EncodeToString(digest[:]), ExpiresAt: flow.ExpiresAt}, nil
}

func (m *manifestFlowManager) RecordOAuthCode(state, code string) (*githubManifestFlow, error) {
	if !manifestCodePatternAPI.MatchString(code) {
		return nil, errManifestFlowNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	flow, err := m.consumeStateLocked(state, manifestStateOAuth, manifestStageAwaitingOAuth)
	if err != nil {
		return nil, err
	}
	flow.OAuthCode = []byte(code)
	flow.OAuthState = ""
	flow.Stage = manifestStageOAuthCodeReceived
	copy := *flow
	return &copy, nil
}

type manifestCompletion struct {
	FlowID, Origin, AppID, ClientID, AppSlug, AppName, InstallationID string
	PrivateKey, ClientSecret, OAuthCode, OAuthVerifier                []byte
	ExpiresAt                                                         time.Time
}

func (m *manifestFlowManager) BeginCompletion(flowID, sessionID, origin string) (manifestCompletion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	flow, err := m.boundFlowLocked(flowID, sessionID, origin)
	if err != nil || flow.Stage != manifestStageOAuthCodeReceived || len(flow.PrivateKey) == 0 || len(flow.ClientSecret) == 0 || len(flow.OAuthCode) == 0 || len(flow.OAuthVerifier) == 0 {
		return manifestCompletion{}, errManifestFlowStage
	}
	completion := manifestCompletion{FlowID: flow.ID, Origin: flow.Origin, AppID: flow.AppID, ClientID: flow.ClientID, AppSlug: flow.AppSlug, AppName: flow.AppName, InstallationID: flow.ClaimedInstallationID, PrivateKey: flow.PrivateKey, ClientSecret: flow.ClientSecret, OAuthCode: flow.OAuthCode, OAuthVerifier: flow.OAuthVerifier, ExpiresAt: flow.ExpiresAt}
	flow.PrivateKey, flow.ClientSecret, flow.OAuthCode, flow.OAuthVerifier = nil, nil, nil, nil
	flow.Stage = manifestStageFinalizing
	return completion, nil
}

func (m *manifestFlowManager) Finalize(flowID, connectionID, accountID, accountLogin string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[flowID]
	if flow == nil || flow.Stage != manifestStageFinalizing {
		return errManifestFlowStage
	}
	flow.ConnectionID, flow.InstallationAccountID, flow.InstallationAccountLogin = connectionID, accountID, accountLogin
	flow.Stage = manifestStageFinalized
	flow.TerminalAt = m.now().UTC()
	flow.zeroSecrets()
	m.removeStatesLocked(flow)
	m.pruneLocked()
	return nil
}

func (m *manifestFlowManager) Fail(flowID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	flow := m.flows[flowID]
	if flow == nil || flow.Stage == manifestStageFinalized {
		return
	}
	flow.Failure = strings.TrimSpace(reason)
	if flow.Failure == "" {
		flow.Failure = "GitHub setup could not be completed. Start a new setup flow."
	}
	flow.Stage = manifestStageFailed
	flow.TerminalAt = m.now().UTC()
	flow.zeroSecrets()
	m.removeStatesLocked(flow)
	m.pruneLocked()
}

func (m *manifestFlowManager) boundFlowLocked(flowID, sessionID, origin string) (*githubManifestFlow, error) {
	flow := m.flows[flowID]
	if flow == nil || flow.SessionID != sessionID || flow.Origin != origin || !flow.ExpiresAt.After(m.now().UTC()) {
		return nil, errManifestFlowNotFound
	}
	return flow, nil
}

func (m *manifestFlowManager) consumeStateLocked(state string, kind manifestStateKind, stage manifestFlowStage) (*githubManifestFlow, error) {
	m.pruneLocked()
	ref, ok := m.states[state]
	if !ok || ref.Kind != kind {
		return nil, errManifestFlowNotFound
	}
	flow := m.flows[ref.FlowID]
	if flow == nil || flow.Stage != stage || !flow.ExpiresAt.After(m.now().UTC()) {
		return nil, errManifestFlowNotFound
	}
	delete(m.states, state)
	return flow, nil
}

func (m *manifestFlowManager) pruneLocked() {
	now := m.now().UTC()
	for id, flow := range m.flows {
		if flow.Stage == manifestStageConverting || flow.Stage == manifestStageFinalizing {
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

func (m *manifestFlowManager) evictOldestTerminalLocked() bool {
	var oldest *githubManifestFlow
	for _, flow := range m.flows {
		if flow.Stage != manifestStageFinalized && flow.Stage != manifestStageFailed {
			continue
		}
		if oldest == nil || manifestTerminalLess(flow, oldest) {
			oldest = flow
		}
	}
	if oldest == nil {
		return false
	}
	m.removeFlowLocked(oldest.ID, oldest)
	return true
}

func manifestTerminalLess(left, right *githubManifestFlow) bool {
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

func (m *manifestFlowManager) removeFlowLocked(id string, flow *githubManifestFlow) {
	flow.zeroSecrets()
	m.removeStatesLocked(flow)
	delete(m.flows, id)
}

func (m *manifestFlowManager) removeStatesLocked(flow *githubManifestFlow) {
	for _, state := range []string{flow.ManifestState, flow.SetupState, flow.OAuthState} {
		delete(m.states, state)
	}
	flow.ManifestState, flow.SetupState, flow.OAuthState = "", "", ""
}

func (f *githubManifestFlow) zeroSecrets() {
	for _, secret := range [][]byte{f.ManifestCode, f.OAuthCode, f.OAuthVerifier, f.PrivateKey, f.ClientSecret} {
		zeroSecret(secret)
	}
	f.ManifestCode, f.OAuthCode, f.OAuthVerifier, f.PrivateKey, f.ClientSecret = nil, nil, nil, nil, nil
}

func zeroSecret(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

var (
	manifestCodePatternAPI = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
	appSlugPatternAPI      = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$`)
	githubClientIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)
	numericIDPattern       = regexp.MustCompile(`^[1-9][0-9]{0,31}$`)
)
