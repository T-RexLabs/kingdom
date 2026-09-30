// Package external defines provider-neutral discovery and capture contracts.
package external

import (
	"context"
	"time"

	"kingdom/internal/state"
)

type FailureClass string

const (
	FailureAuthentication        FailureClass = "revoked"
	FailurePermissionMismatch    FailureClass = "permission_mismatch"
	FailureInaccessibleOrMissing FailureClass = "inaccessible_or_missing"
	FailureDeleted               FailureClass = "deleted"
	FailureRateLimited           FailureClass = "rate_limited"
	FailureTimeout               FailureClass = "timeout"
	FailureMalformedResponse     FailureClass = "malformed_response"
	FailureOversizedResponse     FailureClass = "oversized_response"
	FailureUnavailable           FailureClass = "service_unavailable"
)

type ProviderError struct {
	Class   FailureClass
	RetryAt *time.Time
	Status  int
}

func (e *ProviderError) Error() string { return "external provider request failed: " + string(e.Class) }

type Repository struct {
	ID           string `json:"id"`
	Locator      string `json:"locator"`
	CanonicalURL string `json:"canonical_url"`
	// AccountID and InstallationID are internal witnesses, never serialized.
	// InstallationID is what lets a repository be authenticated for without a
	// second lookup when a connection covers several installations.
	AccountID      string `json:"-"`
	InstallationID string `json:"-"`
}
type IssueSummary struct {
	NativeID     string                        `json:"native_id"`
	Issue        state.NormalizedExternalIssue `json:"issue"`
	CommentCount int                           `json:"comment_count"`
}
type RepositoryPage struct {
	Items      []Repository
	NextCursor string
}
type IssuePage struct {
	Items             []IssueSummary
	NextCursor        string
	IncompleteResults bool
	SearchResultCap   int
}

type IssueRequest struct {
	Connection          state.ExternalConnection
	RepositoryID        string
	IssueNumber         int
	Profile             state.ExternalImportProfile
	ETag                string
	CommentsETag        string
	ProviderNativeID    string
	CurrentIssue        state.NormalizedExternalIssue
	CurrentCommentCount int
}

type IssueResult struct {
	RepositoryID, NativeID                    string
	Issue                                     state.NormalizedExternalIssue
	Profile                                   state.ExternalImportProfile
	Comments                                  []state.ExternalComment
	ProviderCommentCount, FetchedCommentCount int
	FetchedCommentPages, OmittedCommentCount  int
	OmittedCommentPages                       int
	ETag, CommentsETag, LastModified          string
	NotModified                               bool
	CommentsNotModified                       bool
	Transferred                               bool
}

// ConnectionValidation is the verdict for a whole connection. Every installation
// it covers must validate, and each carries the account identity its own
// validation proved.
type ConnectionValidation struct {
	AppSlug       string
	Installations []state.ConnectionInstallation
}

type Adapter interface {
	Validate(context.Context, state.ExternalConnection) (ConnectionValidation, error)
	ListRepositories(context.Context, state.ExternalConnection, string, string) (RepositoryPage, error)
	ListIssues(context.Context, state.ExternalConnection, string, string, string, string) (IssuePage, error)
	FetchIssue(context.Context, IssueRequest) (IssueResult, error)
	Evict(string)
}

type Registry struct{ adapters map[string]Adapter }

func NewRegistry(adapters map[string]Adapter) *Registry {
	copy := make(map[string]Adapter, len(adapters))
	for k, v := range adapters {
		copy[k] = v
	}
	return &Registry{adapters: copy}
}
func (r *Registry) Adapter(provider string) (Adapter, bool) {
	v, ok := r.adapters[provider]
	return v, ok
}

// Installation is one App installation a connection could cover, as the
// provider reports it.
type Installation struct {
	InstallationID, AccountID, AccountLogin, DisplayName string
	AppID, ClientID, AppSlug                             string
	RepositorySelection                                  string
}

// InstallationLister is implemented by adapters that can enumerate the
// installations a saved connection's own credential reaches, so the covered set
// can be changed without a fresh authorization.
type InstallationLister interface {
	ListInstallations(context.Context, state.ExternalConnection) ([]Installation, error)
}
