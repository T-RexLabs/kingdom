package github

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

type githubRepository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	HTMLURL  string `json:"html_url"`
	Owner    struct {
		ID int64 `json:"id"`
	} `json:"owner"`
}

type githubComment struct {
	ID        int64   `json:"id"`
	Body      *string `json:"body"`
	HTMLURL   string  `json:"html_url"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
	User      *struct {
		Login string `json:"login"`
	} `json:"user"`
}

func malformedResponse() error {
	return &external.ProviderError{Class: external.FailureMalformedResponse}
}

func normalizeRepositories(raw []githubRepository) ([]external.Repository, error) {
	if len(raw) > 100 {
		return nil, malformedResponse()
	}
	seenIDs, seenLocators := map[int64]struct{}{}, map[string]struct{}{}
	out := make([]external.Repository, 0, len(raw))
	for _, repository := range raw {
		locator, err := validateRepositoryLocator(repository.FullName)
		if err != nil || repository.ID <= 0 || repository.Owner.ID <= 0 || !validGitHubURL(repository.HTMLURL, "/"+locator) {
			return nil, malformedResponse()
		}
		locatorKey := strings.ToLower(locator)
		if _, exists := seenIDs[repository.ID]; exists {
			return nil, malformedResponse()
		}
		if _, exists := seenLocators[locatorKey]; exists {
			return nil, malformedResponse()
		}
		seenIDs[repository.ID] = struct{}{}
		seenLocators[locatorKey] = struct{}{}
		out = append(out, external.Repository{
			ID: strconv.FormatInt(repository.ID, 10), Locator: locator,
			CanonicalURL: "https://github.com/" + locator,
			AccountID:    strconv.FormatInt(repository.Owner.ID, 10),
		})
	}
	return out, nil
}

func normalizeIssues(raw []githubIssue, expectedRepository string) ([]external.IssueSummary, error) {
	if len(raw) > 100 {
		return nil, malformedResponse()
	}
	seen := map[int64]struct{}{}
	out := make([]external.IssueSummary, 0, len(raw))
	for _, issue := range raw {
		isPullRequest, err := validatePullRequestMarker(issue.PullRequest)
		if err != nil {
			return nil, err
		}
		if isPullRequest {
			continue
		}
		summary, err := normalizeIssue(issue, expectedRepository, 0)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[issue.ID]; exists {
			return nil, malformedResponse()
		}
		seen[issue.ID] = struct{}{}
		out = append(out, summary)
	}
	return out, nil
}

func normalizeIssue(issue githubIssue, expectedRepository string, expectedNumber int) (external.IssueSummary, error) {
	repository, err := repositoryFromAPIURL(issue.RepositoryURL)
	if err != nil || !strings.EqualFold(repository, expectedRepository) || issue.ID <= 0 || issue.Number <= 0 || expectedNumber > 0 && issue.Number != expectedNumber {
		return external.IssueSummary{}, malformedResponse()
	}
	if issue.State != "open" && issue.State != "closed" || issue.Comments < 0 {
		return external.IssueSummary{}, malformedResponse()
	}
	if strings.TrimSpace(issue.Title) == "" || !validText(issue.Title, false) {
		return external.IssueSummary{}, malformedResponse()
	}
	body := ""
	if issue.Body != nil {
		body = *issue.Body
	}
	if !validText(body, true) {
		return external.IssueSummary{}, malformedResponse()
	}
	createdAt, updatedAt, err := validateTimestamps(issue.CreatedAt, issue.UpdatedAt)
	if err != nil || !validGitHubURL(issue.HTMLURL, fmt.Sprintf("/%s/issues/%d", repository, issue.Number)) {
		return external.IssueSummary{}, malformedResponse()
	}
	if issue.Repository != nil {
		locator, locatorErr := validateRepositoryLocator(issue.Repository.FullName)
		if locatorErr != nil || issue.Repository.ID <= 0 || issue.Repository.Owner.ID <= 0 || !strings.EqualFold(locator, repository) {
			return external.IssueSummary{}, malformedResponse()
		}
	}
	return external.IssueSummary{
		NativeID: strconv.FormatInt(issue.ID, 10),
		Issue: state.NormalizedExternalIssue{
			Title: issue.Title, Body: body, State: issue.State, Repository: repository,
			IssueNumber: issue.Number, CanonicalURL: fmt.Sprintf("https://github.com/%s/issues/%d", repository, issue.Number),
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		},
		CommentCount: issue.Comments,
	}, nil
}

func normalizeComments(raw []githubComment, repository string, issueNumber int) ([]state.ExternalComment, error) {
	if len(raw) > 100 {
		return nil, malformedResponse()
	}
	seen := map[int64]struct{}{}
	out := make([]state.ExternalComment, 0, len(raw))
	var previousID int64
	for _, comment := range raw {
		if comment.ID <= 0 || comment.User == nil || !validIdentifier(comment.User.Login, 100) {
			return nil, malformedResponse()
		}
		if previousID != 0 && comment.ID <= previousID {
			return nil, malformedResponse()
		}
		if _, exists := seen[comment.ID]; exists {
			return nil, malformedResponse()
		}
		seen[comment.ID] = struct{}{}
		previousID = comment.ID
		body := ""
		if comment.Body != nil {
			body = *comment.Body
		}
		if !validText(body, true) {
			return nil, malformedResponse()
		}
		createdAt, updatedAt, err := validateTimestamps(comment.CreatedAt, comment.UpdatedAt)
		expectedPath := fmt.Sprintf("/%s/issues/%d", repository, issueNumber)
		if err != nil || !validGitHubCommentURL(comment.HTMLURL, expectedPath, comment.ID) {
			return nil, malformedResponse()
		}
		out = append(out, state.ExternalComment{
			ProviderCommentID: strconv.FormatInt(comment.ID, 10), Author: comment.User.Login, Body: body,
			CanonicalURL: fmt.Sprintf("https://github.com%s#issuecomment-%d", expectedPath, comment.ID),
			CreatedAt:    createdAt, UpdatedAt: updatedAt,
		})
	}
	return out, nil
}

func validatePullRequestMarker(raw json.RawMessage) (bool, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return false, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return false, malformedResponse()
	}
	return true, nil
}

func validateRepositoryLocator(raw string) (string, error) {
	if raw != strings.TrimSpace(raw) || len(raw) > 202 {
		return "", fmt.Errorf("invalid repository locator")
	}
	parts := strings.Split(raw, "/")
	if len(parts) != 2 || !validRepositorySegment(parts[0]) || !validRepositorySegment(parts[1]) {
		return "", fmt.Errorf("invalid repository locator")
	}
	return raw, nil
}

func validRepositorySegment(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 100 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func repositoryFromAPIURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "api.github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", fmt.Errorf("invalid repository url")
	}
	const prefix = "/repos/"
	if !strings.HasPrefix(u.Path, prefix) {
		return "", fmt.Errorf("invalid repository url")
	}
	return validateRepositoryLocator(strings.TrimPrefix(u.Path, prefix))
}

func validGitHubURL(raw, expectedPath string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host == "github.com" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.RawPath == "" && u.Path == expectedPath
}

func validGitHubCommentURL(raw, expectedPath string, id int64) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host == "github.com" && u.User == nil && u.RawQuery == "" && u.RawPath == "" && u.Path == expectedPath && u.Fragment == "issuecomment-"+strconv.FormatInt(id, 10)
}

func validateTimestamps(created, updated string) (string, string, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return "", "", err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updated)
	if err != nil || updatedAt.Before(createdAt) {
		return "", "", fmt.Errorf("invalid timestamps")
	}
	return createdAt.UTC().Format(time.RFC3339Nano), updatedAt.UTC().Format(time.RFC3339Nano), nil
}

func validText(value string, multiline bool) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && !(multiline && (r == '\n' || r == '\r' || r == '\t')) {
			return false
		}
	}
	return true
}

func validIdentifier(value string, max int) bool {
	return value == strings.TrimSpace(value) && value != "" && len(value) <= max && validText(value, false)
}

func validateOpaqueToken(value string, expiresAt, now time.Time) error {
	if !validIdentifier(value, 4096) || !now.Add(5*time.Minute).Before(expiresAt) || expiresAt.After(now.Add(2*time.Hour)) {
		return malformedResponse()
	}
	return nil
}

func validateResponseValidators(etag, lastModified string) error {
	if etag != "" {
		value := etag
		if strings.HasPrefix(value, "W/") {
			value = strings.TrimPrefix(value, "W/")
		}
		if len(value) < 2 || len(value) > 1024 || value[0] != '"' || value[len(value)-1] != '"' || !validText(value[1:len(value)-1], false) || strings.Contains(value[1:len(value)-1], `"`) {
			return malformedResponse()
		}
	}
	if lastModified != "" {
		if _, err := http.ParseTime(lastModified); err != nil {
			return malformedResponse()
		}
	}
	return nil
}
