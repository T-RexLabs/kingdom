// Package resultsource authorizes publication reads through the same shared
// project runtime as the central project API.
package resultsource

import (
	"context"
	"io"
	"net/http"

	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/publishedresult"
)

type Factory func(context.Context, string, api.Authenticator) (http.Handler, error)
type Service struct {
	catalog  *catalog.Store
	sessions *auth.Manager
	factory  Factory
}

func New(authority *catalog.Store, sessions *auth.Manager, factory Factory) (*Service, error) {
	if authority == nil || sessions == nil || factory == nil {
		return nil, publishedresult.ErrUnavailable
	}
	return &Service{catalog: authority, sessions: sessions, factory: factory}, nil
}
func (s *Service) reader(ctx context.Context, actor, project string) (publishedresult.Reader, error) {
	if s == nil || s.catalog.Authorize(ctx, actor, project, catalog.UseProject) != nil {
		return nil, publishedresult.ErrUnavailable
	}
	runtime, err := s.factory(ctx, project, s.sessions.ProjectAuthenticator(project))
	if err != nil {
		return nil, publishedresult.ErrUnavailable
	}
	provider, ok := runtime.(interface{ PublishedResults() publishedresult.Reader })
	if !ok {
		return nil, publishedresult.ErrUnavailable
	}
	reader := provider.PublishedResults()
	if reader == nil {
		return nil, publishedresult.ErrUnavailable
	}
	// Startup can take time. Recheck access before returning any metadata or bytes.
	if s.catalog.Authorize(ctx, actor, project, catalog.UseProject) != nil {
		return nil, publishedresult.ErrUnavailable
	}
	return reader, nil
}
func (s *Service) Resolve(ctx context.Context, actor, project, run string) (publishedresult.ResultSnapshot, error) {
	reader, err := s.reader(ctx, actor, project)
	if err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	result, err := reader.Resolve(ctx, run)
	if err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	if s.catalog.Authorize(ctx, actor, project, catalog.UseProject) != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	return result, nil
}
func (s *Service) Open(ctx context.Context, actor, project, run, snapshot, path string) (io.ReadCloser, error) {
	reader, err := s.reader(ctx, actor, project)
	if err != nil {
		return nil, err
	}
	result, err := reader.Open(ctx, run, snapshot, path)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, publishedresult.ErrUnavailable
	}
	if s.catalog.Authorize(ctx, actor, project, catalog.UseProject) != nil {
		result.Close()
		return nil, publishedresult.ErrUnavailable
	}
	return result, nil
}
