package hostedbuilder

import (
	"context"
	"database/sql"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/openshell/workspacesource"
	runtime "kingdom/internal/projectruntime"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/publishedresult"
	"kingdom/internal/theme"
	"net/http"
	"sync"
)

type project struct {
	mu                 sync.Mutex
	db                 *sql.DB
	instance           *runtime.Instance
	provider           *opencode.ManagedService
	models             *modelproxy.Broker
	gateway            *worker.Gateway
	sources            *workspacesource.Store
	results            publishedresult.Reader
	themes             *theme.Store
	service            *Service
	id                 string
	registered, closed bool
	closing            bool
	closeToken         chan struct{}
}

func (p *project) PublishedResults() publishedresult.Reader {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.closing {
		return nil
	}
	return p.results
}

func (p *project) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	instance, closed := p.instance, p.closed || p.closing
	p.mu.Unlock()
	if closed || instance == nil {
		http.Error(w, "project runtime unavailable", http.StatusServiceUnavailable)
		return
	}
	instance.Handler.ServeHTTP(w, r)
}

// Failed drains retain handlers, transports and database for a later Close.
func (p *project) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeToken:
	}
	defer func() { p.closeToken <- struct{}{} }()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closing = true
	p.mu.Unlock()
	if p.instance != nil {
		if e := p.instance.Close(ctx); e != nil {
			return e
		}
	}
	if p.provider != nil {
		if e := p.provider.CloseContext(ctx); e != nil {
			return e
		}
	}
	if p.models != nil {
		if e := p.models.Close(ctx); e != nil {
			return e
		}
	}
	if p.registered {
		p.service.mu.Lock()
		delete(p.service.handlers, p.id)
		p.service.mu.Unlock()
		p.registered = false
	}
	if p.gateway != nil {
		if e := p.gateway.Close(); e != nil {
			return e
		}
		p.gateway = nil
	}
	if p.sources != nil {
		if e := p.sources.Close(); e != nil {
			return e
		}
		p.sources = nil
	}
	if p.themes != nil {
		if e := p.themes.Close(); e != nil {
			return e
		}
		p.themes = nil
	}
	if p.db != nil {
		if e := p.db.Close(); e != nil {
			return e
		}
		p.db = nil
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}
