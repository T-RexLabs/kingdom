package chatgpt

import "context"

func (s *Service) enter(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, ErrUnavailable
	}
	s.active.Add(1)
	s.mu.Unlock()
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	return child, func() { stop(); cancel(); s.active.Done() }, nil
}

// Close cancels and joins callbacks, catalog requests, and refresh operations
// before the caller can close the credential database.
func (s *Service) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		s.mu.Unlock()
		go func() { s.active.Wait(); close(s.joined) }()
	})
	select {
	case <-s.joined:
		s.client.CloseIdleConnections()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
