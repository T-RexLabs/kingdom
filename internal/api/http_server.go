package api

import (
	"net/http"
	"time"
)

const (
	defaultHTTPReadHeaderTimeout = 5 * time.Second
	defaultHTTPReadTimeout       = 15 * time.Second
	defaultHTTPIdleTimeout       = 60 * time.Second
)

// NewHTTPServer applies the transport bounds shared by standalone and
// installed mode. WriteTimeout intentionally remains unset because Kingdom's
// authenticated SSE responses are long-lived.
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: defaultHTTPReadHeaderTimeout,
		ReadTimeout:       defaultHTTPReadTimeout,
		IdleTimeout:       defaultHTTPIdleTimeout,
	}
}
