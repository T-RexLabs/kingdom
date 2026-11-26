package repositories

import "net/http"

// NewTestClient changes only the transport in a test build. Production origin,
// App-key validation, JWT signing, scopes, and API version remain mandatory.
func NewTestClient(config Config, transport http.RoundTripper) (*Client, error) {
	if transport == nil {
		return nil, ErrInvalid
	}
	client, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	client.http.Transport = transport
	return client, nil
}
