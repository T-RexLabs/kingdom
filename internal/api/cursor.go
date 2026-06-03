package api

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrCursorTampered is returned when a pagination cursor's HMAC tag does
// not match the payload — either the cursor was forged, or the process key
// has rotated (e.g. server restart). Both cases force the client to
// re-fetch from the start.
var ErrCursorTampered = errors.New("cursor tampered or expired")

// cursorSigner produces and validates HMAC-tagged opaque cursor strings.
//
// Every cursor is signed with a per-process random 32-byte key. Cursor
// shape: `base64url(payload "|" hex(hmac(scope|sessionID|payload)))`.
// Including `scope` and `sessionID` in the HMAC input means a cursor minted
// for "planning-messages" in session A cannot be replayed against
// "timeline" in session B, even if the payload looks superficially valid.
// Process restart invalidates outstanding cursors — acceptable, as
// pagination is short-lived and clients refetch.
type cursorSigner struct {
	key []byte
}

func newCursorSigner() (*cursorSigner, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate cursor signing key: %w", err)
	}
	return &cursorSigner{key: key}, nil
}

func (s *cursorSigner) Encode(scope, sessionID, payload string) string {
	tag := s.tag(scope, sessionID, payload)
	combined := payload + "|" + tag
	return base64.RawURLEncoding.EncodeToString([]byte(combined))
}

func (s *cursorSigner) Decode(scope, sessionID, token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return "", ErrCursorTampered
	}
	combined := string(raw)
	idx := strings.LastIndex(combined, "|")
	if idx < 0 {
		return "", ErrCursorTampered
	}
	payload := combined[:idx]
	tag := combined[idx+1:]
	expected := s.tag(scope, sessionID, payload)
	if !hmac.Equal([]byte(tag), []byte(expected)) {
		return "", ErrCursorTampered
	}
	return payload, nil
}

func (s *cursorSigner) tag(scope, sessionID, payload string) string {
	mac := hmac.New(sha256.New, s.key)
	// Length-prefix each field so different (scope, sessionID, payload)
	// tuples can never collide on the concatenated input.
	for _, part := range []string{scope, sessionID, payload} {
		fmt.Fprintf(mac, "%d|%s|", len(part), part)
	}
	return hex.EncodeToString(mac.Sum(nil))
}
