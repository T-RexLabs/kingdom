package idempotency

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	Version       = "kd1"
	DefaultTTL    = 24 * time.Hour
	MaxTTL        = 7 * 24 * time.Hour
	MaxFutureSkew = 5 * time.Minute
	nonceBytes    = 16
	maxKeyLength  = 128
)

var (
	ErrInvalidKey = errors.New("invalid idempotency key")
	ErrExpiredKey = errors.New("idempotency key expired")
	ErrFutureKey  = errors.New("idempotency key issued too far in the future")
)

type Key struct {
	Raw       string
	Scope     string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func Generate(scope string, now time.Time) (string, error) {
	return GenerateWithReader(scope, now, DefaultTTL, rand.Reader)
}

func GenerateWithReader(scope string, now time.Time, ttl time.Duration, random io.Reader) (string, error) {
	if !validScope(scope) {
		return "", fmt.Errorf("%w: scope must be lowercase alphanumeric with optional hyphens", ErrInvalidKey)
	}
	if ttl < time.Second || ttl > MaxTTL || ttl%time.Second != 0 {
		return "", fmt.Errorf("%w: ttl must be whole seconds between 1 second and %s", ErrInvalidKey, MaxTTL)
	}
	if random == nil {
		return "", fmt.Errorf("%w: random source is required", ErrInvalidKey)
	}
	nonce := make([]byte, nonceBytes)
	if _, err := io.ReadFull(random, nonce); err != nil {
		return "", fmt.Errorf("generate idempotency nonce: %w", err)
	}
	issuedAt := now.UTC().Truncate(time.Second)
	return fmt.Sprintf("%s.%d.%d.%s.%s", Version, issuedAt.Unix(), int64(ttl/time.Second), scope, base64.RawURLEncoding.EncodeToString(nonce)), nil
}

func Parse(raw string) (Key, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxKeyLength {
		return Key{}, fmt.Errorf("%w: length", ErrInvalidKey)
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 5 || parts[0] != Version || !validScope(parts[3]) {
		return Key{}, fmt.Errorf("%w: format", ErrInvalidKey)
	}
	issuedUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || issuedUnix < 0 {
		return Key{}, fmt.Errorf("%w: issued timestamp", ErrInvalidKey)
	}
	ttlSeconds, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || ttlSeconds < 1 || ttlSeconds > int64(MaxTTL/time.Second) {
		return Key{}, fmt.Errorf("%w: ttl", ErrInvalidKey)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || len(nonce) != nonceBytes {
		return Key{}, fmt.Errorf("%w: nonce", ErrInvalidKey)
	}
	issuedAt := time.Unix(issuedUnix, 0).UTC()
	expiresAt := issuedAt.Add(time.Duration(ttlSeconds) * time.Second)
	if expiresAt.Before(issuedAt) {
		return Key{}, fmt.Errorf("%w: expiry overflow", ErrInvalidKey)
	}
	return Key{Raw: raw, Scope: parts[3], IssuedAt: issuedAt, ExpiresAt: expiresAt}, nil
}

func (k Key) ValidateAt(now time.Time) error {
	now = now.UTC()
	if k.IssuedAt.After(now.Add(MaxFutureSkew)) {
		return ErrFutureKey
	}
	if !k.ExpiresAt.After(now) {
		return ErrExpiredKey
	}
	return nil
}

func validScope(scope string) bool {
	if len(scope) < 1 || len(scope) > 40 || scope[0] == '-' || scope[len(scope)-1] == '-' {
		return false
	}
	for _, char := range scope {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' {
			continue
		}
		return false
	}
	return true
}
