package idempotency

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGenerateParseAndValidate(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 900_000_000, time.UTC)
	key, err := GenerateWithReader("run-start", now, DefaultTTL, bytes.NewReader(make([]byte, nonceBytes)))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(key)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scope != "run-start" || !parsed.IssuedAt.Equal(now.Truncate(time.Second)) || !parsed.ExpiresAt.Equal(now.Truncate(time.Second).Add(DefaultTTL)) {
		t.Fatalf("parsed key=%#v", parsed)
	}
	if err := parsed.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	if err := parsed.ValidateAt(parsed.ExpiresAt); !errors.Is(err, ErrExpiredKey) {
		t.Fatalf("boundary expiry error=%v", err)
	}
}

func TestParseRejectsMalformedKeys(t *testing.T) {
	validNonce := strings.Repeat("A", 22)
	for _, raw := range []string{
		"", "legacy-key", "kd2.1.60.scope." + validNonce,
		"kd1.bad.60.scope." + validNonce, "kd1.1.0.scope." + validNonce,
		"kd1.1.604801.scope." + validNonce, "kd1.1.60.BadScope." + validNonce,
		"kd1.1.60.scope.short",
	} {
		if _, err := Parse(raw); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("Parse(%q) error=%v", raw, err)
		}
	}
}

func TestValidateRejectsFutureIssuedKey(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	key, err := GenerateWithReader("scope", now.Add(MaxFutureSkew+time.Second), time.Hour, bytes.NewReader(make([]byte, nonceBytes)))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.ValidateAt(now); !errors.Is(err, ErrFutureKey) {
		t.Fatalf("future key error=%v", err)
	}
}
