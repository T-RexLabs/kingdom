package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"kingdom/internal/core/providerexecution"
)

// readArray streams a JSON array element by element. The source is read
// through EOF under its content reference, so a returned nil error means every
// byte was hashed, sized and consumed, with no trailing JSON.
func readArray[T any](ctx context.Context, name string, src Source, invocationID string, each func(*T) error) error {
	if err := src.Ref.Validate(invocationID); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if src.Open == nil {
		return fmt.Errorf("%s: content opener is required", name)
	}
	opened, err := src.Open(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if opened == nil {
		return fmt.Errorf("%s: content reader is unavailable", name)
	}
	reader := providerexecution.VerifyReader(ctx, src.Ref, opened)
	defer reader.Close()

	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err == nil && token != json.Delim('[') {
		err = errors.New("JSON array is required")
	}
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	for decoder.More() {
		var item T
		if err := decoder.Decode(&item); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := each(&item); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing JSON")
		}
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
