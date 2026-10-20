package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

// SessionCheckpoint identifies an idle projected conversation. It is context
// continuity, not evidence that the interrupted request succeeded.
type SessionCheckpoint struct {
	MessageID      string `json:"message_id"`
	MessagesSHA256 string `json:"messages_sha256"`
}

// SessionCheckpointer captures provider context after old ownership is closed.
// The caller reserves the workspace until it has captured the matching files.
type SessionCheckpointer interface {
	CaptureSessionCheckpoint(context.Context, providerexecution.RequestIdentity) (SessionCheckpoint, error)
}

// CaptureSessionCheckpoint parks the known session without delivering pending
// input and fingerprints its full projected conversation, including history
// retained across a server restart. Unknown pending work makes Resume unavailable.
func (s *Service) CaptureSessionCheckpoint(ctx context.Context, identity providerexecution.RequestIdentity) (SessionCheckpoint, error) {
	if err := s.requests.authority.RequireSessionClosed(ctx, identity); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("close provider checkpoint ownership: %w", err)
	}
	started, err := s.ready(ctx)
	if err != nil {
		return SessionCheckpoint{}, err
	}
	client, directory := started.client, identity.Location
	info, err := client.Session(ctx, directory, identity.SessionID)
	if err != nil {
		return SessionCheckpoint{}, err
	}
	if filepath.Clean(info.Location.Directory) != filepath.Clean(directory) || info.Agent != identity.ProfileID {
		return SessionCheckpoint{}, fmt.Errorf("provider checkpoint session has a different workspace or profile")
	}
	if err := client.Interrupt(ctx, directory, identity.SessionID); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("%w: interrupt provider checkpoint session: %w", providerrequest.ErrResourcesOpen, err)
	}
	if err := client.Wait(ctx, directory, identity.SessionID); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("%w: await provider checkpoint session: %w", providerrequest.ErrResourcesOpen, err)
	}
	if err := requireEmptyInbox(ctx, client, directory, identity.SessionID); err != nil {
		return SessionCheckpoint{}, err
	}
	watermark, err := client.Log(ctx, directory, identity.SessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		return SessionCheckpoint{}, err
	}
	checkpoint, err := captureProjectedCheckpoint(ctx, client, identity)
	if err != nil {
		return SessionCheckpoint{}, err
	}
	end, err := client.Log(ctx, directory, identity.SessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		return SessionCheckpoint{}, err
	}
	if end != watermark {
		return SessionCheckpoint{}, fmt.Errorf("provider session changed while capturing its checkpoint")
	}
	if err := s.requests.authority.RequireSessionClosed(ctx, identity); err != nil {
		return SessionCheckpoint{}, fmt.Errorf("provider checkpoint ownership changed: %w", err)
	}
	return checkpoint, nil
}

func captureProjectedCheckpoint(ctx context.Context, client *Client, identity providerexecution.RequestIdentity) (SessionCheckpoint, error) {
	digest := sha256.New()
	// Domain and length framing make the newest-first message sequence
	// unambiguous without retaining the conversation in Kingdom memory.
	digest.Write([]byte("kingdom-opencode-session-checkpoint-v1\x00"))
	checkpoint := SessionCheckpoint{}
	foundSource, foundUser := false, false
	seen := map[string]struct{}{}
	err := client.Messages(ctx, identity.Location, identity.SessionID, func(raw json.RawMessage) error {
		var message struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &message); err != nil {
			return err
		}
		if message.ID == "" {
			return fmt.Errorf("provider checkpoint message has no identity")
		}
		if _, exists := seen[message.ID]; exists {
			return fmt.Errorf("provider checkpoint repeats a message")
		}
		seen[message.ID] = struct{}{}
		if checkpoint.MessageID == "" {
			checkpoint.MessageID = message.ID
		}
		if message.Type == "user" && !foundUser {
			if message.ID != identity.RequestID {
				return fmt.Errorf("provider checkpoint contains input after the source request")
			}
			foundUser = true
		}
		foundSource = foundSource || message.ID == identity.RequestID
		// Normalize object key order while preserving JSON number precision.
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			return err
		}
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(canonical)))
		digest.Write(length[:])
		digest.Write(canonical)
		return nil
	})
	if err != nil {
		return SessionCheckpoint{}, err
	}
	if !foundSource || !foundUser {
		return SessionCheckpoint{}, fmt.Errorf("provider checkpoint lacks the source request message")
	}
	checkpoint.MessagesSHA256 = hex.EncodeToString(digest.Sum(nil))
	return checkpoint, nil
}
