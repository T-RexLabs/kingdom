package opencode

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"

	"kingdom/internal/core/providerexecution"
)

// SessionResume links an explicit continuation to its closed predecessor and
// the exact projected history saved alongside the partial workspace.
type SessionResume struct {
	Source     providerexecution.RequestIdentity
	Checkpoint SessionCheckpoint
}

func (s *Service) verifyResumeCheckpoint(ctx context.Context, req ExecuteRequest) error {
	resume := req.ResumeFrom
	if err := resume.Source.Validate(); err != nil {
		return err
	}
	digest, err := hex.DecodeString(resume.Checkpoint.MessagesSHA256)
	if resume.Checkpoint.MessageID == "" || err != nil || len(digest) != 32 || hex.EncodeToString(digest) != resume.Checkpoint.MessagesSHA256 {
		return fmt.Errorf("complete resume conversation checkpoint is required")
	}
	if err := s.requests.authority.RequireSessionClosed(ctx, resume.Source); err != nil {
		return err
	}
	started, err := s.ready(ctx)
	if err != nil {
		return err
	}
	info, err := started.client.Session(ctx, resume.Source.Location, resume.Source.SessionID)
	if err != nil {
		return err
	}
	location := filepath.Clean(info.Location.Directory)
	if location != filepath.Clean(resume.Source.Location) && location != filepath.Clean(req.WorkspacePath) {
		return fmt.Errorf("resume conversation has an unexpected workspace")
	}
	// Profile selection may have been staged by an earlier preparation whose
	// process stopped before binding. It grants no tools without a canonical
	// request. The immutable history and closed source remain the continuity
	// authority, and Prepare will select a fresh profile before dispatch.
	if err := requireEmptyInbox(ctx, started.client, location, resume.Source.SessionID); err != nil {
		return err
	}
	watermark, err := started.client.Log(ctx, location, resume.Source.SessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		return err
	}
	identity := resume.Source
	identity.Location = location
	actual, err := captureProjectedCheckpoint(ctx, started.client, identity)
	if err != nil {
		return fmt.Errorf("verify resume conversation: %w", err)
	}
	if actual != resume.Checkpoint {
		return fmt.Errorf("provider conversation changed since the resume checkpoint")
	}
	end, err := started.client.Log(ctx, location, resume.Source.SessionID, func(json.RawMessage) error { return nil })
	if err != nil {
		return err
	}
	if end != watermark {
		return fmt.Errorf("provider conversation changed while preparing resume")
	}
	return s.requests.authority.RequireSessionClosed(ctx, resume.Source)
}

func resumeSession(ctx context.Context, started *requestSession, resume SessionResume, prepared *request) (string, bool, error) {
	client, sessionID := started.client, resume.Source.SessionID
	info, err := client.Session(ctx, resume.Source.Location, sessionID)
	if err != nil {
		return "", false, err
	}
	oldDirectory, directory := info.Location.Directory, prepared.directory
	if err := requireEmptyInbox(ctx, client, oldDirectory, sessionID); err != nil {
		return "", false, err
	}
	// Enroll before any control item is applied. Only the later request's
	// delivery interval becomes execution evidence; old history stays context.
	if err := started.capture.track(sessionID); err != nil {
		return "", false, err
	}
	if filepath.Clean(oldDirectory) != directory {
		body := map[string]string{"directory": directory, "delivery": "steer"}
		if err := client.send(ctx, client.http, http.MethodPost, sessionPath(sessionID, "/move"), atLocation(oldDirectory), body, nil); err != nil {
			return "", false, fmt.Errorf("move resume conversation: %w", err)
		}
		if err := client.Wait(ctx, directory, sessionID); err != nil {
			return "", false, err
		}
	}
	info, err = client.Session(ctx, directory, sessionID)
	if err != nil {
		return "", false, err
	}
	if filepath.Clean(info.Location.Directory) != directory {
		return "", false, fmt.Errorf("resume conversation has a different workspace")
	}
	if err := requireEmptyInbox(ctx, client, directory, sessionID); err != nil {
		return "", false, err
	}
	if err := client.SwitchAgent(ctx, directory, sessionID, prepared.profile.id); err != nil {
		return "", false, err
	}
	if info.Model != prepared.model {
		if err := client.SwitchModel(ctx, directory, sessionID, prepared.model); err != nil {
			return "", false, err
		}
	}
	return sessionID, true, nil
}
