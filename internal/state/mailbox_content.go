package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/mailboxview"
)

var ErrMailboxContentNotFound = errors.New("mailbox content not found")

type MailboxContentDescription struct {
	HasContent bool
	IsPreview  bool
	Preview    string
}

type mailboxContentPayload struct {
	RunID              string          `json:"run_id"`
	NodeID             string          `json:"node_id"`
	NodeRunID          string          `json:"node_run_id"`
	AttemptID          string          `json:"attempt_id"`
	CommunicationType  string          `json:"communication_type"`
	Instruction        string          `json:"instruction_full"`
	InstructionRef     json.RawMessage `json:"instruction_full_ref"`
	InstructionPreview string          `json:"delegator_instruction_scoped"`
	Response           string          `json:"response_full"`
	ResponseRef        json.RawMessage `json:"response_full_ref"`
	ResponsePreview    string          `json:"response_preview"`
	Markdown           string          `json:"body_markdown"`
	Text               string          `json:"body_text"`
}

// Select the same primary handoff body for metadata and reads. A reference is
// authoritative even when malformed: never silently fall back to summary text.
func (p mailboxContentPayload) body() (text string, ref json.RawMessage, instruction bool) {
	kind := strings.ToLower(strings.TrimSpace(p.CommunicationType))
	responseFirst := kind == "response_received" || kind == "approval_response"
	if !responseFirst && (len(p.InstructionRef) > 0 || strings.TrimSpace(p.Instruction) != "") {
		return p.Instruction, p.InstructionRef, true
	}
	if len(p.ResponseRef) > 0 || strings.TrimSpace(p.Response) != "" {
		return p.Response, p.ResponseRef, false
	}
	if len(p.InstructionRef) > 0 || strings.TrimSpace(p.Instruction) != "" {
		return p.Instruction, p.InstructionRef, true
	}
	if strings.TrimSpace(p.Markdown) != "" {
		return p.Markdown, nil, false
	}
	return p.Text, nil, false
}

func DescribeMailboxContent(raw string) MailboxContentDescription {
	var p mailboxContentPayload
	if json.Unmarshal([]byte(raw), &p) != nil {
		return MailboxContentDescription{}
	}
	text, ref, instruction := p.body()
	d := MailboxContentDescription{HasContent: len(ref) > 0 || strings.TrimSpace(text) != "", IsPreview: len(ref) > 0}
	if d.IsPreview {
		if instruction {
			d.Preview = p.InstructionPreview
		} else {
			d.Preview = p.ResponsePreview
		}
	}
	return d
}

// OpenMessageContent resolves only a reference carried by this visible mailbox
// row, independently checks durable execution lineage, and stages verified bytes
// before callers send response headers. Close removes the private staging file.
func (s *MailboxStore) OpenMessageContent(ctx context.Context, runID, messageID string) (io.ReadCloser, int64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT payload_json FROM mailbox_messages WHERE run_id=? AND message_id=? AND `+mailboxview.UserFacingMailboxPredicate(""), runID, messageID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, ErrMailboxContentNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	var p mailboxContentPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, 0, err
	}
	text, ref, instruction := p.body()
	if len(ref) == 0 {
		if strings.TrimSpace(text) == "" {
			return nil, 0, ErrMailboxContentNotFound
		}
		return io.NopCloser(strings.NewReader(text)), int64(len(text)), nil
	}
	if p.RunID != runID || p.AttemptID == "" || p.NodeRunID == "" || p.NodeID == "" {
		return nil, 0, ErrMailboxContentNotFound
	}
	var lineage bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempt_executions a JOIN node_runs n ON n.node_run_id=a.node_run_id AND n.run_id=a.run_id WHERE a.attempt_id=? AND a.run_id=? AND a.node_run_id=? AND n.node_id=?)`, p.AttemptID, runID, p.NodeRunID, p.NodeID).Scan(&lineage); err != nil {
		return nil, 0, err
	}
	if !lineage {
		return nil, 0, ErrMailboxContentNotFound
	}
	if instruction {
		var value struct {
			ContentID string `json:"content_id"`
			SHA256    string `json:"sha256"`
			SizeBytes int64  `json:"size_bytes"`
		}
		if err := json.Unmarshal(ref, &value); err != nil {
			return nil, 0, err
		}
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM execution_prompt_refs WHERE attempt_id=? AND run_id=? AND node_run_id=? AND content_id=?)`, p.AttemptID, runID, p.NodeRunID, value.ContentID).Scan(&lineage); err != nil {
			return nil, 0, err
		}
		if !lineage {
			return nil, 0, ErrMailboxContentNotFound
		}
		reader, err := OpenExecutionPrompt(ctx, s.db, p.AttemptID, RequestContentReference{ContentID: value.ContentID, SHA256: value.SHA256, SizeBytes: value.SizeBytes})
		return reader, value.SizeBytes, err
	}
	var value providerexecution.ContentReference
	if err := json.Unmarshal(ref, &value); err != nil {
		return nil, 0, err
	}
	if err := value.Validate(value.InvocationID); err != nil {
		return nil, 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_invocations WHERE provider_invocation_id=? AND attempt_id=? AND run_id=? AND node_run_id=?)`, value.InvocationID, p.AttemptID, runID, p.NodeRunID).Scan(&lineage); err != nil {
		return nil, 0, err
	}
	if !lineage {
		return nil, 0, ErrMailboxContentNotFound
	}
	file, err := os.CreateTemp("", "kingdom-mailbox-content-*")
	if err != nil {
		return nil, 0, err
	}
	staged := &requestContentFile{File: file}
	if err := executionstore.New(s.db).CopyProviderContent(ctx, value, file); err != nil {
		_ = staged.Close()
		return nil, 0, fmt.Errorf("verify mailbox response content: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = staged.Close()
		return nil, 0, err
	}
	return staged, value.SizeBytes, nil
}
