package state

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"kingdom/internal/core/providerexecution"
)

func validatePlanningTextContent(ctx context.Context, source *PlanningTextSource, required bool) error {
	if source == nil {
		return nil
	}
	if source.Open == nil {
		return fmt.Errorf("planning text opener is required")
	}
	ref := providerexecution.ContentReference{InvocationID: "planning-text", SHA256: source.SHA256, SizeBytes: source.SizeBytes}
	if err := ref.Validate(ref.InvocationID); err != nil {
		return err
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return err
	}
	verified := providerexecution.VerifyReader(ctx, ref, reader)
	defer verified.Close()
	r := bufio.NewReader(verified)
	nonempty := false
	for {
		ch, _, err := r.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !unicode.IsSpace(ch) {
			nonempty = true
			break
		}
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	if required && !nonempty {
		return fmt.Errorf("planning text content is required")
	}
	return nil
}

// PlanningTextSource is replayable transport. Its owner retains the source
// until the publication call returns; only verified SQLite chunks are durable.
type PlanningTextSource struct {
	Open      func(context.Context) (io.ReadCloser, error)
	SHA256    string
	SizeBytes int64
}

func PlanningTextFromString(text string) *PlanningTextSource {
	hash := sha256.Sum256([]byte(text))
	return &PlanningTextSource{Open: func(ctx context.Context) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return io.NopCloser(strings.NewReader(text)), nil
	}, SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(text))}
}

func PlanningMessageContentID(id string) string    { return "planning-message:" + id + ":markdown" }
func PlanningAnswerTextContentID(id string) string { return "planning-message:" + id + ":answer-text" }
func PlanningDraftContentID(id string) string      { return "planning-draft:" + id + ":text" }

func planningContentID(ref *RequestContentReference) any {
	if ref == nil {
		return nil
	}
	return ref.ContentID
}

func persistPlanningText(ctx context.Context, exec sqlExecutor, id string, source *PlanningTextSource, at string) (*RequestContentReference, error) {
	if source == nil {
		return nil, nil
	}
	tx, ok := exec.(*sql.Tx)
	if !ok {
		return nil, fmt.Errorf("planning text publication requires a transaction")
	}
	if source.Open == nil {
		return nil, fmt.Errorf("planning text opener is required")
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	ref, writeErr := WriteRequestContentTx(ctx, tx, id, RequestContentSource{Reader: reader, SHA256: source.SHA256, SizeBytes: source.SizeBytes}, at)
	if err := errors.Join(writeErr, reader.Close()); err != nil {
		return nil, err
	}
	return &ref, nil
}

func (s *PlanningStore) planningTextSource(ctx context.Context, id string) (*PlanningTextSource, error) {
	var ref RequestContentReference
	ref.ContentID = id
	if err := s.db.QueryRowContext(ctx, `SELECT sha256,size_bytes FROM request_contents WHERE content_id=?`, id).Scan(&ref.SHA256, &ref.SizeBytes); err != nil {
		return nil, err
	}
	return &PlanningTextSource{SHA256: ref.SHA256, SizeBytes: ref.SizeBytes, Open: func(ctx context.Context) (io.ReadCloser, error) {
		return providerexecution.OpenContentCopy(ctx, func(ctx context.Context, out io.Writer) error {
			_, err := CopyRequestContent(ctx, s.db, id, out)
			return err
		}), nil
	}}, nil
}

func readPlanningText(ctx context.Context, source *PlanningTextSource) (string, error) {
	if source == nil {
		return "", nil
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	_, err = io.Copy(&out, reader)
	return out.String(), errors.Join(err, reader.Close())
}

func (s *PlanningStore) loadMessageTextSources(ctx context.Context, message *PlanningMessage) error {
	var contentID, answerID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT content_content_id,answer_text_content_id FROM planning_messages WHERE planning_message_id=? AND planning_session_id=?`, message.PlanningMessageID, message.PlanningSessionID).Scan(&contentID, &answerID); err != nil {
		return err
	}
	var err error
	if contentID.Valid {
		message.ContentSource, err = s.planningTextSource(ctx, contentID.String)
		if err != nil {
			return err
		}
	}
	if answerID.Valid {
		message.AnswerTextSource, err = s.planningTextSource(ctx, answerID.String)
		if err != nil {
			return err
		}
	}
	return nil
}

// Hydration is a read-projection boundary. Execution and commit readback use
// the sources directly; no prefix is substituted for canonical content.
func hydratePlanningMessage(ctx context.Context, message *PlanningMessage) error {
	var err error
	if message.ContentSource != nil {
		message.ContentMarkdown, err = readPlanningText(ctx, message.ContentSource)
		if err != nil {
			return err
		}
	}
	if message.AnswerTextSource != nil {
		text, err := readPlanningText(ctx, message.AnswerTextSource)
		if err != nil {
			return err
		}
		var answer map[string]any
		if err := json.Unmarshal([]byte(message.AnswerPayloadJSON), &answer); err != nil {
			return err
		}
		if answer == nil {
			answer = map[string]any{}
		}
		answer["text"] = text
		raw, err := json.Marshal(answer)
		if err != nil {
			return err
		}
		message.AnswerPayloadJSON = string(raw)
	}
	return nil
}

func (s *PlanningStore) VerifyMessageText(ctx context.Context, message PlanningMessage, expected AppendPlanningMessageRequest) error {
	for _, pair := range []struct{ actual, expected *PlanningTextSource }{{message.ContentSource, expected.ContentSource}, {message.AnswerTextSource, expected.AnswerTextSource}} {
		if pair.expected == nil {
			if pair.actual != nil {
				return fmt.Errorf("unexpected planning text source")
			}
			continue
		}
		if pair.actual == nil || pair.actual.SHA256 != pair.expected.SHA256 || pair.actual.SizeBytes != pair.expected.SizeBytes {
			return fmt.Errorf("planning text reference mismatch")
		}
		reader, err := pair.actual.Open(ctx)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, reader)
		if err = errors.Join(err, reader.Close()); err != nil {
			return err
		}
	}
	return nil
}

func (s *PlanningStore) hydratePlanningEvent(ctx context.Context, event *PlanningOperationEvent) error {
	rows, err := s.db.QueryContext(ctx, `SELECT field,content_id FROM planning_event_text_refs WHERE event_id=? ORDER BY field`, event.EventID)
	if err != nil {
		return err
	}
	refs := map[string]string{}
	for rows.Next() {
		var field, id string
		if err := rows.Scan(&field, &id); err != nil {
			rows.Close()
			return err
		}
		refs[field] = id
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return err
	}
	texts := map[string]string{}
	for field, id := range refs {
		text, found := texts[id]
		if !found {
			source, err := s.planningTextSource(ctx, id)
			if err != nil {
				return err
			}
			text, err = readPlanningText(ctx, source)
			if err != nil {
				return err
			}
			texts[id] = text
		}
		if field == "answer_payload.text" {
			answer, _ := payload["answer_payload"].(map[string]any)
			if answer == nil {
				answer = map[string]any{}
				payload["answer_payload"] = answer
			}
			answer["text"] = text
		} else {
			payload[field] = text
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	event.PayloadJSON = string(raw)
	return nil
}

func (s *PlanningStore) hydrateSessionTitle(ctx context.Context, session *PlanningSession) error {
	var id sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT title_content_id FROM planning_sessions WHERE planning_session_id=?`, session.PlanningSessionID).Scan(&id); err != nil {
		return err
	}
	if !id.Valid {
		return nil
	}
	source, err := s.planningTextSource(ctx, id.String)
	if err != nil {
		return err
	}
	session.Title, err = readPlanningText(ctx, source)
	return err
}

func (s *PlanningStore) VerifyPlanningText(ctx context.Context, id string, source *PlanningTextSource) (bool, error) {
	stored, err := s.planningTextSource(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stored.SHA256 != source.SHA256 || stored.SizeBytes != source.SizeBytes {
		return false, fmt.Errorf("planning text witness mismatch")
	}
	reader, err := stored.Open(ctx)
	if err != nil {
		return false, err
	}
	_, err = io.Copy(io.Discard, reader)
	return err == nil, errors.Join(err, reader.Close())
}

func (s *PlanningStore) VerifyPlanningEventTextRefs(ctx context.Context, id string, expected map[string]string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT field,content_id FROM planning_event_text_refs WHERE event_id=?`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var field, content string
		if err := rows.Scan(&field, &content); err != nil {
			return err
		}
		if expected[field] != content {
			return fmt.Errorf("planning event text binding mismatch")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(expected) {
		return fmt.Errorf("planning event text binding is incomplete")
	}
	return nil
}
