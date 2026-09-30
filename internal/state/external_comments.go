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
	"os"
)

// ExternalCommentSource is a replayable, ordered capture. Its owner closes it
// after all comparison, persistence, and presentation work has completed.
type ExternalCommentSource interface {
	Replay(context.Context, func(ExternalComment) error) error
	Close() error
}

// ExternalCommentList is useful for already-materialized caller data and tests.
// Network intake uses ExternalCommentSpool instead.
type ExternalCommentList []ExternalComment

func (items ExternalCommentList) Replay(ctx context.Context, visit func(ExternalComment) error) error {
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(item); err != nil {
			return err
		}
	}
	return nil
}
func (ExternalCommentList) Close() error { return nil }

type ExternalCommentSpool struct {
	file   *os.File
	writer *bufio.Writer
	sealed bool
	closed bool
}

func NewExternalCommentSpool() (*ExternalCommentSpool, error) {
	file, err := os.CreateTemp("", "kingdom-external-comments-*")
	if err != nil {
		return nil, err
	}
	return &ExternalCommentSpool{file: file, writer: bufio.NewWriter(file)}, nil
}

func (s *ExternalCommentSpool) Append(comment ExternalComment) error {
	if s.closed || s.sealed {
		return fmt.Errorf("comment spool is not writable")
	}
	return json.NewEncoder(s.writer).Encode(NormalizeExternalComment(comment))
}

func (s *ExternalCommentSpool) Seal() error {
	if s.closed {
		return fmt.Errorf("comment spool is closed")
	}
	if s.sealed {
		return nil
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}
	s.sealed = true
	return nil
}

func (s *ExternalCommentSpool) Replay(ctx context.Context, visit func(ExternalComment) error) error {
	if s.closed || !s.sealed {
		return fmt.Errorf("comment spool is not readable")
	}
	// Independent section readers allow repeated reads without mutating shared
	// cursor state. The capture is sealed before it crosses this boundary.
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(io.NewSectionReader(s.file, 0, info.Size()))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var item ExternalComment
		if err := decoder.Decode(&item); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if err := visit(item); err != nil {
			return err
		}
	}
}

func (s *ExternalCommentSpool) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.file.Close(), os.Remove(s.file.Name()))
}

func NormalizeExternalComment(comment ExternalComment) ExternalComment {
	if comment.OriginalBytes < len(comment.Body) {
		comment.OriginalBytes = len(comment.Body)
	}
	comment.RetainedBytes = len(comment.Body)
	return comment
}

func ReplayExternalComments(ctx context.Context, source ExternalCommentSource, visit func(ExternalComment) error) error {
	if source == nil {
		return nil
	}
	return source.Replay(ctx, func(c ExternalComment) error { return visit(NormalizeExternalComment(c)) })
}

func CloseExternalComments(source ExternalCommentSource) error {
	if source == nil {
		return nil
	}
	return source.Close()
}

// ExternalCommentsDigest compares normalized source content without an
// aggregate comment slice or JSON allocation. Historical truncation metadata
// is retained, so an incomplete historical import cannot masquerade as full.
func ExternalCommentsDigest(ctx context.Context, source ExternalCommentSource) (string, error) {
	h := sha256.New()
	err := ReplayExternalComments(ctx, source, func(c ExternalComment) error { return json.NewEncoder(h).Encode(c) })
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type snapshotCommentSource struct {
	db         *sql.DB
	snapshotID string
}

func (s snapshotCommentSource) Replay(ctx context.Context, visit func(ExternalComment) error) error {
	return replaySnapshotComments(ctx, s.db, s.snapshotID, visit)
}
func (snapshotCommentSource) Close() error { return nil }
func (s *ExternalStore) SnapshotComments(snapshotID string) ExternalCommentSource {
	return snapshotCommentSource{s.db, snapshotID}
}

func replayCaptureComments(ctx context.Context, tx *sql.Tx, source ExternalCommentSource, visit func(ExternalComment) error) error {
	if snapshot, ok := source.(snapshotCommentSource); ok {
		return replaySnapshotComments(ctx, tx, snapshot.snapshotID, func(c ExternalComment) error { return visit(NormalizeExternalComment(c)) })
	}
	return ReplayExternalComments(ctx, source, visit)
}

func replaySnapshotComments(ctx context.Context, q sqlQuerier, snapshotID string, visit func(ExternalComment) error) error {
	rows, err := q.QueryContext(ctx, `SELECT provider_comment_id,author_display_identifier,body,canonical_url,created_at,updated_at,original_bytes,retained_bytes,truncation_reason FROM external_source_snapshot_comments WHERE snapshot_id=? ORDER BY ordinal,provider_comment_id`, snapshotID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c ExternalComment
		if err := rows.Scan(&c.ProviderCommentID, &c.Author, &c.Body, &c.CanonicalURL, &c.CreatedAt, &c.UpdatedAt, &c.OriginalBytes, &c.RetainedBytes, &c.TruncationReason); err != nil {
			return err
		}
		if err := visit(c); err != nil {
			return err
		}
	}
	return rows.Err()
}
