package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type ArtifactService struct{ db *sql.DB }
type ArtifactNotFoundError struct{ ArtifactID string }
type ArtifactContentUnavailableError struct{ ArtifactID, Reason string }

func (e *ArtifactNotFoundError) Error() string {
	return fmt.Sprintf("artifact not found: %q", e.ArtifactID)
}
func (e *ArtifactContentUnavailableError) Error() string {
	return fmt.Sprintf("artifact content unavailable: %q (%s)", e.ArtifactID, e.Reason)
}
func NewArtifactService(db *sql.DB) *ArtifactService { return &ArtifactService{db: db} }

const artifactColumns = `artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at`

func scanArtifact(row interface{ Scan(...any) error }) (ArtifactRecord, error) {
	var r ArtifactRecord
	err := row.Scan(&r.ArtifactID, &r.RunID, &r.NodeRunID, &r.AttemptID, &r.ArtifactKind, &r.ArtifactAction, &r.Name, &r.MIME, &r.Path, &r.SHA256, &r.SizeBytes, &r.ProducerRole, &r.CreatedAt)
	return r, err
}

func (s *ArtifactService) GetArtifact(ctx context.Context, id string) (ArtifactRecord, error) {
	r, err := scanArtifact(s.db.QueryRowContext(ctx, `SELECT `+artifactColumns+` FROM artifacts WHERE artifact_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, &ArtifactNotFoundError{ArtifactID: id}
	}
	return r, err
}

func (s *ArtifactService) ListRunArtifacts(ctx context.Context, runID string) ([]ArtifactRecord, error) {
	var records []ArtifactRecord
	err := s.WalkRunArtifacts(ctx, runID, func(r ArtifactRecord) error { records = append(records, r); return nil })
	rank := func(kind string) int {
		switch kind {
		case "deliverable":
			return 0
		case "verification":
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(records, func(i, j int) bool {
		if rank(records[i].ArtifactKind) != rank(records[j].ArtifactKind) {
			return rank(records[i].ArtifactKind) < rank(records[j].ArtifactKind)
		}
		if records[i].CreatedAt != records[j].CreatedAt {
			return records[i].CreatedAt > records[j].CreatedAt
		}
		return records[i].ArtifactID < records[j].ArtifactID
	})
	return records, err
}

// WalkRunArtifacts enumerates one SQLite read snapshot with bounded Go memory.
// The visitor must not reenter this database (single-connection pools are used).
// Exports spool this inventory before opening any artifact content readers.
func (s *ArtifactService) WalkRunArtifacts(ctx context.Context, runID string, visit func(ArtifactRecord) error) error {
	rows, err := s.db.QueryContext(ctx, `SELECT `+artifactColumns+` FROM artifacts WHERE run_id = ? ORDER BY artifact_id`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanArtifact(rows)
		if err != nil {
			return err
		}
		if err := visit(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

// CopyArtifactContent verifies the complete canonical SQLite content. Callers
// must stage the output before publishing it: a checksum can fail at EOF.
// Missing/corrupt SQLite content never falls back to a workspace file.
func (s *ArtifactService) CopyArtifactContent(ctx context.Context, id string, dst io.Writer) (ArtifactRecord, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ArtifactRecord{}, err
	}
	defer tx.Rollback()
	r, err := scanArtifact(tx.QueryRowContext(ctx, `SELECT `+artifactColumns+` FROM artifacts WHERE artifact_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, &ArtifactNotFoundError{ArtifactID: id}
	}
	if err != nil {
		return r, err
	}
	unavailable := func(reason string) (ArtifactRecord, error) {
		return r, &ArtifactContentUnavailableError{ArtifactID: id, Reason: reason}
	}
	if r.ArtifactAction == "delete" {
		return unavailable("deletion has no content")
	}
	var checksum string
	var size int64
	err = tx.QueryRowContext(ctx, `SELECT sha256, size_bytes FROM artifact_contents WHERE artifact_id = ?`, id).Scan(&checksum, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return unavailable("stored artifact content unavailable")
	}
	if err != nil {
		return r, err
	}
	if size != r.SizeBytes || size < 0 || !strings.EqualFold(checksum, r.SHA256) {
		return unavailable("artifact metadata mismatch")
	}
	rows, err := tx.QueryContext(ctx, `SELECT chunk_index, content FROM artifact_content_chunks WHERE artifact_id = ? ORDER BY chunk_index`, id)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	hash := sha256.New()
	var total int64
	index := 0
	for rows.Next() {
		var ordinal int
		var chunk []byte
		if err := rows.Scan(&ordinal, &chunk); err != nil {
			return r, err
		}
		if ordinal != index || len(chunk) == 0 || len(chunk) > artifactChunkBytes || int64(len(chunk)) > size-total {
			return unavailable("stored artifact chunk sequence or size mismatch")
		}
		n, err := io.Copy(io.MultiWriter(dst, hash), bytes.NewReader(chunk))
		if err != nil {
			return r, err
		}
		total += n
		index++
	}
	if err := rows.Err(); err != nil {
		return r, err
	}
	if total != size {
		return unavailable("stored artifact size mismatch")
	}
	if !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), checksum) {
		return unavailable("stored artifact checksum mismatch")
	}
	return r, nil
}

// StageArtifactContent returns a private, verified, rewound file. The caller
// owns Close and Remove. Nothing becomes visible before checksum verification.
func (s *ArtifactService) StageArtifactContent(ctx context.Context, id string) (ArtifactRecord, *os.File, error) {
	f, err := os.CreateTemp("", "kingdom-artifact-*")
	if err != nil {
		return ArtifactRecord{}, nil, err
	}
	r, err := s.CopyArtifactContent(ctx, id, f)
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return r, nil, err
	}
	return r, f, nil
}
