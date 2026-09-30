package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"kingdom/internal/core/providerexecution"
)

func planningProviderReferenceJSON(ref *providerexecution.ContentReference) string {
	if ref == nil {
		return ""
	}
	raw, _ := json.Marshal(ref)
	return string(raw)
}

func (s *PlanningStore) HydrateOperationOutput(ctx context.Context, operation *PlanningOperation) error {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT observed_output_ref_json FROM planning_operations WHERE operation_id=?`, operation.OperationID).Scan(&raw); err != nil {
		return err
	}
	if raw == "" {
		return nil
	}
	var ref providerexecution.ContentReference
	if err := json.Unmarshal([]byte(raw), &ref); err != nil {
		return err
	}
	var out strings.Builder
	if err := s.CopyPlanningProviderContent(ctx, ref, &out); err != nil {
		return err
	}
	operation.ObservedOutputText = out.String()
	return nil
}

func (s *PlanningStore) CopyPlanningProviderContent(ctx context.Context, ref providerexecution.ContentReference, out io.Writer) error {
	return copyPlanningProviderContent(ctx, s.db, ref, out)
}

func copyPlanningProviderContent(ctx context.Context, q sqlQuerier, ref providerexecution.ContentReference, out io.Writer) error {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return err
	}
	var size int64
	if err := q.QueryRowContext(ctx, `SELECT size_bytes FROM planning_provider_contents WHERE planning_provider_invocation_id=? AND sha256=?`, ref.InvocationID, ref.SHA256).Scan(&size); err != nil {
		return err
	}
	if size != ref.SizeBytes {
		return fmt.Errorf("planning provider content size mismatch")
	}
	rows, err := q.QueryContext(ctx, `SELECT ordinal,content FROM planning_provider_content_chunks WHERE planning_provider_invocation_id=? AND sha256=? ORDER BY ordinal`, ref.InvocationID, ref.SHA256)
	if err != nil {
		return err
	}
	defer rows.Close()
	hash := sha256.New()
	total := int64(0)
	next := 0
	for rows.Next() {
		var ordinal int
		var raw []byte
		if err := rows.Scan(&ordinal, &raw); err != nil {
			return err
		}
		if ordinal != next || len(raw) == 0 || int64(len(raw)) != min(int64(providerexecution.ContentChunkBytes), ref.SizeBytes-total) {
			return fmt.Errorf("planning provider content chunks are incomplete")
		}
		n, err := io.MultiWriter(out, hash).Write(raw)
		if err != nil {
			return err
		}
		if n != len(raw) {
			return io.ErrShortWrite
		}
		total += int64(n)
		next++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if total != ref.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("planning provider content integrity mismatch")
	}
	return nil
}

func importPlanningProviderContent(ctx context.Context, tx *sql.Tx, invocationID string, ref providerexecution.ContentReference, staged []providerexecution.StagedContent) error {
	if err := ref.Validate(invocationID); err != nil {
		return err
	}
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT size_bytes FROM planning_provider_contents WHERE planning_provider_invocation_id=? AND sha256=?`, invocationID, ref.SHA256).Scan(&size)
	if err == nil {
		return copyPlanningProviderContent(ctx, tx, ref, io.Discard)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var source *providerexecution.StagedContent
	for i := range staged {
		if staged[i].Reference == ref {
			source = &staged[i]
			break
		}
	}
	if source == nil {
		return fmt.Errorf("planning provider content source is unavailable")
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	if _, err := tx.ExecContext(ctx, `INSERT INTO planning_provider_contents(planning_provider_invocation_id,sha256,size_bytes) VALUES(?,?,?)`, invocationID, ref.SHA256, ref.SizeBytes); err != nil {
		return err
	}
	buffer := make([]byte, providerexecution.ContentChunkBytes)
	for ordinal := 0; ; ordinal++ {
		n, err := io.ReadFull(reader, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		if n > 0 {
			if _, writeErr := tx.ExecContext(ctx, `INSERT INTO planning_provider_content_chunks(planning_provider_invocation_id,sha256,ordinal,content) VALUES(?,?,?,?)`, invocationID, ref.SHA256, ordinal, buffer[:n]); writeErr != nil {
				return writeErr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	// Verifying both ingress and stored chunks makes restart replay independent
	// of process-local paths. The caller's settlement transaction owns publication.
	return copyPlanningProviderContent(ctx, tx, ref, io.Discard)
}
