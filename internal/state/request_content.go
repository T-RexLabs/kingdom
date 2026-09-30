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
	"strings"
)

const requestContentChunkBytes = 64 << 10

type RequestContentReference struct {
	ContentID string
	SHA256    string
	SizeBytes int64
}

// RequestContentSource binds a staged reader to the exact bytes validated by
// the request decoder. A short or changed source cannot publish a decree.
type RequestContentSource struct {
	Reader    io.Reader
	SHA256    string
	SizeBytes int64
}

// WriteRequestContentTx participates in the caller's publication transaction.
// Chunks precede their immutable manifest; foreign keys are deferred until the
// same commit that publishes the parent resource and its provenance.
func WriteRequestContentTx(ctx context.Context, tx *sql.Tx, id string, source RequestContentSource, createdAt string) (RequestContentReference, error) {
	ref := RequestContentReference{ContentID: id, SHA256: source.SHA256, SizeBytes: source.SizeBytes}
	digest, err := hex.DecodeString(source.SHA256)
	if id == "" || source.Reader == nil || source.SizeBytes < 0 || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != source.SHA256 {
		return ref, fmt.Errorf("invalid request content source")
	}
	hash := sha256.New()
	buffer := make([]byte, requestContentChunkBytes)
	var size int64
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return ref, err
		}
		n, readErr := io.ReadFull(source.Reader, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return ref, readErr
		}
		if int64(n) > source.SizeBytes-size {
			return ref, fmt.Errorf("request content exceeds its declared size")
		}
		if n > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO request_content_chunks(content_id,ordinal,content) VALUES (?,?,?)`, id, count, buffer[:n]); err != nil {
				return ref, err
			}
			_, _ = hash.Write(buffer[:n])
			size += int64(n)
			count++
		}
		if readErr != nil {
			break
		}
	}
	if size != source.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != source.SHA256 {
		return ref, fmt.Errorf("request content checksum or size mismatch")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO request_contents(content_id,sha256,size_bytes,chunk_count,created_at) VALUES (?,?,?,?,?)`, id, source.SHA256, size, count, createdAt)
	return ref, err
}

// CopyRequestContent verifies canonical chunks. The destination must remain
// staged until this returns successfully because corruption may surface at EOF.
func CopyRequestContent(ctx context.Context, db *sql.DB, id string, dst io.Writer) (RequestContentReference, error) {
	ref := RequestContentReference{ContentID: id}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ref, err
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT sha256,size_bytes,chunk_count FROM request_contents WHERE content_id=?`, id).Scan(&ref.SHA256, &ref.SizeBytes, &count); err != nil {
		return ref, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,content FROM request_content_chunks WHERE content_id=? ORDER BY ordinal`, id)
	if err != nil {
		return ref, err
	}
	defer rows.Close()
	hash := sha256.New()
	var size int64
	index := 0
	for rows.Next() {
		var ordinal int
		var content []byte
		if err := rows.Scan(&ordinal, &content); err != nil {
			return ref, err
		}
		if ordinal != index || len(content) == 0 || len(content) > requestContentChunkBytes || int64(len(content)) > ref.SizeBytes-size {
			return ref, fmt.Errorf("request content chunk sequence or size mismatch")
		}
		if _, err := io.Copy(io.MultiWriter(dst, hash), bytes.NewReader(content)); err != nil {
			return ref, err
		}
		size += int64(len(content))
		index++
	}
	if err := rows.Err(); err != nil {
		return ref, err
	}
	if size != ref.SizeBytes || index != count || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return ref, fmt.Errorf("request content checksum or size mismatch")
	}
	return ref, nil
}

func persistDecreeTextTx(ctx context.Context, tx *sql.Tx, req CreateDecreeRequest) error {
	source := req.TextContent
	if source == nil {
		sum := sha256.Sum256([]byte(req.Text))
		source = &RequestContentSource{Reader: strings.NewReader(req.Text), SizeBytes: int64(len(req.Text)), SHA256: hex.EncodeToString(sum[:])}
	}
	ref, err := WriteRequestContentTx(ctx, tx, "decree-text:"+req.DecreeID, *source, req.CreatedAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO decree_text_refs(decree_id,content_id) VALUES (?,?)`, req.DecreeID, ref.ContentID)
	return err
}

// CopyDecreeText selects canonical chunk authority for new decrees. Historical
// SQLite TEXT rows are read in bounded substrings until migrated by their owner;
// a corrupt/missing chunk manifest never falls back to the old column.
func CopyDecreeText(ctx context.Context, db *sql.DB, decreeID string, dst io.Writer) error {
	var id sql.NullString
	var storage string
	if err := db.QueryRowContext(ctx, `SELECT r.content_id,d.text_storage FROM decrees d LEFT JOIN decree_text_refs r ON r.decree_id=d.decree_id WHERE d.decree_id=?`, decreeID).Scan(&id, &storage); err != nil {
		return err
	}
	if storage == "chunked" {
		if !id.Valid {
			return fmt.Errorf("canonical decree text reference is missing")
		}
		_, err := CopyRequestContent(ctx, db, id.String, dst)
		return err
	}
	if storage != "inline" || id.Valid {
		return fmt.Errorf("decree text authority is inconsistent")
	}
	for offset := int64(1); ; offset += requestContentChunkBytes {
		var chunk []byte
		if err := db.QueryRowContext(ctx, `SELECT substr(CAST(decree_text AS BLOB),?,?) FROM decrees WHERE decree_id=?`, offset, requestContentChunkBytes, decreeID).Scan(&chunk); err != nil {
			return err
		}
		if _, err := io.Copy(dst, bytes.NewReader(chunk)); err != nil {
			return err
		}
		if len(chunk) < requestContentChunkBytes {
			return nil
		}
	}
}

// OpenDecreeText returns a verified, replayable private file; Close removes it.
// SQLite remains authoritative, and no path supplied by a caller is opened.
func OpenDecreeText(ctx context.Context, db *sql.DB, id string) (io.ReadCloser, error) {
	file, err := os.CreateTemp("", "kingdom-decree-text-*")
	if err != nil {
		return nil, err
	}
	reader := &requestContentFile{File: file}
	err = CopyDecreeText(ctx, db, id, file)
	if err == nil {
		_, err = file.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}

type requestContentFile struct{ *os.File }

func (f *requestContentFile) Close() error { return errors.Join(f.File.Close(), os.Remove(f.Name())) }
