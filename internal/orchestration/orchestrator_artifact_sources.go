package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

// builderArtifactSource resolves the immutable delta before the validator takes
// its single-connection write transaction. Metadata lives in a private indexed
// spool; neither the snapshot inventory nor a path-to-blob map lives in Go.
func (o *orchestrationLoop) builderArtifactSource(node nodeDefinition, workspace, base, result string) state.WorkerArtifactSource {
	return func(ctx context.Context) (_ state.WorkerArtifactIterator, err error) {
		snapshot := base != "" && result != ""
		if snapshot && o.fsStore == nil {
			return nil, fmt.Errorf("runtime filesystem store is required for snapshot artifacts")
		}
		file, err := os.CreateTemp("", "kingdom-artifact-metadata-*.sqlite")
		if err != nil {
			return nil, err
		}
		cursor := &builderArtifactCursor{path: file.Name(), loop: o, snapshot: snapshot}
		if err := file.Close(); err != nil {
			os.Remove(cursor.path)
			return nil, err
		}
		defer func() {
			if err != nil {
				_ = cursor.Close()
			}
		}()
		cursor.db, err = sql.Open("sqlite", (&url.URL{Scheme: "file", Path: cursor.path}).String())
		if err != nil {
			return nil, err
		}
		cursor.db.SetMaxOpenConns(1)
		if _, err = cursor.db.ExecContext(ctx, `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; PRAGMA temp_store=FILE; CREATE TABLE metadata(path TEXT PRIMARY KEY, descriptor BLOB NOT NULL, blob_id TEXT NOT NULL) WITHOUT ROWID`); err != nil {
			return nil, err
		}
		tx, err := cursor.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		statement, err := tx.PrepareContext(ctx, `INSERT INTO metadata VALUES (?,?,?)`)
		if err != nil {
			return nil, err
		}
		defer statement.Close()
		stage := func(artifact state.WorkerArtifact, blob string) error {
			artifact.Path = strings.TrimSpace(artifact.Path)
			artifact.Kind = determineArtifactKind(node.NodeID, node.NodeClass, node.RequiredRole, artifact.Path)
			raw, err := json.Marshal(artifact)
			if err != nil {
				return err
			}
			_, err = statement.ExecContext(ctx, artifact.Path, raw, blob)
			return err
		}
		if snapshot {
			err = o.fsStore.VisitSnapshotDiff(ctx, base, result, func(diff runtimefs.SnapshotDiffEntry) error {
				artifact := state.WorkerArtifact{Path: diff.Path, Name: filepath.Base(diff.Path), MIME: inferMIMEType(diff.Path), Action: "write"}
				if diff.EntryType == "delete" {
					artifact.Action = "delete"
				} else {
					mode := diff.ModeBits
					artifact.ModeBits = &mode
					size := diff.SizeBytes
					artifact.SizeBytes = &size
					artifact.SHA256 = diff.BlobID
				}
				return stage(artifact, diff.BlobID)
			})
		} else {
			err = visitWorkspaceArtifacts(ctx, workspace, func(artifact executionArtifact) error {
				return stage(state.WorkerArtifact{Path: artifact.Path, Name: artifact.Name, MIME: artifact.MIME, Action: artifact.Action, ModeBits: artifact.ModeBits}, "")
			})
		}
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		cursor.rows, err = cursor.db.QueryContext(ctx, `SELECT descriptor,blob_id FROM metadata ORDER BY path`)
		if err != nil {
			return nil, err
		}
		return cursor, nil
	}
}

type builderArtifactCursor struct {
	db       *sql.DB
	rows     *sql.Rows
	path     string
	loop     *orchestrationLoop
	snapshot bool
}

func (c *builderArtifactCursor) Next(ctx context.Context) (state.WorkerArtifact, error) {
	if err := ctx.Err(); err != nil {
		return state.WorkerArtifact{}, err
	}
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			return state.WorkerArtifact{}, err
		}
		return state.WorkerArtifact{}, io.EOF
	}
	var raw []byte
	var blob string
	if err := c.rows.Scan(&raw, &blob); err != nil {
		return state.WorkerArtifact{}, err
	}
	var artifact state.WorkerArtifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return artifact, err
	}
	if artifact.Action != "delete" && c.snapshot {
		if blob == "" || artifact.SHA256 != blob || artifact.SizeBytes == nil {
			return artifact, fmt.Errorf("snapshot artifact content binding is incomplete")
		}
		artifact.OpenContent = func(ctx context.Context) (io.ReadCloser, error) {
			reader, writer := io.Pipe()
			go func() { _, err := c.loop.fsStore.CopyBlob(ctx, blob, writer); _ = writer.CloseWithError(err) }()
			return reader, nil
		}
	}
	return artifact, nil
}

func (c *builderArtifactCursor) Close() error {
	var err error
	if c.rows != nil {
		err = errors.Join(err, c.rows.Close())
		c.rows = nil
	}
	if c.db != nil {
		err = errors.Join(err, c.db.Close())
		c.db = nil
	}
	if c.path != "" {
		err = errors.Join(err, os.Remove(c.path))
		c.path = ""
	}
	return err
}

type discoveredArtifactCursor struct {
	node   nodeDefinition
	items  []executionArtifact
	offset int
}

func (c *discoveredArtifactCursor) Next(ctx context.Context) (state.WorkerArtifact, error) {
	if err := ctx.Err(); err != nil {
		return state.WorkerArtifact{}, err
	}
	if c.offset == len(c.items) {
		return state.WorkerArtifact{}, io.EOF
	}
	item := c.items[c.offset]
	c.offset++
	return state.WorkerArtifact{Kind: determineArtifactKind(c.node.NodeID, c.node.NodeClass, c.node.RequiredRole, item.Path), Action: firstNonEmptyString(item.Action, "write"), Name: item.Name, MIME: item.MIME, Path: item.Path, ModeBits: item.ModeBits}, nil
}
func (*discoveredArtifactCursor) Close() error { return nil }

// A replayable projection source visits only bounded pages and releases the
// canonical cursor before content reads or audit writes on the same DB pool.
type artifactProjectionSource func(context.Context, func(artifactOutputRecord) error) error

func (o *orchestrationLoop) persistedArtifactProjection(node nodeDefinition, attempt nodeAttempt, attemptID string) artifactProjectionSource {
	return func(ctx context.Context, visit func(artifactOutputRecord) error) error {
		after := ""
		for {
			rows, err := o.db.QueryContext(ctx, `SELECT artifact_id,artifact_kind,artifact_action,path,producer_role,sha256,mode_bits FROM artifacts WHERE attempt_id=? AND artifact_id>? ORDER BY artifact_id LIMIT 128`, attemptID, after)
			if err != nil {
				return err
			}
			page := make([]artifactOutputRecord, 0, 128)
			for rows.Next() {
				r := artifactOutputRecord{NodeID: node.NodeID, Attempt: attempt.Attempt}
				if err := rows.Scan(&r.ArtifactID, &r.ArtifactKind, &r.ArtifactAction, &r.RelativePath, &r.ProducerRole, &r.SHA256, &r.ModeBits); err != nil {
					rows.Close()
					return err
				}
				page = append(page, r)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, r := range page {
				if err := visit(r); err != nil {
					return err
				}
				after = r.ArtifactID
			}
			if len(page) < 128 {
				return nil
			}
		}
	}
}

func (o *orchestrationLoop) persistArtifactSource(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, workspace, now string, source state.WorkerArtifactSource, inline map[string][]byte, open func(context.Context, string) (io.ReadCloser, error), project bool) error {
	header, err := json.Marshal(map[string]any{"schema_version": "v1", "status": "succeeded", "summary": "Canonical node artifacts", "payload_json": map[string]any{"node_id": node.NodeID}, "artifacts": []any{}, "error_reason": nil})
	if err != nil {
		return err
	}
	result, err := o.validator.ValidateAndPersist(ctx, state.WorkerOutputValidationRequest{RunID: run.RunID, NodeRunID: attempt.NodeRunID, AttemptID: attemptID, ProducerRole: node.RequiredRole, WorkspacePath: workspace, ArtifactContentByPath: inline, OpenArtifact: open, OutputJSON: header, CreatedAt: now, Artifacts: source})
	if err != nil {
		return fmt.Errorf("persist node artifacts: %w", err)
	}
	if result.ArtifactCount == 0 || !project || !executionSpecForNode(node.NodeID, node.NodeClass).ProjectArtifacts {
		return nil
	}
	return o.republishPersistedArtifacts(ctx, run, node, attempt, attemptID)
}
