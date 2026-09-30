package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type generatedWorkerArtifacts struct {
	count, position, contentClosed, closed int
	failure                                string
	pathPrefix                             string
	observe                                func(int)
}

type trackedArtifactReader struct {
	io.Reader
	close func()
}

func (r *trackedArtifactReader) Close() error { r.close(); return nil }

func (c *generatedWorkerArtifacts) Next(ctx context.Context) (WorkerArtifact, error) {
	if err := ctx.Err(); err != nil {
		return WorkerArtifact{}, err
	}
	if c.contentClosed != c.position {
		return WorkerArtifact{}, fmt.Errorf("metadata advanced before preceding content was consumed and closed")
	}
	if c.observe != nil {
		c.observe(c.position)
	}
	if c.position == c.count {
		if c.failure == "eof_error" {
			return WorkerArtifact{}, errors.Join(io.EOF, errors.New("injected metadata trailer failure"))
		}
		if c.failure == "metadata" {
			return WorkerArtifact{}, errors.New("injected late metadata failure")
		}
		return WorkerArtifact{}, io.EOF
	}
	index := c.position
	c.position++
	size := int64(0)
	artifact := WorkerArtifact{ModeBits: artifactTestMode(0644), Kind: "deliverable", Action: "write", Name: fmt.Sprintf("file-%06d", index), MIME: "text/plain", Path: fmt.Sprintf("%sfile-%06d", c.pathPrefix, index), SHA256: checksumHex(nil), SizeBytes: &size}
	artifact.OpenContent = func(context.Context) (io.ReadCloser, error) {
		body := io.Reader(strings.NewReader(""))
		if index == c.count-1 && c.failure == "hash" {
			body = strings.NewReader("corrupt")
		}
		return &trackedArtifactReader{Reader: body, close: func() { c.contentClosed++ }}, nil
	}
	if index == c.count-1 && c.failure == "schema" {
		artifact.Kind = "unsupported"
	}
	return artifact, nil
}
func (c *generatedWorkerArtifacts) Close() error {
	c.closed++
	if c.failure == "close" {
		return errors.New("injected late metadata close failure")
	}
	return nil
}

func streamedWorkerRequest(source WorkerArtifactSource) WorkerOutputValidationRequest {
	return WorkerOutputValidationRequest{RunID: "run_1", NodeRunID: "nr_1", AttemptID: "streamed-attempt", ProducerRole: "governance", CreatedAt: "2026-03-01T12:00:00Z", OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"streamed artifacts","payload_json":{},"artifacts":[],"error_reason":null}`), Artifacts: source}
}

func TestWorkerArtifactSourceRollsBackLateMetadataCloseSchemaAndHashFailures(t *testing.T) {
	for _, failure := range []string{"metadata", "eof_error", "close", "schema", "hash"} {
		t.Run(failure, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			cursor := &generatedWorkerArtifacts{count: 2049, failure: failure}
			req := streamedWorkerRequest(func(ctx context.Context) (WorkerArtifactIterator, error) {
				// This read would deadlock if Open ran inside the validator's
				// transaction on its single-connection SQLite pool.
				var n int
				if err := db.QueryRowContext(ctx, `SELECT count(*) FROM artifacts`).Scan(&n); err != nil {
					return nil, err
				}
				return cursor, nil
			})
			if _, err := NewWorkerOutputValidator(db, loadSchemaSet(t)).ValidateAndPersist(context.Background(), req); err == nil {
				t.Fatal("late source failure committed artifacts")
			}
			if cursor.closed != 1 {
				t.Fatalf("metadata cursor closed %d times", cursor.closed)
			}
			for _, table := range []string{"artifacts", "artifact_contents", "artifact_content_chunks"} {
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil || n != 0 {
					t.Fatalf("partial %s publication: %d %v", table, n, err)
				}
			}
		})
	}
}

func TestWorkerArtifactSourceMetadataBeyondSixteenMiBHasBoundedLiveMemory(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "artifacts.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	mustExec(t, db, `PRAGMA foreign_keys=ON; PRAGMA cache_size=-1024; PRAGMA mmap_size=0; PRAGMA temp_store=FILE`)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	validator := NewWorkerOutputValidator(db, loadSchemaSet(t))
	cursor := &generatedWorkerArtifacts{count: 10000, pathPrefix: strings.Repeat("p", 2048) + "/"}
	runtime.GC()
	var initial runtime.MemStats
	runtime.ReadMemStats(&initial)
	var peak uint64
	cursor.observe = func(n int) {
		if n%1000 != 0 {
			return
		}
		runtime.GC()
		var current runtime.MemStats
		runtime.ReadMemStats(&current)
		if current.HeapAlloc > initial.HeapAlloc && current.HeapAlloc-initial.HeapAlloc > peak {
			peak = current.HeapAlloc - initial.HeapAlloc
		}
	}
	result, err := validator.ValidateAndPersist(context.Background(), streamedWorkerRequest(func(context.Context) (WorkerArtifactIterator, error) { return cursor, nil }))
	if err != nil || result.ArtifactCount != cursor.count || cursor.contentClosed != cursor.count || cursor.closed != 1 {
		t.Fatalf("result=%+v cursor=%+v err=%v", result, cursor, err)
	}
	if peak > 12<<20 {
		t.Fatalf("artifact inventory retained in Go heap: peak growth=%d bytes", peak)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM artifacts`).Scan(&n); err != nil || n != cursor.count {
		t.Fatalf("published=%d err=%v", n, err)
	}
}

func TestWorkerArtifactSourceRejectsCompetingMetadataInputs(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	opened := false
	req := streamedWorkerRequest(func(context.Context) (WorkerArtifactIterator, error) {
		opened = true
		return &generatedWorkerArtifacts{}, nil
	})
	req.OutputJSON = []byte(`{"schema_version":"v1","status":"succeeded","summary":"complete","payload_json":{},"artifacts":[{"name":"a","mime":"text/plain","mode_bits":420,"path":"a"}],"error_reason":null}`)
	if _, err := NewWorkerOutputValidator(db, loadSchemaSet(t)).ValidateAndPersist(context.Background(), req); err == nil || opened {
		t.Fatalf("competing source accepted or opened: %v %t", err, opened)
	}
}

func artifactTestMode(bits int64) *int64 { return &bits }
