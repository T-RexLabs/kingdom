package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
)

// The first source read is after the prompt transaction's authority reads and
// before its first write. A second WAL writer then invalidates that snapshot.
type gatedPromptSource struct {
	*strings.Reader
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *gatedPromptSource) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})
	return r.Reader.Read(p)
}

func TestPersistExecutionPromptRetriesConcurrentWALWriterWithExactSource(t *testing.T) {
	for _, scenario := range []string{"parallel_prompt", "lease_replaced"} {
		t.Run(scenario, func(t *testing.T) {
			db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", filepath.Join(t.TempDir(), "prompts.db")))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			db.SetMaxOpenConns(4)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_prompt_2','run_1','decree_intake',2,1,'ready','king','agent_king',1)`)
			const now = "2026-03-01T12:00:00Z"
			store := executionstore.New(db)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := func(id, nodeRunID, content string) ExecutionPromptRequest {
				t.Helper()
				if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: id, RunID: "run_1", NodeRunID: nodeRunID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
				claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: id, LeaseOwner: "owner_" + id, LeaseUntil: "2026-03-01T12:01:00Z", ClaimedAt: now})
				if err != nil || !acquired {
					t.Fatalf("claim prompt lease: acquired=%t err=%v", acquired, err)
				}
				sum := sha256.Sum256([]byte(content))
				return ExecutionPromptRequest{AttemptID: id, RunID: "run_1", NodeRunID: nodeRunID, LeaseOwner: claim.LeaseOwner, LeaseVersion: claim.LeaseVersion, CreatedAt: now, Source: RequestContentSource{Reader: strings.NewReader(content), SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(content))}}
			}
			content := strings.Repeat("first prompt\n", 6000) + "complete ending"
			first := request("prompt_1", "nr_1", content)
			second := request("prompt_2", "nr_prompt_2", "independent second prompt")
			gate := &gatedPromptSource{Reader: strings.NewReader(content), entered: make(chan struct{}), release: make(chan struct{})}
			first.Source.Reader = gate
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			defer release()
			result := make(chan error, 1)
			go func() {
				_, err := PersistExecutionPrompt(ctx, db, first)
				result <- err
			}()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("first prompt did not reach its read-to-write boundary")
			}
			if scenario == "parallel_prompt" {
				if _, err := PersistExecutionPrompt(ctx, db, second); err != nil {
					t.Fatalf("persist concurrent prompt: %v", err)
				}
			} else {
				mustExec(t, db, `UPDATE attempt_executions SET lease_owner='replacement',lease_version=lease_version+1 WHERE attempt_id='prompt_1'`)
			}
			release()
			select {
			case err := <-result:
				if scenario == "lease_replaced" {
					if err == nil || !strings.Contains(err.Error(), "lineage mismatch") {
						t.Fatalf("revoked prompt lease result=%v", err)
					}
				} else if err != nil {
					t.Fatalf("persist first prompt across concurrent writer: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("prompt persistence did not finish")
			}
			if scenario == "lease_replaced" {
				for _, table := range []string{"execution_prompt_refs", "request_contents", "request_content_chunks"} {
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("revoked prompt left %s rows=%d err=%v", table, count, err)
					}
				}
				return
			}
			var copied bytes.Buffer
			ref, err := CopyRequestContent(ctx, db, "execution-prompt:prompt_1", &copied)
			if err != nil || copied.String() != content || ref.SizeBytes != int64(len(content)) {
				t.Fatalf("retried prompt was truncated or changed: bytes=%d err=%v", copied.Len(), err)
			}
			first.Source.Reader = strings.NewReader(content)
			if replay, err := PersistExecutionPrompt(ctx, db, first); err != nil || replay != ref {
				t.Fatalf("idempotent prompt replay=%#v err=%v", replay, err)
			}
			reader, err := OpenExecutionPrompt(ctx, db, first.AttemptID, ref)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if n, err := io.Copy(io.Discard, reader); err != nil || n != int64(len(content)) {
				t.Fatalf("opened prompt bytes=%d err=%v", n, err)
			}
		})
	}
}
