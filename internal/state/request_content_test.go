package state_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"
	_ "modernc.org/sqlite"
)

func requestContentDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	requestContentExec(t, db, `PRAGMA foreign_keys=ON`)
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatal(err)
		}
		requestContentExec(t, db, string(raw))
	}
	return db
}

func requestContentExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

type requestContentRepeatReader byte

func (r requestContentRepeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

type requestContentReadFailure struct{}

func (requestContentReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("injected content read failure")
}

func TestCreateWorkOrderPublishesCanonicalChunksAtomically(t *testing.T) {
	db := requestContentDB(t)
	const size = (17 << 20) + 7
	hash := sha256.New()
	io.CopyN(hash, requestContentRepeatReader('d'), size)
	source := state.RequestContentSource{Reader: io.LimitReader(requestContentRepeatReader('d'), size), SizeBytes: size, SHA256: hex.EncodeToString(hash.Sum(nil))}
	err := state.NewExternalStore(db).CreateWorkOrder(context.Background(), state.CreateWorkOrderRequest{WorkOrderID: "streamed", Title: "Streamed", TextContent: &source, CreatedAt: "2026-03-01T12:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	var inline, storage string
	if err := db.QueryRow(`SELECT work_order_text,text_storage FROM work_orders WHERE work_order_id='streamed'`).Scan(&inline, &storage); err != nil || inline != "" || storage != "chunked" {
		t.Fatalf("inline=%q storage=%q err=%v", inline, storage, err)
	}
	var chunks, largest int
	if err := db.QueryRow(`SELECT count(*),max(length(content)) FROM request_content_chunks`).Scan(&chunks, &largest); err != nil || chunks != 273 || largest != 65536 {
		t.Fatalf("chunks=%d largest=%d err=%v", chunks, largest, err)
	}
	actual := sha256.New()
	if err := state.CopyWorkOrderText(context.Background(), db, "streamed", actual); err != nil || !bytes.Equal(actual.Sum(nil), hash.Sum(nil)) {
		t.Fatalf("canonical bytes changed: %v", err)
	}
	reader, err := state.OpenWorkOrderText(context.Background(), db, "streamed")
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, reader)
	reader.Close()
	if err != nil || n != size {
		t.Fatalf("staged size=%d err=%v", n, err)
	}
}

func TestCreateWorkOrderRollsBackChunksOnInputAndLatePublicationFailures(t *testing.T) {
	for _, failure := range []string{"short", "changed", "read", "late"} {
		t.Run(failure, func(t *testing.T) {
			db := requestContentDB(t)
			const size = (1 << 20) + 3
			h := sha256.New()
			io.CopyN(h, requestContentRepeatReader('x'), size)
			source := state.RequestContentSource{Reader: io.LimitReader(requestContentRepeatReader('x'), size), SizeBytes: size, SHA256: hex.EncodeToString(h.Sum(nil))}
			switch failure {
			case "short":
				source.Reader = io.LimitReader(requestContentRepeatReader('x'), size-1)
			case "changed":
				source.Reader = io.LimitReader(requestContentRepeatReader('y'), size)
			case "read":
				source.Reader = io.MultiReader(io.LimitReader(requestContentRepeatReader('x'), 65536), requestContentReadFailure{})
			case "late":
				requestContentExec(t, db, `CREATE TRIGGER fail_text_reference BEFORE INSERT ON work_order_text_refs BEGIN SELECT RAISE(ABORT,'injected reference failure'); END`)
			}
			if err := state.NewExternalStore(db).CreateWorkOrder(context.Background(), state.CreateWorkOrderRequest{WorkOrderID: "failed", Title: "Failed", TextContent: &source, CreatedAt: "2026-03-01T12:00:00Z"}); err == nil {
				t.Fatal("failed text was published")
			}
			for _, table := range []string{"request_contents", "request_content_chunks", "work_order_text_refs"} {
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil || n != 0 {
					t.Fatalf("partial %s=%d err=%v", table, n, err)
				}
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM work_orders WHERE work_order_id='failed'`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("partial work order=%d err=%v", n, err)
			}
		})
	}
}

func TestCanonicalWorkOrderContentNeverFallsBackOnCorruption(t *testing.T) {
	db := requestContentDB(t)
	if err := state.NewExternalStore(db).CreateWorkOrder(context.Background(), state.CreateWorkOrderRequest{WorkOrderID: "corrupt", Title: "Corrupt", Text: "original", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	requestContentExec(t, db, `UPDATE work_orders SET work_order_text='unsafe fallback' WHERE work_order_id='corrupt'`)
	requestContentExec(t, db, `DROP TRIGGER request_content_chunks_no_update`)
	requestContentExec(t, db, `UPDATE request_content_chunks SET content=x'636f727275707421'`)
	if _, err := state.OpenWorkOrderText(context.Background(), db, "corrupt"); err == nil {
		t.Fatal("corrupt chunks accepted")
	}
	requestContentExec(t, db, `DROP TRIGGER work_order_text_refs_no_delete`)
	requestContentExec(t, db, `DELETE FROM work_order_text_refs WHERE work_order_id='corrupt'`)
	if err := state.CopyWorkOrderText(context.Background(), db, "corrupt", io.Discard); err == nil || !strings.Contains(err.Error(), "reference is missing") {
		t.Fatalf("missing reference fell back: %v", err)
	}
}
