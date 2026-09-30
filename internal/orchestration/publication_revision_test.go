package orchestration

import (
	"context"
	"testing"
)

func TestPublicationEntryRevisionTracksAtomicScopedRetries(t *testing.T) {
	loop, db, _, artifacts := trackedProjectionFixture(t)
	for _, batch := range []string{"revision-batch", "unrelated-batch"} {
		if _, err := db.Exec(`INSERT INTO run_publication_batches(publication_batch_id,run_id,attempt_id,target_mode,status,created_at) VALUES (?,'run_1','audit-attempt','direct','failed','2026-03-01T12:00:00Z')`, batch); err != nil {
			t.Fatal(err)
		}
	}
	revision := func(batch string) int64 {
		t.Helper()
		var value int64
		if err := db.QueryRow(`SELECT entries_revision FROM run_publication_batches WHERE publication_batch_id=?`, batch).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	record := func(status string) error {
		return loop.recordPublicationEntry(context.Background(), "revision-batch", "run_1", "audit-attempt", "output.txt", artifacts[0], status, "reason", "explanation", "blob", publicationFileState{SHA256: "expected", ModeBits: artifacts[0].ModeBits}, publicationFileState{SHA256: "observed", ModeBits: artifacts[0].ModeBits}, "2026-03-01T12:00:00Z")
	}
	if revision("revision-batch") != 0 {
		t.Fatal("nonzero initial revision")
	}
	if err := record("failed"); err != nil {
		t.Fatal(err)
	}
	if revision("revision-batch") != 1 || revision("unrelated-batch") != 0 {
		t.Fatal("entry revision advanced wrong batch")
	}
	mustExec(t, db, `CREATE TRIGGER fail_revision BEFORE UPDATE OF entries_revision ON run_publication_batches BEGIN SELECT RAISE(ABORT,'injected revision failure'); END`)
	if err := record("published"); err == nil {
		t.Fatal("counter failure did not roll back publication")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM run_publication_entries WHERE publication_batch_id='revision-batch'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	var paths int
	if err := db.QueryRow(`SELECT COUNT(*) FROM run_publication_paths`).Scan(&paths); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || paths != 0 || revision("revision-batch") != 1 {
		t.Fatalf("non-atomic failure: status=%s paths=%d revision=%d", status, paths, revision("revision-batch"))
	}
	mustExec(t, db, `DROP TRIGGER fail_revision`)
	if err := record("published"); err != nil {
		t.Fatal(err)
	}
	if revision("revision-batch") != 2 {
		t.Fatal("failed-to-published retry did not advance revision")
	}
	if err := record("published"); err != nil {
		t.Fatal(err)
	}
	if revision("revision-batch") != 2 {
		t.Fatal("terminal replay advanced revision")
	}
}
