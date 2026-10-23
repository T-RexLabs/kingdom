package hostedbuilder

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCloseWaitHonorsDeadlineAndRetainsOwnership(t *testing.T) {
	p := &project{closeToken: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := p.Close(ctx); e != context.Canceled {
		t.Fatal("close wait ignored cancellation", e)
	}
	if p.closed {
		t.Fatal("unacquired close claimed completion")
	}
	p.closeToken <- struct{}{}
	if e := p.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !p.closed {
		t.Fatal("completed close not retained")
	}
	if e := p.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("closed project remained available")
	}
}

func TestProjectDatabaseDSNPreservesReservedPathCharacters(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "project ?# Unicode å")
	if e := os.Mkdir(directory, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(directory, "project.db")
	db, e := sql.Open("sqlite", databaseDSN(path))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec(`CREATE TABLE exact_path(value TEXT)`); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal("database opened wrong pathname", e)
	}
	var enabled int
	if e = db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); e != nil || enabled != 1 {
		t.Fatal("lost database safety pragma", e)
	}
}
