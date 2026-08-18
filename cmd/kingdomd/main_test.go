package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestEnvOrDefaultAllowEmpty(t *testing.T) {
	const key = "KINGDOM_TEST_ALLOW_EMPTY"
	previous, existed := os.LookupEnv(key)
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(key, previous)
		} else {
			_ = os.Unsetenv(key)
		}
	})

	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	if got := envOrDefaultAllowEmpty(key, "official"); got != "official" {
		t.Fatalf("unset value=%q", got)
	}
	if err := os.Setenv(key, ""); err != nil {
		t.Fatal(err)
	}
	if got := envOrDefaultAllowEmpty(key, "official"); got != "" {
		t.Fatalf("explicit blank value=%q", got)
	}
	if err := os.Setenv(key, " override "); err != nil {
		t.Fatal(err)
	}
	if got := envOrDefaultAllowEmpty(key, "official"); got != "override" {
		t.Fatalf("override value=%q", got)
	}
}

func TestKingdomdSQLiteDSNAppliesConnectionPragmas(t *testing.T) {
	dsn, err := kingdomdSQLiteDSN(filepath.Join(t.TempDir(), "kingdom.db"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	connections := make([]*sql.Conn, 0, 2)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for index := 0; index < 2; index++ {
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var busyTimeout, foreignKeys, synchronous int
		if err := conn.QueryRowContext(t.Context(), `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(t.Context(), `PRAGMA synchronous`).Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if busyTimeout != 5000 || foreignKeys != 1 || synchronous != 1 {
			t.Fatalf("connection %d pragmas: busy_timeout=%d foreign_keys=%d synchronous=%d", index+1, busyTimeout, foreignKeys, synchronous)
		}
	}
}

func TestKingdomdSQLiteDSNAcceptsRelativePath(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relativePath, err := filepath.Rel(workingDirectory, filepath.Join(t.TempDir(), "kingdom.db"))
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := kingdomdSQLiteDSN(relativePath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("open relative sqlite path through %q: %v", dsn, err)
	}
}
