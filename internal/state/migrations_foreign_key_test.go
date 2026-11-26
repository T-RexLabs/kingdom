package state

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestApplySQLMigrationsForeignKeySemantics(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name   string
		parent string
		child  string
		count  int
	}{
		{
			name:   "parent collation controls comparison",
			parent: `CREATE TABLE parent (id TEXT COLLATE NOCASE PRIMARY KEY); INSERT INTO parent VALUES ('Parent');`,
			child:  `CREATE TABLE child (parent_id TEXT COLLATE BINARY REFERENCES parent(id)); INSERT INTO child VALUES ('parent'), ('PARENT');`,
			count:  2,
		},
		{
			name:   "parent table identifiers are case insensitive",
			parent: `CREATE TABLE Parent (id INTEGER PRIMARY KEY); INSERT INTO Parent VALUES (7);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES pArEnT(id)); INSERT INTO child VALUES (7);`,
			count:  1,
		},
		{
			name:   "quoted identifiers retain spaces and quotes",
			parent: `CREATE TABLE " Parent"" " (" Key"" " TEXT PRIMARY KEY); INSERT INTO " Parent"" " VALUES ('parent');`,
			child:  `CREATE TABLE child (" Ref"" " TEXT REFERENCES " Parent"" "(" Key"" ")); INSERT INTO child VALUES ('parent');`,
			count:  1,
		},
		{
			name:   "implicit integer parent primary key",
			parent: `CREATE TABLE parent (id INTEGER PRIMARY KEY); INSERT INTO parent VALUES (7);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES parent); INSERT INTO child VALUES (7);`,
			count:  1,
		},
		{
			name:   "implicit composite parent primary key",
			parent: `CREATE TABLE parent (a TEXT, b INTEGER, PRIMARY KEY(a, b)) WITHOUT ROWID; INSERT INTO parent VALUES ('parent', 7);`,
			child:  `CREATE TABLE child (a TEXT, b INTEGER, FOREIGN KEY(a, b) REFERENCES parent); INSERT INTO child VALUES ('parent', 7);`,
			count:  1,
		},
		{
			name:   "composite key allows any null component",
			parent: `CREATE TABLE parent (a TEXT, b INTEGER, PRIMARY KEY(a, b)) WITHOUT ROWID; INSERT INTO parent VALUES ('parent', 7);`,
			child:  `CREATE TABLE child (a TEXT, b INTEGER, FOREIGN KEY(a, b) REFERENCES parent(a, b)); INSERT INTO child VALUES ('parent', 7), ('missing', NULL), (NULL, 99), (NULL, NULL);`,
			count:  4,
		},
		{
			name:   "deferred child can precede its parent in one migration",
			parent: `CREATE TABLE parent (id INTEGER PRIMARY KEY);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); INSERT INTO child VALUES (7); INSERT INTO parent VALUES (7);`,
			count:  1,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			db := openTestDB(t)
			migrations := []SQLMigration{
				{Name: "0001_parent.up.sql", SQL: []byte(scenario.parent)},
				{Name: "0002_child.up.sql", SQL: []byte(scenario.child)},
			}
			if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)); err != nil {
				t.Fatalf("valid SQLite foreign-key migration was rejected: %v", err)
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM child`).Scan(&count); err != nil || count != scenario.count {
				t.Fatalf("child rows=%d, want %d: %v", count, scenario.count, err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil || count != 2 {
				t.Fatalf("committed migration records=%d, want 2: %v", count, err)
			}
		})
	}
}

func TestApplySQLMigrationsForeignKeyFailureRollsBack(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name          string
		parent        string
		child         string
		wantViolation string
	}{
		{
			name:          "without rowid child orphan",
			parent:        `CREATE TABLE parent (id INTEGER PRIMARY KEY);`,
			child:         `CREATE TABLE child (a TEXT, b INTEGER, parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED, PRIMARY KEY(a, b)) WITHOUT ROWID; INSERT INTO child VALUES ('orphan', 1, 99);`,
			wantViolation: "table=child parent=parent fk=0 count=1",
		},
		{
			name:   "missing parent is rejected even when child is empty",
			parent: `CREATE TABLE unrelated (id INTEGER PRIMARY KEY);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES missing_parent(id));`,
		},
		{
			name:   "main child cannot use a temp only parent",
			parent: `CREATE TEMP TABLE parent (id INTEGER PRIMARY KEY);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES parent(id));`,
		},
		{
			name:   "parent key must be unique even when child is empty",
			parent: `CREATE TABLE parent (id INTEGER);`,
			child:  `CREATE TABLE child (parent_id INTEGER REFERENCES parent(id));`,
		},
		{
			name:   "unique parent index must use declared parent collation",
			parent: `CREATE TABLE parent (id TEXT COLLATE BINARY); CREATE UNIQUE INDEX parent_id ON parent(id COLLATE NOCASE);`,
			child:  `CREATE TABLE child (parent_id TEXT REFERENCES parent(id));`,
		},
		{
			name:   "implicit composite parent key requires matching cardinality",
			parent: `CREATE TABLE parent (a TEXT, b INTEGER, PRIMARY KEY(a, b)) WITHOUT ROWID;`,
			child:  `CREATE TABLE child (parent_id TEXT REFERENCES parent);`,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			db := openTestDB(t)
			migrations := []SQLMigration{
				{Name: "0001_parent.up.sql", SQL: []byte(`CREATE TABLE retained (id INTEGER PRIMARY KEY); INSERT INTO retained VALUES (1); ` + scenario.parent)},
				{Name: "0002_child.up.sql", SQL: []byte(`CREATE TABLE pending_marker (id INTEGER); ` + scenario.child)},
			}
			err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
			if err == nil || !strings.Contains(err.Error(), "validate foreign keys for migration 0002_child.up.sql") {
				t.Fatalf("expected migration foreign-key validation failure, got %v", err)
			}
			if scenario.wantViolation != "" && !strings.Contains(err.Error(), scenario.wantViolation) {
				t.Fatalf("expected a counted foreign-key violation, got %v", err)
			}
			if tableExists(t, db, "child") || tableExists(t, db, "pending_marker") {
				t.Fatal("failed migration left schema changes behind")
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM retained WHERE id=1`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("previous committed migration was not preserved: rows=%d err=%v", count, err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM schema_migrations WHERE name='0002_child.up.sql'`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed migration was recorded: records=%d err=%v", count, err)
			}
			if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("foreign-key enforcement changed: enabled=%d err=%v", count, err)
			}
		})
	}
}

func TestApplySQLMigrationsForeignKeyViolationCountsAreDeterministic(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	migrations := []SQLMigration{
		{Name: "0001_parents.up.sql", SQL: []byte(`CREATE TABLE p (id INTEGER PRIMARY KEY); CREATE TABLE q (id INTEGER PRIMARY KEY);`)},
		{Name: "0002_children.up.sql", SQL: []byte(`
			CREATE TABLE z_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);
			CREATE TABLE a_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES q(id) DEFERRABLE INITIALLY DEFERRED);
			INSERT INTO z_child VALUES (1, 1), (2, 2);
			INSERT INTO a_child VALUES (1, 1);
		`)},
	}
	want := "table=a_child parent=q fk=0 count=1; table=z_child parent=p fk=0 count=2"
	var messages []string
	for i := 0; i < 2; i++ {
		err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("expected exact grouped violation counts in stable order, got %v", err)
		}
		messages = append(messages, err.Error())
		if tableExists(t, db, "a_child") || tableExists(t, db, "z_child") {
			t.Fatal("violation report allowed partial migration commit")
		}
	}
	if messages[0] != messages[1] {
		t.Fatalf("same rejected migration produced different diagnostics: %q", messages)
	}
}
