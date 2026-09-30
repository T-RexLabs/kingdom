package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// Intercept the first mailbox mutation after SendMessage has read its WAL
// snapshot. A separate real connection commits before that mutation executes.
type mailboxWALConnector struct {
	dsn         string
	base        driver.Driver
	beforeWrite func()
	insertIDs   []string
}

func (c *mailboxWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &mailboxWALConn{Conn: conn, connector: c}, nil
}

func (c *mailboxWALConnector) Driver() driver.Driver { return c.base }

type mailboxWALConn struct {
	driver.Conn
	connector *mailboxWALConnector
}

func (c *mailboxWALConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	insert := strings.HasPrefix(query, "INSERT INTO mailbox_messages")
	if insert {
		c.connector.insertIDs = append(c.connector.insertIDs, args[0].Value.(string))
	}
	if insert || strings.HasPrefix(query, "DELETE FROM mailbox_messages") {
		if before := c.connector.beforeWrite; before != nil {
			c.connector.beforeWrite = nil
			before()
		}
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestMailboxSendRetriesConcurrentWALWriter(t *testing.T) {
	for _, scenario := range []string{"generated_id", "same_message", "replacement_read_marker"} {
		t.Run(scenario, func(t *testing.T) {
			dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", filepath.Join(t.TempDir(), "mailbox.db"))
			otherDB, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { otherDB.Close() })
			applyMigration(t, otherDB, "0001_initial_schema.up.sql")
			seedDelegationPolicyFixture(t, otherDB)
			connector := &mailboxWALConnector{dsn: dsn, base: &sqlite.Driver{}}
			db := sql.OpenDB(connector)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { db.Close() })
			schemas := loadStateTestSchemas(t)
			store, other := NewMailboxStore(db, schemas), NewMailboxStore(otherDB, schemas)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := SendMailboxMessageRequest{
				MessageID: "msg_contended", RunID: "run_1", FromAgentID: "agent_council", ToAgentID: "agent_chancellor",
				ThreadID: "thread_wal", MessageType: MailboxMessageTypeStatus, Priority: MailboxPriorityNormal,
				PayloadJSON: `{"status":"prepared"}`, CreatedAt: "2026-03-01T16:01:00Z",
			}
			if scenario == "generated_id" {
				req.MessageID = ""
			}
			if scenario == "replacement_read_marker" {
				initial := req
				initial.PayloadJSON, initial.CreatedAt = `{"status":"preparing"}`, "2026-03-01T16:00:00Z"
				if _, err := other.SendMessage(ctx, initial); err != nil {
					t.Fatal(err)
				}
			}
			interleaved := false
			var writerErr error
			connector.beforeWrite = func() {
				interleaved = true
				if scenario == "replacement_read_marker" {
					writerErr = other.MarkRead(ctx, MarkMailboxMessageReadRequest{RunID: req.RunID, MessageID: req.MessageID, ReadAt: "2026-03-01T16:02:00Z"})
					return
				}
				concurrent := req
				concurrent.CreatedAt = "2026-03-01T16:00:30Z"
				if scenario == "generated_id" {
					concurrent.MessageID = "msg_independent"
				}
				_, writerErr = other.SendMessage(ctx, concurrent)
			}
			message, err := store.SendMessage(ctx, req)
			if !interleaved || writerErr != nil {
				t.Fatalf("concurrent writer did not commit: entered=%t err=%v", interleaved, writerErr)
			}
			if err != nil {
				t.Fatalf("mailbox send must recover its invalidated WAL snapshot: %v", err)
			}
			if scenario == "generated_id" {
				if message.MessageID == "" || len(connector.insertIDs) != 2 || connector.insertIDs[0] != message.MessageID || connector.insertIDs[1] != message.MessageID {
					t.Fatalf("retry changed generated message identity: message=%q inserts=%v", message.MessageID, connector.insertIDs)
				}
			}
			if scenario == "same_message" && message.CreatedAt != "2026-03-01T16:00:30Z" {
				t.Fatalf("same-message retry must return the committed message: %+v", message)
			}
			if scenario == "replacement_read_marker" && (!message.ReadAt.Valid || message.ReadAt.String != "2026-03-01T16:02:00Z") {
				t.Fatalf("replacement lost concurrently committed read marker: %+v", message.ReadAt)
			}
			req.MessageID = message.MessageID
			if replay, err := store.SendMessage(ctx, req); err != nil || replay != message {
				t.Fatalf("idempotent replay changed message: replay=%+v err=%v", replay, err)
			}
			var messages, revisions, participants int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox_messages`).Scan(&messages); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_type = 'mailbox.message_recorded' AND json_extract(payload_json, '$.message_id') = ?`, message.MessageID).Scan(&revisions); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_participants WHERE run_id = 'run_1'`).Scan(&participants); err != nil {
				t.Fatal(err)
			}
			wantMessages, wantRevisions := 1, 1
			if scenario == "generated_id" {
				wantMessages = 2
			}
			if scenario == "replacement_read_marker" {
				wantRevisions = 2
			}
			if messages != wantMessages || revisions != wantRevisions || participants != 2 {
				t.Fatalf("retry duplicated durable effects: messages=%d revisions=%d participants=%d", messages, revisions, participants)
			}
		})
	}
}
