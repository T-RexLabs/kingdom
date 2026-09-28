package testutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"

	"modernc.org/sqlite"
)

// ErrDurableCommit reports a test commit that reached SQLite but returned an
// error to database/sql, modeling an ambiguous commit outcome.
var ErrDurableCommit = errors.New("injected durable commit error")

// SQLiteCommitFaultConnector wraps SQLite and can fail one Commit after the
// underlying transaction has durably committed.
type SQLiteCommitFaultConnector struct {
	dsn            string
	base           driver.Driver
	next           atomic.Pointer[sqliteCommitFault]
	nextQueryFault atomic.Pointer[sqliteQueryFault]
}

type sqliteCommitFault struct {
	afterCommit func()
}

type sqliteQueryFault struct {
	fragment string
}

func NewSQLiteCommitFaultConnector(dsn string) *SQLiteCommitFaultConnector {
	return &SQLiteCommitFaultConnector{dsn: dsn, base: &sqlite.Driver{}}
}

func (c *SQLiteCommitFaultConnector) OpenDB() *sql.DB {
	return sql.OpenDB(c)
}

func (c *SQLiteCommitFaultConnector) FailNextDurableCommit(afterCommit func()) {
	c.next.Store(&sqliteCommitFault{afterCommit: afterCommit})
}

func (c *SQLiteCommitFaultConnector) FailNextQueryContaining(fragment string) {
	fragment = strings.ToLower(strings.TrimSpace(fragment))
	if fragment == "" {
		return
	}
	c.nextQueryFault.Store(&sqliteQueryFault{fragment: fragment})
}

func (c *SQLiteCommitFaultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &sqliteCommitFaultConn{Conn: conn, connector: c}, nil
}

func (c *SQLiteCommitFaultConnector) Driver() driver.Driver {
	return c.base
}

type sqliteCommitFaultConn struct {
	driver.Conn
	connector *SQLiteCommitFaultConnector
}

func (c *sqliteCommitFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	for fault := c.connector.nextQueryFault.Load(); fault != nil; fault = c.connector.nextQueryFault.Load() {
		if !strings.Contains(strings.ToLower(query), fault.fragment) {
			return c.queryUnderlying(ctx, query, args)
		}
		if !c.connector.nextQueryFault.CompareAndSwap(fault, nil) {
			continue
		}
		return nil, errors.New("injected SQLite query failure")
	}
	return c.queryUnderlying(ctx, query, args)
}

func (c *sqliteCommitFaultConn) queryUnderlying(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if conn, ok := c.Conn.(driver.QueryerContext); ok {
		return conn.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *sqliteCommitFaultConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &sqliteCommitFaultTx{Tx: tx, connector: c.connector}, nil
}

func (c *sqliteCommitFaultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		err error
	)
	if conn, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = conn.BeginTx(ctx, opts)
	} else {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if opts != (driver.TxOptions{}) {
			return nil, errors.New("SQLite test connector does not support transaction options")
		}
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &sqliteCommitFaultTx{Tx: tx, connector: c.connector}, nil
}

type sqliteCommitFaultTx struct {
	driver.Tx
	connector *SQLiteCommitFaultConnector
}

func (tx *sqliteCommitFaultTx) Commit() error {
	fault := tx.connector.next.Swap(nil)
	if fault == nil {
		return tx.Tx.Commit()
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if fault.afterCommit != nil {
		fault.afterCommit()
	}
	return ErrDurableCommit
}
