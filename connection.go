package catnip

import (
	"context"
	"database/sql/driver"
	"unsafe"
)

// Conn implements driver.Conn, driver.ExecerContext, driver.QueryerContext
type Conn struct {
	handle unsafe.Pointer
}

func newConn(dsn string) (*Conn, error) {
	h, err := nativeConnect(dsn)
	if err != nil {
		return nil, err
	}
	return &Conn{handle: h}, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{conn: c, query: query}, nil
}

func (c *Conn) Close() error {
	if c.handle != nil {
		nativeClose(c.handle)
		c.handle = nil
	}
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	_, err := nativeExecute(c.handle, "BEGIN TRANSACTION")
	if err != nil {
		return nil, err
	}
	return &Tx{conn: c}, nil
}

func (c *Conn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	result, err := nativeQuery(c.handle, query)
	if err != nil {
		return nil, err
	}
	affected := nativeResultRowsAffected(result)
	nativeResultFree(result)
	return &execResult{rowsAffected: affected}, nil
}

func (c *Conn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	result, err := nativeQuery(c.handle, query)
	if err != nil {
		return nil, err
	}
	return newRows(result), nil
}

type execResult struct {
	rowsAffected int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
