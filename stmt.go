package catnip

import (
	"database/sql/driver"
)

// Stmt implements driver.Stmt
type Stmt struct {
	conn  *Conn
	query string
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return -1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	result, err := nativeQuery(s.conn.handle, s.query)
	if err != nil {
		return nil, err
	}
	affected := nativeResultRowsAffected(result)
	nativeResultFree(result)
	return &execResult{rowsAffected: affected}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}
	result, err := nativeQuery(s.conn.handle, s.query)
	if err != nil {
		return nil, err
	}
	return newRows(result), nil
}
