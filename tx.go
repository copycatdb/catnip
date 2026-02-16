package catnip

import "database/sql/driver"

// Tx implements driver.Tx
type Tx struct {
	conn *Conn
}

func (t *Tx) Commit() error {
	_, err := nativeExecute(t.conn.handle, "COMMIT")
	return err
}

func (t *Tx) Rollback() error {
	_, err := nativeExecute(t.conn.handle, "ROLLBACK")
	return err
}

var _ driver.Tx = (*Tx)(nil)
