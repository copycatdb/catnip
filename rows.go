package catnip

import (
	"database/sql/driver"
	"io"
	"unsafe"
)

// Rows implements driver.Rows
type Rows struct {
	result  unsafe.Pointer
	cols    int64
	rows    int64
	current int64
	columns []string
}

func newRows(result unsafe.Pointer) *Rows {
	cols := nativeResultColCount(result)
	rows := nativeResultRowCount(result)
	columns := make([]string, cols)
	for i := int64(0); i < cols; i++ {
		columns[i] = nativeResultColName(result, i)
	}
	return &Rows{
		result:  result,
		cols:    cols,
		rows:    rows,
		current: 0,
		columns: columns,
	}
}

func (r *Rows) Columns() []string {
	return r.columns
}

func (r *Rows) Close() error {
	if r.result != nil {
		nativeResultFree(r.result)
		r.result = nil
	}
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.current >= r.rows {
		return io.EOF
	}
	for i := int64(0); i < r.cols; i++ {
		dest[i] = nativeResultGetValue(r.result, r.current, i)
	}
	r.current++
	return nil
}
