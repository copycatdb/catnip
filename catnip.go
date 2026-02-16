package catnip

/*
#cgo LDFLAGS: -L${SRCDIR}/native/target/release -lcatnip_native -lm -ldl -lpthread
#cgo darwin LDFLAGS: -framework Security -framework SystemConfiguration

#include <stdlib.h>

typedef struct {
	int type_tag;
	long long int_val;
	double float_val;
	const char* str_val;
	int str_len;
	const unsigned char* bytes_val;
	int bytes_len;
} CatnipValue;

extern void* catnip_connect(const char* conn_str);
extern void catnip_close(void* conn);
extern long long catnip_execute(void* conn, const char* sql);
extern void* catnip_query(void* conn, const char* sql);
extern long long catnip_result_row_count(void* result);
extern long long catnip_result_col_count(void* result);
extern const char* catnip_result_col_name(void* result, long long col);
extern CatnipValue catnip_result_get_value(void* result, long long row, long long col);
extern void catnip_result_free(void* result);
extern long long catnip_result_rows_affected(void* result);
extern const char* catnip_last_error();
*/
import "C"

import (
	"database/sql"
	"fmt"
	"unsafe"
)

func init() {
	sql.Register("sqlserver", &Driver{})
}

func lastError() error {
	p := C.catnip_last_error()
	if p == nil {
		return fmt.Errorf("unknown error")
	}
	return fmt.Errorf("%s", C.GoString(p))
}

func nativeConnect(dsn string) (unsafe.Pointer, error) {
	cs := C.CString(dsn)
	defer C.free(unsafe.Pointer(cs))
	handle := C.catnip_connect(cs)
	if handle == nil {
		return nil, lastError()
	}
	return handle, nil
}

func nativeClose(handle unsafe.Pointer) {
	C.catnip_close(handle)
}

func nativeExecute(handle unsafe.Pointer, query string) (int64, error) {
	cs := C.CString(query)
	defer C.free(unsafe.Pointer(cs))
	n := C.catnip_execute(handle, cs)
	if n < 0 {
		return 0, lastError()
	}
	return int64(n), nil
}

func nativeQuery(handle unsafe.Pointer, query string) (unsafe.Pointer, error) {
	cs := C.CString(query)
	defer C.free(unsafe.Pointer(cs))
	result := C.catnip_query(handle, cs)
	if result == nil {
		return nil, lastError()
	}
	return result, nil
}

func nativeResultRowCount(result unsafe.Pointer) int64 {
	return int64(C.catnip_result_row_count(result))
}

func nativeResultColCount(result unsafe.Pointer) int64 {
	return int64(C.catnip_result_col_count(result))
}

func nativeResultColName(result unsafe.Pointer, col int64) string {
	p := C.catnip_result_col_name(result, C.longlong(col))
	if p == nil {
		return ""
	}
	return C.GoString(p)
}

const (
	typeNull   = 0
	typeBool   = 1
	typeI64    = 2
	typeF64    = 3
	typeString = 4
	typeBytes  = 5
)

func nativeResultGetValue(result unsafe.Pointer, row, col int64) interface{} {
	v := C.catnip_result_get_value(result, C.longlong(row), C.longlong(col))
	switch v.type_tag {
	case typeNull:
		return nil
	case typeBool:
		return v.int_val != 0
	case typeI64:
		return int64(v.int_val)
	case typeF64:
		return float64(v.float_val)
	case typeString:
		if v.str_val == nil {
			return ""
		}
		return C.GoStringN((*C.char)(unsafe.Pointer(v.str_val)), C.int(v.str_len))
	case typeBytes:
		if v.bytes_val == nil {
			return []byte{}
		}
		return C.GoBytes(unsafe.Pointer(v.bytes_val), C.int(v.bytes_len))
	default:
		return nil
	}
}

func nativeResultRowsAffected(result unsafe.Pointer) int64 {
	return int64(C.catnip_result_rows_affected(result))
}

func nativeResultFree(result unsafe.Pointer) {
	C.catnip_result_free(result)
}
