package catnip

import (
	"database/sql"
	"testing"
)

const testDSN = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlserver", testDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	return db
}

func TestConnect(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var num int64
	var greeting string
	err := db.QueryRow("SELECT 1 AS num, 'hello' AS greeting").Scan(&num, &greeting)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if num != 1 {
		t.Errorf("expected 1, got %d", num)
	}
	if greeting != "hello" {
		t.Errorf("expected 'hello', got %q", greeting)
	}
}

func TestMultipleRows(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sys.databases")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one database")
	}
}

func TestExec(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS catnip_test_exec")
	_, err := db.Exec("CREATE TABLE catnip_test_exec (id INT, name NVARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE catnip_test_exec")

	result, err := db.Exec("INSERT INTO catnip_test_exec VALUES (1, N'alice'), (2, N'bob')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Errorf("expected 2 rows affected, got %d", affected)
	}
}

func TestTypes(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	row := db.QueryRow(`SELECT 
		CAST(42 AS INT) AS int_val,
		CAST(9999999999 AS BIGINT) AS bigint_val,
		CAST(3.14 AS FLOAT) AS float_val,
		CAST(N'hello' AS NVARCHAR(50)) AS str_val,
		CAST(1 AS BIT) AS bit_val,
		CAST(0x48454C4C4F AS VARBINARY(10)) AS bytes_val,
		CAST(123.45 AS DECIMAL(10,2)) AS dec_val`)

	var intVal, bigintVal int64
	var floatVal float64
	var strVal, decVal string
	var bitVal bool
	var bytesVal []byte

	err := row.Scan(&intVal, &bigintVal, &floatVal, &strVal, &bitVal, &bytesVal, &decVal)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if intVal != 42 {
		t.Errorf("int: expected 42, got %d", intVal)
	}
	if bigintVal != 9999999999 {
		t.Errorf("bigint: expected 9999999999, got %d", bigintVal)
	}
	if strVal != "hello" {
		t.Errorf("str: expected 'hello', got %q", strVal)
	}
	if !bitVal {
		t.Error("bit: expected true")
	}
	if string(bytesVal) != "HELLO" {
		t.Errorf("bytes: expected 'HELLO', got %q", bytesVal)
	}
}

func TestNulls(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var s sql.NullString
	var i sql.NullInt64
	err := db.QueryRow("SELECT NULL, NULL").Scan(&s, &i)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if s.Valid {
		t.Error("expected NullString to be invalid")
	}
	if i.Valid {
		t.Error("expected NullInt64 to be invalid")
	}
}

func TestTransaction(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS catnip_test_tx")
	db.Exec("CREATE TABLE catnip_test_tx (id INT)")
	defer db.Exec("DROP TABLE catnip_test_tx")

	// Test rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	tx.Exec("INSERT INTO catnip_test_tx VALUES (1)")
	tx.Rollback()

	var count int64
	db.QueryRow("SELECT COUNT(*) FROM catnip_test_tx").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 after rollback, got %d", count)
	}

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	tx.Exec("INSERT INTO catnip_test_tx VALUES (2)")
	tx.Commit()

	db.QueryRow("SELECT COUNT(*) FROM catnip_test_tx").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 after commit, got %d", count)
	}
}
