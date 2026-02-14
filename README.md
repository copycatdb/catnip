# catnip 🌿

Go driver for SQL Server. pgx walked so catnip could run.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A native Go driver for SQL Server using [tabby](https://github.com/copycatdb/tabby) via CGO. Implements `database/sql` interface because Go developers love interfaces almost as much as they love error handling.

```go
import "github.com/copycatdb/catnip"

db, err := sql.Open("sqlserver", "Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes")
if err != nil {
    // you know the drill
}

rows, err := db.QueryContext(ctx, "SELECT id, name FROM users WHERE id = @p1", 42)
```

## Why not go-mssqldb?

go-mssqldb is fine. Its a pure Go TDS implementation. We respect that. But catnip uses tabby, which means one TDS implementation across every language. Fix a bug in tabby, every driver gets the fix. Thats the CopyCat way.

## Status

🚧 Coming soon.

## Attribution

Inspired by [pgx](https://github.com/jackc/pgx), the Go Postgres driver that made us believe database drivers could actually be... pleasant? In Go?

## License

MIT
