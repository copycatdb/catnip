# catnip 🐱

Go `database/sql` driver for SQL Server, powered by [tabby](https://github.com/copycatdb/tabby).

Part of the [CopyCat](https://github.com/copycatdb) project.

## Architecture

Catnip uses a Rust shared library (cdylib) that wraps tabby's TDS protocol implementation, exposed to Go via CGO. This gives you a pure `database/sql` interface with the performance of a native Rust TDS client.

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/copycatdb/catnip"
)

func main() {
    db, err := sql.Open("sqlserver",
        "Server=localhost,1433;UID=sa;PWD=yourpassword;Database=mydb;TrustServerCertificate=yes")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    var name string
    db.QueryRow("SELECT name FROM sys.databases WHERE database_id = 1").Scan(&name)
    fmt.Println(name) // master
}
```

## Building

### Prerequisites

- Go 1.22+
- Rust (stable)
- CGO enabled

### Build

```bash
# Build the native Rust library
make build

# Run tests (requires SQL Server on localhost:1433)
make test

# Or manually:
cd native && cargo build --release
CGO_ENABLED=1 LD_LIBRARY_PATH=native/target/release go test -v ./...
```

## Connection String

Supports ADO.NET-style connection strings:

```
Server=host,port;UID=user;PWD=password;Database=dbname;TrustServerCertificate=yes
```

Supported keys:
- `Server` / `Data Source` — host and optional port (comma-separated)
- `Database` / `Initial Catalog`
- `UID` / `User ID` / `User`
- `PWD` / `Password`
- `TrustServerCertificate` — `yes`/`true` to skip certificate validation

## License

MIT
