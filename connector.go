package catnip

import (
	"context"
	"database/sql/driver"
)

// Driver implements database/sql/driver.Driver
type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	return newConn(dsn)
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return &Connector{dsn: dsn, driver: d}, nil
}

// Connector implements database/sql/driver.Connector
type Connector struct {
	dsn    string
	driver *Driver
}

func (c *Connector) Connect(_ context.Context) (driver.Conn, error) {
	return newConn(c.dsn)
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}
