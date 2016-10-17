package godb

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-gorp/gorp"
	"database/sql"
	"errors"
	"strings"
)

type Dbx struct {
	OnError func(err error, sql string, args ...interface{})
	OnInfo func(sql string, args ...interface{})
	*gorp.DbMap
}

type Config struct {
	Driver string
	Dsn    string
}

const (
	DriverMYSql = "mysql"
)

// Connect to the database
func Open(d Config) (dbx *Dbx, err error) {
	var _db sql.DB
	switch strings.ToLower(d.Driver) {
	case "mysql":
		// Connect to MySQL
		_db, err = sql.Open("mysql", d.Dsn)
		if err != nil {
			return
		}

		// Check if is alive
		if err = _db.Ping(); err != nil {
			err = errors.New(fmt.Sprintf("Database ping error: %#v, config: %v", err, d.Dsn))
			return
		}

		//DB
		dbx = &Dbx{
			DbMap: &gorp.DbMap{
				Db: _db,
				Dialect: gorp.MySQLDialect{},
				//Dialect: gorp.MySQLDialect{"InnoDB", "utf8mb4"},
			},
		}
	default:
		//log.Fatalf("No registered database in config type: %v", d.Type)
		err = errors.New(fmt.Sprintf("No registered database in config type: %v", d.Driver))
		return
	}
	return
}

func (db *Dbx) Db() *sql.DB {
	return db.DbMap.Db
}

func (db *Dbx) Close() {
	db.Db().Close()
}