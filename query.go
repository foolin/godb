package godb

import (
	"strings"
	"fmt"
	"database/sql"
)

type QueryBuilder struct {
	db         *Dbx
	sqlTable   string
	sqlWhere   string
	sqlArgs    []interface{}
	sqlSelect  string
	sqlGroupBy string
	sqlOrderBy string
	sqlOffset  int
	sqlLimit   int
}

func NewQuery(db *Dbx, table string) QueryBuilder {
	return QueryBuilder{
		db: db,
		sqlTable: table,
		sqlSelect: "*",
	}
}

func (db *Dbx) NewQuery(table string) QueryBuilder {
	return NewQuery(db, table)
}

func (q QueryBuilder) Select(fields ...string) QueryBuilder {
	q.sqlSelect = strings.Join(fields, ",")
	return q
}

func (db *Dbx) Where(table string, where string, args ...interface{}) QueryBuilder {
	return db.NewQuery(table).Where(where, args...)
}

func (q QueryBuilder) Where(where string, args ...interface{}) QueryBuilder {
	q.sqlWhere = where
	q.sqlArgs = args
	return q
}

func (q QueryBuilder) GroupBy(fields ...string) QueryBuilder {
	q.sqlGroupBy = strings.Join(fields, ",")
	return q
}

func (q QueryBuilder) OrderBy(fields ...string) QueryBuilder {
	q.sqlOrderBy = strings.Join(fields, ",")
	return q
}

func (q QueryBuilder) Offset(offset int) QueryBuilder {
	q.sqlOffset = offset
	return q
}

func (q QueryBuilder) Limit(limit int) QueryBuilder {
	q.sqlLimit = limit
	return q
}

func (q QueryBuilder) Page(page int, pageSize int) QueryBuilder {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	q.sqlOffset = (page - 1) * pageSize
	q.sqlLimit = pageSize
	return q
}

func (q QueryBuilder) Sql() string {
	sql := fmt.Sprintf("SELECT %v", q.sqlSelect)
	sql += " FROM " + q.sqlTable
	if q.sqlWhere != "" {
		sql += " WHERE " + q.sqlWhere
	}
	if q.sqlGroupBy != "" {
		sql += " GROUP BY " + q.sqlGroupBy
	}
	if q.sqlOrderBy != "" {
		sql += " ORDER BY " + q.sqlOrderBy
	}
	if q.sqlLimit > 0 {
		sql += fmt.Sprintf(" LIMIT %v", q.sqlLimit)
	}
	if q.sqlOffset > 0 {
		sql += fmt.Sprintf(" OFFSET %v", q.sqlOffset)
	}
	return sql
}

func (q QueryBuilder) Args() []interface{} {
	return q.sqlArgs
}

func (q QueryBuilder) One(dest interface{}) error {
	if q.sqlLimit <= 0 {
		q.sqlLimit = 1
	}
	return q.db.QueryOne(dest, q.Sql(), q.Args()...)
}

func (q QueryBuilder) All(dest interface{}) error {
	return q.db.QueryAll(dest, q.Sql(), q.Args()...)
}

func (q QueryBuilder) Row() *sql.Row {
	if q.sqlLimit <= 0 {
		q.sqlLimit = 1
	}
	return q.db.QueryRow(q.Sql(), q.Args()...)
}

func (q QueryBuilder) Rows() (*sql.Rows, error) {
	return q.db.Query(q.Sql(), q.Args()...)
}

func (q QueryBuilder) MapRow() (MapRow, error) {
	if q.sqlLimit <= 0 {
		q.sqlLimit = 1
	}
	return q.db.QueryMapRow(q.Sql(), q.Args()...)
}

func (q QueryBuilder) MapRows() ([]MapRow, error) {
	return q.db.QueryMapRows(q.Sql(), q.Args()...)
}

func (q QueryBuilder) Count() (int64, error) {
	var count int64
	err := q.Select(" count(0) as num ").One(&count)
	return count, err
}