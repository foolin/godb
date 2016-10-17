package godb
import (
	"database/sql"
	"strings"
	"fmt"
	"github.com/go-gorp/gorp"
)


func (db *Dbx)  errLog(err error, sql string, args ...interface{})  {
	faltErr := IgnoreNonFatalError(err)
	if db.OnError != nil && faltErr != nil{
		db.OnError(err, sql, args...)
	}
}

func (db *Dbx)  infoLog(sql string, args ...interface{})  {
	if db.OnInfo != nil{
		db.OnInfo(sql, args...)
	}
}

func (db *Dbx) AddTable(model Modeler) error{
	db.infoLog("#AddTable()", model.TableName())
	db.DbMap.AddTableWithName(model, model.TableName())
	return nil
}

func (db *Dbx) QueryOne(dest interface{}, querySql string, sqlArgs ...interface{}) error{
	db.infoLog(querySql, sqlArgs)
	err := db.DbMap.SelectOne(dest, querySql, sqlArgs...)
	if err != nil && err != sql.ErrNoRows{
		db.errLog(err, querySql, sqlArgs...)
	}
	return IgnoreNonFatalError(err)
}


func (db *Dbx) QueryMapRow(querySql string, queryArgs ...interface{}) (MapRow, error){
	db.infoLog(querySql, queryArgs)
	rows, err := db.DbMap.Db.Query(querySql, queryArgs...)
	if err != nil && err != sql.ErrNoRows{
		db.errLog(err, querySql, queryArgs...)
		return nil, IgnoreNonFatalError(err)
	}
	if rows.Next(){
		return ScanMapRow(rows, true)
	}
	return nil, rows.Err()
}


func (db *Dbx) QueryAll(dest interface{}, querysql string, queryArgs ...interface{}) error{
	db.infoLog(querysql, queryArgs)
	_, err := db.DbMap.Select(dest, querysql, queryArgs...)
	if err != nil && err != sql.ErrNoRows{
		db.errLog(err, querysql, queryArgs...)
		return IgnoreNonFatalError(err)
	}
	return IgnoreNonFatalError(err)
}

func (db *Dbx) QueryMapRows(querySql string, queryArgs ...interface{}) ([]MapRow, error){
	db.infoLog(querySql, queryArgs)
	rows, err := db.DbMap.Db.Query(querySql, queryArgs...)
	if err != nil && err != sql.ErrNoRows{
		db.errLog(err, querySql, queryArgs...)
	}
	list := make([]MapRow, 0)
	for rows.Next(){
		item, err :=ScanMapRow(rows, true)
		if err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, IgnoreNonFatalError(err)
}

func (db *Dbx) Insert(list ...interface{})  (err error){
	err = db.DbMap.Insert(list...)
	if err != nil {
		db.errLog(err, "#Insert():", list)
	}
	return
}


func (db *Dbx) Update(list ...interface{})  (rows int64, err error){
	rows, err = db.DbMap.Update(list...)
	if err != nil {
		db.errLog(err, "#Update()", list)
	}
	return
}


func (db *Dbx) UpdateFields(fields []string, list ...interface{})(rows int64, err error){
	set := make(map[string]bool)
	for _, v := range fields {
		set[strings.ToLower(v)] = true
	}
	rows, err = db.DbMap.UpdateColumns(func(col *gorp.ColumnMap) bool {
		return set[strings.ToLower(col.ColumnName)]
	}, list...)
	if err != nil {
		db.errLog(err, fmt.Sprintf("#UpdateFields(%v)", strings.Join(fields, ",")), list)
	}
	return
}

func (db *Dbx) Execute(query string, args ...interface{}) (res sql.Result, err error){
	db.infoLog(query, args)
	res, err = db.DbMap.Exec(query, args...)
	if err != nil {
		db.errLog(err, query, args...)
	}
	return
}

func IgnoreNonFatalError(err error) error {
	if gorp.NonFatalError(err){
		return nil
	}
	return err
}

