package godb

import (
	"strings"
	"database/sql"
	"fmt"
	"time"
	"github.com/foolin/goutils/utilx"
)

type MapRow map[string]interface{}

func ScanMapRow(r *sql.Rows, lowerCase bool) (MapRow, error) {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return nil, err
	}

	dest := make(MapRow)
	for i, column := range columns {
		key := column
		if lowerCase{
			key = strings.ToLower(key)
		}
		dest[key] = *(values[i].(*interface{}))
	}

	return dest, r.Err()
}

//exists
func (this MapRow) Exists(key string) bool {
	_, ok := this[key]
	return ok
}

//get
func (this MapRow) Get(key string) interface{} {
	return this[key]
}

//get
func (this MapRow) GetString(key string) string {
	return fmt.Sprintf("%s", this[key])
}

//get
func (this MapRow) GetInt(key string, defaultValue int) int {
	mValue := this[key]
	if v, ok := mValue.(int); ok{
		return v
	}
	if v, ok := mValue.(int32); ok{
		return int(v)
	}
	if v, ok := mValue.(int64); ok{
		return int(v)
	}
	return utilx.Int(fmt.Sprintf("%s", mValue), defaultValue)
}


func (this MapRow) GetInt64(key string, defaultValue int64) int64 {
	mValue := this[key]
	if v, ok := mValue.(int64); ok{
		return v
	}
	if v, ok := mValue.(int); ok{
		return int64(v)
	}
	if v, ok := mValue.(int32); ok{
		return int64(v)
	}
	return utilx.Int64(fmt.Sprintf("%s", mValue), defaultValue)
}



//get
func (this MapRow) GetFloat32(key string, defaultValue float32) float32 {
	value := this.GetString(key)
	if value == ""{
		return defaultValue
	}
	return utilx.Float32(value, defaultValue)
}


func (this MapRow) GetFloat64(key string, defaultValue float64) float64 {
	value := this.GetString(key)
	if value == ""{
		return defaultValue
	}
	return utilx.Float64(value, defaultValue)
}

func (this MapRow) GetTime(key string, defaultValue time.Time) time.Time {
	obj := this[key]
	if v, ok := obj.(time.Time); ok{
		return v
	}
	return defaultValue
}

