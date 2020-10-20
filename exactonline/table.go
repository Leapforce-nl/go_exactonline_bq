package exactonline_bq

import (
	"cloud.google.com/go/storage"
	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
)

// type Table contains all necessary constants to synchronize an api-call with a BigQuery table.
//
type Table struct {
	objectName string
	tableName  string
	Schema     interface{}
}

// type insertable is an interface that contains all necessary functionality to synchronize an api-call with a BigQuery table.
//
type ITable interface {
	Table() *Table
	GetDataAndWriteToBucket(bq *bigquerytools.BigQuery, obj *storage.ObjectHandle, clients []*Client) int
}

func NewTable(objectName string, tableName string, schema interface{}) *Table {
	return &Table{objectName, tableName, schema}
}

// TableName returns tablename
//
func (t Table) TableName(isTest bool) string {
	tableName := t.tableName

	if isTest {
		suffix := "_test"
		tableName += suffix
	}

	return tableName
}

// ObjectName returns ObjectName
//
func (t Table) ObjectName(isTest bool) string {
	objectName := t.objectName

	if isTest {
		suffix := "_test"
		objectName += suffix
	}

	return objectName
}
