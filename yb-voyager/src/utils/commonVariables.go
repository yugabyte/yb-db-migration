/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package utils

import (
	"sync"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

const (
	TABLE_MIGRATION_NOT_STARTED = iota
	TABLE_MIGRATION_IN_PROGRESS
	TABLE_MIGRATION_DONE
	TABLE_MIGRATION_COMPLETED
	YB_VOYAGER_NULL_STRING = "__YBV_NULL__"
)

type TableProgressMetadata struct {
	TableName            sqlname.NameTuple
	InProgressFilePath   string
	FinalFilePath        string
	Status               int //(0: NOT-STARTED, 1: IN-PROGRESS, 2: DONE, 3: COMPLETED)
	CountLiveRows        int64
	CountTotalRows       int64
	FileOffsetToContinue int64 // This might be removed later
	IsPartition          bool
	ParentTable          string
	//timeTakenByLast1000Rows int64; TODO: for ESTIMATED time calculation
}

var TableMetadataStatusMap = map[int]string{
	0: "NOT-STARTED",
	1: "EXPORTING",
	2: "DONE",
	3: "DONE",
}

// the list elements order is same as the import objects order
// TODO: Need to make each of the list comprehensive, not missing any database object category
var oracleSchemaObjectList = []string{"TYPE", "SEQUENCE", "TABLE", "PARTITION", "INDEX", "PACKAGE", "VIEW",
	/*"GRANT",*/ "TRIGGER", "FUNCTION", "PROCEDURE",
	"MVIEW" /*"DBLINK",*/, "SYNONYM" /*, "DIRECTORY"*/}
var oracleSchemaObjectListForExport = []string{"TYPE", "SEQUENCE", "TABLE", "PACKAGE", "TRIGGER", "FUNCTION", "PROCEDURE", "SYNONYM", "VIEW", "MVIEW"}

// In PG, PARTITION are exported along with TABLE
var postgresSchemaObjectList = []string{"SCHEMA", "COLLATION", "EXTENSION", "TYPE", "DOMAIN", "SEQUENCE",
	"TABLE", "INDEX", "FUNCTION", "AGGREGATE", "PROCEDURE", "VIEW", "TRIGGER",
	"MVIEW", "RULE", "COMMENT" /* GRANT, ROLE*/}
var postgresSchemaObjectListForExport = []string{"TYPE", "DOMAIN", "SEQUENCE", "TABLE", "FUNCTION", "PROCEDURE", "AGGREGATE", "VIEW", "MVIEW", "TRIGGER", "COMMENT"}

// In MYSQL, TYPE and SEQUENCE are not supported
var mysqlSchemaObjectList = []string{"TABLE", "PARTITION", "INDEX", "VIEW", /*"GRANT*/
	"TRIGGER", "FUNCTION", "PROCEDURE"}
var mysqlSchemaObjectListForExport = []string{"TABLE", "VIEW", "TRIGGER", "FUNCTION", "PROCEDURE"}

var WaitGroup sync.WaitGroup
var WaitChannel = make(chan int)

// ================== Schema Report ==============================

type SchemaReport struct {
	SchemaSummary SchemaSummary `json:"Summary"`
	Issues        []Issue       `json:"Issues"`
}

type SchemaSummary struct {
	DBName      string     `json:"DbName,omitempty"`
	SchemaNames []string   `json:"SchemaNames,omitempty"`
	DBVersion   string     `json:"DbVersion,omitempty"`
	Notes       []string   `json:"Notes,omitempty"`
	DBObjects   []DBObject `json:"DatabaseObjects"`
}

type DBObject struct {
	ObjectType   string `json:"ObjectType"`
	TotalCount   int    `json:"TotalCount"`
	InvalidCount int    `json:"InvalidCount,omitempty"`
	ObjectNames  string `json:"ObjectNames"`
	Details      string `json:"Details,omitempty"`
}

type Issue struct {
	ObjectType   string `json:"ObjectType"`
	ObjectName   string `json:"ObjectName"`
	Reason       string `json:"Reason"`
	SqlStatement string `json:"SqlStatement,omitempty"`
	FilePath     string `json:"FilePath"`
	Suggestion   string `json:"Suggestion"`
	GH           string `json:"GH"`
}

type IndexInfo struct {
	// TODO: ADD SchemaName string `json:"SchemaName"`
	IndexName string   `json:"IndexName"`
	IndexType string   `json:"IndexType"`
	TableName string   `json:"TableName"`
	Columns   []string `json:"Columns"`
}

type TableColumnsDataTypes struct {
	SchemaName string `json:"SchemaName"`
	TableName  string `json:"TableName"`
	ColumnName string `json:"ColumnName"`
	DataType   string `json:"DataType"`
}

// ================== Segment ==============================
type Segment struct {
	Num      int
	FilePath string
}

const (
	SNAPSHOT_ONLY        = "snapshot-only"
	SNAPSHOT_AND_CHANGES = "snapshot-and-changes"
	CHANGES_ONLY         = "changes-only"
)
