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
package tgtdb

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type TargetDB interface {
	Init() error
	Finalize()
	InitConnPool() error
	PrepareForStreaming()
	GetVersion() string
	CreateVoyagerSchema() error
	GetNonEmptyTables(tableNames []sqlname.NameTuple) []sqlname.NameTuple
	IsNonRetryableCopyError(err error) bool
	ImportBatch(batch Batch, args *ImportBatchArgs, exportDir string, tableSchema map[string]map[string]string) (int64, error)
	QuoteAttributeNames(tableNameTup sqlname.NameTuple, columns []string) ([]string, error)
	ExecuteBatch(migrationUUID uuid.UUID, batch *EventBatch) error
	GetListOfTableAttributes(tableNameTup sqlname.NameTuple) ([]string, error)
	QuoteAttributeName(tableNameTup sqlname.NameTuple, columnName string) (string, error)
	MaxBatchSizeInBytes() int64
	RestoreSequences(sequencesLastValue map[string]int64) error
	GetIdentityColumnNamesForTable(tableNameTup sqlname.NameTuple, identityType string) ([]string, error)
	DisableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error
	EnableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error
	EnableGeneratedByDefaultAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error
	ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error
	GetCallhomeTargetDBInfo() *callhome.TargetDBDetails
	// NOTE: The following four methods should not be used for arbitrary query
	// execution on TargetDB. The should be only used from higher level
	// abstractions like ImportDataState.
	Query(query string) (*sql.Rows, error)
	QueryRow(query string) *sql.Row
	Exec(query string) (int64, error)
	WithTx(fn func(tx *sql.Tx) error) error
}

//=============================================================

const (
	ORACLE     = "oracle"
	MYSQL      = "mysql"
	POSTGRESQL = "postgresql"
	YUGABYTEDB = "yugabytedb"
)

type Batch interface {
	Open() (*os.File, error)
	GetFilePath() string
	GetTableName() sqlname.NameTuple
	GetQueryIsBatchAlreadyImported() string
	GetQueryToRecordEntryInDB(rowsAffected int64) string
}

func NewTargetDB(tconf *TargetConf) TargetDB {
	switch tconf.TargetDBType {
	case ORACLE:
		return newTargetOracleDB(tconf)
	case POSTGRESQL:
		return newTargetPostgreSQL(tconf)
	case YUGABYTEDB:
		return newTargetYugabyteDB(tconf)
	}
	return nil
}

type ImportBatchArgs struct {
	FilePath     string
	TableNameTup sqlname.NameTuple
	Columns      []string

	FileFormat string
	HasHeader  bool
	Delimiter  string
	QuoteChar  byte
	EscapeChar byte
	NullString string

	RowsPerTransaction int64
}

func (args *ImportBatchArgs) GetYBCopyStatement() string {
	options := args.copyOptions()
	options = append(options, fmt.Sprintf("ROWS_PER_TRANSACTION %v", args.RowsPerTransaction))
	columns := ""
	if len(args.Columns) > 0 {
		columns = fmt.Sprintf("(%s)", strings.Join(args.Columns, ", "))
	}
	return fmt.Sprintf(`COPY %s %s FROM STDIN WITH (%s)`, args.TableNameTup.ForUserQuery(), columns, strings.Join(options, ", "))
}

func (args *ImportBatchArgs) GetPGCopyStatement() string {
	options := args.copyOptions()
	columns := ""
	if len(args.Columns) > 0 {
		columns = fmt.Sprintf("(%s)", strings.Join(args.Columns, ", "))
	}
	return fmt.Sprintf(`COPY %s %s FROM STDIN WITH (%s)`, args.TableNameTup.ForUserQuery(), columns, strings.Join(options, ", "))
}

func (args *ImportBatchArgs) copyOptions() []string {

	options := []string{
		fmt.Sprintf("FORMAT '%s'", args.FileFormat),
	}
	if args.HasHeader {
		options = append(options, "HEADER")
	}
	if args.Delimiter != "" {
		options = append(options, fmt.Sprintf("DELIMITER E'%c'", []rune(args.Delimiter)[0]))
	}
	if args.QuoteChar != 0 {
		quoteChar := string(args.QuoteChar)
		if quoteChar == `'` || quoteChar == `\` {
			quoteChar = `\` + quoteChar
		}
		options = append(options, fmt.Sprintf("QUOTE E'%s'", quoteChar))
	}
	if args.EscapeChar != 0 {
		escapeChar := string(args.EscapeChar)
		if escapeChar == `'` || escapeChar == `\` {
			escapeChar = `\` + escapeChar
		}
		options = append(options, fmt.Sprintf("ESCAPE E'%s'", escapeChar))
	}
	if args.NullString != "" {
		options = append(options, fmt.Sprintf("NULL '%s'", args.NullString))
	}
	return options
}

func (args *ImportBatchArgs) GetSqlLdrControlFile(tableSchema map[string]map[string]string) string {
	var columns string
	if len(args.Columns) > 0 {
		var columnsList []string
		for _, column := range args.Columns {
			//setting the null string for each column
			dataType, ok := tableSchema[column]["__debezium.source.column.type"] //TODO: rename this to some thing like source-db-datatype
			charLength, okLen := tableSchema[column]["__debezium.source.column.length"]
			switch true {
			case ok && strings.Contains(dataType, "INTERVAL"):
				columnsList = append(columnsList, fmt.Sprintf(`%s %s NULLIF %s='%s'`, column, dataType, column, args.NullString))
			case ok && strings.HasPrefix(dataType, "DATE"):
				columnsList = append(columnsList, fmt.Sprintf(`%s DATE "DD-MM-YYYY" NULLIF %s='%s'`, column, column, args.NullString))
			case ok && strings.HasPrefix(dataType, "TIMESTAMP"):
				switch true {
				case strings.Contains(dataType, "TIME ZONE"):
					columnsList = append(columnsList, fmt.Sprintf(`%s TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH:MI:SS.FF9 AM TZR" NULLIF %s='%s'`, column, column, args.NullString))
				default:
					columnsList = append(columnsList, fmt.Sprintf(`%s TIMESTAMP "DD-MM-YYYY HH:MI:SS.FF9 AM" NULLIF %s='%s'`, column, column, args.NullString))
				}
			case ok && okLen && strings.Contains(dataType, "CHAR"):
				columnsList = append(columnsList, fmt.Sprintf(`%s CHAR(%s) NULLIF %s='%s'`, column, charLength, column, args.NullString))
			case ok && dataType == "LONG":
				columnsList = append(columnsList, fmt.Sprintf(`%s CHAR(2000000000) NULLIF %s='%s'`, column, column, args.NullString)) // for now mentioning max 2GB length, TODO: figure out if there is any other way to handle LONG data type
			default:
				columnsList = append(columnsList, fmt.Sprintf("%s NULLIF %s='%s'", column, column, args.NullString))
			}
		}
		columns = fmt.Sprintf("(%s)", strings.Join(columnsList, ",\n"))
	}

	configTemplate := `LOAD DATA
INFILE '%s'
APPEND
INTO TABLE %s
REENABLE DISABLED_CONSTRAINTS
FIELDS CSV WITH EMBEDDED 
TRAILING NULLCOLS
%s`
	return fmt.Sprintf(configTemplate, args.FilePath, args.TableNameTup.ForUserQuery(), columns)
	/*
	   reference for sqlldr control file
	   https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-sql-loader-control-file-contents.html#GUID-D1762699-8154-40F6-90DE-EFB8EB6A9AB0
	   REENABLE DISABLED_CONSTRAINTS - reenables all disabled constraints on the table
	   FIELDS CSV WITH EMBEDDED - specifies that the data file contains comma-separated values (CSV) with embedded newlines
	   TRAILING NULLCOLS - allows SQL*Loader to load a table when the record contains trailing null fields
	*/
}
