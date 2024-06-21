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
package srcdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type SourceDB interface {
	Connect() error
	Disconnect()
	GetConnectionUriWithoutPassword() string
	GetTableRowCount(tableName sqlname.NameTuple) int64
	GetTableApproxRowCount(tableName sqlname.NameTuple) int64
	CheckRequiredToolsAreInstalled()
	GetVersion() string
	GetAllTableNames() []*sqlname.SourceName
	GetAllTableNamesRaw(schemaName string) ([]string, error)
	GetAllSequencesRaw(schemaName string) ([]string, error)
	ExportSchema(exportDir string, schemaDir string)
	GetIndexesInfo() []utils.IndexInfo
	ExportData(ctx context.Context, exportDir string, tableList []sqlname.NameTuple, quitChan chan bool, exportDataStart chan bool, exportSuccessChan chan bool, tablesColumnList *utils.StructMap[sqlname.NameTuple, []string], snapshotName string)
	ExportDataPostProcessing(exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata)
	GetCharset() (string, error)
	FilterUnsupportedTables(migrationUUID uuid.UUID, tableList []sqlname.NameTuple, useDebezium bool) ([]sqlname.NameTuple, []sqlname.NameTuple)
	FilterEmptyTables(tableList []sqlname.NameTuple) ([]sqlname.NameTuple, []sqlname.NameTuple)
	GetColumnsWithSupportedTypes(tableList []sqlname.NameTuple, useDebezium bool, isStreamingEnabled bool) (*utils.StructMap[sqlname.NameTuple, []string], *utils.StructMap[sqlname.NameTuple, []string], error)
	ParentTableOfPartition(table sqlname.NameTuple) string
	GetColumnToSequenceMap(tableList []sqlname.NameTuple) map[string]string
	GetAllSequences() []string
	GetServers() []string
	GetPartitions(table sqlname.NameTuple) []string
	GetTableToUniqueKeyColumnsMap(tableList []sqlname.NameTuple) (map[string][]string, error)
	ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error
	GetNonPKTables() ([]string, error)
	ValidateTablesReadyForLiveMigration(tableList []sqlname.NameTuple) error
	GetDatabaseSize() (int64, error)
}

func newSourceDB(source *Source) SourceDB {
	switch source.DBType {
	case "postgresql":
		return newPostgreSQL(source)
	case "yugabytedb":
		return newYugabyteDB(source)
	case "mysql":
		return newMySQL(source)
	case "oracle":
		return newOracle(source)
	default:
		panic(fmt.Sprintf("unknown source database type %q", source.DBType))
	}
}

func IsTableEmpty(db *sql.DB, query string) bool {
	var rowsExist int
	err := db.QueryRow(query).Scan(&rowsExist)
	if err == sql.ErrNoRows {
		return true
	}
	if err != nil {
		utils.ErrExit("Failed to query %q to check table is empty: %s", query, err)
	}
	return false
}
