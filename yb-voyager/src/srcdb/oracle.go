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
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type Oracle struct {
	source *Source

	db *sql.DB
}

// In addition to the types listed below, user-defined types (UDTs) are also not supported if Debezium is used for data export. The UDT case is handled inside the `GetColumnsWithSupportedTypes()`.
var oracleUnsupportedDataTypes = []string{"BLOB", "CLOB", "NCLOB", "BFILE", "URITYPE", "XMLTYPE",
	"AnyData", "AnyType", "AnyDataSet", "ROWID", "UROWID", "SDO_GEOMETRY", "SDO_POINT_TYPE", "SDO_ELEM_INFO_ARRAY", "SDO_ORDINATE_ARRAY", "SDO_GTYPE", "SDO_SRID", "SDO_POINT", "SDO_ORDINATES", "SDO_DIM_ARRAY", "SDO_ORGSCL_TYPE", "SDO_STRING_ARRAY", "JSON"}

func newOracle(s *Source) *Oracle {
	return &Oracle{source: s}
}

func (ora *Oracle) Connect() error {
	db, err := sql.Open("godror", ora.getConnectionUri())
	ora.db = db
	return err
}

func (ora *Oracle) Disconnect() {
	if ora.db == nil {
		log.Infof("No connection to the source database to close")
		return
	}

	err := ora.db.Close()
	if err != nil {
		log.Infof("Failed to close connection to the source database: %s", err)
	}
}

func (ora *Oracle) CheckRequiredToolsAreInstalled() {
	checkTools("ora2pg", "sqlplus")
}

func (ora *Oracle) GetTableRowCount(tableName string) int64 {
	var rowCount int64
	query := fmt.Sprintf("select count(*) from %s", tableName)

	log.Infof("Querying row count of table %q", tableName)
	err := ora.db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for row count of %q: %s", query, tableName, err)
	}
	log.Infof("Table %q has %v rows.", tableName, rowCount)
	return rowCount
}

func (ora *Oracle) GetTableApproxRowCount(tableName *sqlname.SourceName) int64 {
	var approxRowCount sql.NullInt64 // handles case: value of the row is null, default for int64 is 0
	query := fmt.Sprintf("SELECT NUM_ROWS FROM ALL_TABLES "+
		"WHERE TABLE_NAME = '%s' and OWNER =  '%s'",
		tableName.ObjectName.Unquoted, tableName.SchemaName.Unquoted)

	log.Infof("Querying '%s' approx row count of table %q", query, tableName.String())
	err := ora.db.QueryRow(query).Scan(&approxRowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for approx row count of %q: %s", query, tableName.String(), err)
	}

	log.Infof("Table %q has approx %v rows.", tableName.String(), approxRowCount)
	return approxRowCount.Int64
}

func (ora *Oracle) GetVersion() string {
	var version string
	query := "SELECT BANNER FROM V$VERSION"
	// query sample output: Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
	err := ora.db.QueryRow(query).Scan(&version)
	if err != nil {
		utils.ErrExit("run query %q on source: %s", query, err)
	}
	ora.source.DBVersion = version
	return version
}

func (ora *Oracle) GetAllTableNamesRaw(schemaName string) ([]string, error) {
	var tableNames []string
	query := fmt.Sprintf(`SELECT table_name
		FROM all_tables
		WHERE owner = '%s' AND TEMPORARY = 'N' AND table_name NOT LIKE 'DR$%%'
		AND table_name NOT LIKE 'AQ$%%' AND
		(owner, table_name) not in (
			SELECT owner, mview_name
			FROM all_mviews
			UNION ALL
			SELECT log_owner, log_table
			FROM all_mview_logs)
		ORDER BY table_name ASC`, schemaName)
	log.Infof(`query used to GetAllTableNamesRaw(): "%s"`, query)

	rows, err := ora.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying source database for table names: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for table names: %v", err)
		}
		tableNames = append(tableNames, tableName)
	}

	log.Infof("Table Name List: %q", tableNames)

	return tableNames, nil
}

func (ora *Oracle) GetAllTableNames() []*sqlname.SourceName {
	var tableNames []*sqlname.SourceName
	tableNamesRaw, err := ora.GetAllTableNamesRaw(ora.source.Schema)
	if err != nil {
		utils.ErrExit("error in querying source database for table names: %v", err)
	}
	for _, tableName := range tableNamesRaw {
		tableName = fmt.Sprintf(`"%s"`, tableName)
		tableNames = append(tableNames, sqlname.NewSourceName(ora.source.Schema, tableName))
	}
	log.Infof("Table Name List: %q", tableNames)
	return tableNames
}

func (ora *Oracle) getConnectionUri() string {
	source := ora.source
	if source.Uri != "" {
		return source.Uri
	}

	connectionString := GetOracleConnectionString(source.Host, source.Port, source.DBName, source.DBSid, source.TNSAlias)
	source.Uri = fmt.Sprintf(`user="%s" password="%s" connectString="%s"`, source.User, source.Password, connectionString)
	return source.Uri
}

func GetOracleConnectionString(host string, port int, dbname string, dbsid string, tnsalias string) string {
	switch true {
	case dbsid != "":
		return fmt.Sprintf(`(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SID=%s)))`,
			host, port, dbsid)

	case tnsalias != "":
		return tnsalias

	case dbname != "":
		return fmt.Sprintf(`(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s)))`,
			host, port, dbname)
	}
	return ""
}

func (ora *Oracle) ExportSchema(exportDir string) {
	ora2pgExtractSchema(ora.source, exportDir)
}

// return list of jsons having index info like index name, index type, table name, column name
func (ora *Oracle) GetIndexesInfo() []utils.IndexInfo {
	// TODO(future): once we implement table-list/object-type for export schema
	// we will have to filter out indexes based on tables or object types that are not being exported
	query := fmt.Sprintf(`SELECT AIN.INDEX_NAME, AIN.INDEX_TYPE, AIN.TABLE_NAME, 
	LISTAGG(AIC.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY AIC.COLUMN_POSITION) AS COLUMNS
	FROM ALL_INDEXES AIN
	INNER JOIN ALL_IND_COLUMNS AIC ON AIN.INDEX_NAME = AIC.INDEX_NAME AND AIN.TABLE_NAME = AIC.TABLE_NAME
	LEFT JOIN ALL_CONSTRAINTS AC 
	ON AIN.TABLE_NAME = AC.TABLE_NAME 
	AND AIN.INDEX_NAME = AC.CONSTRAINT_NAME
	WHERE AIN.OWNER = '%s' 
	AND NOT (AIN.INDEX_NAME LIKE 'SYS%%' OR AIN.INDEX_NAME LIKE 'DR$%%')
	AND AC.CONSTRAINT_TYPE IS NULL -- Exclude primary keys
	GROUP BY AIN.INDEX_NAME, AIN.INDEX_TYPE, AIN.TABLE_NAME`, ora.source.Schema)
	rows, err := ora.db.Query(query)
	if err != nil {
		utils.ErrExit("error in querying source database for indexes info: %v", err)
	}
	defer rows.Close()
	var indexesInfo []utils.IndexInfo
	for rows.Next() {
		var indexName, indexType, tableName, columns string
		err = rows.Scan(&indexName, &indexType, &tableName, &columns)
		if err != nil {
			utils.ErrExit("error in scanning query rows for reverse indexes: %v", err)
		}
		indexInfo := utils.IndexInfo{
			IndexName: indexName,
			IndexType: indexType,
			TableName: tableName,
			Columns:   strings.Split(columns, ", "),
		}
		indexesInfo = append(indexesInfo, indexInfo)
	}
	if len(indexesInfo) == 0 {
		log.Infof("No indexes found in the source database")
		return nil
	}
	log.Infof("Indexes Info: %+v", indexesInfo)
	return indexesInfo
}

func (ora *Oracle) ExportData(ctx context.Context, exportDir string, tableList []*sqlname.SourceName, quitChan chan bool, exportDataStart, exportSuccessChan chan bool, tablesColumnList map[*sqlname.SourceName][]string, snapshotName string) {
	ora2pgExportDataOffline(ctx, ora.source, exportDir, tableList, tablesColumnList, quitChan, exportDataStart, exportSuccessChan)
}

func (ora *Oracle) ExportDataPostProcessing(exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	renameDataFilesForReservedWords(tablesProgressMetadata)
	dfd := datafile.Descriptor{
		FileFormat:                 datafile.SQL,
		DataFileList:               getExportedDataFileList(tablesProgressMetadata),
		Delimiter:                  "\t",
		HasHeader:                  false,
		ExportDir:                  exportDir,
		NullString:                 `\N`,
		TableNameToExportedColumns: getOra2pgExportedColumnsMap(exportDir, tablesProgressMetadata),
	}
	dfd.Save()

	identityColumns := getIdentityColumnSequences(exportDir)
	sourceTargetSequenceNames := make(map[string]string)
	for _, identityColumn := range identityColumns {
		targetSequenceName := ora.GetTargetIdentityColumnSequenceName(identityColumn)
		if targetSequenceName == "" {
			continue
		}
		sourceTargetSequenceNames[identityColumn] = targetSequenceName
	}

	replaceAllIdentityColumns(exportDir, sourceTargetSequenceNames)
}

func (ora *Oracle) GetCharset() (string, error) {
	var charset string
	query := "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET'"
	err := ora.db.QueryRow(query).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to query %q for database encoding: %s", query, err)
	}
	return charset, nil
}

func (ora *Oracle) FilterUnsupportedTables(tableList []*sqlname.SourceName, useDebezium bool) ([]*sqlname.SourceName, []*sqlname.SourceName) {
	var filteredTableList, unsupportedTableList []*sqlname.SourceName

	// query to find unsupported queue tables
	query := fmt.Sprintf("SELECT queue_table from ALL_QUEUE_TABLES WHERE OWNER = '%s'", ora.source.Schema)
	log.Infof("query for queue tables: %q\n", query)
	rows, err := ora.db.Query(query)
	if err != nil {
		utils.ErrExit("failed to query %q for filtering unsupported queue tables: %v", query, err)
	}
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			utils.ErrExit("failed to scan tableName from output of query %q: %v", query, err)
		}
		tableName = fmt.Sprintf(`"%s"`, tableName)
		tableSrcName := sqlname.NewSourceName(ora.source.Schema, tableName)
		if slices.Contains(tableList, tableSrcName) {
			unsupportedTableList = append(unsupportedTableList, tableSrcName)
		}
	}

	if useDebezium {
		for _, tableName := range tableList {
			if ora.IsNestedTable(tableName) || ora.IsParentOfNestedTable(tableName) {
				//In case of nested tables there are two tables created in the oracle one is this main parent table created by user and other is nested table for a column which is created by oracle
				unsupportedTableList = append(unsupportedTableList, tableName)
			}
		}
	}

	for _, table := range tableList {
		if !slices.Contains(unsupportedTableList, table) && table.ObjectName.MinQuoted != "LOG_MINING_FLUSH" {
			filteredTableList = append(filteredTableList, table)
		}
	}

	return filteredTableList, unsupportedTableList
}

func (ora *Oracle) FilterEmptyTables(tableList []*sqlname.SourceName) ([]*sqlname.SourceName, []*sqlname.SourceName) {
	nonEmptyTableList := make([]*sqlname.SourceName, 0)
	skippedTableList := make([]*sqlname.SourceName, 0)
	for _, tableName := range tableList {
		query := fmt.Sprintf("SELECT 1 FROM %s WHERE ROWNUM=1", tableName.Qualified.MinQuoted)
		if ora.IsNestedTable(tableName) {
			// query to check empty nested oracle tables
			query = fmt.Sprintf(`SELECT 1 from dba_segments 
			where owner = '%s' AND segment_name = '%s' AND segment_type = 'NESTED TABLE'`,
				tableName.SchemaName.Unquoted, tableName.ObjectName.Unquoted)
		}

		if !IsTableEmpty(ora.db, query) {
			nonEmptyTableList = append(nonEmptyTableList, tableName)
		} else {
			skippedTableList = append(skippedTableList, tableName)
		}
	}
	return nonEmptyTableList, skippedTableList
}

func (ora *Oracle) IsNestedTable(tableName *sqlname.SourceName) bool {
	// sql query to find out if it is oracle nested table
	query := fmt.Sprintf("SELECT 1 FROM ALL_NESTED_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
		tableName.SchemaName.Unquoted, tableName.ObjectName.Unquoted)
	isNestedTable := 0
	err := ora.db.QueryRow(query).Scan(&isNestedTable)
	if err != nil && err != sql.ErrNoRows {
		utils.ErrExit("error in query to check if table %v is a nested table: %v", tableName, err)
	}
	return isNestedTable == 1
}

func (ora *Oracle) IsParentOfNestedTable(tableName *sqlname.SourceName) bool {
	// sql query to find out if it is parent of oracle nested table
	query := fmt.Sprintf("SELECT 1 FROM ALL_NESTED_TABLES WHERE OWNER = '%s' AND PARENT_TABLE_NAME= '%s'",
		tableName.SchemaName.Unquoted, tableName.ObjectName.Unquoted)
	isParentNestedTable := 0
	err := ora.db.QueryRow(query).Scan(&isParentNestedTable)
	if err != nil && err != sql.ErrNoRows {
		utils.ErrExit("error in query to check if table %v is parent of nested table: %v", tableName, err)
	}
	return isParentNestedTable == 1
}

func (ora *Oracle) GetTargetIdentityColumnSequenceName(sequenceName string) string {
	var tableName, columnName string
	query := fmt.Sprintf("SELECT table_name, column_name FROM all_tab_identity_cols WHERE owner = '%s' AND sequence_name = '%s'", ora.source.Schema, strings.ToUpper(sequenceName))
	err := ora.db.QueryRow(query).Scan(&tableName, &columnName)

	if err == sql.ErrNoRows {
		return ""
	} else if err != nil {
		utils.ErrExit("failed to query %q for finding identity sequence table and column: %v", query, err)
	}

	return fmt.Sprintf("%s_%s_seq", tableName, columnName)
}

func (ora *Oracle) ParentTableOfPartition(table *sqlname.SourceName) string {
	panic("not implemented")
}

func (ora *Oracle) ValidateTablesReadyForLiveMigration(tableList []*sqlname.SourceName) error {
	panic("not implemented")
}

/*
GetColumnToSequenceMap returns a map of column name to sequence name for all identity columns in the given list of tables.
Note: There can be only one identity column per table in Oracle
*/
func (ora *Oracle) GetColumnToSequenceMap(tableList []*sqlname.SourceName) map[string]string {
	columnToSequenceMap := make(map[string]string)
	for _, table := range tableList {
		// query to find out if table has a identity column
		query := fmt.Sprintf("SELECT column_name FROM all_tab_identity_cols WHERE owner = '%s' AND table_name = '%s'", table.SchemaName.Unquoted, table.ObjectName.Unquoted)
		rows, err := ora.db.Query(query)
		if err != nil {
			utils.ErrExit("failed to query %q for finding identity column: %v", query, err)
		}
		for rows.Next() {
			var columnName string
			err := rows.Scan(&columnName)
			if err != nil {
				utils.ErrExit("failed to scan columnName from output of query %q: %v", query, err)
			}
			qualifiedColumnName := fmt.Sprintf("%s.%s.%s", table.SchemaName.Unquoted, table.ObjectName.Unquoted, columnName)
			columnToSequenceMap[qualifiedColumnName] = fmt.Sprintf("%s_%s_seq", table.ObjectName.Unquoted, columnName)
		}
	}

	return columnToSequenceMap
}

func (ora *Oracle) GetAllSequences() []string {
	return nil
}

func (ora *Oracle) GetTableColumns(tableName *sqlname.SourceName) ([]string, []string, []string) {
	var columns, dataTypes, dataTypesOwner []string
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE, DATA_TYPE_OWNER FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s'", tableName.SchemaName.Unquoted, tableName.ObjectName.Unquoted)
	rows, err := ora.db.Query(query)
	if err != nil {
		utils.ErrExit("failed to query %q for finding table columns: %v", query, err)
	}
	for rows.Next() {
		var column, dataType, dataTypeOwner string
		err := rows.Scan(&column, &dataType, &dataTypeOwner)
		if err != nil {
			utils.ErrExit("failed to scan column name from output of query %q: %v", query, err)
		}
		columns = append(columns, column)
		dataTypes = append(dataTypes, dataType)
		dataTypesOwner = append(dataTypesOwner, dataTypeOwner)
	}
	return columns, dataTypes, dataTypesOwner
}

func (ora *Oracle) GetColumnsWithSupportedTypes(tableList []*sqlname.SourceName, useDebezium bool, isStreamingEnabled bool) (map[*sqlname.SourceName][]string, []string) {
	tableColumnMap := make(map[*sqlname.SourceName][]string)
	var unsupportedColumnNames []string
	if isStreamingEnabled {
		oracleUnsupportedDataTypes = append(oracleUnsupportedDataTypes, "NCHAR", "NVARCHAR2")
	}
	for _, tableName := range tableList {
		columns, dataTypes, dataTypesOwner := ora.GetTableColumns(tableName)
		var supportedColumnNames []string
		for i := 0; i < len(columns); i++ {
			isUdtWithDebezium := (dataTypesOwner[i] == tableName.SchemaName.Unquoted) && useDebezium // datatype owner check is for UDT type detection as VARRAY are created using UDT
			if isUdtWithDebezium || utils.ContainsAnySubstringFromSlice(oracleUnsupportedDataTypes, dataTypes[i]) {
				log.Infof("Skipping unsupproted column %s.%s of type %s", tableName.ObjectName.MinQuoted, columns[i], dataTypes[i])
				unsupportedColumnNames = append(unsupportedColumnNames, fmt.Sprintf("%s.%s of type %s", tableName.ObjectName.MinQuoted, columns[i], dataTypes[i]))
			} else {
				supportedColumnNames = append(supportedColumnNames, columns[i])
			}

		}
		if len(supportedColumnNames) == len(columns) {
			tableColumnMap[tableName] = []string{"*"}
		} else {
			tableColumnMap[tableName] = supportedColumnNames
		}
	}

	return tableColumnMap, unsupportedColumnNames
}

func (ora *Oracle) GetServers() []string {
	return []string{ora.source.Host}
}

func (ora *Oracle) GetPartitions(tableName *sqlname.SourceName) []*sqlname.SourceName {
	panic("not implemented")
}

func (ora *Oracle) GetTableToUniqueKeyColumnsMap(tableList []*sqlname.SourceName) (map[string][]string, error) {
	result := make(map[string][]string)
	queryTemplate := `
		SELECT TABLE_NAME, COLUMN_NAME
		FROM ALL_CONS_COLUMNS
		WHERE CONSTRAINT_NAME IN (
			SELECT CONSTRAINT_NAME
			FROM ALL_CONSTRAINTS
			WHERE CONSTRAINT_TYPE = 'U'
			AND OWNER = '%s'
			AND TABLE_NAME IN ('%s')
		)`

	var queryTableList []string
	for _, table := range tableList {
		queryTableList = append(queryTableList, table.ObjectName.Unquoted)
	}
	query := fmt.Sprintf(queryTemplate, ora.source.Schema, strings.Join(queryTableList, "','"))
	log.Infof("query to get unique key columns for tables: %q", query)
	rows, err := ora.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying unique key columns for tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var columnName string
		err := rows.Scan(&tableName, &columnName)
		if err != nil {
			return nil, fmt.Errorf("scanning row for unique key column name: %w", err)
		}
		result[tableName] = append(result[tableName], columnName)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating over rows for unique key columns: %w", err)
	}
	log.Infof("unique key columns for tables: %+v", result)
	return result, nil
}

func (ora *Oracle) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("Clearing migration state for migration %q", migrationUUID)
	log.Infof("Dropping table LOG_MINING_FLUSH")
	logMiningFlushTableName := "LOG_MINING_FLUSH"
	_, err := ora.db.Exec(fmt.Sprintf("DROP TABLE %s", logMiningFlushTableName))
	if err != nil {
		if strings.Contains(err.Error(), "ORA-00942") {
			// Table does not exist, so nothing to drop
			return nil
		}
		return fmt.Errorf("drop table %s: %w", logMiningFlushTableName, err)
	}
	return nil
}

func (ora *Oracle) GetNonPKTables() ([]string, error) {
	query := fmt.Sprintf(`SELECT NVL(pk_count.count, 0) AS pk_count, at.table_name
	FROM ALL_TABLES at
	LEFT JOIN (
		SELECT COUNT(constraint_name) AS count, table_name
		FROM ALL_CONSTRAINTS
		WHERE constraint_type = 'P' AND owner = '%s'
		GROUP BY table_name
	) pk_count ON at.table_name = pk_count.table_name
	WHERE at.owner = '%s'`, ora.source.Schema, ora.source.Schema)
	rows, err := ora.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying source database for unsupported tables: %v", err)
	}
	defer rows.Close()
	var nonPKTables []string
	for rows.Next() {
		var count int
		var tableName string
		err = rows.Scan(&count, &tableName)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for unsupported tables: %v", err)
		}
		table := sqlname.NewSourceName(ora.source.Schema, fmt.Sprintf(`"%s"`, tableName))
		if count == 0 {
			nonPKTables = append(nonPKTables, table.Qualified.MinQuoted)
		}
	}
	return nonPKTables, nil
}
