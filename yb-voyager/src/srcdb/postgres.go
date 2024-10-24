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
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mcuadros/go-version"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

const MIN_SUPPORTED_PG_VERSION_OFFLINE = "9"
const MIN_SUPPORTED_PG_VERSION_LIVE = "10"
const MAX_SUPPORTED_PG_VERSION = "16"
const MISSING = "MISSING"
const GRANTED = "GRANTED"
const NO_USAGE_PERMISSION = "NO USAGE PERMISSION"

var pg_catalog_tables_required = []string{"regclass", "pg_class", "pg_inherits", "setval", "pg_index", "pg_relation_size", "pg_namespace", "pg_tables", "pg_sequences", "pg_roles", "pg_database"}
var information_schema_tables_required = []string{"schemata", "tables", "columns", "key_column_usage", "sequences"}
var PostgresUnsupportedDataTypes = []string{"GEOMETRY", "GEOGRAPHY", "RASTER", "PG_LSN", "TXID_SNAPSHOT", "XML", "XID" }
var PostgresUnsupportedDataTypesForDbzm = []string{"POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "GEOMETRY", "GEOGRAPHY", "RASTER", "PG_LSN", "TXID_SNAPSHOT", "XML"}

var PG_COMMAND_VERSION = map[string]string{
	"pg_dump":    "14.0",
	"pg_restore": "14.0",
	"psql":       "9.0", //psql features we need are available in 7.1 onwards, keeping it to 9.0 for safety
}

const FETCH_COLUMN_SEQUENCES_QUERY_TEMPLATE = `SELECT
a.attname AS column_name,
COALESCE(seq.relname, '') AS sequence_name,
COALESCE(ns.nspname, '') AS schema_name
FROM pg_class AS t
JOIN pg_attribute AS a ON a.attrelid = t.oid
JOIN pg_namespace AS tn ON tn.oid = t.relnamespace
LEFT JOIN pg_attrdef AS ad ON ad.adrelid = t.oid AND ad.adnum = a.attnum
LEFT JOIN pg_depend AS d ON d.objid = ad.oid
LEFT JOIN pg_class AS seq ON seq.oid = d.refobjid
LEFT JOIN pg_namespace AS ns ON ns.oid = seq.relnamespace
WHERE
tn.nspname = '%s' -- schema name
AND t.relname = '%s' -- table name
AND a.attnum > 0
AND NOT a.attisdropped
AND t.relkind IN ('r', 'P')
AND seq.relkind = 'S';`

const GET_TABLE_COLUMNS_QUERY_TEMPLATE_PG_AND_YB = `SELECT a.attname AS column_name, t.typname AS data_type, rol.rolname AS data_type_owner 
FROM pg_attribute AS a 
JOIN pg_type AS t ON t.oid = a.atttypid 
JOIN pg_class AS c ON c.oid = a.attrelid 
JOIN pg_namespace AS n ON n.oid = c.relnamespace 
JOIN pg_roles AS rol ON rol.oid = t.typowner 
WHERE c.relname = '%s' AND n.nspname = '%s' AND a.attname NOT IN ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid');`

type PostgreSQL struct {
	source *Source

	db *sql.DB
}

func newPostgreSQL(s *Source) *PostgreSQL {
	return &PostgreSQL{source: s}
}

func (pg *PostgreSQL) Connect() error {
	db, err := sql.Open("pgx", pg.getConnectionUri())
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	pg.db = db
	return err
}

func (pg *PostgreSQL) Disconnect() {
	if pg.db == nil {
		log.Infof("No connection to the source database to close")
		return
	}

	err := pg.db.Close()
	if err != nil {
		log.Infof("Failed to close connection to the source database: %s", err)
	}
}

func (pg *PostgreSQL) getTrimmedSchemaList() []string {
	list := strings.Split(pg.source.Schema, "|")
	var trimmedList []string
	for _, schema := range list {
		if utils.IsQuotedString(schema) {
			schema = strings.Trim(schema, `"`)
		}
		trimmedList = append(trimmedList, schema)
	}
	return trimmedList
}

func (pg *PostgreSQL) CheckRequiredToolsAreInstalled() {
	checkTools("strings")
}

func (pg *PostgreSQL) GetTableRowCount(tableName sqlname.NameTuple) int64 {
	var rowCount int64
	query := fmt.Sprintf("select count(*) from %s", tableName.ForUserQuery())
	log.Infof("Querying row count of table %q", tableName)
	err := pg.db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for row count of %q: %s", query, tableName, err)
	}
	log.Infof("Table %q has %v rows.", tableName, rowCount)
	return rowCount
}

func (pg *PostgreSQL) GetTableApproxRowCount(tableName sqlname.NameTuple) int64 {
	var approxRowCount sql.NullInt64 // handles case: value of the row is null, default for int64 is 0
	query := fmt.Sprintf("SELECT reltuples::bigint FROM pg_class "+
		"where oid = '%s'::regclass", tableName.ForOutput())

	log.Infof("Querying '%s' approx row count of table %q", query, tableName.String())
	err := pg.db.QueryRow(query).Scan(&approxRowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for approx row count of %q: %s", query, tableName.String(), err)
	}

	log.Infof("Table %q has approx %v rows.", tableName.String(), approxRowCount)
	return approxRowCount.Int64
}

func (pg *PostgreSQL) GetVersion() string {
	if pg.source.DBVersion != "" {
		return pg.source.DBVersion
	}

	var version string
	query := "SELECT setting from pg_settings where name = 'server_version'"
	err := pg.db.QueryRow(query).Scan(&version)
	if err != nil {
		utils.ErrExit("run query %q on source: %s", query, err)
	}
	pg.source.DBVersion = version
	return version
}

func (pg *PostgreSQL) CheckSchemaExists() bool {
	schemaList := pg.checkSchemasExists()
	return schemaList != nil
}

func (pg *PostgreSQL) checkSchemasExists() []string {
	trimmedSchemaList := pg.getTrimmedSchemaList()
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"
	chkSchemaExistsQuery := fmt.Sprintf(`SELECT nspname AS schema_name
	FROM pg_namespace
	WHERE nspname IN (%s);`, querySchemaList)
	rows, err := pg.db.Query(chkSchemaExistsQuery)
	if err != nil {
		utils.ErrExit("error in querying(%q) source database for checking mentioned schema(s) present or not: %v\n", chkSchemaExistsQuery, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", chkSchemaExistsQuery, closeErr)
		}
	}()
	var listOfSchemaPresent []string
	var tableSchemaName string

	for rows.Next() {
		err = rows.Scan(&tableSchemaName)
		if err != nil {
			utils.ErrExit("error in scanning query rows for schema names: %v\n", err)
		}
		listOfSchemaPresent = append(listOfSchemaPresent, tableSchemaName)
	}

	schemaNotPresent := utils.SetDifference(trimmedSchemaList, listOfSchemaPresent)
	if len(schemaNotPresent) > 0 {
		utils.ErrExit("Following schemas are not present in source database %v, please provide a valid schema list.\n", schemaNotPresent)
	}
	return trimmedSchemaList
}

func (pg *PostgreSQL) GetAllTableNamesRaw(schemaName string) ([]string, error) {
	query := fmt.Sprintf(`SELECT table_name
			  FROM information_schema.tables
			  WHERE table_type = 'BASE TABLE' AND
			        table_schema = '%s';`, schemaName)

	rows, err := pg.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying(%q) source database for table names: %w", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	var tableNames []string
	var tableName string

	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for table names: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	log.Infof("Query found %d tables in the source db: %v", len(tableNames), tableNames)
	return tableNames, nil
}

func (pg *PostgreSQL) GetAllTableNames() []*sqlname.SourceName {
	schemaList := pg.checkSchemasExists()
	querySchemaList := "'" + strings.Join(schemaList, "','") + "'"
	query := fmt.Sprintf(`SELECT table_schema, table_name
			  FROM information_schema.tables
			  WHERE table_type = 'BASE TABLE' AND
			        table_schema IN (%s);`, querySchemaList)

	rows, err := pg.db.Query(query)
	if err != nil {
		utils.ErrExit("error in querying(%q) source database for table names: %v\n", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	var tableNames []*sqlname.SourceName
	var tableName, tableSchema string

	for rows.Next() {
		err = rows.Scan(&tableSchema, &tableName)
		if err != nil {
			utils.ErrExit("error in scanning query rows for table names: %v\n", err)
		}
		tableName = fmt.Sprintf("\"%s\"", tableName)
		tableNames = append(tableNames, sqlname.NewSourceName(tableSchema, tableName))
	}
	log.Infof("Query found %d tables in the source db: %v", len(tableNames), tableNames)
	return tableNames
}

func (pg *PostgreSQL) getConnectionUri() string {
	source := pg.source
	if source.Uri != "" {
		return source.Uri
	}
	hostAndPort := fmt.Sprintf("%s:%d", source.Host, source.Port)
	sourceUrl := &url.URL{
		Scheme:   "postgresql",
		User:     url.UserPassword(source.User, source.Password),
		Host:     hostAndPort,
		Path:     source.DBName,
		RawQuery: generateSSLQueryStringIfNotExists(source),
	}

	source.Uri = sourceUrl.String()
	return source.Uri
}

func (pg *PostgreSQL) GetConnectionUriWithoutPassword() string {
	source := pg.source
	hostAndPort := fmt.Sprintf("%s:%d", source.Host, source.Port)
	sourceUrl := &url.URL{
		Scheme:   "postgresql",
		User:     url.User(source.User),
		Host:     hostAndPort,
		Path:     source.DBName,
		RawQuery: generateSSLQueryStringIfNotExists(source),
	}
	return sourceUrl.String()
}

func (pg *PostgreSQL) ExportSchema(exportDir string, schemaDir string) {
	if utils.FileOrFolderExists(filepath.Join(schemaDir, "schema.sql")) {
		// case for assess-migration cmd workflow
		log.Infof("directly parsing the '%s/schema.sql' file", schemaDir)
		parseSchemaFile(exportDir, schemaDir, pg.source.ExportObjectTypeList)
	} else {
		pg.checkSchemasExists()

		fmt.Printf("exporting the schema %10s", "")
		go utils.Wait("done\n", "")
		pgdumpExtractSchema(pg.source, pg.GetConnectionUriWithoutPassword(), exportDir, schemaDir)

		//Parsing the single file to generate multiple database object files
		returnCode := parseSchemaFile(exportDir, schemaDir, pg.source.ExportObjectTypeList)

		log.Info("Export of schema completed.")
		utils.WaitChannel <- returnCode
		<-utils.WaitChannel
	}
}

func (pg *PostgreSQL) GetIndexesInfo() []utils.IndexInfo {
	return nil
}

func (pg *PostgreSQL) ExportData(ctx context.Context, exportDir string, tableList []sqlname.NameTuple, quitChan chan bool, exportDataStart, exportSuccessChan chan bool, tablesColumnList *utils.StructMap[sqlname.NameTuple, []string], snapshotName string) {
	pgdumpExportDataOffline(ctx, pg.source, pg.GetConnectionUriWithoutPassword(), exportDir, tableList, quitChan, exportDataStart, exportSuccessChan, snapshotName)
}

func (pg *PostgreSQL) ExportDataPostProcessing(exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	renameDataFiles(tablesProgressMetadata)
	dfd := datafile.Descriptor{
		FileFormat:                 datafile.TEXT,
		DataFileList:               getExportedDataFileList(tablesProgressMetadata),
		Delimiter:                  "\t",
		HasHeader:                  false,
		ExportDir:                  exportDir,
		NullString:                 `\N`,
		TableNameToExportedColumns: pg.getExportedColumnsMap(exportDir, tablesProgressMetadata),
	}

	dfd.Save()
}

func (pg *PostgreSQL) getExportedColumnsMap(
	exportDir string, tablesMetadata map[string]*utils.TableProgressMetadata) map[string][]string {

	result := make(map[string][]string)
	for _, tableMetadata := range tablesMetadata {
		// TODO: Use tableMetadata.TableName instead of parsing the file name.
		// We need a new method in sqlname.SourceName that returns MaybeQuoted and MaybeQualified names.
		tableName := strings.TrimSuffix(filepath.Base(tableMetadata.FinalFilePath), "_data.sql")
		result[tableName] = pg.getExportedColumnsListForTable(exportDir, tableName)
	}
	return result
}

func (pg *PostgreSQL) getExportedColumnsListForTable(exportDir, tableName string) []string {
	var columnsList []string
	var re *regexp.Regexp
	if len(strings.Split(tableName, ".")) == 1 {
		// happens only when table is in public schema, use public schema with table name for regexp
		re = regexp.MustCompile(fmt.Sprintf(`(?i)COPY public.%s[\s]+\((.*)\) FROM STDIN`, tableName))
	} else {
		re = regexp.MustCompile(fmt.Sprintf(`(?i)COPY %s[\s]+\((.*)\) FROM STDIN`, tableName))
	}
	tocFilePath := filepath.Join(exportDir, "data", "toc.dat")
	err := utils.ForEachMatchingLineInFile(tocFilePath, re, func(matches []string) bool {
		columnsList = strings.Split(matches[1], ",")
		for i, column := range columnsList {
			columnsList[i] = strings.TrimSpace(column)
		}
		return false // stop reading file
	})
	if err != nil {
		utils.ErrExit("error in reading toc file: %v\n", err)
	}
	log.Infof("columns list for table %s: %v", tableName, columnsList)
	return columnsList
}

// Given a PG command name ("pg_dump", "pg_restore"), find absolute path of
// the executable file having version >= `PG_COMMAND_VERSION[cmd]`.
func GetAbsPathOfPGCommandAboveVersion(cmd string, sourceDBVersion string) (path string, binaryCheckIssue string, err error) {
	paths, err := findAllExecutablesInPath(cmd)
	if err != nil {
		err = fmt.Errorf("error in finding executables in PATH for %v: %w", cmd, err)
		return "", "", err
	}
	if len(paths) == 0 {
		binaryCheckIssue = fmt.Sprintf("Could not find %v with version greater than or equal to %v in the PATH", cmd, max(PG_COMMAND_VERSION[cmd], sourceDBVersion))
		return "", binaryCheckIssue, nil
	}

	for _, path := range paths {
		checkVersiomCmd := exec.Command(path, "--version")
		stdout, err := checkVersiomCmd.Output()
		if err != nil {
			err = fmt.Errorf("error in fetching version of %v from path %v: %w", cmd, path, err)
			return "", "", err
		}

		// example output centos: pg_restore (PostgreSQL) 14.5
		// example output Ubuntu: pg_dump (PostgreSQL) 14.5 (Ubuntu 14.5-1.pgdg22.04+1)
		currVersion := strings.Fields(string(stdout))[2]

		// Check if the version of the command is greater or equalt to the source DB version and greater than the min required version
		if version.CompareSimple(currVersion, PG_COMMAND_VERSION[cmd]) >= 0 {
			// In case of psql we dont need the version to be greater than the sourceDBVersion
			if version.CompareSimple(currVersion, sourceDBVersion) < 0 && cmd != "psql" {
				continue
			}
			return path, "", nil
		}
	}

	binaryCheckIssue = fmt.Sprintf("Could not find %v with version greater than or equal to %v in the PATH", cmd, max(PG_COMMAND_VERSION[cmd], sourceDBVersion))
	return "", binaryCheckIssue, nil
}

// GetAllSequences returns all the sequence names in the database for the given schema list
func (pg *PostgreSQL) GetAllSequences() []string {
	schemaList := pg.checkSchemasExists()
	querySchemaList := "'" + strings.Join(schemaList, "','") + "'"
	var sequenceNames []string
	query := fmt.Sprintf(`SELECT sequence_schema, sequence_name FROM information_schema.sequences where sequence_schema IN (%s);`, querySchemaList)
	rows, err := pg.db.Query(query)
	if err != nil {
		utils.ErrExit("error in querying(%q) source database for sequence names: %v\n", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	var sequenceName, sequenceSchema string
	for rows.Next() {
		err = rows.Scan(&sequenceSchema, &sequenceName)
		if err != nil {
			utils.ErrExit("error in scanning query rows for sequence names: %v\n", err)
		}
		sequenceNames = append(sequenceNames, fmt.Sprintf(`%s."%s"`, sequenceSchema, sequenceName))
	}
	return sequenceNames
}

// GetAllSequencesRaw returns all the sequence names in the database for the schema
func (pg *PostgreSQL) GetAllSequencesRaw(schemaName string) ([]string, error) {
	var sequenceNames []string
	//pg_sequences table is available from PG 10 and consist info of normal sequences and sequences generated by identity columns
	query := fmt.Sprintf(`SELECT sequencename FROM pg_sequences where schemaname = '%s';`, schemaName)
	rows, err := pg.db.Query(query)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			//For PG version before 10 as identity columns are also introduced in PG 10 so using information_schema.sequences should be fine
			query = fmt.Sprintf(`SELECT sequence_name FROM information_schema.sequences where sequence_schema = '%s';`, schemaName)
			rows, err = pg.db.Query(query)
			if err != nil {
				return nil, fmt.Errorf("error in querying(%q) source database for sequence names: %v", query, err)
			}
		} else {
			return nil, fmt.Errorf("error in querying(%q) source database for sequence names: %v", query, err)
		}
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	var sequenceName string
	for rows.Next() {
		err = rows.Scan(&sequenceName)
		if err != nil {
			utils.ErrExit("error in scanning query rows for sequence names: %v", err)
		}
		sequenceNames = append(sequenceNames, sequenceName)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("error in scanning query rows for sequence names: %v", rows.Err())
	}
	return sequenceNames, nil
}

func (pg *PostgreSQL) GetCharset() (string, error) {
	query := fmt.Sprintf("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = '%s';", pg.source.DBName)
	encoding := ""
	err := pg.db.QueryRow(query).Scan(&encoding)
	if err != nil {
		return "", fmt.Errorf("error in querying database encoding: %w", err)
	}
	return encoding, nil
}

func (pg *PostgreSQL) GetDatabaseSize() (int64, error) {
	var totalSchemasSize int64
	schemaList := strings.Replace(pg.source.Schema, "|", "','", -1)
	query := fmt.Sprintf(`SELECT
    nspname AS schema_name,
    SUM(pg_total_relation_size(pg_class.oid)) AS total_size
FROM
    pg_class
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE
    nspname in ('%s')  
GROUP BY
    nspname;`, schemaList)

	rows, err := pg.db.Query(query)
	if err != nil {
		return -1, fmt.Errorf("error in querying(%q) source database for sequence names: %v", query, err)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	var schemaName string
	var totalSize sql.NullInt64
	for rows.Next() {
		err = rows.Scan(&schemaName, &totalSize)
		if err != nil {
			return -1, fmt.Errorf("error in scanning query rows for schemas ('%s'): %v", schemaList, err)
		}
		totalSchemasSize += totalSize.Int64
	}
	if rows.Err() != nil {
		return -1, fmt.Errorf("error in scanning query rows for schemas('%s'): %v", schemaList, rows.Err())
	}
	log.Infof("Total size of all PG sourceDB schemas ('%s'): %d", schemaList, totalSchemasSize)
	return totalSchemasSize, nil
}

func (pg *PostgreSQL) FilterUnsupportedTables(migrationUUID uuid.UUID, tableList []sqlname.NameTuple, useDebezium bool) ([]sqlname.NameTuple, []sqlname.NameTuple) {
	return tableList, nil
}

func (pg *PostgreSQL) FilterEmptyTables(tableList []sqlname.NameTuple) ([]sqlname.NameTuple, []sqlname.NameTuple) {
	var nonEmptyTableList, emptyTableList []sqlname.NameTuple

	for _, tableName := range tableList {
		query := fmt.Sprintf(`SELECT false FROM %s LIMIT 1;`, tableName.ForUserQuery())
		var empty bool
		err := pg.db.QueryRow(query).Scan(&empty)
		if err != nil {
			if err == sql.ErrNoRows {
				empty = true
			} else {
				utils.ErrExit("error in querying table %v: %v", tableName, err)
			}
		}
		if !empty {
			nonEmptyTableList = append(nonEmptyTableList, tableName)
		} else {
			emptyTableList = append(emptyTableList, tableName)
		}
	}
	return nonEmptyTableList, emptyTableList
}

func (pg *PostgreSQL) getTableColumns(tableName sqlname.NameTuple) ([]string, []string, []string, error) {
	var columns, dataTypes, dataTypesOwner []string
	sname, tname := tableName.ForCatalogQuery()
	query := fmt.Sprintf(GET_TABLE_COLUMNS_QUERY_TEMPLATE_PG_AND_YB, tname, sname)
	rows, err := pg.db.Query(query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error in querying(%q) source database for table columns: %w", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	for rows.Next() {
		var column, dataType, dataTypeOwner string
		err = rows.Scan(&column, &dataType, &dataTypeOwner)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error in scanning query(%q) rows for table columns: %w", query, err)
		}
		columns = append(columns, column)
		dataTypes = append(dataTypes, dataType)
		dataTypesOwner = append(dataTypesOwner, dataTypeOwner)
	}
	return columns, dataTypes, dataTypesOwner, nil
}

func (pg *PostgreSQL) GetColumnsWithSupportedTypes(tableList []sqlname.NameTuple, useDebezium bool, isStreamingEnabled bool) (*utils.StructMap[sqlname.NameTuple, []string], *utils.StructMap[sqlname.NameTuple, []string], error) {
	supportedTableColumnsMap := utils.NewStructMap[sqlname.NameTuple, []string]()
	unsupportedTableColumnsMap := utils.NewStructMap[sqlname.NameTuple, []string]()
	for _, tableName := range tableList {
		columns, dataTypes, _, err := pg.getTableColumns(tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error in getting table columns and datatypes: %w", err)
		}
		var unsupportedColumnNames []string
		var supportedColumnNames []string
		for i, column := range columns {
			if useDebezium || isStreamingEnabled {
				//Using this ContainsAnyStringFromSlice as the catalog we use for fetching datatypes uses the data_type only
				// which just contains the base type for example VARCHARs it won't include any length, precision or scale information
				//of these types there are other columns available for these information so we just do string match of types with our list
				//And also for geometry or complex types like if a column is defined with  public.geometry(Point,4326) then also only geometry is available
				//in the typname column of those catalog tables  and further details (Point,4326) is managed by Postgis extension.
				if utils.ContainsAnyStringFromSlice(PostgresUnsupportedDataTypesForDbzm, dataTypes[i]) {
					unsupportedColumnNames = append(unsupportedColumnNames, column)
				} else {
					supportedColumnNames = append(supportedColumnNames, column)
				}
			}
		}
		if len(supportedColumnNames) == len(columns) {
			supportedTableColumnsMap.Put(tableName, []string{"*"})
		} else {
			supportedTableColumnsMap.Put(tableName, supportedColumnNames)
			if len(unsupportedColumnNames) > 0 {
				unsupportedTableColumnsMap.Put(tableName, unsupportedColumnNames)
			}
		}
	}
	return supportedTableColumnsMap, unsupportedTableColumnsMap, nil
}

func (pg *PostgreSQL) ParentTableOfPartition(table sqlname.NameTuple) string {
	var parentTable string
	// For this query in case of case sensitive tables, minquoting is required
	query := fmt.Sprintf(` SELECT i.inhparent::pg_catalog.regclass AS parent_table
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_inherits i ON c.oid = i.inhrelid
		JOIN pg_catalog.pg_class p ON i.inhparent = p.oid
		JOIN pg_catalog.pg_partitioned_table pt ON p.oid = pt.partrelid  -- this clause ensures it filters only partitioned table
		WHERE c.oid = '%s'::regclass::oid;`, table.ForOutput())

	err := pg.db.QueryRow(query).Scan(&parentTable)
	if err != sql.ErrNoRows && err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			//pg_partitioned_table table is available from PG 10 and consist info of partitioned tables
			return ""
		} else {
			log.Errorf("failed to list partitions of table %s: query = [ %s ], error = %s", table, query, err)
			utils.ErrExit("failed to find the partitions for table %s:", table, err)
		}
	}

	return parentTable
}

func (pg *PostgreSQL) GetColumnToSequenceMap(tableList []sqlname.NameTuple) map[string]string {
	columnToSequenceMap := make(map[string]string)
	for _, table := range tableList {
		// query to find out column name vs sequence name for a table
		// this query also covers the case of identity columns
		sname, tname := table.ForCatalogQuery()
		query := fmt.Sprintf(FETCH_COLUMN_SEQUENCES_QUERY_TEMPLATE, sname, tname)

		var columeName, sequenceName, schemaName string
		rows, err := pg.db.Query(query)
		if err != nil {
			log.Infof("Query to find column to sequence mapping: %s", query)
			utils.ErrExit("Error in querying for sequences in table=%s: %v", table, err)
		}
		defer func() {
			closeErr := rows.Close()
			if closeErr != nil {
				log.Warnf("close rows for table %s query %q: %v", table.String(), query, closeErr)
			}
		}()
		for rows.Next() {
			err := rows.Scan(&columeName, &sequenceName, &schemaName)
			if err != nil {
				utils.ErrExit("Error in scanning for sequences in table=%s: %v", table, err)
			}
			qualifiedColumnName := fmt.Sprintf("%s.%s", table.AsQualifiedCatalogName(), columeName)
			// quoting sequence name as it can be case sensitive - required during import data restore sequences
			columnToSequenceMap[qualifiedColumnName] = fmt.Sprintf(`%s."%s"`, schemaName, sequenceName)
		}
		err = rows.Close()
		if err != nil {
			utils.ErrExit("close rows for table %s query %q: %s", table.String(), query, err)
		}
	}

	return columnToSequenceMap
}

func generateSSLQueryStringIfNotExists(s *Source) string {

	if s.Uri == "" {
		SSLQueryString := ""
		if s.SSLQueryString == "" {

			if s.SSLMode == "disable" || s.SSLMode == "allow" || s.SSLMode == "prefer" || s.SSLMode == "require" || s.SSLMode == "verify-ca" || s.SSLMode == "verify-full" {
				SSLQueryString = "sslmode=" + s.SSLMode
				if s.SSLMode == "require" || s.SSLMode == "verify-ca" || s.SSLMode == "verify-full" {
					SSLQueryString = fmt.Sprintf("sslmode=%s", s.SSLMode)
					if s.SSLCertPath != "" {
						SSLQueryString += "&sslcert=" + s.SSLCertPath
					}
					if s.SSLKey != "" {
						SSLQueryString += "&sslkey=" + s.SSLKey
					}
					if s.SSLRootCert != "" {
						SSLQueryString += "&sslrootcert=" + s.SSLRootCert
					}
					if s.SSLCRL != "" {
						SSLQueryString += "&sslcrl=" + s.SSLCRL
					}
				}
			} else {
				utils.ErrExit("Invalid sslmode: %q", s.SSLMode)
			}
		} else {
			SSLQueryString = s.SSLQueryString
		}
		return SSLQueryString
	} else {
		return ""
	}
}

func (pg *PostgreSQL) GetServers() []string {
	return []string{pg.source.Host}
}

func (pg *PostgreSQL) GetPartitions(tableName sqlname.NameTuple) []string {
	partitions := make([]string, 0)
	sname, tname := tableName.ForCatalogQuery()
	query := fmt.Sprintf(`SELECT
    nmsp_child.nspname  AS child_schema,
    child.relname       AS child_table
	FROM pg_inherits
		JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
		JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
		JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
		JOIN pg_partitioned_table pt    ON parent.oid = pt.partrelid  -- this clause ensures it filters only partitioned table
	WHERE parent.relname = '%s' 
	AND nmsp_parent.nspname = '%s';`, tname, sname)

	rows, err := pg.db.Query(query)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			//pg_partitioned_table table is available from PG 10 and consist info of partitioned tables
			return partitions
		} else {
			log.Errorf("failed to list partitions of table %s: query = [ %s ], error = %s", tableName, query, err)
			utils.ErrExit("failed to find the partitions for table %s:", tableName, err)
		}
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	for rows.Next() {
		var childSchema, childTable string
		err := rows.Scan(&childSchema, &childTable)
		if err != nil {
			utils.ErrExit("Error in scanning for child partitions of table=%s: %v", tableName, err)
		}
		partitions = append(partitions, fmt.Sprintf(`%s.%s`, childSchema, childTable))
	}
	if rows.Err() != nil {
		utils.ErrExit("Error in scanning for child partitions of table=%s: %v", tableName, rows.Err())
	}
	return partitions
}

func (pg *PostgreSQL) GetTableToUniqueKeyColumnsMap(tableList []sqlname.NameTuple) (map[string][]string, error) {
	log.Infof("getting unique key columns for tables: %v", tableList)
	result := make(map[string][]string)
	var querySchemaList, queryTableList []string
	for i := 0; i < len(tableList); i++ {
		sname, tname := tableList[i].ForCatalogQuery()
		querySchemaList = append(querySchemaList, sname)
		queryTableList = append(queryTableList, tname)
	}

	querySchemaList = lo.Uniq(querySchemaList)
	query := fmt.Sprintf(ybQueryTmplForUniqCols, strings.Join(querySchemaList, ","), strings.Join(queryTableList, ","),
		strings.Join(querySchemaList, ","), strings.Join(queryTableList, ","))
	log.Infof("query to get unique key columns: %s", query)
	rows, err := pg.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying unique key columns: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()

	for rows.Next() {
		var schemaName, tableName, colName string
		err := rows.Scan(&schemaName, &tableName, &colName)
		if err != nil {
			return nil, fmt.Errorf("scanning row for unique key column name: %w", err)
		}
		if schemaName != "public" {
			tableName = fmt.Sprintf("%s.%s", schemaName, tableName)
		}
		result[tableName] = append(result[tableName], colName)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating over rows for unique key columns: %w", err)
	}
	log.Infof("unique key columns for tables: %v", result)
	return result, nil
}

func (pg *PostgreSQL) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("ClearMigrationState not implemented yet for PostgreSQL")
	return nil
}

func (pg *PostgreSQL) GetReplicationConnection() (*pgconn.PgConn, error) {
	return pgconn.Connect(context.Background(), pg.getConnectionUri()+"&replication=database")
}

func (pg *PostgreSQL) CreateLogicalReplicationSlot(conn *pgconn.PgConn, replicationSlotName string, dropIfAlreadyExists bool) (*pglogrepl.CreateReplicationSlotResult, error) {
	if dropIfAlreadyExists {
		log.Infof("dropping replication slot %s if already exists", replicationSlotName)
		err := pg.DropLogicalReplicationSlot(conn, replicationSlotName)
		if err != nil {
			return nil, err
		}
	}

	log.Infof("creating replication slot %s", replicationSlotName)
	res, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, replicationSlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		return nil, fmt.Errorf("create replication slot: %v", err)
	}

	return &res, nil
}

func (pg *PostgreSQL) DropLogicalReplicationSlot(conn *pgconn.PgConn, replicationSlotName string) error {
	var err error
	if conn == nil {
		conn, err = pg.GetReplicationConnection()
		if err != nil {
			utils.ErrExit("failed to create replication connection for dropping replication slot: %s", err)
		}
		defer conn.Close(context.Background())
	}
	log.Infof("dropping replication slot: %s", replicationSlotName)
	err = pglogrepl.DropReplicationSlot(context.Background(), conn, replicationSlotName, pglogrepl.DropReplicationSlotOptions{})
	if err != nil {
		// ignore "does not exist" error while dropping replication slot
		if !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("delete existing replication slot(%s): %v", replicationSlotName, err)
		}
	}
	return nil
}

func (pg *PostgreSQL) CreatePublication(conn *pgconn.PgConn, publicationName string, tableList []sqlname.NameTuple, dropIfAlreadyExists bool, leafPartitions *utils.StructMap[sqlname.NameTuple, []string]) error {
	if dropIfAlreadyExists {
		err := pg.DropPublication(publicationName)
		if err != nil {
			return fmt.Errorf("drop publication: %v", err)
		}
	}
	tablelistQualifiedQuoted := []string{}
	for _, tableName := range tableList {
		_, ok := leafPartitions.Get(tableName)
		if ok {
			//In case of partiitons, tablelist in CREATE PUBLICATION query should not have root
			continue
		}
		tablelistQualifiedQuoted = append(tablelistQualifiedQuoted, tableName.ForKey())
	}
	stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", publicationName, strings.Join(tablelistQualifiedQuoted, ","))
	result := conn.Exec(context.Background(), stmt)
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("create publication with stmt %s: %v", err, stmt)
	}
	log.Infof("created publication with stmt %s", stmt)
	return nil
}

func (pg *PostgreSQL) DropPublication(publicationName string) error {
	log.Infof("dropping publication: %s", publicationName)
	res, err := pg.db.Exec(fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	log.Infof("drop publication result: %v", res)
	if err != nil {
		return fmt.Errorf("drop publication(%s): %v", publicationName, err)
	}
	return nil
}

var PG_QUERY_TO_CHECK_IF_TABLE_HAS_PK = `SELECT nspname AS schema_name, relname AS table_name, COUNT(conname) AS pk_count
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_constraint con ON con.conrelid = c.oid AND con.contype = 'p'
GROUP BY schema_name, table_name HAVING nspname IN (%s);`

func (pg *PostgreSQL) GetNonPKTables() ([]string, error) {
	var nonPKTables []string
	schemaList := strings.Split(pg.source.Schema, "|")
	querySchemaList := "'" + strings.Join(schemaList, "','") + "'"
	query := fmt.Sprintf(PG_QUERY_TO_CHECK_IF_TABLE_HAS_PK, querySchemaList)
	rows, err := pg.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying(%q) source database for primary key: %v", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	for rows.Next() {
		var schemaName, tableName string
		var pkCount int
		err := rows.Scan(&schemaName, &tableName, &pkCount)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for primary key: %v", err)
		}
		table := sqlname.NewSourceName(schemaName, fmt.Sprintf(`"%s"`, tableName))
		if pkCount == 0 {
			nonPKTables = append(nonPKTables, table.Qualified.Quoted)
		}
	}
	return nonPKTables, nil
}

func (pg *PostgreSQL) ValidateTablesReadyForLiveMigration(tableList []sqlname.NameTuple) error {
	var tablesWithReplicaIdentityNotFull []string
	var qualifiedTableNames []string
	for _, table := range tableList {
		sname, tname := table.ForCatalogQuery()
		qualifiedTableNames = append(qualifiedTableNames, fmt.Sprintf("'%s.%s'", sname, tname))
	}
	query := fmt.Sprintf(`SELECT n.nspname || '.' || c.relname AS table_name_with_schema
    FROM pg_class AS c
    JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE (n.nspname || '.' || c.relname) IN (%s)
    AND c.relkind = 'r'
    AND c.relreplident <> 'f';`, strings.Join(qualifiedTableNames, ","))
	rows, err := pg.db.Query(query)
	if err != nil {
		return fmt.Errorf("error in querying(%q) source database for replica identity: %v", query, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	for rows.Next() {
		var tableWithSchema string
		err := rows.Scan(&tableWithSchema)
		if err != nil {
			return fmt.Errorf("error in scanning query rows for replica identity: %v", err)
		}
		tablesWithReplicaIdentityNotFull = append(tablesWithReplicaIdentityNotFull, tableWithSchema)
	}
	if len(tablesWithReplicaIdentityNotFull) > 0 {
		return fmt.Errorf("tables %v do not have REPLICA IDENTITY FULL\nPlease ALTER the tables and set their REPLICA IDENTITY to FULL", tablesWithReplicaIdentityNotFull)
	}
	return nil
}

// =============================== Guardrails ===============================

func (pg *PostgreSQL) CheckSourceDBVersion() error {
	utils.PrintAndLog("checking postgres version")
	pgVersion := pg.GetVersion()
	if pgVersion == "" {
		return fmt.Errorf("failed to get source database version")
	}
	supportedVersionRange := fmt.Sprintf("from %s to %s", MIN_SUPPORTED_PG_VERSION_OFFLINE, MAX_SUPPORTED_PG_VERSION)

	if version.CompareSimple(pgVersion, MAX_SUPPORTED_PG_VERSION) > 0 || version.CompareSimple(pgVersion, MIN_SUPPORTED_PG_VERSION_OFFLINE) < 0 {
		return fmt.Errorf("source database version %s is not supported. Supported versions are %s", pgVersion, supportedVersionRange)
	}
	if version.CompareSimple(pgVersion, MIN_SUPPORTED_PG_VERSION_LIVE) < 0 {
		utils.PrintAndLog(color.RedString("Warning: Source database version %s is not supported for live migration. Supported versions are %s", pgVersion, supportedVersionRange))
	}

	return nil
}

/*
GetMissingExportSchemaPermissions checks for missing permissions required for exporting schema in a PostgreSQL database.
It verifies if schemas have USAGE permission
and if tables in the provided schemas + pg_catalog + information_schema have SELECT permission.
Returns:
  - []string: A slice of strings describing the missing permissions, if any.
  - error: An error if any issues occur during the permission checks.
*/
func (pg *PostgreSQL) GetMissingExportSchemaPermissions() ([]string, error) {
	var combinedResult []string

	// Check if schemas have USAGE permission
	missingSchemas, err := pg.listSchemasMissingUsagePermission()
	if err != nil {
		return nil, fmt.Errorf("error checking schema usage permissions: %w", err)
	}
	if len(missingSchemas) > 0 {
		combinedResult = append(combinedResult, fmt.Sprintf("Schemas missing USAGE permission: %s", missingSchemas))
	}

	// Check if tables have SELECT permission
	missingTables, tablesWithNoUsagePerm, err := pg.listTablesMissingSelectPermission()
	if err != nil {
		return nil, fmt.Errorf("error checking table select permissions: %w", err)
	}
	if len(missingTables) > 0 {
		combinedResult = append(combinedResult, fmt.Sprintf("Tables missing SELECT permission: %v", missingTables))
	}
	if len(tablesWithNoUsagePerm) > 0 {
		// Print that some tables could not be checked for SELECT permission as the parent schema does not have USAGE permission
		combinedResult = append(combinedResult, fmt.Sprintf("Following tables could not be checked for SELECT permission as the parent schema does not have USAGE permission: %v", tablesWithNoUsagePerm))
	}

	// Return combined result of checks if any issues, else return nothing (empty string and nil)
	return combinedResult, nil

}

/*
GetMissingExportDataPermissions checks for missing permissions required for exporting data from PostgreSQL.
It verifies various permissions based on the export type (offline or live migration).

Parameters:
  - exportType: A string indicating the type of export. It can be one of the following:
  - utils.SNAPSHOT_ONLY: For offline migration.
  - utils.CHANGES_ONLY: For live migration with changes only.
  - utils.SNAPSHOT_AND_CHANGES: For live migration with snapshot and changes.

Returns:
  - []string: A slice of strings describing the missing permissions or issues found.
  - error: An error object if any error occurs during the permission checks.

The function performs the following checks:
  - For offline migration:
  - Checks if provided schemas + pg_catalog + information_schema have USAGE permission and if tables in the provided schemas + pg_catalog + information_schema have SELECT permission.
  - Checks if sequences have SELECT permission.
  - For live migration:
  - Checks if wal_level is set to logical.
  - Checks if tables have replica identity set to FULL.
  - Checks if the user has replication permission.
  - Checks if the user has create permission on the database.
  - Checks if the user has ownership over all tables.
*/
func (pg *PostgreSQL) GetMissingExportDataPermissions(exportType string) ([]string, error) {
	var combinedResult []string

	// For offline migration
	// Check if schemas have USAGE permission and check if tables in the provided schemas have SELECT permission
	res, err := pg.GetMissingExportSchemaPermissions()
	if err != nil {
		return nil, fmt.Errorf("error in getting missing export data permissions: %w", err)
	}
	combinedResult = append(combinedResult, res...)

	// Check if sequences have SELECT permission
	sequencesWithMissingPerm, sequencesWithNoUsagePerm, err := pg.listSequencesMissingSelectPermission()
	if err != nil {
		return nil, fmt.Errorf("error in checking sequence select permissions: %w", err)
	}
	if len(sequencesWithMissingPerm) > 0 {
		combinedResult = append(combinedResult, fmt.Sprintf("Sequences missing SELECT permission: %v", sequencesWithMissingPerm))
	}
	if len(sequencesWithNoUsagePerm) > 0 {
		combinedResult = append(combinedResult, fmt.Sprintf("Following sequences could not be checked for SELECT permission as the parent schema does not have USAGE permission: %v", sequencesWithNoUsagePerm))
	}

	// For live migration
	if exportType == utils.CHANGES_ONLY || exportType == utils.SNAPSHOT_AND_CHANGES {
		// Check wal_level is set to logical
		correctlySet := pg.checkWalLevel()
		if !correctlySet {
			combinedResult = append(combinedResult, "wal_level is not set to logical")
		}

		// Check replica identity of tables
		missingTables, err := pg.listTablesMissingReplicaIdentityFull()
		if err != nil {
			return nil, fmt.Errorf("error in checking table replica identity: %w", err)
		}
		if len(missingTables) > 0 {
			combinedResult = append(combinedResult, fmt.Sprintf("Tables missing replica identity: %v", missingTables))
		}

		// Check Replication permission for user
		hasReplicationPermission, err := pg.checkReplicationPermission()
		if err != nil {
			return nil, fmt.Errorf("error in checking replication permission: %w", err)
		}
		if !hasReplicationPermission {
			combinedResult = append(combinedResult, "User does not have replication permission")
		}

		// Check user has create permission on db
		hasCreatePermission, err := pg.checkCreatePermissionOnDB()
		if err != nil {
			return nil, fmt.Errorf("error in checking create permission: %w", err)
		}
		if !hasCreatePermission {
			combinedResult = append(combinedResult, "User does not have create permission on database")
		}

		// Check if user has ownership over all tables
		missingTables, err = pg.listTablesMissingOwnerPermission()
		if err != nil {
			return nil, fmt.Errorf("error in checking table owner permissions: %w", err)
		}
		if len(missingTables) > 0 {
			combinedResult = append(combinedResult, fmt.Sprintf("Tables over which user does not have ownership: %v", missingTables))
		}
	}

	return combinedResult, nil
}

func (pg *PostgreSQL) listTablesMissingOwnerPermission() ([]string, error) {
	trimmedSchemaList := pg.getTrimmedSchemaList()
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"

	checkTableOwnerPermissionQuery := fmt.Sprintf(`
	WITH table_ownership AS (
		SELECT
			n.nspname AS schema_name,
			c.relname AS table_name,
			pg_get_userbyid(c.relowner) AS owner_name
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relkind IN ('r', 'p') -- 'r' indicates a table 'p' indicates a partitioned table
		AND n.nspname IN (%s)
	)
	SELECT
		schema_name,
		table_name,
		CASE
			WHEN owner_name = '%s' THEN true
			WHEN EXISTS (
				SELECT 1
				FROM pg_roles r
				JOIN pg_auth_members am ON r.oid = am.roleid
				JOIN pg_roles ur ON am.member = ur.oid
				WHERE r.rolname = owner_name
				  AND ur.rolname = '%s'
			) THEN true
			ELSE false
		END AS has_ownership
	FROM table_ownership;`, querySchemaList, pg.source.User, pg.source.User)

	rows, err := pg.db.Query(checkTableOwnerPermissionQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying source database for checking table owner permission: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("error closing rows for query %q: %v", checkTableOwnerPermissionQuery, closeErr)
		}
	}()

	var missingTables []string
	var tableSchemaName, tableName string
	var hasOwnership bool

	for rows.Next() {
		err = rows.Scan(&tableSchemaName, &tableName, &hasOwnership)
		if err != nil {
			return nil, fmt.Errorf("error scanning query rows for table names: %w", err)
		}
		if !hasOwnership {
			missingTables = append(missingTables, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
		}
	}

	// Check for errors during row iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over query rows: %w", err)
	}

	return missingTables, nil
}

func (pg *PostgreSQL) checkCreatePermissionOnDB() (bool, error) {
	query := `SELECT
	EXISTS (
		SELECT 1
		FROM pg_database
		WHERE datname = current_database()
		  AND has_database_privilege($1, datname, 'CREATE')
	) AS has_create_permission;`
	var hasCreatePermission bool
	err := pg.db.QueryRow(query, pg.source.User).Scan(&hasCreatePermission)
	if err != nil {
		return false, fmt.Errorf("error in checking create permission: %w", err)
	}
	return hasCreatePermission, nil
}

func (pg *PostgreSQL) checkReplicationPermission() (bool, error) {
	query := `
	WITH instance_check AS (
		SELECT
			CASE
				WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_superuser')
				THEN 'rds'
				ELSE 'standalone'
			END AS db_instance_type
	)
	SELECT
		CASE
			WHEN db_instance_type = 'rds' THEN
				EXISTS (
					SELECT 1
					FROM pg_roles
					WHERE rolname = $1
					  AND pg_has_role($1, 'rds_replication', 'USAGE')
				)
			ELSE
				EXISTS (
					SELECT 1
					FROM pg_roles
					WHERE rolname = $1
					  AND rolreplication
				)
		END AS has_permission
	FROM instance_check;`

	var hasPermission bool
	err := pg.db.QueryRow(query, pg.source.User).Scan(&hasPermission)
	if err != nil {
		return false, fmt.Errorf("error in checking replication permission: %w", err)
	}
	return hasPermission, nil
}

func (pg *PostgreSQL) listTablesMissingReplicaIdentityFull() ([]string, error) {
	trimmedSchemaList := pg.getTrimmedSchemaList()
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"
	checkTableReplicaIdentityQuery := fmt.Sprintf(`SELECT
	n.nspname AS schema_name,
	c.relname AS table_name,
	c.relreplident AS replica_identity,
	CASE 
		WHEN c.relreplident <> 'f' 
		THEN '%s' 
		ELSE '%s' 
	END AS status
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE quote_ident(n.nspname) IN (%s)
	AND c.relkind IN ('r', 'p');`, MISSING, GRANTED, querySchemaList)
	rows, err := pg.db.Query(checkTableReplicaIdentityQuery)
	if err != nil {
		return nil, fmt.Errorf("error in querying(%q) source database for checking table replica identity: %w", checkTableReplicaIdentityQuery, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", checkTableReplicaIdentityQuery, closeErr)
		}
	}()

	var missingTables []string
	var tableSchemaName, tableName, replicaIdentity, status string

	for rows.Next() {
		err = rows.Scan(&tableSchemaName, &tableName, &replicaIdentity, &status)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for table names: %w", err)
		}
		if status == MISSING {
			missingTables = append(missingTables, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
		}
	}

	// Check for errors during row iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over query rows: %w", err)
	}

	return missingTables, nil
}

func (pg *PostgreSQL) checkWalLevel() bool {
	query := `SELECT
	CASE WHEN current_setting('wal_level') = 'logical'
		THEN true
		ELSE false
	END AS wal_level_status_correct;`
	var walLevelStatusCorrect bool
	err := pg.db.QueryRow(query).Scan(&walLevelStatusCorrect)
	if err != nil {
		utils.ErrExit("error in querying(%q) source database for checking wal_level: %v\n", query, err)
	}
	return walLevelStatusCorrect
}

func (pg *PostgreSQL) listSequencesMissingSelectPermission() (sequencesWithMissingPerm []string, sequencesWithNoUsagePerm []string, err error) {
	trimmedSchemaList := pg.getTrimmedSchemaList()
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"

	checkSequenceSelectPermissionQuery := fmt.Sprintf(`
	WITH schema_permissions AS (
		SELECT
			n.nspname AS schema_name,
			CASE
				WHEN has_schema_privilege('%s', quote_ident(n.nspname), 'USAGE') THEN '%s'
				ELSE '%s'
			END AS usage_status
		FROM pg_namespace n
		WHERE n.nspname IN (%s)
	),
	sequence_permissions AS (
		SELECT
			n.nspname AS schema_name,
			c.relname AS sequence_name,
			CASE
				WHEN sp.usage_status = '%s' THEN
					CASE
						WHEN has_sequence_privilege('%s', quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT') THEN '%s'
						ELSE '%s'
					END
				ELSE '%s'
			END AS select_status
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN schema_permissions sp ON n.nspname = sp.schema_name
		WHERE c.relkind = 'S'  -- 'S' indicates a sequence
	)
	SELECT
		schema_name,
		sequence_name,
		select_status
	FROM sequence_permissions
	ORDER BY schema_name, sequence_name;
`, pg.source.User, GRANTED, MISSING, querySchemaList, GRANTED, pg.source.User, GRANTED, MISSING, NO_USAGE_PERMISSION)
	rows, err := pg.db.Query(checkSequenceSelectPermissionQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("error in querying(%q) source database for checking sequence select permission: %w", checkSequenceSelectPermissionQuery, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", checkSequenceSelectPermissionQuery, closeErr)
		}
	}()

	var sequenceSchemaName, sequenceName, selectStatus string
	for rows.Next() {
		err = rows.Scan(&sequenceSchemaName, &sequenceName, &selectStatus)
		if err != nil {
			return nil, nil, fmt.Errorf("error in scanning query rows for sequence names: %w", err)
		}
		if selectStatus == MISSING {
			sequencesWithMissingPerm = append(sequencesWithMissingPerm, fmt.Sprintf("%s.%s", sequenceSchemaName, sequenceName))
		} else if selectStatus == NO_USAGE_PERMISSION {
			sequencesWithNoUsagePerm = append(sequencesWithNoUsagePerm, fmt.Sprintf("%s.%s", sequenceSchemaName, sequenceName))
		}
	}

	// Check for errors during row iteration
	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating over query rows: %w", err)
	}

	return sequencesWithMissingPerm, sequencesWithNoUsagePerm, nil
}

func (pg *PostgreSQL) listTablesMissingSelectPermission() (tablesWithMissingPerm []string, tablesWithNoUsagePerm []string, err error) {
	// Users only need SELECT permissions on the tables of the schema they want to export for export schema
	trimmedSchemaList := pg.getTrimmedSchemaList()
	trimmedSchemaList = append(trimmedSchemaList, "pg_catalog", "information_schema")
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"

	checkTableSelectPermissionQuery := fmt.Sprintf(`
	WITH schema_list AS (
		SELECT unnest(ARRAY[%s]) AS schema_name
	),
	accessible_schemas AS (
		SELECT schema_name
		FROM schema_list
		WHERE has_schema_privilege('%s', quote_ident(schema_name), 'USAGE')
	)
	SELECT
		t.schemaname AS schema_name,
		t.tablename AS table_name,
		CASE 
			WHEN has_table_privilege('%s', quote_ident(t.schemaname) || '.' || quote_ident(t.tablename), 'SELECT') 
			THEN '%s' 
			ELSE '%s' 
		END AS status
	FROM pg_tables t
	JOIN accessible_schemas a ON t.schemaname = a.schema_name
	UNION ALL
	SELECT
		t.schemaname AS schema_name,
		t.tablename AS table_name,
		'%s' AS status
	FROM pg_tables t
	WHERE t.schemaname IN (SELECT schema_name FROM schema_list)
	AND NOT EXISTS (
		SELECT 1
		FROM accessible_schemas a
		WHERE t.schemaname = a.schema_name
	);`, querySchemaList, pg.source.User, pg.source.User, GRANTED, MISSING, NO_USAGE_PERMISSION)
	rows, err := pg.db.Query(checkTableSelectPermissionQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("error in querying(%q) source database for checking table select permission: %w", checkTableSelectPermissionQuery, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", checkTableSelectPermissionQuery, closeErr)
		}
	}()

	// If result is No Usage Permission On The Table Parent Schema, then the schema itself doesn't have USAGE permission store them in tablesWithNoUsagePerm
	var tableSchemaName, tableName, status string
	for rows.Next() {
		err = rows.Scan(&tableSchemaName, &tableName, &status)
		if err != nil {
			return nil, nil, fmt.Errorf("error in scanning query rows for table names: %w", err)
		}
		if status == MISSING {
			if tableSchemaName == "pg_catalog" || tableSchemaName == "information_schema" {
				// If table name is in pg_catalog_tables_required or information_schema_tables_required and missing SELECT permission, then add to tablesWithMissingPerm
				if slices.Contains(pg_catalog_tables_required, tableName) || slices.Contains(information_schema_tables_required, tableName) {
					tablesWithMissingPerm = append(tablesWithMissingPerm, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
				}
			} else {
				tablesWithMissingPerm = append(tablesWithMissingPerm, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
			}
		} else if status == NO_USAGE_PERMISSION {
			if tableSchemaName == "pg_catalog" || tableSchemaName == "information_schema" {
				// If table name is in pg_catalog_tables_required or information_schema_tables_required and missing USAGE permission, then add to tablesWithNoUsagePerm
				if slices.Contains(pg_catalog_tables_required, tableName) || slices.Contains(information_schema_tables_required, tableName) {
					tablesWithNoUsagePerm = append(tablesWithNoUsagePerm, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
				}
			} else {
				// If table name is not in pg_catalog_tables_required or information_schema_tables_required and missing USAGE permission, then add to tablesWithMissingPerm
				tablesWithNoUsagePerm = append(tablesWithNoUsagePerm, fmt.Sprintf("%s.%s", tableSchemaName, tableName))
			}
		}
	}

	// Check for errors during row iteration
	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating over query rows: %w", err)
	}

	return tablesWithMissingPerm, tablesWithNoUsagePerm, nil
}

func (pg *PostgreSQL) listSchemasMissingUsagePermission() ([]string, error) {
	// Users need usage permissions on the schemas they want to export and the pg_catalog and information_schema schemas
	trimmedSchemaList := pg.getTrimmedSchemaList()
	trimmedSchemaList = append(trimmedSchemaList, "pg_catalog", "information_schema")
	querySchemaList := "'" + strings.Join(trimmedSchemaList, "','") + "'"
	chkSchemaUsagePermissionQuery := fmt.Sprintf(`
	SELECT 
		quote_ident(nspname) AS schema_name,
		CASE 
			WHEN has_schema_privilege('%s', quote_ident(nspname), 'USAGE') THEN '%s' 
			ELSE '%s' 
		END AS usage_permission_status
	FROM 
		pg_namespace
	WHERE 
		quote_ident(nspname) IN (%s);
	`, pg.source.User, GRANTED, MISSING, querySchemaList)
	// Currently we don't support case sensitive schema names but in the future we might and hence using quote_ident to handle that case

	rows, err := pg.db.Query(chkSchemaUsagePermissionQuery)
	if err != nil {
		return nil, fmt.Errorf("error in querying(%q) source database for checking schema usage permission: %w", chkSchemaUsagePermissionQuery, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", chkSchemaUsagePermissionQuery, closeErr)
		}
	}()
	var schemasMissingUsagePermission []string
	var schemaName, usagePermissionStatus string

	for rows.Next() {
		err = rows.Scan(&schemaName, &usagePermissionStatus)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for schema names: %w", err)
		}
		if usagePermissionStatus == MISSING {
			schemasMissingUsagePermission = append(schemasMissingUsagePermission, schemaName)
		}
	}

	// Check for errors during row iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over query rows: %w", err)
	}

	return schemasMissingUsagePermission, nil
}
