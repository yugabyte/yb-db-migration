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

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mcuadros/go-version"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var PostgresUnsupportedDataTypesForDbzm = []string{"POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "GEOMETRY", "GEOGRAPHY", "RASTER", "PG_LSN", "TXID_SNAPSHOT"}

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

const GET_TABLE_COLUMNS_QUERY_TEMPLATE_PG_AND_YB = `SELECT a.attname AS column_name, t.typname::regtype AS data_type, rol.rolname AS data_type_owner 
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
	var version string
	query := "SELECT setting from pg_settings where name = 'server_version'"
	err := pg.db.QueryRow(query).Scan(&version)
	if err != nil {
		utils.ErrExit("run query %q on source: %s", query, err)
	}
	pg.source.DBVersion = version
	return version
}

func (pg *PostgreSQL) checkSchemasExists() []string {
	list := strings.Split(pg.source.Schema, "|")
	var trimmedList []string
	for _, schema := range list {
		if utils.IsQuotedString(schema) {
			schema = strings.Trim(schema, `"`)
		}
		trimmedList = append(trimmedList, schema)
	}
	querySchemaList := "'" + strings.Join(trimmedList, "','") + "'"
	chkSchemaExistsQuery := fmt.Sprintf(`SELECT schema_name
	FROM information_schema.schemata where schema_name IN (%s);`, querySchemaList)
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

	schemaNotPresent := utils.SetDifference(trimmedList, listOfSchemaPresent)
	if len(schemaNotPresent) > 0 {
		utils.ErrExit("Following schemas are not present in source database %v, please provide a valid schema list.\n", schemaNotPresent)
	}
	return trimmedList
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
func GetAbsPathOfPGCommand(cmd string) (string, error) {
	paths, err := findAllExecutablesInPath(cmd)
	if err != nil {
		err = fmt.Errorf("error in finding executables: %w", err)
		return "", err
	}
	if len(paths) == 0 {
		err = fmt.Errorf("the command %v is not installed", cmd)
		return "", err
	}

	for _, path := range paths {
		checkVersiomCmd := exec.Command(path, "--version")
		stdout, err := checkVersiomCmd.Output()
		if err != nil {
			err = fmt.Errorf("error in finding version of %v from path %v: %w", checkVersiomCmd, path, err)
			return "", err
		}

		// example output centos: pg_restore (PostgreSQL) 14.5
		// example output Ubuntu: pg_dump (PostgreSQL) 14.5 (Ubuntu 14.5-1.pgdg22.04+1)
		currVersion := strings.Fields(string(stdout))[2]

		if version.CompareSimple(currVersion, PG_COMMAND_VERSION[cmd]) >= 0 {
			return path, nil
		}
	}

	err = fmt.Errorf("could not find %v with version greater than or equal to %v", cmd, PG_COMMAND_VERSION)
	return "", err
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
	query := fmt.Sprintf(`SELECT sequencename FROM pg_sequences where schemaname = '%s';`, schemaName)
	rows, err := pg.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying(%q) source database for sequence names: %v", query, err)
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
	var totalSchemasSize, totalSize int64
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
	for rows.Next() {
		err = rows.Scan(&schemaName, &totalSize)
		if err != nil {
			return -1, fmt.Errorf("error in scanning query rows for schemas ('%s'): %v", schemaList, err)
		}
		totalSchemasSize += totalSize
	}
	if rows.Err() != nil {
		return -1, fmt.Errorf("error in scanning query rows for schemas('%s'): %v", schemaList, rows.Err())
	}
	log.Infof("Total size of all PG sourceDB schemas ('%s'): %v", schemaList, totalSchemasSize)
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
	query := fmt.Sprintf(GET_TABLE_COLUMNS_QUERY_TEMPLATE_PG_AND_YB, sname, tname)
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
				if utils.ContainsAnySubstringFromSlice(PostgresUnsupportedDataTypesForDbzm, dataTypes[i]) {
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
	query := fmt.Sprintf(`SELECT inhparent::pg_catalog.regclass
	FROM pg_catalog.pg_class c JOIN pg_catalog.pg_inherits ON c.oid = inhrelid
	WHERE c.oid = '%s'::regclass::oid`, table.ForOutput())

	err := pg.db.QueryRow(query).Scan(&parentTable)
	if err != sql.ErrNoRows && err != nil {
		utils.ErrExit("Error in query=%s for parent tablename of table=%s: %v", query, table, err)
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
    child.relname       AS child
FROM pg_inherits
    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE parent.relname='%s' AND nmsp_parent.nspname = '%s' `, tname, sname)

	rows, err := pg.db.Query(query)
	if err != nil {
		log.Errorf("failed to list partitions of table %s: query = [ %s ], error = %s", tableName, query, err)
		utils.ErrExit("failed to find the partitions for table %s:", tableName, err)
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
	query := fmt.Sprintf(ybQueryTmplForUniqCols, strings.Join(querySchemaList, ","), strings.Join(queryTableList, ","))
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
