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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type MySQL struct {
	source *Source

	db *sql.DB
}

var mysqlUnsupportedDataTypes = []string{"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"}

func newMySQL(s *Source) *MySQL {
	return &MySQL{source: s}
}

func (ms *MySQL) Connect() error {
	db, err := sql.Open("mysql", ms.getConnectionUri())
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ms.db = db
	return err
}

func (ms *MySQL) Disconnect() {
	if ms.db == nil {
		log.Infof("No connection to the source database to close")
		return
	}

	err := ms.db.Close()
	if err != nil {
		log.Infof("Failed to close connection to the source database: %s", err)
	}
}

func (ms *MySQL) CheckRequiredToolsAreInstalled() {
	checkTools("ora2pg")
}

func (ms *MySQL) GetTableRowCount(tableName sqlname.NameTuple) int64 {
	var rowCount int64
	query := fmt.Sprintf("select count(*) from %s", tableName.AsQualifiedCatalogName())

	log.Infof("Querying row count of table %s", tableName)
	err := ms.db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for row count of %q: %s", query, tableName, err)
	}
	log.Infof("Table %q has %v rows.", tableName, rowCount)
	return rowCount
}

func (ms *MySQL) GetTableApproxRowCount(tableName sqlname.NameTuple) int64 {
	var approxRowCount sql.NullInt64 // handles case: value of the row is null, default for int64 is 0
	sname, tname := tableName.ForCatalogQuery()
	query := fmt.Sprintf("SELECT table_rows from information_schema.tables "+
		"where table_name = '%s' and table_schema = '%s'",
		tname, sname)

	log.Infof("Querying '%s' approx row count of table %q", query, tableName.String())
	err := ms.db.QueryRow(query).Scan(&approxRowCount)
	if err != nil {
		utils.ErrExit("Failed to query %q for approx row count of %q: %s", query, tableName.String(), err)
	}

	log.Infof("Table %q has approx %v rows.", tableName.String(), approxRowCount)
	return approxRowCount.Int64
}

func (ms *MySQL) GetVersion() string {
	var version string
	query := "SELECT VERSION()"
	err := ms.db.QueryRow(query).Scan(&version)
	if err != nil {
		utils.ErrExit("run query %q on source: %s", query, err)
	}
	ms.source.DBVersion = version
	return version
}

func (ms *MySQL) GetAllTableNamesRaw(schemaName string) ([]string, error) {
	var tableNames []string
	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables "+
		"WHERE table_schema = '%s' && table_type = 'BASE TABLE'", schemaName)
	log.Infof(`query used to GetAllTableNamesRaw(): "%s"`, query)

	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error in querying source database for table names: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error in scanning query rows for table names: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}
	log.Infof("GetAllTableNamesRaw(): %s", tableNames)
	return tableNames, nil

}

func (ms *MySQL) GetAllTableNames() []*sqlname.SourceName {
	var tableNames []*sqlname.SourceName
	tableNamesRaw, err := ms.GetAllTableNamesRaw(ms.source.DBName)
	if err != nil {
		utils.ErrExit("Failed to get all table names: %s", err)
	}
	for _, tableName := range tableNamesRaw {
		tableNames = append(tableNames, sqlname.NewSourceName(ms.source.DBName, tableName))
	}
	log.Infof("GetAllTableNames(): %s", tableNames)
	return tableNames
}

func (ms *MySQL) getConnectionUri() string {
	source := ms.source
	if source.Uri != "" {
		return source.Uri
	}

	parseSSLString(source)
	var tlsString string
	switch source.SSLMode {
	case "disable":
		tlsString = "tls=false"
	case "prefer":
		tlsString = "tls=preferred"
	case "require":
		tlsString = "tls=skip-verify"
	case "verify-ca", "verify-full":
		tlsConf := createTLSConf(source)
		err := mysql.RegisterTLSConfig("custom", &tlsConf)
		if err != nil {
			utils.ErrExit("Failed to register TLS config: %s", err)
		}
		tlsString = "tls=custom"
	default:
		errMsg := "Incorrect SSL Mode Provided. Please enter a valid sslmode."
		panic(errMsg)
	}

	source.Uri = fmt.Sprintf("%s:%s@(%s:%d)/%s?%s", source.User, source.Password,
		source.Host, source.Port, source.DBName, tlsString)
	return source.Uri
}

func (ms *MySQL) GetConnectionUriWithoutPassword() string {
	source := ms.source
	parseSSLString(source)
	var tlsString string
	switch source.SSLMode {
	case "disable":
		tlsString = "tls=false"
	case "prefer":
		tlsString = "tls=preferred"
	case "require":
		tlsString = "tls=skip-verify"
	case "verify-ca", "verify-full":
		tlsConf := createTLSConf(source)
		err := mysql.RegisterTLSConfig("custom", &tlsConf)
		if err != nil {
			utils.ErrExit("Failed to register TLS config: %s", err)
		}
		tlsString = "tls=custom"
	default:
		errMsg := "Incorrect SSL Mode Provided. Please enter a valid sslmode."
		panic(errMsg)
	}

	sourceUriWithoutPassword := fmt.Sprintf("%s@(%s:%d)/%s?%s", source.User, source.Host, source.Port, source.DBName, tlsString)
	return sourceUriWithoutPassword
}

func (ms *MySQL) ExportSchema(exportDir string, schemaDir string) {
	ora2pgExtractSchema(ms.source, exportDir, schemaDir)
}

func (ms *MySQL) GetIndexesInfo() []utils.IndexInfo {
	return nil
}

func (ms *MySQL) ExportData(ctx context.Context, exportDir string, tableList []sqlname.NameTuple, quitChan chan bool, exportDataStart, exportSuccessChan chan bool, tablesColumnList *utils.StructMap[sqlname.NameTuple, []string], snapshotName string) {
	ora2pgExportDataOffline(ctx, ms.source, exportDir, tableList, tablesColumnList, quitChan, exportDataStart, exportSuccessChan)
}

func (ms *MySQL) ExportDataPostProcessing(exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	renameDataFilesForReservedWords(tablesProgressMetadata)
	dfd := datafile.Descriptor{
		FileFormat:                 datafile.SQL,
		Delimiter:                  "\t",
		HasHeader:                  false,
		ExportDir:                  exportDir,
		NullString:                 `\N`,
		DataFileList:               getExportedDataFileList(tablesProgressMetadata),
		TableNameToExportedColumns: getOra2pgExportedColumnsMap(exportDir, tablesProgressMetadata),
	}
	dfd.Save()
}

func (ms *MySQL) GetCharset() (string, error) {
	var charset string
	query := "SELECT @@character_set_database"
	err := ms.db.QueryRow(query).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("run query %q on source: %w", query, err)
	}
	return charset, nil
}

func (ms *MySQL) FilterUnsupportedTables(migrationUUID uuid.UUID, tableList []sqlname.NameTuple, useDebezium bool) ([]sqlname.NameTuple, []sqlname.NameTuple) {
	return tableList, nil
}

func (ms *MySQL) FilterEmptyTables(tableList []sqlname.NameTuple) ([]sqlname.NameTuple, []sqlname.NameTuple) {
	var nonEmptyTableList, emptyTableList []sqlname.NameTuple
	for _, tableName := range tableList {
		query := fmt.Sprintf(`SELECT 1 FROM %s LIMIT 1;`, tableName.AsQualifiedCatalogName())
		if !IsTableEmpty(ms.db, query) {
			nonEmptyTableList = append(nonEmptyTableList, tableName)
		} else {
			emptyTableList = append(emptyTableList, tableName)
		}
	}
	return nonEmptyTableList, emptyTableList
}

func (ms *MySQL) getTableColumns(tableName sqlname.NameTuple) ([]string, []string, []string, error) {
	var columns, dataTypes []string
	sname, tname := tableName.ForCatalogQuery()
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s' and table_name='%s'", sname, tname)
	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error in querying(%q) source database for table columns: %w", query, err)
	}
	for rows.Next() {
		var column, dataType string
		err := rows.Scan(&column, &dataType)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error in scanning query(%q) rows for table columns: %w", query, err)
		}
		columns = append(columns, column)
		dataTypes = append(dataTypes, dataType)
	}
	return columns, dataTypes, nil, nil
}

func (ms *MySQL) GetAllSequences() []string {
	return nil
}

func (ms *MySQL) GetAllSequencesRaw(schemaName string) ([]string, error) {

	query := fmt.Sprintf(`SELECT table_name, column_name FROM information_schema.columns
		WHERE table_schema = '%s' AND extra = 'auto_increment'`,
		schemaName)
	log.Infof("Querying '%s' for auto increment column of all tables in the database %q", query, schemaName)
	var sequences []string
	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %q for auto increment columns in tables: %s", query, err)
	}
	for rows.Next() {
		var columnName, tableName string
		err = rows.Scan(&tableName, &columnName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan %q for auto increment columns in tables: %s", query, err)
		}
		// sequence name as per PG naming convention for bigserial datatype's sequence
		sequenceName := fmt.Sprintf("%s_%s_seq", tableName, columnName)
		sequences = append(sequences, sequenceName)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to scan all rows of query %q for auto increment columns in tables: %s", query, rows.Err())
	}
	return sequences, nil
}

func (ms *MySQL) GetColumnsWithSupportedTypes(tableList []sqlname.NameTuple, useDebezium bool, _ bool) (*utils.StructMap[sqlname.NameTuple, []string], *utils.StructMap[sqlname.NameTuple, []string], error) {
	supportedTableColumnsMap := utils.NewStructMap[sqlname.NameTuple, []string]()
	unsupportedTableColumnsMap := utils.NewStructMap[sqlname.NameTuple, []string]()
	for _, tableName := range tableList {
		columns, dataTypes, _, err := ms.getTableColumns(tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get columns of table %q: %w", tableName.String(), err)
		}
		var supportedColumnNames []string
		_, tname := tableName.ForCatalogQuery()
		var unsupportedColumnNames []string
		for i := 0; i < len(columns); i++ {
			if utils.ContainsAnySubstringFromSlice(mysqlUnsupportedDataTypes, dataTypes[i]) {
				log.Infof("Skipping unsupproted column %s.%s of type %s", tname, columns[i], dataTypes[i])
				unsupportedColumnNames = append(unsupportedColumnNames, fmt.Sprintf("%s.%s of type %s", tname, columns[i], dataTypes[i]))
			} else {
				supportedColumnNames = append(supportedColumnNames, columns[i])
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

func (ms *MySQL) ParentTableOfPartition(table sqlname.NameTuple) string {
	panic("not implemented")
}

func (ms *MySQL) ValidateTablesReadyForLiveMigration(tableList []sqlname.NameTuple) error {
	panic("not implemented")
}

/*
Only valid case is when the table has a auto increment column
Note: a mysql table can have only one auto increment column
*/
func (ms *MySQL) GetColumnToSequenceMap(tableList []sqlname.NameTuple) map[string]string {
	columnToSequenceMap := make(map[string]string)
	for _, table := range tableList {
		// query to find out auto increment column
		sname, tname := table.ForCatalogQuery()
		query := fmt.Sprintf(`SELECT column_name FROM information_schema.columns
		WHERE table_schema = '%s' AND table_name = '%s' AND extra = 'auto_increment'`,
			sname, tname)
		log.Infof("Querying '%s' for auto increment column of table %q", query, table.String())

		var columnName string
		rows, err := ms.db.Query(query)
		if err != nil {
			utils.ErrExit("Failed to query %q for auto increment column of %q: %s", query, table.String(), err)
		}
		defer func() {
			closeErr := rows.Close()
			if closeErr != nil {
				log.Warnf("close rows for table %s query %q: %v", table.String(), query, closeErr)
			}
		}()
		if rows.Next() {
			err = rows.Scan(&columnName)
			if err != nil {
				utils.ErrExit("Failed to scan %q for auto increment column of %q: %s", query, table.String(), err)
			}
			qualifiedColumeName := fmt.Sprintf("%s.%s", table.AsQualifiedCatalogName(), columnName)
			// sequence name as per PG naming convention for bigserial datatype's sequence
			sequenceName := fmt.Sprintf("%s_%s_seq", tname, columnName)
			columnToSequenceMap[qualifiedColumeName] = sequenceName
		}
		err = rows.Close()
		if err != nil {
			utils.ErrExit("close rows for table %s query %q: %s", table.String(), query, err)
		}
	}
	return columnToSequenceMap
}

func createTLSConf(source *Source) tls.Config {
	rootCertPool := x509.NewCertPool()
	if source.SSLRootCert != "" {
		pem, err := os.ReadFile(source.SSLRootCert)
		if err != nil {
			utils.ErrExit("error in reading SSL Root Certificate: %v", err)
		}

		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			utils.ErrExit("Failed to append PEM.")
		}
	} else {
		utils.ErrExit("Root Certificate Needed for verify-ca and verify-full SSL Modes")
	}
	clientCert := make([]tls.Certificate, 0, 1)

	if source.SSLCertPath != "" && source.SSLKey != "" {
		certs, err := tls.LoadX509KeyPair(source.SSLCertPath, source.SSLKey)
		if err != nil {
			utils.ErrExit("error in reading and parsing SSL KeyPair: %v", err)
		}

		clientCert = append(clientCert, certs)
	}

	if source.SSLMode == "verify-ca" {
		return tls.Config{
			RootCAs:            rootCertPool,
			Certificates:       clientCert,
			InsecureSkipVerify: true,
		}
	} else { //if verify-full

		return tls.Config{
			RootCAs:            rootCertPool,
			Certificates:       clientCert,
			InsecureSkipVerify: false,
			ServerName:         source.Host,
		}
	}
}

func (ms *MySQL) GetServers() []string {
	return []string{ms.source.Host}
}

func (ms *MySQL) GetPartitions(tableName sqlname.NameTuple) []string {
	panic("not implemented")
}

func (ms *MySQL) GetTableToUniqueKeyColumnsMap(tableList []sqlname.NameTuple) (map[string][]string, error) {
	return nil, nil
}

func (ms *MySQL) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("ClearMigrationState not implemented yet for MySQL")
	return nil
}

func (ms *MySQL) GetNonPKTables() ([]string, error) {
	query := fmt.Sprintf(`SELECT 
		IFNULL(pk_count.count, 0) AS pk_count, 
		t.table_name
	FROM 
		information_schema.TABLES t
	LEFT JOIN (
		SELECT 
			COUNT(CONSTRAINT_NAME) AS count, 
			table_name
		FROM 
			information_schema.KEY_COLUMN_USAGE
		WHERE 
			TABLE_SCHEMA = '%s' 
			AND CONSTRAINT_NAME = 'PRIMARY' 
		GROUP BY 
			table_name
	) pk_count ON t.table_name = pk_count.table_name
	WHERE 
		t.TABLE_SCHEMA = '%s'`, ms.source.DBName, ms.source.DBName)
	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query %q for primary key of %q: %w", query, ms.source.DBName, err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Warnf("close rows for query %q: %v", query, closeErr)
		}
	}()
	var nonPKTables []string
	for rows.Next() {
		var count int
		var tableName string
		err = rows.Scan(&count, &tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan count from output of query %q: %w", query, err)
		}
		table := sqlname.NewSourceName(ms.source.DBName, tableName)
		if count == 0 {
			nonPKTables = append(nonPKTables, table.Qualified.Quoted)
		}
	}
	return nonPKTables, nil
}
