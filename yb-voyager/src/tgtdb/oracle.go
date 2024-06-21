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
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/sqlldr"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type TargetOracleDB struct {
	sync.Mutex
	*AttributeNameRegistry
	tconf *TargetConf
	oraDB *sql.DB
	conn  *sql.Conn

	attrNames map[string][]string
}

func newTargetOracleDB(tconf *TargetConf) *TargetOracleDB {
	tdb := &TargetOracleDB{
		tconf:     tconf,
		attrNames: make(map[string][]string),
	}
	tdb.AttributeNameRegistry = NewAttributeNameRegistry(tdb, tconf)
	return tdb
}

func (tdb *TargetOracleDB) connect() error {
	conn, err := tdb.oraDB.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("connect to target db: %w", err)
	}
	tdb.setTargetSchema(conn)
	tdb.conn = conn
	return err
}

func (tdb *TargetOracleDB) Init() error {
	db, err := sql.Open("godror", tdb.getConnectionUri(tdb.tconf))
	if err != nil {
		return fmt.Errorf("open connection to target db: %w", err)
	}
	tdb.oraDB = db

	err = tdb.connect()
	if err != nil {
		return err
	}
	tdb.tconf.Schema = strings.ToUpper(tdb.tconf.Schema)
	checkSchemaExistsQuery := fmt.Sprintf(
		"SELECT 1 FROM ALL_USERS WHERE USERNAME = '%s'",
		tdb.tconf.Schema)
	var cntSchemaName int
	if err = tdb.QueryRow(checkSchemaExistsQuery).Scan(&cntSchemaName); err != nil {
		err = fmt.Errorf("run query %q on target %q to check schema exists: %s", checkSchemaExistsQuery, tdb.tconf.Host, err)
	} else if cntSchemaName == 0 {
		err = fmt.Errorf("schema '%s' does not exist in target", tdb.tconf.Schema)
	}
	return err
}

func (tdb *TargetOracleDB) Query(query string) (*sql.Rows, error) {
	return tdb.oraDB.Query(query)
}

func (tdb *TargetOracleDB) QueryRow(query string) *sql.Row {
	return tdb.oraDB.QueryRow(query)
}

func (tdb *TargetOracleDB) Exec(query string) (int64, error) {
	var rowsAffected int64

	res, err := tdb.oraDB.Exec(query)
	if err != nil {
		return rowsAffected, fmt.Errorf("run query %q on target %q: %w", query, tdb.tconf.Host, err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		return rowsAffected, fmt.Errorf("rowsAffected on query %q on target %q: %w", query, tdb.tconf.Host, err)
	}
	return rowsAffected, err
}

func (tdb *TargetOracleDB) WithTx(fn func(tx *sql.Tx) error) error {
	tx, err := tdb.oraDB.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction on target %q: %w", tdb.tconf.Host, err)
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction on target %q: %w", tdb.tconf.Host, err)
	}
	return nil
}

func (tdb *TargetOracleDB) disconnect() {
	if tdb.conn != nil {
		log.Infof("No connection to the target database to close")
	}

	err := tdb.conn.Close()
	if err != nil {
		log.Errorf("Failed to close connection to the target database: %v", err)
	}
	tdb.conn = nil
}

func (tdb *TargetOracleDB) GetConnection() *sql.Conn {
	if tdb.conn == nil {
		utils.ErrExit("Called target db GetConnection() before Init()")
	}
	return tdb.conn
}

func (tdb *TargetOracleDB) Finalize() {
	tdb.disconnect()
}

func (tdb *TargetOracleDB) GetVersion() string {
	var version string
	query := "SELECT BANNER FROM V$VERSION"
	// query sample output: Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
	err := tdb.QueryRow(query).Scan(&version)
	if err != nil {
		utils.ErrExit("run query %q on source: %s", query, err)
	}
	return version
}

func (tdb *TargetOracleDB) GetCallhomeTargetDBInfo() *callhome.TargetDBDetails {
	return &callhome.TargetDBDetails{
		DBVersion: tdb.GetVersion(),
		Host:      tdb.tconf.Host,
	}
}

func (tdb *TargetOracleDB) CreateVoyagerSchema() error {
	return nil
}

func (tdb *TargetOracleDB) GetNonEmptyTables(tables []sqlname.NameTuple) []sqlname.NameTuple {
	result := []sqlname.NameTuple{}

	for _, table := range tables {
		log.Infof("Checking if table %s is empty", table.ForUserQuery())
		rowCount := 0
		stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.ForUserQuery())
		err := tdb.QueryRow(stmt).Scan(&rowCount)
		if err != nil {
			utils.ErrExit("run query %q on target: %s", stmt, err)
		}
		if rowCount > 0 {
			result = append(result, table)
		}
	}

	return result
}

func (tdb *TargetOracleDB) IsNonRetryableCopyError(err error) bool {
	return false
}

// NOTE: TODO support for identity columns sequences
func (tdb *TargetOracleDB) RestoreSequences(sequencesLastVal map[string]int64) error {
	return nil
}

func (tdb *TargetOracleDB) ImportBatch(batch Batch, args *ImportBatchArgs, exportDir string, tableSchema map[string]map[string]string) (int64, error) {
	tdb.Lock()
	defer tdb.Unlock()

	var rowsAffected int64
	var err error
	copyFn := func(conn *sql.Conn) (bool, error) {
		rowsAffected, err = tdb.importBatch(conn, batch, args, exportDir, tableSchema)
		return false, err
	}
	err = tdb.WithConnFromPool(copyFn)
	return rowsAffected, err
}

func (tdb *TargetOracleDB) WithConnFromPool(fn func(*sql.Conn) (bool, error)) error {
	var err error
	retry := true

	for retry {
		var maxAttempts = 5
		var conn *sql.Conn
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			conn, err = tdb.oraDB.Conn(context.Background())
			if err == nil {
				break
			}

			if attempt < maxAttempts {
				log.Warnf("Connection pool is busy. Sleeping for 2 seconds: %s", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}

		if conn == nil {
			return fmt.Errorf("failed to get connection from target db: %w", err)
		}

		retry, err = fn(conn)
		conn.Close()

		if retry {
			time.Sleep(2 * time.Second)
		}
	}
	return err
}

func (tdb *TargetOracleDB) importBatch(conn *sql.Conn, batch Batch, args *ImportBatchArgs, exportDir string, tableSchema map[string]map[string]string) (rowsAffected int64, err error) {
	var file *os.File
	file, err = batch.Open()
	if err != nil {
		return 0, fmt.Errorf("open batch file %q: %w", batch.GetFilePath(), err)
	}
	defer file.Close()

	//setting the schema so that the table is created in the correct schema
	tdb.setTargetSchema(conn)

	ctx := context.Background()
	var tx *sql.Tx
	tx, err = conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		var err2 error
		if err != nil {
			err2 = tx.Rollback()
			if err2 != nil {
				rowsAffected = 0
				err = fmt.Errorf("rollback transaction: %w (while processing %s)", err2, err)
			}
		} else {
			err2 = tx.Commit()
			if err2 != nil {
				rowsAffected = 0
				err = fmt.Errorf("commit transaction: %w", err2)
			}
		}
	}()

	var alreadyImported bool
	alreadyImported, rowsAffected, err = tdb.isBatchAlreadyImported(tx, batch)
	if err != nil {
		return 0, err
	}
	if alreadyImported {
		return rowsAffected, nil
	}

	tableName := batch.GetTableName()
	sqlldrConfig := args.GetSqlLdrControlFile(tableSchema)
	fileName := filepath.Base(batch.GetFilePath())

	err = sqlldr.CreateSqlldrDir(exportDir)
	if err != nil {
		return 0, err
	}
	var sqlldrControlFilePath string
	sqlldrControlFilePath, err = sqlldr.CreateSqlldrControlFile(exportDir, tableName, sqlldrConfig, fileName)
	if err != nil {
		return 0, err
	}

	var sqlldrLogFilePath string
	var sqlldrLogFile *os.File
	sqlldrLogFilePath, sqlldrLogFile, err = sqlldr.CreateSqlldrLogFile(exportDir, tableName)
	if err != nil {
		return 0, err
	}
	defer sqlldrLogFile.Close()

	user := tdb.tconf.User
	password := tdb.tconf.Password
	connectString := tdb.getConnectionString(tdb.tconf)
	oracleConnectionString := fmt.Sprintf("%s@\"%s\"", user, connectString)
	/*
			reference for sqlldr cli options https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-sql-loader-commands.html#GUID-24205A60-E16F-4DBA-AD82-376C401013DF
		    DIRECT=TRUE for using faster mode (direct path)
			NO_INDEX_ERRORS=TRUE for not ignoring index errors
			SKIP=1 for skipping the first row which is the header
			ERRORS=0 for exiting on first error and 0 errors allowed
	*/
	sqlldrArgs := fmt.Sprintf("userid=%s control=%s log=%s DIRECT=TRUE NO_INDEX_ERRORS=TRUE SKIP=1 ERRORS=0",
		oracleConnectionString, sqlldrControlFilePath, sqlldrLogFilePath)

	var outbuf string
	var errbuf string
	outbuf, errbuf, err = sqlldr.RunSqlldr(sqlldrArgs, password)

	if err != nil {
		// for error related to the stdinPipe of created while running sqlldr
		return 0, fmt.Errorf("run sqlldr error: %w %s\nPlease check the log file for more information - %s", err, errbuf, sqlldrLogFilePath)
	}

	var err2 error
	rowsAffected, err2 = getRowsAffected(outbuf)
	if err2 != nil {
		return 0, fmt.Errorf("get rows affected from sqlldr output: %w", err)
	}

	ignoreError := false
	if err != nil {
		log.Infof("sqlldr out:\n%s", outbuf)
		log.Errorf("sqlldr error:\n%s", errbuf)
		// find ORA-00001: unique constraint * violated in log file
		pattern := regexp.MustCompile(`ORA-00001: unique constraint \(.+?\) violated`)
		scanner := bufio.NewScanner(sqlldrLogFile)
		for scanner.Scan() {
			line := scanner.Text()
			if pattern.MatchString(line) {
				ignoreError = true
				break
			}
		}

		if !ignoreError {
			return rowsAffected, fmt.Errorf("run sqlldr: %w", err)
		}
	}
	log.Infof("rows affected returned from sqlldr for args: %s - %d", sqlldrArgs, rowsAffected)
	err = tdb.recordEntryInDB(tx, batch, rowsAffected)
	if err != nil {
		err = fmt.Errorf("record entry in DB for batch %q: %w", batch.GetFilePath(), err)
	}

	return rowsAffected, err
}

func (tdb *TargetOracleDB) recordEntryInDB(tx *sql.Tx, batch Batch, rowsAffected int64) error {
	cmd := batch.GetQueryToRecordEntryInDB(rowsAffected)
	_, err := tx.ExecContext(context.Background(), cmd)
	if err != nil {
		return fmt.Errorf("insert into %s: %w", BATCH_METADATA_TABLE_NAME, err)
	}
	return nil
}

func getRowsAffected(outbuf string) (int64, error) {
	regex := regexp.MustCompile(`Load completed - logical record count (\d+).`)
	matches := regex.FindStringSubmatch(outbuf)
	if len(matches) < 2 {
		return 0, fmt.Errorf("RowsAffected not found in the sqlldr output")
	}
	return strconv.ParseInt(matches[1], 10, 64)
}

func (tdb *TargetOracleDB) isBatchAlreadyImported(tx *sql.Tx, batch Batch) (bool, int64, error) {
	var rowsImported int64
	query := batch.GetQueryIsBatchAlreadyImported()
	err := tx.QueryRowContext(context.Background(), query).Scan(&rowsImported)
	if err == nil {
		log.Infof("%v rows from %q are already imported", rowsImported, batch.GetFilePath())
		return true, rowsImported, nil
	}
	if err == sql.ErrNoRows {
		log.Infof("%q is not imported yet", batch.GetFilePath())
		return false, 0, nil
	}
	return false, 0, fmt.Errorf("check if %s is already imported: %w", batch.GetFilePath(), err)
}

func (tdb *TargetOracleDB) setTargetSchema(conn *sql.Conn) {
	setSchemaQuery := fmt.Sprintf("ALTER SESSION SET CURRENT_SCHEMA = %s", tdb.tconf.Schema)
	_, err := conn.ExecContext(context.Background(), setSchemaQuery)
	if err != nil {
		utils.ErrExit("run query %q on target %q to set schema: %s", setSchemaQuery, tdb.tconf.Host, err)
	}
}

func (tdb *TargetOracleDB) GetListOfTableAttributes(tableNameTup sqlname.NameTuple) ([]string, error) {
	sname, tname := tableNameTup.ForCatalogQuery()
	query := fmt.Sprintf("SELECT column_name FROM all_tab_columns WHERE table_name = '%s' AND owner = '%s'", tname, sname)
	rows, err := tdb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query meta info for channels: %w", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		err := rows.Scan(&column)
		if err != nil {
			return nil, fmt.Errorf("error while scanning rows returned from DB: %w", err)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

// execute all events sequentially one by one in a single transaction
func (tdb *TargetOracleDB) ExecuteBatch(migrationUUID uuid.UUID, batch *EventBatch) error {
	// TODO: figure out how to avoid round trips to Oracle DB
	log.Infof("executing batch(%s) of %d events", batch.ID(), len(batch.Events))
	err := tdb.WithConnFromPool(func(conn *sql.Conn) (bool, error) {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return false, fmt.Errorf("begin transaction: %w", err)
		}
		defer func() {
			errRollBack := tx.Rollback()
			if errRollBack != nil && errRollBack != sql.ErrTxDone {
				log.Errorf("error rolling back tx for batch id (%s): %v", batch.ID(), err)
			}
		}()
		var rowsAffectedInserts, rowsAffectedDeletes, rowsAffectedUpdates int64
		for i := 0; i < len(batch.Events); i++ {
			event := batch.Events[i]
			stmt, err := event.GetSQLStmt(tdb)
			if err != nil {
				return false, fmt.Errorf("get sql stmt: %w", err)
			}
			if event.Op == "c" && tdb.tconf.EnableUpsert {
				// converting to an UPSERT
				event.Op = "u"
				updateStmt, err := event.GetSQLStmt(tdb)
				if err != nil {
					return false, fmt.Errorf("get sql stmt: %w", err)
				}
				stmt = fmt.Sprintf("BEGIN %s; EXCEPTION WHEN dup_val_on_index THEN %s; END;", stmt, updateStmt)
				event.Op = "c" // reverting state
			}
			var res sql.Result
			res, err = tx.Exec(stmt)
			if err != nil {
				log.Errorf("error executing stmt for event with vsn(%d) via query-%s: %v", event.Vsn, stmt, err)
				return false, fmt.Errorf("failed to execute stmt for event with vsn(%d) via query-%s: %w", event.Vsn, stmt, err)
			}
			var cnt int64
			cnt, err = res.RowsAffected()
			if err != nil {
				log.Errorf("error getting rows Affecterd for event with vsn(%d) in batch(%s)", event.Vsn, batch.ID())
				return false, fmt.Errorf("failed to get rowsAffected for event with vsn(%d)", event.Vsn)
			}
			switch true {
			case event.Op == "c":
				rowsAffectedInserts += cnt
			case event.Op == "d":
				rowsAffectedDeletes += cnt
			case event.Op == "u":
				rowsAffectedUpdates += cnt
			}
			if cnt != 1 {
				log.Warnf("unexpected rows affected for event with vsn(%d) in batch(%s): %d", batch.Events[i].Vsn, batch.ID(), cnt)
			}
		}

		updateVsnQuery := batch.GetChannelMetadataUpdateQuery(migrationUUID)
		res, err := tx.Exec(updateVsnQuery)
		if err != nil {
			log.Errorf("error executing stmt for batch(%s): %v", batch.ID(), err)
			return false, fmt.Errorf("failed to update vsn on target db via query-%s: %w", updateVsnQuery, err)
		} else if rowsAffected, err := res.RowsAffected(); rowsAffected == 0 || err != nil {
			log.Errorf("error executing stmt: %v, rowsAffected: %v", err, rowsAffected)
			return false, fmt.Errorf("failed to update vsn on target db via query-%s: %w, rowsAffected: %v",
				updateVsnQuery, err, rowsAffected)
		}

		tableNames := batch.GetTableNames()
		for _, tableName := range tableNames {
			updatePerTableEvents := batch.GetQueriesToUpdateEventStatsByTable(migrationUUID, tableName)
			res, err = tx.Exec(updatePerTableEvents)
			if err != nil {
				log.Errorf("error executing stmt: %v", err)
				return false, fmt.Errorf("failed to update per table events on target db via query-%s: %w", updatePerTableEvents, err)
			}
			rowsAffected, err := res.RowsAffected()
			if err != nil {
				return false, fmt.Errorf("failed to get number of rows affected in update per table events on target db via query-%s: %w",
					updatePerTableEvents, err)
			}
			if rowsAffected == 0 {
				insertTableStatsQuery := batch.GetQueriesToInsertEventStatsByTable(migrationUUID, tableName)
				_, err = tx.Exec(insertTableStatsQuery)
				if err != nil {
					log.Errorf("error executing stmt: %v ", err)
					return false, fmt.Errorf("failed to insert table stats on target db via query-%s: %w",
						insertTableStatsQuery, err)
				}
			}
		}

		if err = tx.Commit(); err != nil {
			return false, fmt.Errorf("failed to commit transaction : %w", err)
		}

		logDiscrepancyInEventBatchIfAny(batch, rowsAffectedInserts, rowsAffectedDeletes, rowsAffectedUpdates)

		return false, err
	})
	if err != nil {
		return fmt.Errorf("error executing batch: %w", err)
	}

	return nil
}

func (tdb *TargetOracleDB) InitConnPool() error {
	if tdb.tconf.Parallelism == 0 {
		tdb.tconf.Parallelism = 16
		log.Infof("Using %d parallel jobs by default. Use --parallel-jobs to specify a custom value", tdb.tconf.Parallelism)
	}
	tdb.oraDB.SetMaxIdleConns(tdb.tconf.Parallelism + 1)
	tdb.oraDB.SetMaxOpenConns(tdb.tconf.Parallelism + 1)
	return nil
}

func (tdb *TargetOracleDB) PrepareForStreaming() {}

func (tdb *TargetOracleDB) getConnectionUri(tconf *TargetConf) string {
	if tconf.Uri != "" {
		return tconf.Uri
	}

	connectString := tdb.getConnectionString(tconf)
	tconf.Uri = fmt.Sprintf(`user="%s" password="%s" connectString="%s"`, tconf.User, tconf.Password, connectString)

	return tconf.Uri
}

func (tdb *TargetOracleDB) getConnectionString(tconf *TargetConf) string {
	var connectString string
	switch true {
	case tconf.DBSid != "":
		connectString = fmt.Sprintf("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SID=%s)))",
			tconf.Host, tconf.Port, tconf.DBSid)
	case tconf.TNSAlias != "":
		connectString = tconf.TNSAlias
	case tconf.DBName != "":
		connectString = fmt.Sprintf("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s)))",
			tconf.Host, tconf.Port, tconf.DBName)
	}

	return connectString
}

func (tdb *TargetOracleDB) MaxBatchSizeInBytes() int64 {
	return 2 * 1024 * 1024 * 1024 // 2GB
}

func (tdb *TargetOracleDB) GetIdentityColumnNamesForTable(tableNameTup sqlname.NameTuple, identityType string) ([]string, error) {
	sname, tname := tableNameTup.ForCatalogQuery()
	query := fmt.Sprintf(`Select COLUMN_NAME from ALL_TAB_IDENTITY_COLS where OWNER = '%s'
	AND TABLE_NAME = '%s' AND GENERATION_TYPE='%s'`, sname, tname, identityType)
	log.Infof("query of identity(%s) columns for table(%s): %s", identityType, tableNameTup, query)
	var identityColumns []string
	err := tdb.WithConnFromPool(func(conn *sql.Conn) (bool, error) {
		rows, err := conn.QueryContext(context.Background(), query)
		if err != nil {
			if err == sql.ErrNoRows {
				return false, nil
			}
			log.Errorf("querying identity(%s) columns: %v", identityType, err)
			return false, fmt.Errorf("querying identity(%s) columns: %w", identityType, err)
		}
		defer rows.Close()
		for rows.Next() {
			var colName string
			err := rows.Scan(&colName)
			if err != nil {
				log.Errorf("scanning row for identity(%s) column name: %v", identityType, err)
				return false, fmt.Errorf("scanning row for identity(%s) column name: %w", identityType, err)
			}
			identityColumns = append(identityColumns, colName)
		}
		return false, nil
	})
	return identityColumns, err
}

func (tdb *TargetOracleDB) DisableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("disabling generated always as identity columns")
	return tdb.alterColumns(tableColumnsMap, "GENERATED BY DEFAULT AS IDENTITY(START WITH LIMIT VALUE)")
}

func (tdb *TargetOracleDB) EnableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("enabling generated always as identity columns")
	// Oracle needs start value to resumes the value for further inserts correctly
	return tdb.alterColumns(tableColumnsMap, "GENERATED ALWAYS AS IDENTITY(START WITH LIMIT VALUE)")
}

func (tdb *TargetOracleDB) EnableGeneratedByDefaultAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("enabling generated by default as identity columns")
	return tdb.alterColumns(tableColumnsMap, "GENERATED BY DEFAULT AS IDENTITY(START WITH LIMIT VALUE)")
}

func (tdb *TargetOracleDB) alterColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string], alterAction string) error {
	return tableColumnsMap.IterKV(func(table sqlname.NameTuple, columns []string) (bool, error) {
		for _, column := range columns {
			// LIMIT VALUE - ensures that start it is set to the current value of the sequence
			query := fmt.Sprintf(`ALTER TABLE %s MODIFY %s %s`, table.ForUserQuery(), column, alterAction)
			err := tdb.WithConnFromPool(func(conn *sql.Conn) (bool, error) {
				_, err := conn.ExecContext(context.Background(), query)
				if err != nil {
					log.Errorf("executing query-%s to alter column(%s) for table(%s): %v", query, column, table.ForUserQuery(), err)
					return false, fmt.Errorf("executing query to alter column for table(%s): %w", table.ForUserQuery(), err)
				}
				return false, nil
			})
			if err != nil {
				return false, err
			}
		}
		return true, nil
	})
}

func (tdb *TargetOracleDB) isSchemaExists(schema string) bool {
	// TODO: handle case-sensitivity properly
	query := fmt.Sprintf("SELECT 1 FROM ALL_USERS WHERE USERNAME = UPPER('%s')", schema)
	return tdb.isQueryResultNonEmpty(query)
}

func (tdb *TargetOracleDB) isTableExists(nt sqlname.NameTuple) bool {
	sname, tname := nt.ForCatalogQuery()
	query := fmt.Sprintf("SELECT 1 FROM ALL_TABLES WHERE TABLE_NAME = '%s' AND OWNER = '%s'", tname, sname)
	return tdb.isQueryResultNonEmpty(query)
}

func (tdb *TargetOracleDB) isQueryResultNonEmpty(query string) bool {
	rows, err := tdb.Query(query)
	if err != nil {
		utils.ErrExit("error checking if query %s is empty: %v", query, err)
	}
	defer rows.Close()

	return rows.Next()
}

// this will be only called by FallForward or FallBack DBs
func (tdb *TargetOracleDB) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("clearing migration state for migrationUUID: %s", migrationUUID)
	schema := BATCH_METADATA_TABLE_SCHEMA
	if !tdb.isSchemaExists(schema) {
		log.Infof("schema %s does not exist, nothing to clear for migration state", schema)
		return nil
	}

	// clean up all the tables in BATCH_METADATA_TABLE_SCHEMA
	tableNames := []string{BATCH_METADATA_TABLE_NAME, EVENT_CHANNELS_METADATA_TABLE_NAME, EVENTS_PER_TABLE_METADATA_TABLE_NAME} // replace with actual table names
	tables := []sqlname.NameTuple{}
	for _, tableName := range tableNames {
		parts := strings.Split(tableName, ".")
		objName := sqlname.NewObjectName(sqlname.ORACLE, "", parts[0], strings.ToUpper(parts[1]))
		nt := sqlname.NameTuple{
			CurrentName: objName,
			SourceName:  objName,
			TargetName:  objName,
		}
		tables = append(tables, nt)
	}
	for _, table := range tables {
		if !tdb.isTableExists(table) {
			log.Infof("table %s does not exist, nothing to clear for migration state", table)
			continue
		}
		log.Infof("cleaning up table %s for migrationUUID=%s", table, migrationUUID)
		query := fmt.Sprintf("DELETE FROM %s WHERE migration_uuid = '%s'", table.ForUserQuery(), migrationUUID)
		_, err := tdb.Exec(query)
		if err != nil {
			log.Errorf("error cleaning up table %s: %v", table, err)
			return fmt.Errorf("error cleaning up table %s: %w", table, err)
		}
	}

	// ask to manually delete the USER in case of FF or FB
	// TODO: check and inform user if there is another migrationUUID data in metadata schema tables before cleaning up the schema
	utils.PrintAndLog(`Please manually delete the metadata schema '%s' from the '%s' host using the following SQL statement(after making sure no other migration is IN-PROGRESS):
	DROP USER %s CASCADE`, schema, tdb.tconf.Host, schema)
	return nil
}
