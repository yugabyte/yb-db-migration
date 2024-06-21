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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v5/stdlib"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type TargetPostgreSQL struct {
	sync.Mutex
	*AttributeNameRegistry
	tconf    *TargetConf
	db       *sql.DB
	conn_    *pgx.Conn
	connPool *ConnectionPool

	attrNames map[string][]string
}

func newTargetPostgreSQL(tconf *TargetConf) *TargetPostgreSQL {
	tdb := &TargetPostgreSQL{
		tconf:     tconf,
		attrNames: make(map[string][]string),
	}
	tdb.AttributeNameRegistry = NewAttributeNameRegistry(tdb, tconf)
	return tdb
}

func (pg *TargetPostgreSQL) Query(query string) (*sql.Rows, error) {
	return pg.db.Query(query)
}

func (pg *TargetPostgreSQL) QueryRow(query string) *sql.Row {
	return pg.db.QueryRow(query)
}

func (pg *TargetPostgreSQL) Exec(query string) (int64, error) {
	var rowsAffected int64

	res, err := pg.db.Exec(query)
	if err != nil {
		return rowsAffected, fmt.Errorf("run query %q on target %q: %w", query, pg.tconf.Host, err)
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		return rowsAffected, fmt.Errorf("rowsAffected on query %q on target %q: %w", query, pg.tconf.Host, err)
	}
	return rowsAffected, err
}

func (pg *TargetPostgreSQL) WithTx(fn func(tx *sql.Tx) error) error {
	tx, err := pg.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction on target %q: %w", pg.tconf.Host, err)
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction on target %q: %w", pg.tconf.Host, err)
	}
	return nil
}

func (pg *TargetPostgreSQL) Init() error {
	err := pg.connect()
	if err != nil {
		return err
	}
	schemas := strings.Split(pg.tconf.Schema, ",")
	schemaList := strings.Join(schemas, "','") // a','b','c
	checkSchemaExistsQuery := fmt.Sprintf(
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('%s')",
		schemaList)
	rows, err := pg.Query(checkSchemaExistsQuery)
	if err != nil {
		return fmt.Errorf("run query %q on target %q to check schema exists: %s", checkSchemaExistsQuery, pg.tconf.Host, err)
	}
	var returnedSchemas []string
	defer rows.Close()
	for rows.Next() {
		var schemaName string
		err = rows.Scan(&schemaName)
		if err != nil {
			return fmt.Errorf("scan schema name: %w", err)
		}
		returnedSchemas = append(returnedSchemas, schemaName)
	}
	if len(returnedSchemas) != len(schemas) {
		notExistsSchemas := utils.SetDifference(schemas, returnedSchemas)
		return fmt.Errorf("schema '%s' does not exist in target", strings.Join(notExistsSchemas, ","))
	}
	return err
}

func (pg *TargetPostgreSQL) Finalize() {
	pg.disconnect()
}

func (pg *TargetPostgreSQL) reconnect() error {
	pg.Mutex.Lock()
	defer pg.Mutex.Unlock()

	var err error
	pg.disconnect()
	for attempt := 1; attempt < 5; attempt++ {
		err = pg.connect()
		if err == nil {
			return nil
		}
		log.Infof("Failed to reconnect to the target database: %s", err)
		time.Sleep(time.Duration(attempt*2) * time.Second)
		// Retry.
	}
	return fmt.Errorf("reconnect to target db: %w", err)
}

func (pg *TargetPostgreSQL) connect() error {
	if pg.conn_ != nil {
		// Already connected.
		return nil
	}
	connStr := pg.tconf.GetConnectionUri()
	var err error
	pg.db, err = sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("open connection to target db: %w", err)
	}
	// setting this to only 1, because this is used for adhoc queries.
	// We have a separate pool for importing data.
	pg.db.SetMaxOpenConns(1)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("connect to target db: %w", err)
	}
	pg.setTargetSchema(conn)
	pg.conn_ = conn
	return nil
}

func (pg *TargetPostgreSQL) disconnect() {
	if pg.conn_ == nil {
		// Already disconnected.
		return
	}

	err := pg.conn_.Close(context.Background())
	if err != nil {
		log.Infof("Failed to close connection to the target database: %s", err)
	}
	pg.conn_ = nil
}

func (pg *TargetPostgreSQL) EnsureConnected() {
	err := pg.connect()
	if err != nil {
		utils.ErrExit("Failed to connect to the target DB: %s", err)
	}
}

func (pg *TargetPostgreSQL) GetVersion() string {
	if pg.tconf.DBVersion != "" {
		return pg.tconf.DBVersion
	}

	pg.EnsureConnected()
	pg.Mutex.Lock()
	defer pg.Mutex.Unlock()
	query := "SELECT setting FROM pg_settings WHERE name = 'server_version'"
	err := pg.QueryRow(query).Scan(&pg.tconf.DBVersion)
	if err != nil {
		utils.ErrExit("get target db version: %s", err)
	}
	return pg.tconf.DBVersion
}

func (pg *TargetPostgreSQL) PrepareForStreaming() {
	log.Infof("Preparing target DB for streaming - disable throttling")
	pg.connPool.DisableThrottling()
}

const PG_DEFAULT_PARALLELISM_FACTOR = 8 // factor for default parallelism in case fetchDefaultParallelJobs() is not able to get the no of cores

func (pg *TargetPostgreSQL) InitConnPool() error {
	tconfs := []*TargetConf{pg.tconf}
	var targetUriList []string
	for _, tconf := range tconfs {
		targetUriList = append(targetUriList, tconf.Uri)
	}
	log.Infof("targetUriList: %s", utils.GetRedactedURLs(targetUriList))

	if pg.tconf.Parallelism == 0 {
		pg.tconf.Parallelism = fetchDefaultParallelJobs(tconfs, PG_DEFAULT_PARALLELISM_FACTOR)
		log.Infof("Using %d parallel jobs by default. Use --parallel-jobs to specify a custom value", pg.tconf.Parallelism)
	}

	params := &ConnectionParams{
		NumConnections:    pg.tconf.Parallelism,
		ConnUriList:       targetUriList,
		SessionInitScript: getYBSessionInitScript(pg.tconf),
		// works fine as we check the support of any session variable before using it in the script.
		// So upsert and disable transaction will never be used for PG
	}
	pg.connPool = NewConnectionPool(params)
	return nil
}

func (pg *TargetPostgreSQL) GetCallhomeTargetDBInfo() *callhome.TargetDBDetails {
	totalCores, _ := fetchCores([]*TargetConf{pg.tconf})
	return &callhome.TargetDBDetails{
		Host:      pg.tconf.Host,
		NodeCount: 1,
		Cores:     totalCores,
		DBVersion: pg.GetVersion(),
	}
}

func (pg *TargetPostgreSQL) CreateVoyagerSchema() error {
	cmds := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, BATCH_METADATA_TABLE_SCHEMA),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			migration_uuid uuid,
			data_file_name VARCHAR(250),
			batch_number INT,
			schema_name VARCHAR(250),
			table_name VARCHAR(250),
			rows_imported BIGINT,
			PRIMARY KEY (migration_uuid, data_file_name, batch_number, schema_name, table_name)
		);`, BATCH_METADATA_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			migration_uuid uuid,
			channel_no INT,
			last_applied_vsn BIGINT,
			num_inserts BIGINT,
			num_deletes BIGINT,
			num_updates BIGINT,
			PRIMARY KEY (migration_uuid, channel_no));`, EVENT_CHANNELS_METADATA_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			migration_uuid uuid,
			table_name VARCHAR(250), 
			channel_no INT,
			total_events BIGINT,
			num_inserts BIGINT,
			num_deletes BIGINT,
			num_updates BIGINT,
			PRIMARY KEY (migration_uuid, table_name, channel_no));`, EVENTS_PER_TABLE_METADATA_TABLE_NAME),
	}

	maxAttempts := 12
	var err error
outer:
	for _, cmd := range cmds {
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			log.Infof("Executing on target: [%s]", cmd)
			_, err = pg.Exec(cmd)
			if err == nil {
				// No error. Move on to the next command.
				continue outer
			}
			log.Warnf("Error while running [%s] attempt %d: %s", cmd, attempt, err)
			time.Sleep(5 * time.Second)
			err2 := pg.reconnect()
			if err2 != nil {
				log.Warnf("Failed to reconnect to the target database: %s", err2)
				break
			}
		}
		if err != nil {
			return fmt.Errorf("create ybvoyager schema on target: %w", err)
		}
	}
	return nil
}

func (pg *TargetPostgreSQL) GetNonEmptyTables(tables []sqlname.NameTuple) []sqlname.NameTuple {
	result := []sqlname.NameTuple{}

	for _, table := range tables {
		log.Infof("checking if table %q is empty.", table)
		tmp := false
		stmt := fmt.Sprintf("SELECT TRUE FROM %s LIMIT 1;", table.ForUserQuery())
		err := pg.QueryRow(stmt).Scan(&tmp)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			utils.ErrExit("failed to check whether table %q empty: %s", table, err)
		}
		result = append(result, table)
	}
	log.Infof("non empty tables: %v", result)
	return result
}

func (pg *TargetPostgreSQL) ImportBatch(batch Batch, args *ImportBatchArgs, exportDir string, tableSchema map[string]map[string]string) (int64, error) {
	var rowsAffected int64
	var err error
	copyFn := func(conn *pgx.Conn) (bool, error) {
		rowsAffected, err = pg.importBatch(conn, batch, args)
		return false, err // Retries are now implemented in the caller.
	}
	err = pg.connPool.WithConn(copyFn)
	return rowsAffected, err
}

func (pg *TargetPostgreSQL) importBatch(conn *pgx.Conn, batch Batch, args *ImportBatchArgs) (rowsAffected int64, err error) {
	var file *os.File
	file, err = batch.Open()
	if err != nil {
		return 0, fmt.Errorf("open file %s: %w", batch.GetFilePath(), err)
	}
	defer file.Close()

	//setting the schema so that COPY command can acesss the table
	pg.setTargetSchema(conn)

	// NOTE: DO NOT DEFINE A NEW err VARIABLE IN THIS FUNCTION. ELSE, IT WILL MASK THE err FROM RETURN LIST.
	ctx := context.Background()
	var tx pgx.Tx
	tx, err = conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		var err2 error
		if err != nil {
			err2 = tx.Rollback(ctx)
			if err2 != nil {
				rowsAffected = 0
				err = fmt.Errorf("rollback txn: %w (while processing %s)", err2, err)
			}
		} else {
			err2 = tx.Commit(ctx)
			if err2 != nil {
				rowsAffected = 0
				err = fmt.Errorf("commit txn: %w", err2)
			}
		}
	}()

	// Check if the split is already imported.
	var alreadyImported bool
	alreadyImported, rowsAffected, err = pg.isBatchAlreadyImported(tx, batch)
	if err != nil {
		return 0, err
	}
	if alreadyImported {
		return rowsAffected, nil
	}

	// Import the split using COPY command.
	var res pgconn.CommandTag
	copyCommand := args.GetPGCopyStatement()
	log.Infof("Importing %q using COPY command: [%s]", batch.GetFilePath(), copyCommand)
	res, err = tx.Conn().PgConn().CopyFrom(context.Background(), file, copyCommand)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			err = fmt.Errorf("%s, %s in %s", err.Error(), pgerr.Where, batch.GetFilePath())
		}
		return res.RowsAffected(), err
	}

	err = pg.recordEntryInDB(tx, batch, res.RowsAffected())
	if err != nil {
		err = fmt.Errorf("record entry in DB for batch %q: %w", batch.GetFilePath(), err)
	}
	return res.RowsAffected(), err
}

func (pg *TargetPostgreSQL) GetListOfTableAttributes(nt sqlname.NameTuple) ([]string, error) {
	var result []string
	sname, tname := nt.ForCatalogQuery()
	query := fmt.Sprintf(
		`SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name ILIKE '%s'`,
		sname, tname)
	rows, err := pg.Query(query)
	if err != nil {
		return nil, fmt.Errorf("run [%s] on target: %w", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		var colName string
		err = rows.Scan(&colName)
		if err != nil {
			return nil, fmt.Errorf("scan column name: %w", err)
		}
		result = append(result, colName)
	}
	return result, nil
}

func (pg *TargetPostgreSQL) IsNonRetryableCopyError(err error) bool {
	return err != nil && utils.ContainsAnySubstringFromSlice(NonRetryCopyErrors, err.Error()) // not retrying atleast on the syntax errors and unique constraint
}

func (pg *TargetPostgreSQL) RestoreSequences(sequencesLastVal map[string]int64) error {
	log.Infof("restoring sequences on target")
	batch := pgx.Batch{}
	restoreStmt := "SELECT pg_catalog.setval('%s', %d, true)"
	for sequenceName, lastValue := range sequencesLastVal {
		if lastValue == 0 {
			// TODO: can be valid for cases like cyclic sequences
			continue
		}
		// same function logic will work for sequences as well
		// sequenceName, err := pg.qualifyTableName(sequenceName)
		seqName, err := namereg.NameReg.LookupTableName(sequenceName)
		if err != nil {
			return fmt.Errorf("error looking up sequence name %q: %w", sequenceName, err)
		}
		sequenceName := seqName.ForUserQuery()
		log.Infof("restore sequence %s to %d", sequenceName, lastValue)
		batch.Queue(fmt.Sprintf(restoreStmt, sequenceName, lastValue))
	}

	err := pg.connPool.WithConn(func(conn *pgx.Conn) (retry bool, err error) {
		br := conn.SendBatch(context.Background(), &batch)
		for i := 0; i < batch.Len(); i++ {
			_, err := br.Exec()
			if err != nil {
				log.Errorf("error executing restore sequence stmt: %v", err)
				return false, fmt.Errorf("error executing restore sequence stmt: %w", err)
			}
		}
		if err := br.Close(); err != nil {
			log.Errorf("error closing batch: %v", err)
			return false, fmt.Errorf("error closing batch: %w", err)
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("error restoring sequences: %w", err)
	}
	return err
}

/*
TODO(future): figure out the sql error codes for prepared statements which have become invalid
and needs to be prepared again
*/
func (pg *TargetPostgreSQL) ExecuteBatch(migrationUUID uuid.UUID, batch *EventBatch) error {
	log.Infof("executing batch(%s) of %d events", batch.ID(), len(batch.Events))
	ybBatch := pgx.Batch{}
	stmtToPrepare := make(map[string]string)
	// processing batch events to convert into prepared or unprepared statements based on Op type
	for i := 0; i < len(batch.Events); i++ {
		event := batch.Events[i]
		if event.Op == "u" {
			stmt, err := event.GetSQLStmt(pg)
			if err != nil {
				return fmt.Errorf("get sql stmt: %w", err)
			}
			ybBatch.Queue(stmt)
		} else {
			stmt, err := event.GetPreparedSQLStmt(pg, pg.tconf.TargetDBType)
			if err != nil {
				return fmt.Errorf("get prepared sql stmt: %w", err)
			}
			params := event.GetParams()
			if _, ok := stmtToPrepare[stmt]; !ok {
				stmtToPrepare[event.GetPreparedStmtName()] = stmt
			}
			ybBatch.Queue(stmt, params...)
		}
	}

	err := pg.connPool.WithConn(func(conn *pgx.Conn) (retry bool, err error) {
		ctx := context.Background()
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return false, fmt.Errorf("error creating tx: %w", err)
		}
		defer func() {
			errRollBack := tx.Rollback(ctx)
			if errRollBack != nil && errRollBack != pgx.ErrTxClosed {
				log.Errorf("error rolling back tx for batch id (%s): %v", batch.ID(), err)
			}
		}()

		for name, stmt := range stmtToPrepare {
			err := pg.connPool.PrepareStatement(conn, name, stmt)
			if err != nil {
				log.Errorf("error preparing stmt(%q): %v", stmt, err)
				return false, fmt.Errorf("error preparing stmt: %w", err)
			}
		}
		var rowsAffectedInserts, rowsAffectedDeletes, rowsAffectedUpdates int64
		br := tx.SendBatch(ctx, &ybBatch)
		closeBatch := func() error {
			if closeErr := br.Close(); closeErr != nil {
				log.Errorf("error closing batch(%s): %v", batch.ID(), closeErr)
				return closeErr
			}
			return nil
		}
		for i := 0; i < len(batch.Events); i++ {
			res, err := br.Exec()
			if err != nil {
				log.Errorf("error executing stmt for event with vsn(%d) in batch(%s): %v", batch.Events[i].Vsn, batch.ID(), err)
				closeBatch()
				return false, fmt.Errorf("error executing stmt for event with vsn(%d): %v", batch.Events[i].Vsn, err)
			}
			switch true {
			case res.Insert():
				rowsAffectedInserts += res.RowsAffected()
			case res.Delete():
				rowsAffectedDeletes += res.RowsAffected()
			case res.Update():
				rowsAffectedUpdates += res.RowsAffected()
			}
			if res.RowsAffected() != 1 {
				log.Warnf("unexpected rows affected for event with vsn(%d) in batch(%s): %d", batch.Events[i].Vsn, batch.ID(), res.RowsAffected())
			}
		}
		err = closeBatch()
		if err != nil {
			return false, err
		}

		updateVsnQuery := batch.GetChannelMetadataUpdateQuery(migrationUUID)
		res, err := tx.Exec(context.Background(), updateVsnQuery)
		if err != nil || res.RowsAffected() == 0 {
			log.Errorf("error executing stmt for batch(%s): %v , rowsAffected: %v", batch.ID(), err, res.RowsAffected())
			return false, fmt.Errorf("failed to update vsn on target db via query-%s: %w, rowsAffected: %v",
				updateVsnQuery, err, res.RowsAffected())
		}
		log.Debugf("Updated event channel meta info with query = %s; rows Affected = %d", updateVsnQuery, res.RowsAffected())

		tableNames := batch.GetTableNames()
		for _, tableName := range tableNames {
			updateTableStatsQuery := batch.GetQueriesToUpdateEventStatsByTable(migrationUUID, tableName)
			res, err = tx.Exec(context.Background(), updateTableStatsQuery)
			if err != nil {
				log.Errorf("error executing stmt: %v, rowsAffected: %v", err, res.RowsAffected())
				return false, fmt.Errorf("failed to update table stats on target db via query-%s: %w, rowsAffected: %v",
					updateTableStatsQuery, err, res.RowsAffected())
			}
			if res.RowsAffected() == 0 {
				insertTableStatsQuery := batch.GetQueriesToInsertEventStatsByTable(migrationUUID, tableName)
				res, err = tx.Exec(context.Background(), insertTableStatsQuery)
				if err != nil {
					log.Errorf("error executing stmt: %v, rowsAffected: %v", err, res.RowsAffected())
					return false, fmt.Errorf("failed to insert table stats on target db via query-%s: %w, rowsAffected: %v",
						updateTableStatsQuery, err, res.RowsAffected())
				}
			}
			log.Debugf("Updated table stats meta info with query = %s; rows Affected = %d", updateTableStatsQuery, res.RowsAffected())
		}
		if err = tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction : %w", err)
		}

		logDiscrepancyInEventBatchIfAny(batch, rowsAffectedInserts, rowsAffectedDeletes, rowsAffectedUpdates)

		return false, err
	})
	if err != nil {
		return fmt.Errorf("error executing batch: %w", err)
	}

	// Idempotency considerations:
	// Note: Assuming PK column value is not changed via UPDATEs
	// INSERT: The connPool sets `yb_enable_upsert_mode to true`. Hence the insert will be
	// successful even if the row already exists.
	// DELETE does NOT fail if the row does not exist. Rows affected will be 0.
	// UPDATE statement does not fail if the row does not exist. Rows affected will be 0.

	return nil
}

func (pg *TargetPostgreSQL) setTargetSchema(conn *pgx.Conn) {
	setSchemaQuery := fmt.Sprintf("SET SEARCH_PATH TO %s", pg.tconf.Schema)
	_, err := conn.Exec(context.Background(), setSchemaQuery)
	if err != nil {
		utils.ErrExit("run query %q on target %q: %s", setSchemaQuery, pg.tconf.Host, err)
	}
}

func (pg *TargetPostgreSQL) isBatchAlreadyImported(tx pgx.Tx, batch Batch) (bool, int64, error) {
	var rowsImported int64
	query := batch.GetQueryIsBatchAlreadyImported()
	err := tx.QueryRow(context.Background(), query).Scan(&rowsImported)
	if err == nil {
		log.Infof("%v rows from %q are already imported", rowsImported, batch.GetFilePath())
		return true, rowsImported, nil
	}
	if err == pgx.ErrNoRows {
		log.Infof("%q is not imported yet", batch.GetFilePath())
		return false, 0, nil
	}
	return false, 0, fmt.Errorf("check if %s is already imported: %w", batch.GetFilePath(), err)
}

func (pg *TargetPostgreSQL) recordEntryInDB(tx pgx.Tx, batch Batch, rowsAffected int64) error {
	cmd := batch.GetQueryToRecordEntryInDB(rowsAffected)
	_, err := tx.Exec(context.Background(), cmd)
	if err != nil {
		return fmt.Errorf("insert into %s: %w", BATCH_METADATA_TABLE_NAME, err)
	}
	return nil
}

func (pg *TargetPostgreSQL) MaxBatchSizeInBytes() int64 {
	return 200 * 1024 * 1024 // 200 MB //TODO
}

func (pg *TargetPostgreSQL) GetIdentityColumnNamesForTable(tableNameTup sqlname.NameTuple, identityType string) ([]string, error) {
	sname, tname := tableNameTup.ForCatalogQuery()
	query := fmt.Sprintf(`SELECT column_name FROM information_schema.columns where table_schema='%s' AND
		table_name='%s' AND is_identity='YES' AND identity_generation='%s'`, sname, tname, identityType)
	log.Infof("query of identity(%s) columns for table(%s): %s", identityType, tableNameTup, query)
	var identityColumns []string
	err := pg.connPool.WithConn(func(conn *pgx.Conn) (bool, error) {
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			log.Errorf("querying identity(%s) columns: %v", identityType, err)
			return false, fmt.Errorf("querying identity(%s) columns: %w", identityType, err)
		}
		defer rows.Close()
		for rows.Next() {
			var colName string
			err = rows.Scan(&colName)
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

func (pg *TargetPostgreSQL) DisableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("disabling generated always as identity columns")
	return pg.alterColumns(tableColumnsMap, "SET GENERATED BY DEFAULT")
}

func (pg *TargetPostgreSQL) EnableGeneratedAlwaysAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("enabling generated always as identity columns")
	// pg automatically resumes the value for further inserts due to sequence attached
	return pg.alterColumns(tableColumnsMap, "SET GENERATED ALWAYS")
}

func (pg *TargetPostgreSQL) EnableGeneratedByDefaultAsIdentityColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string]) error {
	log.Infof("enabling generated by default as identity columns")
	return pg.alterColumns(tableColumnsMap, "SET GENERATED BY DEFAULT")
}

func (pg *TargetPostgreSQL) alterColumns(tableColumnsMap *utils.StructMap[sqlname.NameTuple, []string], alterAction string) error {
	log.Infof("altering columns for action %s", alterAction)
	return tableColumnsMap.IterKV(func(table sqlname.NameTuple, columns []string) (bool, error) {
		batch := pgx.Batch{}
		for _, column := range columns {
			query := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s %s`, table.ForUserQuery(), column, alterAction)
			batch.Queue(query)
		}

		err := pg.connPool.WithConn(func(conn *pgx.Conn) (bool, error) {
			br := conn.SendBatch(context.Background(), &batch)
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Errorf("executing query to alter columns for table(%s): %v", table.ForUserQuery(), err)
					return false, fmt.Errorf("executing query to alter columns for table(%s): %w", table.ForUserQuery(), err)
				}
			}
			if err := br.Close(); err != nil {
				log.Errorf("closing batch of queries to alter columns for table(%s): %v", table.ForUserQuery(), err)
				return false, fmt.Errorf("closing batch of queries to alter columns for table(%s): %w", table.ForUserQuery(), err)
			}
			return false, nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

func (pg *TargetPostgreSQL) isSchemaExists(schema string) bool {
	query := fmt.Sprintf("SELECT true FROM information_schema.schemata WHERE schema_name = '%s'", schema)
	return pg.isQueryResultNonEmpty(query)
}

func (pg *TargetPostgreSQL) isTableExists(tableNameTup sqlname.NameTuple) bool {
	schema, table := tableNameTup.ForCatalogQuery()
	query := fmt.Sprintf("SELECT true FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", schema, table)
	return pg.isQueryResultNonEmpty(query)
}

func (pg *TargetPostgreSQL) isQueryResultNonEmpty(query string) bool {
	rows, err := pg.Query(query)
	if err != nil {
		utils.ErrExit("error checking if query %s is empty: %v", query, err)
	}
	defer rows.Close()

	return rows.Next()
}

func (pg *TargetPostgreSQL) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("clearing migration state for migrationUUID: %s", migrationUUID)
	schema := BATCH_METADATA_TABLE_SCHEMA
	if !pg.isSchemaExists(schema) {
		log.Infof("schema %s does not exist, nothing to clear migration state", schema)
		return nil
	}

	// clean up all the tables in BATCH_METADATA_TABLE_SCHEMA for given migrationUUID
	tableNames := []string{BATCH_METADATA_TABLE_NAME, EVENT_CHANNELS_METADATA_TABLE_NAME, EVENTS_PER_TABLE_METADATA_TABLE_NAME} // replace with actual table names
	tables := []sqlname.NameTuple{}
	for _, tableName := range tableNames {
		parts := strings.Split(tableName, ".")
		objName := sqlname.NewObjectName(sqlname.POSTGRESQL, "", parts[0], parts[1])
		nt := sqlname.NameTuple{
			CurrentName: objName,
			SourceName:  objName,
			TargetName:  objName,
		}
		tables = append(tables, nt)
	}
	for _, table := range tables {
		if !pg.isTableExists(table) {
			log.Infof("table %s does not exist, nothing to clear migration state", table)
			continue
		}
		log.Infof("cleaning up table %s for migrationUUID=%s", table, migrationUUID)
		query := fmt.Sprintf("DELETE FROM %s WHERE migration_uuid = '%s'", table.ForUserQuery(), migrationUUID)
		_, err := pg.Exec(query)
		if err != nil {
			log.Errorf("error cleaning up table %s for migrationUUID=%s: %v", table, migrationUUID, err)
			return fmt.Errorf("error cleaning up table %s for migrationUUID=%s: %w", table, migrationUUID, err)
		}
	}

	nonEmptyTables := pg.GetNonEmptyTables(tables)
	if len(nonEmptyTables) != 0 {
		log.Infof("tables %v are not empty in schema %s", nonEmptyTables, schema)
		utils.PrintAndLog("removed the current migration state from the target DB. "+
			"But could not remove the schema '%s' as it still contains state of other migrations in '%s' database", schema, pg.tconf.DBName)
		return nil
	}
	utils.PrintAndLog("dropping schema %s", schema)
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)
	_, err := pg.Exec(query)
	if err != nil {
		log.Errorf("error dropping schema %s: %v", schema, err)
		return fmt.Errorf("error dropping schema %s: %w", schema, err)
	}

	return nil
}
