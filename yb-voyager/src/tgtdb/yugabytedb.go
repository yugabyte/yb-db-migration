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
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	tgtdbsuite "github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb/suites"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type TargetYugabyteDB struct {
	sync.Mutex
	tconf    *TargetConf
	conn_    *pgx.Conn
	connPool *ConnectionPool
}

func newTargetYugabyteDB(tconf *TargetConf) *TargetYugabyteDB {
	return &TargetYugabyteDB{tconf: tconf}
}

func (yb *TargetYugabyteDB) Query(query string) (Rows, error) {
	rows, err := yb.conn_.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("run query %q on target %q: %w", query, yb.tconf.Host, err)
	}
	return rows, nil
}

func (yb *TargetYugabyteDB) QueryRow(query string) Row {
	row := yb.conn_.QueryRow(context.Background(), query)
	return row
}

func (yb *TargetYugabyteDB) Exec(query string) (int64, error) {
	res, err := yb.conn_.Exec(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("run query %q on target %q: %w", query, yb.tconf.Host, err)
	}
	return res.RowsAffected(), nil
}

func (yb *TargetYugabyteDB) WithTx(fn func(tx Tx) error) error {
	tx, err := yb.conn_.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("begin transaction on target %q: %w", yb.tconf.Host, err)
	}
	defer tx.Rollback(context.Background())
	err = fn(&pgxTxToTgtdbTxAdapter{tx: tx})
	if err != nil {
		return err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return fmt.Errorf("commit transaction on target %q: %w", yb.tconf.Host, err)
	}
	return nil
}

func (yb *TargetYugabyteDB) Init() error {
	err := yb.connect()
	if err != nil {
		return err
	}

	checkSchemaExistsQuery := fmt.Sprintf(
		"SELECT count(schema_name) FROM information_schema.schemata WHERE schema_name = '%s'",
		yb.tconf.Schema)
	var cntSchemaName int
	if err = yb.conn_.QueryRow(context.Background(), checkSchemaExistsQuery).Scan(&cntSchemaName); err != nil {
		err = fmt.Errorf("run query %q on target %q to check schema exists: %s", checkSchemaExistsQuery, yb.tconf.Host, err)
	} else if cntSchemaName == 0 {
		err = fmt.Errorf("schema '%s' does not exist in target", yb.tconf.Schema)
	}
	return err
}

func (yb *TargetYugabyteDB) Finalize() {
	yb.disconnect()
}

// TODO We should not export `Conn`. This is temporary--until we refactor all target db access.
func (yb *TargetYugabyteDB) Conn() *pgx.Conn {
	if yb.conn_ == nil {
		utils.ErrExit("Called TargetDB.Conn() before TargetDB.Connect()")
	}
	return yb.conn_
}

func (yb *TargetYugabyteDB) reconnect() error {
	yb.Mutex.Lock()
	defer yb.Mutex.Unlock()

	var err error
	yb.disconnect()
	for attempt := 1; attempt < 5; attempt++ {
		err = yb.connect()
		if err == nil {
			return nil
		}
		log.Infof("Failed to reconnect to the target database: %s", err)
		time.Sleep(time.Duration(attempt*2) * time.Second)
		// Retry.
	}
	return fmt.Errorf("reconnect to target db: %w", err)
}

func (yb *TargetYugabyteDB) connect() error {
	if yb.conn_ != nil {
		// Already connected.
		return nil
	}
	connStr := yb.tconf.GetConnectionUri()
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("connect to target db: %w", err)
	}
	yb.setTargetSchema(conn)
	yb.conn_ = conn
	return nil
}

func (yb *TargetYugabyteDB) disconnect() {
	if yb.conn_ == nil {
		// Already disconnected.
		return
	}

	err := yb.conn_.Close(context.Background())
	if err != nil {
		log.Infof("Failed to close connection to the target database: %s", err)
	}
	yb.conn_ = nil
}

func (yb *TargetYugabyteDB) EnsureConnected() {
	err := yb.connect()
	if err != nil {
		utils.ErrExit("Failed to connect to the target DB: %s", err)
	}
}

func (yb *TargetYugabyteDB) GetVersion() string {
	if yb.tconf.DBVersion != "" {
		return yb.tconf.DBVersion
	}

	yb.EnsureConnected()
	yb.Mutex.Lock()
	defer yb.Mutex.Unlock()
	query := "SELECT setting FROM pg_settings WHERE name = 'server_version'"
	err := yb.conn_.QueryRow(context.Background(), query).Scan(&yb.tconf.DBVersion)
	if err != nil {
		utils.ErrExit("get target db version: %s", err)
	}
	return yb.tconf.DBVersion
}

func (yb *TargetYugabyteDB) PrepareForStreaming() {
	log.Infof("Preparing target DB for streaming - disable throttling")
	yb.connPool.DisableThrottling()
}

func (yb *TargetYugabyteDB) InitConnPool() error {
	tconfs := yb.getYBServers()
	var targetUriList []string
	for _, tconf := range tconfs {
		targetUriList = append(targetUriList, tconf.Uri)
	}
	log.Infof("targetUriList: %s", utils.GetRedactedURLs(targetUriList))

	if yb.tconf.Parallelism == 0 {
		yb.tconf.Parallelism = fetchDefaultParallelJobs(tconfs, YB_DEFAULT_PARALLELISM_FACTOR)
		log.Infof("Using %d parallel jobs by default. Use --parallel-jobs to specify a custom value", yb.tconf.Parallelism)
	}

	params := &ConnectionParams{
		NumConnections:    yb.tconf.Parallelism,
		ConnUriList:       targetUriList,
		SessionInitScript: getYBSessionInitScript(yb.tconf),
	}
	yb.connPool = NewConnectionPool(params)
	return nil
}

// The _v2 is appended in the table name so that the import code doesn't
// try to use the similar table created by the voyager 1.3 and earlier.
// Voyager 1.4 uses import data state format that is incompatible from
// the earlier versions.
const BATCH_METADATA_TABLE_SCHEMA = "ybvoyager_metadata"
const BATCH_METADATA_TABLE_NAME = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_import_data_batches_metainfo_v3"
const EVENT_CHANNELS_METADATA_TABLE_NAME = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_import_data_event_channels_metainfo"
const EVENTS_PER_TABLE_METADATA_TABLE_NAME = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_imported_event_count_by_table"
const YB_DEFAULT_PARALLELISM_FACTOR = 2 // factor for default parallelism in case fetchDefaultParallelJobs() is not able to get the no of cores

func (yb *TargetYugabyteDB) CreateVoyagerSchema() error {
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
			conn := yb.Conn()
			_, err = conn.Exec(context.Background(), cmd)
			if err == nil {
				// No error. Move on to the next command.
				continue outer
			}
			log.Warnf("Error while running [%s] attempt %d: %s", cmd, attempt, err)
			time.Sleep(5 * time.Second)
			err2 := yb.reconnect()
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

func (yb *TargetYugabyteDB) qualifyTableName(tableName string) string {
	if len(strings.Split(tableName, ".")) != 2 {
		tableName = fmt.Sprintf("%s.%s", yb.tconf.Schema, tableName)
	}
	return tableName
}

func (yb *TargetYugabyteDB) GetNonEmptyTables(tables []string) []string {
	result := []string{}

	for _, table := range tables {
		log.Infof("checking if table %q is empty.", table)
		tmp := false
		stmt := fmt.Sprintf("SELECT TRUE FROM %s LIMIT 1;", table)
		err := yb.Conn().QueryRow(context.Background(), stmt).Scan(&tmp)
		if err == pgx.ErrNoRows {
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

func (yb *TargetYugabyteDB) ImportBatch(batch Batch, args *ImportBatchArgs, exportDir string, tableSchema map[string]map[string]string) (int64, error) {
	var rowsAffected int64
	var err error
	copyFn := func(conn *pgx.Conn) (bool, error) {
		rowsAffected, err = yb.importBatch(conn, batch, args)
		return false, err // Retries are now implemented in the caller.
	}
	err = yb.connPool.WithConn(copyFn)
	return rowsAffected, err
}

func (yb *TargetYugabyteDB) importBatch(conn *pgx.Conn, batch Batch, args *ImportBatchArgs) (rowsAffected int64, err error) {
	var file *os.File
	file, err = batch.Open()
	if err != nil {
		return 0, fmt.Errorf("open file %s: %w", batch.GetFilePath(), err)
	}
	defer file.Close()

	//setting the schema so that COPY command can acesss the table
	yb.setTargetSchema(conn)

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
	alreadyImported, rowsAffected, err = yb.isBatchAlreadyImported(tx, batch)
	if err != nil {
		return 0, err
	}
	if alreadyImported {
		return rowsAffected, nil
	}

	// Import the split using COPY command.
	var res pgconn.CommandTag
	copyCommand := args.GetYBCopyStatement()
	log.Infof("Importing %q using COPY command: [%s]", batch.GetFilePath(), copyCommand)
	res, err = tx.Conn().PgConn().CopyFrom(context.Background(), file, copyCommand)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			err = fmt.Errorf("%s, %s in %s", err.Error(), pgerr.Where, batch.GetFilePath())
		}
		return res.RowsAffected(), err
	}

	err = yb.recordEntryInDB(tx, batch, res.RowsAffected())
	if err != nil {
		err = fmt.Errorf("record entry in DB for batch %q: %w", batch.GetFilePath(), err)
	}
	return res.RowsAffected(), err
}

func (yb *TargetYugabyteDB) IfRequiredQuoteColumnNames(tableName string, columns []string) ([]string, error) {
	result := make([]string, len(columns))
	// FAST PATH.
	fastPathSuccessful := true
	for i, colName := range columns {
		if strings.ToLower(colName) == colName {
			if sqlname.IsReservedKeywordPG(colName) && colName[0:1] != `"` {
				result[i] = fmt.Sprintf(`"%s"`, colName)
			} else {
				result[i] = colName
			}
		} else {
			// Go to slow path.
			log.Infof("column name (%s) is not all lower-case. Going to slow path.", colName)
			result = make([]string, len(columns))
			fastPathSuccessful = false
			break
		}
	}
	if fastPathSuccessful {
		log.Infof("FAST PATH: columns of table %s after quoting: %v", tableName, result)
		return result, nil
	}
	// SLOW PATH.
	var schemaName string
	schemaName, tableName = yb.splitMaybeQualifiedTableName(tableName)
	targetColumns, err := yb.getListOfTableAttributes(schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("get list of table attributes: %w", err)
	}
	log.Infof("columns of table %s.%s in target db: %v", schemaName, tableName, targetColumns)

	for i, colName := range columns {
		if colName[0] == '"' && colName[len(colName)-1] == '"' {
			colName = colName[1 : len(colName)-1]
		}
		switch true {
		// TODO: Move sqlname.IsReservedKeyword() in this file.
		case sqlname.IsReservedKeywordPG(colName):
			result[i] = fmt.Sprintf(`"%s"`, colName)
		case colName == strings.ToLower(colName): // Name is all lowercase.
			result[i] = colName
		case slices.Contains(targetColumns, colName): // Name is not keyword and is not all lowercase.
			result[i] = fmt.Sprintf(`"%s"`, colName)
		case slices.Contains(targetColumns, strings.ToLower(colName)): // Case insensitive name given with mixed case.
			result[i] = strings.ToLower(colName)
		default:
			return nil, fmt.Errorf("column %q not found in table %s", colName, tableName)
		}
	}
	log.Infof("columns of table %s.%s after quoting: %v", schemaName, tableName, result)
	return result, nil
}

func (yb *TargetYugabyteDB) getListOfTableAttributes(schemaName, tableName string) ([]string, error) {
	var result []string
	if tableName[0] == '"' {
		// Remove the double quotes around the table name.
		tableName = tableName[1 : len(tableName)-1]
	}
	query := fmt.Sprintf(
		`SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name ILIKE '%s'`,
		schemaName, tableName)
	rows, err := yb.Conn().Query(context.Background(), query)
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

var NonRetryCopyErrors = []string{
	"invalid input syntax",
	"violates unique constraint",
	"syntax error at",
}

func (yb *TargetYugabyteDB) IsNonRetryableCopyError(err error) bool {
	NonRetryCopyErrorsYB := NonRetryCopyErrors
	NonRetryCopyErrorsYB = append(NonRetryCopyErrorsYB, "Sending too long RPC message")
	return err != nil && utils.ContainsAnySubstringFromSlice(NonRetryCopyErrorsYB, err.Error())
}

func (yb *TargetYugabyteDB) RestoreSequences(sequencesLastVal map[string]int64) error {
	log.Infof("restoring sequences on target")
	batch := pgx.Batch{}
	restoreStmt := "SELECT pg_catalog.setval('%s', %d, true)"
	for sequenceName, lastValue := range sequencesLastVal {
		if lastValue == 0 {
			// TODO: can be valid for cases like cyclic sequences
			continue
		}
		// same function logic will work for sequences as well
		sequenceName = yb.qualifyTableName(sequenceName)
		log.Infof("restore sequence %s to %d", sequenceName, lastValue)
		batch.Queue(fmt.Sprintf(restoreStmt, sequenceName, lastValue))
	}

	err := yb.connPool.WithConn(func(conn *pgx.Conn) (retry bool, err error) {
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
func (yb *TargetYugabyteDB) ExecuteBatch(migrationUUID uuid.UUID, batch *EventBatch) error {
	log.Infof("executing batch of %d events", len(batch.Events))
	ybBatch := pgx.Batch{}
	stmtToPrepare := make(map[string]string)
	// processing batch events to convert into prepared or unprepared statements based on Op type
	for i := 0; i < len(batch.Events); i++ {
		event := batch.Events[i]
		if event.Op == "u" {
			stmt := event.GetSQLStmt()
			ybBatch.Queue(stmt)
		} else {
			stmt := event.GetPreparedSQLStmt(yb.tconf.TargetDBType)

			params := event.GetParams()
			if _, ok := stmtToPrepare[stmt]; !ok {
				stmtToPrepare[event.GetPreparedStmtName()] = stmt
			}
			ybBatch.Queue(stmt, params...)
		}
	}

	err := yb.connPool.WithConn(func(conn *pgx.Conn) (retry bool, err error) {
		ctx := context.Background()
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return false, fmt.Errorf("error creating tx: %w", err)
		}
		defer tx.Rollback(ctx)

		for name, stmt := range stmtToPrepare {
			err := yb.connPool.PrepareStatement(conn, name, stmt)
			if err != nil {
				log.Errorf("error preparing stmt(%q): %v", stmt, err)
				return false, fmt.Errorf("error preparing stmt: %w", err)
			}
		}

		br := conn.SendBatch(ctx, &ybBatch)
		for i := 0; i < len(batch.Events); i++ {
			_, err := br.Exec()
			if err != nil {
				log.Errorf("error executing stmt for event with vsn(%d): %v", batch.Events[i].Vsn, err)
				return false, fmt.Errorf("error executing stmt for event with vsn(%d): %v", batch.Events[i].Vsn, err)
			}
		}
		if err = br.Close(); err != nil {
			log.Errorf("error closing batch: %v", err)
			return false, fmt.Errorf("error closing batch: %v", err)
		}

		updateVsnQuery := batch.GetChannelMetadataUpdateQuery(migrationUUID)
		res, err := tx.Exec(context.Background(), updateVsnQuery)
		if err != nil || res.RowsAffected() == 0 {
			log.Errorf("error executing stmt: %v, rowsAffected: %v", err, res.RowsAffected())
			return false, fmt.Errorf("failed to update vsn on target db via query-%s: %w, rowsAffected: %v",
				updateVsnQuery, err, res.RowsAffected())
		}
		log.Debugf("Updated event channel meta info with query = %s; rows Affected = %d", updateVsnQuery, res.RowsAffected())

		tableNames := batch.GetTableNames()
		for _, tableName := range tableNames {
			tableName := yb.qualifyTableName(tableName)
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

//==============================================================================

const (
	LB_WARN_MSG = "--target-db-host is a load balancer IP which will be used to create connections for data import.\n" +
		"\t To control the parallelism and servers used, refer to help for --parallel-jobs and --target-endpoints flags.\n"

	GET_YB_SERVERS_QUERY = "SELECT host, port, num_connections, node_type, cloud, region, zone, public_ip FROM yb_servers()"
)

func (yb *TargetYugabyteDB) getYBServers() []*TargetConf {
	var tconfs []*TargetConf
	var loadBalancerUsed bool

	tconf := yb.tconf

	if tconf.TargetEndpoints != "" {
		msg := fmt.Sprintf("given yb-servers for import data: %q\n", tconf.TargetEndpoints)
		log.Infof(msg)

		ybServers := utils.CsvStringToSlice(tconf.TargetEndpoints)
		for _, ybServer := range ybServers {
			clone := tconf.Clone()

			if strings.Contains(ybServer, ":") {
				clone.Host = strings.Split(ybServer, ":")[0]
				var err error
				clone.Port, err = strconv.Atoi(strings.Split(ybServer, ":")[1])

				if err != nil {
					utils.ErrExit("error in parsing useYbServers flag: %v", err)
				}
			} else {
				clone.Host = ybServer
			}

			clone.Uri = getCloneConnectionUri(clone)
			log.Infof("using yb server for import data: %+v", GetRedactedTargetConf(clone))
			tconfs = append(tconfs, clone)
		}
	} else {
		loadBalancerUsed = true
		url := tconf.GetConnectionUri()
		conn, err := pgx.Connect(context.Background(), url)
		if err != nil {
			utils.ErrExit("Unable to connect to database: %v", err)
		}
		defer conn.Close(context.Background())

		rows, err := conn.Query(context.Background(), GET_YB_SERVERS_QUERY)
		if err != nil {
			utils.ErrExit("error in query rows from yb_servers(): %v", err)
		}
		defer rows.Close()

		var hostPorts []string
		for rows.Next() {
			clone := tconf.Clone()
			var host, nodeType, cloud, region, zone, public_ip string
			var port, num_conns int
			if err := rows.Scan(&host, &port, &num_conns,
				&nodeType, &cloud, &region, &zone, &public_ip); err != nil {
				utils.ErrExit("error in scanning rows of yb_servers(): %v", err)
			}

			// check if given host is one of the server in cluster
			if loadBalancerUsed {
				if isSeedTargetHost(tconf, host, public_ip) {
					loadBalancerUsed = false
				}
			}

			if tconf.UsePublicIP {
				if public_ip != "" {
					clone.Host = public_ip
				} else {
					var msg string
					if host == "" {
						msg = fmt.Sprintf("public ip is not available for host: %s."+
							"Refer to help for more details for how to enable public ip.", host)
					} else {
						msg = fmt.Sprintf("public ip is not available for host: %s but private ip are available. "+
							"Either refer to help for how to enable public ip or remove --use-public-up flag and restart the import", host)
					}
					utils.ErrExit(msg)
				}
			} else {
				clone.Host = host
			}

			clone.Port = port
			clone.Uri = getCloneConnectionUri(clone)
			tconfs = append(tconfs, clone)

			hostPorts = append(hostPorts, fmt.Sprintf("%s:%v", host, port))
		}
		log.Infof("Target DB nodes: %s", strings.Join(hostPorts, ","))
	}

	if loadBalancerUsed { // if load balancer is used no need to check direct connectivity
		utils.PrintAndLog(LB_WARN_MSG)
		tconfs = []*TargetConf{tconf}
	} else {
		tconfs = testAndFilterYbServers(tconfs)
	}
	return tconfs
}

func getCloneConnectionUri(clone *TargetConf) string {
	var cloneConnectionUri string
	if clone.Uri == "" {
		//fallback to constructing the URI from individual parameters. If URI was not set for target, then its other necessary parameters must be non-empty (or default values)
		cloneConnectionUri = clone.GetConnectionUri()
	} else {
		targetConnectionUri, err := url.Parse(clone.Uri)
		if err == nil {
			targetConnectionUri.Host = fmt.Sprintf("%s:%d", clone.Host, clone.Port)
			cloneConnectionUri = fmt.Sprint(targetConnectionUri)
		} else {
			panic(err)
		}
	}
	return cloneConnectionUri
}

func isSeedTargetHost(tconf *TargetConf, names ...string) bool {
	var allIPs []string
	for _, name := range names {
		if name != "" {
			allIPs = append(allIPs, utils.LookupIP(name)...)
		}
	}

	seedHostIPs := utils.LookupIP(tconf.Host)
	for _, seedHostIP := range seedHostIPs {
		if slices.Contains(allIPs, seedHostIP) {
			log.Infof("Target.Host=%s matched with one of ips in %v\n", seedHostIP, allIPs)
			return true
		}
	}
	return false
}

// this function will check the reachability to each of the nodes and returns list of ones which are reachable
func testAndFilterYbServers(tconfs []*TargetConf) []*TargetConf {
	var availableTargets []*TargetConf

	for _, tconf := range tconfs {
		log.Infof("testing server: %s\n", spew.Sdump(GetRedactedTargetConf(tconf)))
		conn, err := pgx.Connect(context.Background(), tconf.GetConnectionUri())
		if err != nil {
			utils.PrintAndLog("unable to use yb-server %q: %v", tconf.Host, err)
		} else {
			availableTargets = append(availableTargets, tconf)
			conn.Close(context.Background())
		}
	}

	if len(availableTargets) == 0 {
		utils.ErrExit("no yb servers available for data import")
	}
	return availableTargets
}

func fetchDefaultParallelJobs(tconfs []*TargetConf, defaultParallelismFactor int) int {
	totalCores := 0
	targetCores := 0
	for _, tconf := range tconfs {
		log.Infof("Determining CPU core count on: %s", utils.GetRedactedURLs([]string{tconf.Uri})[0])
		conn, err := pgx.Connect(context.Background(), tconf.Uri)
		if err != nil {
			log.Warnf("Unable to reach target while querying cores: %v", err)
			return len(tconfs) * defaultParallelismFactor
		}
		defer conn.Close(context.Background())

		cmd := "CREATE TEMP TABLE yb_voyager_cores(num_cores int);"
		_, err = conn.Exec(context.Background(), cmd)
		if err != nil {
			log.Warnf("Unable to create tables on target DB: %v", err)
			return len(tconfs) * defaultParallelismFactor
		}

		cmd = "COPY yb_voyager_cores(num_cores) FROM PROGRAM 'grep processor /proc/cpuinfo|wc -l';"
		_, err = conn.Exec(context.Background(), cmd)
		if err != nil {
			log.Warnf("Error while running query %s on host %s: %v", cmd, utils.GetRedactedURLs([]string{tconf.Uri}), err)
			return len(tconfs) * defaultParallelismFactor
		}

		cmd = "SELECT num_cores FROM yb_voyager_cores;"
		if err = conn.QueryRow(context.Background(), cmd).Scan(&targetCores); err != nil {
			log.Warnf("Error while running query %s: %v", cmd, err)
			return len(tconfs) * defaultParallelismFactor
		}
		totalCores += targetCores
	}
	if totalCores == 0 { //if target is running on MacOS, we are unable to determine totalCores
		return 3
	}
	if tconfs[0].TargetDBType == YUGABYTEDB {
		return totalCores / 4
	}
	return totalCores / 2
}

// import session parameters
const (
	SET_CLIENT_ENCODING_TO_UTF8           = "SET client_encoding TO 'UTF8'"
	SET_SESSION_REPLICATE_ROLE_TO_REPLICA = "SET session_replication_role TO replica" //Disable triggers or fkeys constraint checks.
	SET_YB_ENABLE_UPSERT_MODE             = "SET yb_enable_upsert_mode to true"
	SET_YB_DISABLE_TRANSACTIONAL_WRITES   = "SET yb_disable_transactional_writes to true" // Disable transactions to improve ingestion throughput.
)

func getYBSessionInitScript(tconf *TargetConf) []string {
	var sessionVars []string
	if checkSessionVariableSupport(tconf, SET_CLIENT_ENCODING_TO_UTF8) {
		sessionVars = append(sessionVars, SET_CLIENT_ENCODING_TO_UTF8)
	}
	if checkSessionVariableSupport(tconf, SET_SESSION_REPLICATE_ROLE_TO_REPLICA) {
		sessionVars = append(sessionVars, SET_SESSION_REPLICATE_ROLE_TO_REPLICA)
	}

	if tconf.EnableUpsert {
		// upsert_mode parameters was introduced later than yb_disable_transactional writes in yb releases
		// hence if upsert_mode is supported then its safe to assume yb_disable_transactional_writes is already there
		if checkSessionVariableSupport(tconf, SET_YB_ENABLE_UPSERT_MODE) {
			sessionVars = append(sessionVars, SET_YB_ENABLE_UPSERT_MODE)
			// 	SET_YB_DISABLE_TRANSACTIONAL_WRITES is used only with & if upsert_mode is supported
			if tconf.DisableTransactionalWrites {
				if checkSessionVariableSupport(tconf, SET_YB_DISABLE_TRANSACTIONAL_WRITES) {
					sessionVars = append(sessionVars, SET_YB_DISABLE_TRANSACTIONAL_WRITES)
				} else {
					tconf.DisableTransactionalWrites = false
				}
			}
		} else {
			log.Infof("Falling back to transactional inserts of batches during data import")
		}
	}

	sessionVarsPath := "/etc/yb-voyager/ybSessionVariables.sql"
	if !utils.FileOrFolderExists(sessionVarsPath) {
		log.Infof("YBSessionInitScript: %v\n", sessionVars)
		return sessionVars
	}

	varsFile, err := os.Open(sessionVarsPath)
	if err != nil {
		utils.PrintAndLog("Unable to open %s : %v. Using default values.", sessionVarsPath, err)
		log.Infof("YBSessionInitScript: %v\n", sessionVars)
		return sessionVars
	}
	defer varsFile.Close()
	fileScanner := bufio.NewScanner(varsFile)

	var curLine string
	for fileScanner.Scan() {
		curLine = strings.TrimSpace(fileScanner.Text())
		if curLine != "" && checkSessionVariableSupport(tconf, curLine) {
			sessionVars = append(sessionVars, curLine)
		}
	}
	log.Infof("YBSessionInitScript: %v\n", sessionVars)
	return sessionVars
}

func checkSessionVariableSupport(tconf *TargetConf, sqlStmt string) bool {
	conn, err := pgx.Connect(context.Background(), tconf.GetConnectionUri())
	if err != nil {
		utils.ErrExit("error while creating connection for checking session parameter(%q) support: %v", sqlStmt, err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), sqlStmt)
	if err != nil {
		if !strings.Contains(err.Error(), "unrecognized configuration parameter") {
			utils.ErrExit("error while executing sqlStatement=%q: %v", sqlStmt, err)
		} else {
			log.Warnf("Warning: %q is not supported: %v", sqlStmt, err)
		}
	}

	return err == nil
}

func (yb *TargetYugabyteDB) setTargetSchema(conn *pgx.Conn) {
	setSchemaQuery := fmt.Sprintf("SET SCHEMA '%s'", yb.tconf.Schema)
	_, err := conn.Exec(context.Background(), setSchemaQuery)
	if err != nil {
		utils.ErrExit("run query %q on target %q: %s", setSchemaQuery, yb.tconf.Host, err)
	}

	// append oracle schema in the search_path for orafce
	// It is okay even if the schema does not exist in the target.
	updateSearchPath := `SELECT set_config('search_path', current_setting('search_path') || ', oracle', false)`
	_, err = conn.Exec(context.Background(), updateSearchPath)
	if err != nil {
		utils.ErrExit("unable to update search_path for orafce extension: %v", err)
	}

}

func (yb *TargetYugabyteDB) getTargetSchemaName(tableName string) string {
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		return parts[0]
	}
	return yb.tconf.Schema // default set to "public"
}

func (yb *TargetYugabyteDB) isBatchAlreadyImported(tx pgx.Tx, batch Batch) (bool, int64, error) {
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

func (yb *TargetYugabyteDB) recordEntryInDB(tx pgx.Tx, batch Batch, rowsAffected int64) error {
	cmd := batch.GetQueryToRecordEntryInDB(rowsAffected)
	_, err := tx.Exec(context.Background(), cmd)
	if err != nil {
		return fmt.Errorf("insert into %s: %w", BATCH_METADATA_TABLE_NAME, err)
	}
	return nil
}

func (yb *TargetYugabyteDB) GetDebeziumValueConverterSuite() map[string]tgtdbsuite.ConverterFn {
	return tgtdbsuite.YBValueConverterSuite
}

func (yb *TargetYugabyteDB) MaxBatchSizeInBytes() int64 {
	return 200 * 1024 * 1024 // 200 MB
}

func (yb *TargetYugabyteDB) GetIdentityColumnNamesForTable(table string, identityType string) ([]string, error) {
	schema := yb.getTargetSchemaName(table)
	// TODO: handle case-sensitivity correctly
	if utils.IsQuotedString(table) {
		table = table[1 : len(table)-1]
	} else {
		table = strings.ToLower(table)
	}
	query := fmt.Sprintf(`SELECT column_name FROM information_schema.columns where table_schema='%s' AND
		table_name='%s' AND is_identity='YES' AND identity_generation='%s'`, schema, table, identityType)
	log.Infof("query of identity(%s) columns for table(%s): %s", identityType, table, query)
	var identityColumns []string
	err := yb.connPool.WithConn(func(conn *pgx.Conn) (bool, error) {
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

func (yb *TargetYugabyteDB) DisableGeneratedAlwaysAsIdentityColumns(tableColumnsMap map[string][]string) error {
	log.Infof("disabling generated always as identity columns")
	return yb.alterColumns(tableColumnsMap, "SET GENERATED BY DEFAULT")
}

func (yb *TargetYugabyteDB) EnableGeneratedAlwaysAsIdentityColumns(tableColumnsMap map[string][]string) error {
	log.Infof("enabling generated always as identity columns")
	// YB automatically resumes the value for further inserts due to sequence attached
	return yb.alterColumns(tableColumnsMap, "SET GENERATED ALWAYS")
}

func (yb *TargetYugabyteDB) EnableGeneratedByDefaultAsIdentityColumns(tableColumnsMap map[string][]string) error {
	log.Infof("enabling generated by default as identity columns")
	return yb.alterColumns(tableColumnsMap, "SET GENERATED BY DEFAULT")
}

const ybQueryTmplForUniqCols = `
SELECT tc.table_schema, tc.table_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
	AND tc.table_schema = kcu.table_schema
    AND tc.table_name = kcu.table_name
WHERE tc.table_schema = ANY('{%s}') AND tc.table_name = ANY('{%s}') AND tc.constraint_type = 'UNIQUE';
`

func (yb *TargetYugabyteDB) GetTableToUniqueKeyColumnsMap(tableList []string) (map[string][]string, error) {
	log.Infof("getting unique key columns for tables: %v", tableList)
	result := make(map[string][]string)
	var querySchemaList, queryTableList []string
	for i := 0; i < len(tableList); i++ {
		schema, table := yb.splitMaybeQualifiedTableName(tableList[i])
		querySchemaList = append(querySchemaList, schema)
		queryTableList = append(queryTableList, table)
	}

	querySchemaList = lo.Uniq(querySchemaList)
	query := fmt.Sprintf(ybQueryTmplForUniqCols, strings.Join(querySchemaList, ","), strings.Join(queryTableList, ","))
	log.Infof("query to get unique key columns: %s", query)
	rows, err := yb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying unique key columns: %w", err)
	}
	defer rows.Close()

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

func (yb *TargetYugabyteDB) alterColumns(tableColumnsMap map[string][]string, alterAction string) error {
	log.Infof("altering columns for action %s", alterAction)
	for table, columns := range tableColumnsMap {
		qualifiedTableName := yb.qualifyTableName(table)
		batch := pgx.Batch{}
		for _, column := range columns {
			query := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s %s`, qualifiedTableName, column, alterAction)
			batch.Queue(query)
		}

		err := yb.connPool.WithConn(func(conn *pgx.Conn) (bool, error) {
			br := conn.SendBatch(context.Background(), &batch)
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					log.Errorf("executing query to alter columns for table(%s): %v", qualifiedTableName, err)
					return false, fmt.Errorf("executing query to alter columns for table(%s): %w", qualifiedTableName, err)
				}
			}
			if err := br.Close(); err != nil {
				log.Errorf("closing batch of queries to alter columns for table(%s): %v", qualifiedTableName, err)
				return false, fmt.Errorf("closing batch of queries to alter columns for table(%s): %w", qualifiedTableName, err)
			}
			return false, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (yb *TargetYugabyteDB) splitMaybeQualifiedTableName(tableName string) (string, string) {
	if strings.Contains(tableName, ".") {
		parts := strings.Split(tableName, ".")
		return parts[0], parts[1]
	}
	return yb.tconf.Schema, tableName
}

func (yb *TargetYugabyteDB) isSchemaExists(schema string) bool {
	query := fmt.Sprintf("SELECT true FROM information_schema.schemata WHERE schema_name = '%s'", schema)
	return yb.isQueryResultNonEmpty(query)
}

func (yb *TargetYugabyteDB) isTableExists(qualifiedTableName string) bool {
	schema, table := yb.splitMaybeQualifiedTableName(qualifiedTableName)
	query := fmt.Sprintf("SELECT true FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", schema, table)
	return yb.isQueryResultNonEmpty(query)
}

func (yb *TargetYugabyteDB) isQueryResultNonEmpty(query string) bool {
	rows, err := yb.Query(query)
	if err != nil {
		utils.ErrExit("error checking if query %s is empty: %v", query, err)
	}
	defer rows.Close()

	return rows.Next()
}

func (yb *TargetYugabyteDB) ClearMigrationState(migrationUUID uuid.UUID, exportDir string) error {
	log.Infof("clearing migration state for migrationUUID: %s", migrationUUID)
	schema := BATCH_METADATA_TABLE_SCHEMA
	if !yb.isSchemaExists(schema) {
		log.Infof("schema %s does not exist, nothing to clear migration state", schema)
		return nil
	}

	// clean up all the tables in BATCH_METADATA_TABLE_SCHEMA for given migrationUUID
	tables := []string{BATCH_METADATA_TABLE_NAME, EVENT_CHANNELS_METADATA_TABLE_NAME, EVENTS_PER_TABLE_METADATA_TABLE_NAME} // replace with actual table names
	for _, table := range tables {
		if !yb.isTableExists(table) {
			log.Infof("table %s does not exist, nothing to clear migration state", table)
			continue
		}
		log.Infof("cleaning up table %s for migrationUUID=%s", table, migrationUUID)
		query := fmt.Sprintf("DELETE FROM %s WHERE migration_uuid = '%s'", table, migrationUUID)
		_, err := yb.Exec(query)
		if err != nil {
			log.Errorf("error cleaning up table %s for migrationUUID=%s: %v", table, migrationUUID, err)
			return fmt.Errorf("error cleaning up table %s for migrationUUID=%s: %w", table, migrationUUID, err)
		}
	}

	nonEmptyTables := yb.GetNonEmptyTables(tables)
	if len(nonEmptyTables) != 0 {
		log.Infof("tables %v are not empty in schema %s", nonEmptyTables, schema)
		utils.PrintAndLog("removed the current migration state from the target DB. "+
			"But could not remove the schema '%s' as it still contains state of other migrations in '%s' database", schema, yb.tconf.DBName)
		return nil
	}
	utils.PrintAndLog("dropping schema %s", schema)
	query := fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)
	_, err := yb.conn_.Exec(context.Background(), query)
	if err != nil {
		log.Errorf("error dropping schema %s: %v", schema, err)
		return fmt.Errorf("error dropping schema %s: %w", schema, err)
	}

	return nil
}
