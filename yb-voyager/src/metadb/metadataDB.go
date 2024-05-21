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
package metadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var (
	QUEUE_SEGMENT_META_TABLE_NAME              = "queue_segment_meta"
	EXPORTED_EVENTS_STATS_TABLE_NAME           = "exported_events_stats"
	EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME = "exported_events_stats_per_table"
	JSON_OBJECTS_TABLE_NAME                    = "json_objects"
	TARGET_DB_IDENTITY_COLUMNS_KEY             = "target_db_identity_columns_key"
	FF_DB_IDENTITY_COLUMNS_KEY                 = "ff_db_identity_columns_key"
	SOURCE_INDEXES_INFO_KEY                    = "source_indexes_info_key"
	TABLE_TO_UNIQUE_KEY_COLUMNS_KEY            = "table_to_unique_key_columns_key"
	ErrNoQueueSegmentsFound                    = errors.New("no queue segments found")
)

const SQLITE_OPTIONS = "?_txlock=exclusive&_timeout=30000"

func GetMetaDBPath(exportDir string) string {
	return filepath.Join(exportDir, "metainfo", "meta.db")
}

func CreateAndInitMetaDBIfRequired(exportDir string) error {
	metaDBPath := GetMetaDBPath(exportDir)
	if utils.FileOrFolderExists(metaDBPath) {
		// already created and initiated
		return nil
	}
	err := createMetaDBFile(metaDBPath)
	if err != nil {
		return err
	}
	err = initMetaDB(metaDBPath)
	if err != nil {
		return err
	}
	return nil
}

func createMetaDBFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("not able to create meta db file :%w", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("error while closing meta db file: %w", err)
	}
	return nil
}

func initMetaDB(path string) error {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", path, SQLITE_OPTIONS))
	if err != nil {
		return fmt.Errorf("error while opening meta db :%w", err)
	}
	cmds := []string{

		fmt.Sprintf(`CREATE TABLE %s 
      (segment_no INTEGER PRIMARY KEY, 
       file_path TEXT, size_committed INTEGER, 
	   total_events INTEGER,
	   exporter_role TEXT,
       imported_by_target_db_importer INTEGER DEFAULT 0, 
       imported_by_source_replica_db_importer INTEGER DEFAULT 0, 
       imported_by_source_db_importer INTEGER DEFAULT 0, 
       archived INTEGER DEFAULT 0,
	   deleted INTEGER DEFAULT 0,
	   archive_location TEXT);`, QUEUE_SEGMENT_META_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			run_id TEXT, 
			exporter_role TEXT,
			timestamp INTEGER, 
			num_total INTEGER, 
			num_inserts INTEGER, 
			num_updates INTEGER, 
			num_deletes INTEGER, 
			PRIMARY KEY(run_id, exporter_role, timestamp) );`, EXPORTED_EVENTS_STATS_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			exporter_role TEXT,
			schema_name TEXT, 
			table_name TEXT, 
			num_total INTEGER, 
			num_inserts INTEGER, 
			num_updates INTEGER, 
			num_deletes INTEGER, 
			PRIMARY KEY(exporter_role, schema_name, table_name) );`, EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME),
		fmt.Sprintf(`CREATE TABLE %s (
			key TEXT PRIMARY KEY,
			json_text TEXT);`, JSON_OBJECTS_TABLE_NAME),
	}
	for _, cmd := range cmds {
		_, err = conn.Exec(cmd)
		if err != nil {
			return fmt.Errorf("error while initializating meta db with query-%s :%w", cmd, err)
		}
		log.Infof("Executed query on meta db - %s", cmd)
	}
	return nil
}

func TruncateTablesInMetaDb(exportDir string, tableNames []string) error {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", GetMetaDBPath(exportDir), SQLITE_OPTIONS))
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close connection to meta db: %v", err)
		}
	}()
	if err != nil {
		return fmt.Errorf("error while opening meta db :%w", err)
	}
	for _, tableName := range tableNames {
		query := fmt.Sprintf(`DELETE FROM %s;`, tableName)
		_, err = conn.Exec(query)
		if err != nil {
			return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
		log.Infof("Executed query on meta db - %s", query)
	}
	return nil
}

// =====================================================================================================================

type MetaDB struct {
	db *sql.DB
}

func NewMetaDB(exportDir string) (*MetaDB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", GetMetaDBPath(exportDir), SQLITE_OPTIONS))
	if err != nil {
		return nil, fmt.Errorf("error while opening meta db :%w", err)
	}
	return &MetaDB{db: db}, nil
}

func (m *MetaDB) MarkEventQueueSegmentAsProcessed(segmentNum int64, importerRole string) error {
	query := fmt.Sprintf(`UPDATE %s SET imported_by_%s = 1 WHERE segment_no = %d;`, QUEUE_SEGMENT_META_TABLE_NAME, importerRole, segmentNum)

	result, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error while getting rows updated -%s :%w", query, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row to be updated, got %d", rowsAffected)
	}

	log.Infof("Executed query on meta db - %s", query)
	return nil
}

func (m *MetaDB) GetLastValidOffsetInSegmentFile(segmentNum int64) (int64, error) {
	query := fmt.Sprintf(`SELECT size_committed FROM %s WHERE segment_no = %d;`, QUEUE_SEGMENT_META_TABLE_NAME, segmentNum)
	row := m.db.QueryRow(query)
	var sizeCommitted int64
	err := row.Scan(&sizeCommitted)
	if err != nil {
		return -1, fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	return sizeCommitted, nil
}

func (m *MetaDB) GetTotalExportedEvents(runId string) (int64, int64, error) {
	var totalCount int64
	var totalCountRun int64

	query := fmt.Sprintf(`SELECT sum(num_total) from %s`, EXPORTED_EVENTS_STATS_TABLE_NAME)
	err := m.db.QueryRow(query).Scan(&totalCount)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}

	query = fmt.Sprintf(`SELECT sum(num_total) from %s WHERE run_id = '%s'`, EXPORTED_EVENTS_STATS_TABLE_NAME, runId)
	err = m.db.QueryRow(query).Scan(&totalCountRun)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}

	return totalCount, totalCountRun, nil
}

func (m *MetaDB) GetTotalExportedEventsByExporterRole(exporterRole string, runId string) (int64, int64, error) {
	var totalCount int64
	var totalCountRun int64

	query := fmt.Sprintf(`SELECT sum(num_total) from %s WHERE exporter_role='%s'`, EXPORTED_EVENTS_STATS_TABLE_NAME, exporterRole)
	err := m.db.QueryRow(query).Scan(&totalCount)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}

	query = fmt.Sprintf(`SELECT sum(num_total) from %s WHERE exporter_role='%s' AND run_id = '%s'`, EXPORTED_EVENTS_STATS_TABLE_NAME, exporterRole, runId)
	err = m.db.QueryRow(query).Scan(&totalCountRun)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}
	return totalCount, totalCountRun, nil
}

func (m *MetaDB) GetExportedEventsRateInLastNMinutes(runId string, n int) (int64, error) {
	var totalCount int64
	now := time.Now()
	startTimeStamp := now.Add(-time.Minute * time.Duration(n))
	query := fmt.Sprintf(`select sum(num_total) from %s WHERE run_id='%s' AND timestamp >= %d`,
		EXPORTED_EVENTS_STATS_TABLE_NAME, runId, startTimeStamp.Unix())
	err := m.db.QueryRow(query).Scan(&totalCount)
	if err != nil {
		if !strings.Contains(err.Error(), "converting NULL to int64 is unsupported") {
			return 0, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
		}
	}
	return totalCount / int64(n*60), nil
}

func (m *MetaDB) InsertJsonObject(tx *sql.Tx, key string, obj any) error {
	jsonText, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error while marshalling json: %w", err)
	}
	log.Infof("Inserting json object for key: %s", key)
	query := fmt.Sprintf(`INSERT INTO %s (key, json_text) VALUES (?, ?)`, JSON_OBJECTS_TABLE_NAME)
	if tx == nil {
		_, err = m.db.Exec(query, key, jsonText)
	} else {
		_, err = tx.Exec(query, key, jsonText)
	}
	if err != nil {
		return fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	return nil
}

func (m *MetaDB) GetJsonObject(tx *sql.Tx, key string, obj any) (bool, error) {
	query := fmt.Sprintf(`SELECT json_text FROM %s WHERE key = ?`, JSON_OBJECTS_TABLE_NAME)
	var row *sql.Row
	if tx == nil {
		row = m.db.QueryRow(query, key)
	} else {
		row = tx.QueryRow(query, key)
	}
	var jsonText string
	err := row.Scan(&jsonText)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Infof("No json object found for key: %s", key)
			return false, nil
		}
		return false, fmt.Errorf("error while running query on meta db - %s :%w", query, err)
	}
	err = json.Unmarshal([]byte(jsonText), obj)
	if err != nil {
		log.Infof("Found json object for key: %s, but failed to unmarshal it: %v", key, err)
		return true, fmt.Errorf("error while unmarshalling json: %w", err)
	}
	log.Infof("Found json object for key: %s", key)
	return true, nil
}

func (m *MetaDB) DeleteJsonObject(key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE key = '%s'`, JSON_OBJECTS_TABLE_NAME, key)
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}
	return nil
}

func UpdateJsonObjectInMetaDB[T any](m *MetaDB, key string, updateFn func(obj *T)) error {
	// Get a connection to the meta db.
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("error while getting connection to meta db: %w", err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close connection to meta db: %v", err)
		}
	}()
	// Start a transaction.
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("error while starting transaction on meta db: %w", err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Errorf("failed to rollback transaction on meta db: %v", err)
		}
	}()
	// Get the json object.
	obj := new(T)
	found, err := m.GetJsonObject(tx, key, obj)
	if err != nil {
		return fmt.Errorf("error while getting json object from meta db: %w", err)
	}
	// Update the json object.
	updateFn(obj)
	// Update the json object in the meta db.
	newJsonText, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error while marshalling json: %w", err)
	}
	if !found {
		err = m.InsertJsonObject(tx, key, obj)
		if err != nil {
			return fmt.Errorf("error while inserting json object into meta db: %w", err)
		}
	} else {
		query := fmt.Sprintf(`UPDATE %s SET json_text = ? WHERE key = ?`, JSON_OBJECTS_TABLE_NAME)
		_, err = tx.Exec(query, string(newJsonText), key)
		if err != nil {
			return fmt.Errorf("error while running query on meta db - %s :%w", query, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error while commiting transaction on meta db: %w", err)
	}
	return nil
}

func (m *MetaDB) GetMinSegmentExportedByAndNotImportedBy(importerRole string, exporterRole string) (int64, error) {
	query := fmt.Sprintf(`SELECT MIN(segment_no) FROM %s WHERE`, QUEUE_SEGMENT_META_TABLE_NAME)
	query = fmt.Sprintf("%s imported_by_%s = 0", query, importerRole)
	if exporterRole != "" {
		query = fmt.Sprintf("%s AND exporter_role = '%s'", query, exporterRole)
	}
	query = fmt.Sprintf("%s;", query)

	row := m.db.QueryRow(query)
	var segmentNum sql.NullInt64
	err := row.Scan(&segmentNum)
	if err != nil {
		return -1, fmt.Errorf("run query on meta db - %s : %w", query, err)
	}
	if !segmentNum.Valid {
		return -1, ErrNoQueueSegmentsFound
	}
	return segmentNum.Int64, nil
}

func (m *MetaDB) GetExportedEventsStatsForTable(schemaName string, tableName string) (*tgtdb.EventCounter, error) {
	var totalCount int64
	var inserts int64
	var updates int64
	var deletes int64
	// Using SUM + LOWER case comparison here to deal with case sensitivity across stats published by source (ORACLE) and target (YB) (in ff workflow)
	query := fmt.Sprintf(`select COALESCE(SUM(num_total), 0), COALESCE(SUM(num_inserts),0),
	 	COALESCE(SUM(num_updates),0), COALESCE(SUM(num_deletes),0)
	  	from %s WHERE LOWER(schema_name)=LOWER('%s') AND LOWER(table_name)=LOWER('%s')`,
		EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME, schemaName, tableName)

	err := m.db.QueryRow(query).Scan(&totalCount, &inserts, &updates, &deletes)
	if err != nil {
		return &tgtdb.EventCounter{
			TotalEvents: 0,
			NumInserts:  0,
			NumUpdates:  0,
			NumDeletes:  0,
		}, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}
	return &tgtdb.EventCounter{
		TotalEvents: totalCount,
		NumInserts:  inserts,
		NumUpdates:  updates,
		NumDeletes:  deletes,
	}, nil
}

func (m *MetaDB) GetExportedEventsStatsForTableAndExporterRole(exporterRole string, schemaName string, tableName string) (*tgtdb.EventCounter, error) {
	var totalCount int64
	var inserts int64
	var updates int64
	var deletes int64
	// Using SUM + LOWER case comparison here to deal with case sensitivity across stats published by source (ORACLE) and target (YB) (in ff workflow)
	query := fmt.Sprintf(`select COALESCE(SUM(num_total), 0), COALESCE(SUM(num_inserts),0),
	 	COALESCE(SUM(num_updates),0), COALESCE(SUM(num_deletes),0)
	  	from %s WHERE LOWER(schema_name)=LOWER('%s') AND LOWER(table_name)=LOWER('%s') AND exporter_role='%s'`,
		EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME, schemaName, tableName, exporterRole)

	err := m.db.QueryRow(query).Scan(&totalCount, &inserts, &updates, &deletes)
	if err != nil {
		return nil, fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}
	return &tgtdb.EventCounter{
		TotalEvents: totalCount,
		NumInserts:  inserts,
		NumUpdates:  updates,
		NumDeletes:  deletes,
	}, nil
}

func (m *MetaDB) GetExportedEventsStatsForExporterRole(exporterRole string) (*utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter], error) {
	res := utils.NewStructMap[sqlname.NameTuple, *tgtdb.EventCounter]()
	// Using SUM + LOWER case comparison here to deal with case sensitivity across stats published by source (ORACLE) and target (YB) (in ff workflow)
	query := fmt.Sprintf(`select schema_name, table_name, COALESCE(SUM(num_total), 0), COALESCE(SUM(num_inserts),0),
	 	COALESCE(SUM(num_updates),0), COALESCE(SUM(num_deletes),0)
	  	from %s WHERE exporter_role='%s' GROUP BY schema_name, table_name`,
		EXPORTED_EVENTS_STATS_PER_TABLE_TABLE_NAME, exporterRole)

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("run query on meta db -%s :%v", query, err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Errorf("failed to close rows while fetching exported events stats from query %s : %v", query, err)
		}
	}()
	for rows.Next() {
		var schemaName string
		var tableName string
		var totalCount int64
		var inserts int64
		var updates int64
		var deletes int64
		err := rows.Scan(&schemaName, &tableName, &totalCount, &inserts, &updates, &deletes)
		if err != nil {
			return nil, fmt.Errorf("scan rows while fetching exported events stats from query %s : %v", query, err)
		}
		if tableName == "null" {
			continue
		}
		lookupName := tableName
		if schemaName != "" {
			lookupName = fmt.Sprintf("%s.%s", schemaName, lookupName)
		}
		nt, err := namereg.NameReg.LookupTableName(lookupName)
		if err != nil {
			return nil, fmt.Errorf("lookup %s from name registry: %v", lookupName, err)
		}

		var ec *tgtdb.EventCounter
		var found bool
		ec, found = res.Get(nt)
		if !found {
			ec = &tgtdb.EventCounter{}
			res.Put(nt, ec)
		}
		ec.TotalEvents += totalCount
		ec.NumInserts += inserts
		ec.NumUpdates += updates
		ec.NumDeletes += deletes
	}

	return res, nil
}

func (m *MetaDB) GetSegmentsToBeArchived(importCount int) ([]utils.Segment, error) {
	predicate := fmt.Sprintf(`((exporter_role == 'source_db_exporter' AND (imported_by_target_db_importer + imported_by_source_replica_db_importer + imported_by_source_db_importer = %d)) OR
	(exporter_role LIKE 'target_db_exporter%%' AND (imported_by_target_db_importer + imported_by_source_replica_db_importer + imported_by_source_db_importer = 1)))
	AND archived = 0`, importCount)
	segmentsToBeArchived, err := m.querySegments(predicate)
	if err != nil {
		return nil, fmt.Errorf("fetch segments to be archived: %v", err)
	}
	return segmentsToBeArchived, nil
}

func (m *MetaDB) GetSegmentsToBeDeleted() ([]utils.Segment, error) {
	// sample query: SELECT segment_no, file_path FROM queue_segment_meta WHERE archived = 1 AND deleted = 0 ORDER BY segment_no;
	predicate := "archived = 1 AND deleted = 0"
	segmentsToBeDeleted, err := m.querySegments(predicate)
	if err != nil {
		return nil, fmt.Errorf("fetch segments to be deleted: %v", err)
	}
	return segmentsToBeDeleted, nil
}

func (m *MetaDB) GetPendingSegments(importCount int) ([]utils.Segment, error) {
	predicate := fmt.Sprintf(`(exporter_role == 'source_db_exporter' AND (imported_by_target_db_importer + imported_by_source_replica_db_importer + imported_by_source_db_importer < %d)) OR
		(exporter_role LIKE 'target_db_exporter%%' AND (imported_by_target_db_importer + imported_by_source_replica_db_importer + imported_by_source_db_importer < 1))`, importCount)
	segments, err := m.querySegments(predicate)
	if err != nil {
		return nil, fmt.Errorf("fetch pending segments: %v", err)
	}
	return segments, nil
}

func (m *MetaDB) querySegments(predicate string) ([]utils.Segment, error) {
	var segments []utils.Segment
	query := fmt.Sprintf(`SELECT segment_no, file_path FROM %s WHERE %s ORDER BY segment_no;`, QUEUE_SEGMENT_META_TABLE_NAME, predicate)
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("run query on meta db -%s :%v", query, err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Errorf("failed to close rows while fetching segments from query %s : %v", query, err)
		}
	}()
	for rows.Next() {
		var segmentNo int64
		var filePath string
		err := rows.Scan(&segmentNo, &filePath)
		if err != nil {
			return nil, fmt.Errorf("scan rows while fetching segments from query %s : %v", query, err)
		}
		segment := utils.Segment{
			Num:      int(segmentNo),
			FilePath: filePath,
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

func (m *MetaDB) updateSegment(segmentNum int, setterExprs string) error {
	query := fmt.Sprintf(`UPDATE %s SET %s WHERE segment_no = ?;`, QUEUE_SEGMENT_META_TABLE_NAME, setterExprs)
	result, err := m.db.Exec(query, segmentNum)
	if err != nil {
		return fmt.Errorf("run query on meta db -%s :%v", query, err)
	}

	err = checkRowsAffected(result, 1)
	if err != nil {
		return fmt.Errorf("run query on meta db -%s :%v", query, err)
	}

	log.Infof("Executed query on meta db - %s", query)
	return nil
}

func (m *MetaDB) MarkSegmentDeleted(segmentNum int) error {
	// sample query: UPDATE queue_segment_meta SET deleted = 1 WHERE segment_no = 1;
	queryParams := "deleted = 1"
	err := m.updateSegment(segmentNum, queryParams)
	if err != nil {
		return fmt.Errorf("mark segment deleted in metaDB for segment %d: %v", segmentNum, err)
	}
	return nil
}

func (m *MetaDB) UpdateSegmentArchiveLocation(segmentNum int, archiveLocation string) error {
	// sample query: UPDATE queue_segment_meta SET archived = 1, archive_location = "/tmp/1" WHERE segment_no = 1;
	queryParams := fmt.Sprintf(`archived = 1, archive_location = '%s'`, archiveLocation)
	err := m.updateSegment(segmentNum, queryParams)
	if err != nil {
		return fmt.Errorf("mark segment archived in metaDB for segment %d: %v", segmentNum, err)
	}
	return nil
}

func checkRowsAffected(result sql.Result, expectedRows int) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows updated: %v", err)
	}
	if rowsAffected != int64(expectedRows) {
		return fmt.Errorf("expected %d rows to be updated, got %d", expectedRows, rowsAffected)
	}
	return nil
}

func (m *MetaDB) ResetQueueSegmentMeta(importerRole string) error {
	query := fmt.Sprintf(`UPDATE %s SET imported_by_%s = 0;`, QUEUE_SEGMENT_META_TABLE_NAME, importerRole)
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("error while running query on meta db -%s :%w", query, err)
	}
	return nil
}
