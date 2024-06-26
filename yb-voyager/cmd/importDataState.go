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
package cmd

import (
	"bufio"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

const (
	// The _v2 is appended in the table name so that the import code doesn't
	// try to use the similar table created by the voyager 1.3 and earlier.
	// Voyager 1.4 uses import data state format that is incompatible from
	// the earlier versions.
	BATCH_METADATA_TABLE_SCHEMA          = "ybvoyager_metadata"
	BATCH_METADATA_TABLE_NAME            = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_import_data_batches_metainfo_v3"
	EVENT_CHANNELS_METADATA_TABLE_NAME   = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_import_data_event_channels_metainfo"
	EVENTS_PER_TABLE_METADATA_TABLE_NAME = BATCH_METADATA_TABLE_SCHEMA + "." + "ybvoyager_imported_event_count_by_table"
)

/*
metainfo/import_data_state/table::<table_name>/file::<base_name>:<path_hash>/

	link -> dataFile
	batch::<batch_num>.<offset_end>.<record_count>.<byte_count>.<state>
*/
type ImportDataState struct {
	exportDir string
	stateDir  string
}

func NewImportDataState(exportDir string) *ImportDataState {
	return &ImportDataState{
		exportDir: exportDir,
		stateDir:  filepath.Join(exportDir, "metainfo", "import_data_state", importerRole),
	}
}

func (s *ImportDataState) PrepareForFileImport(filePath string, tableNameTup sqlname.NameTuple) error {
	fileStateDir := s.getFileStateDir(filePath, tableNameTup)
	log.Infof("Creating %q.", fileStateDir)
	err := os.MkdirAll(fileStateDir, 0755)
	if err != nil {
		return fmt.Errorf("error while creating %q: %w", fileStateDir, err)
	}
	// Create a symlink to the filePath. The symLink is only for human consumption.
	// It helps in easily distinguishing in files with same names but different paths.
	symlinkPath := filepath.Join(fileStateDir, "link")
	log.Infof("Creating symlink %q -> %q.", symlinkPath, filePath)
	err = os.Symlink(filePath, symlinkPath)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("error while creating symlink %q -> %q: %w", symlinkPath, filePath, err)
	}
	return nil
}

func (s *ImportDataState) GetPendingBatches(filePath string, tableNameTup sqlname.NameTuple) ([]*Batch, error) {
	return s.getBatches(filePath, tableNameTup, "CP")
}

func (s *ImportDataState) GetCompletedBatches(filePath string, tableNameTup sqlname.NameTuple) ([]*Batch, error) {
	return s.getBatches(filePath, tableNameTup, "D")
}

func (s *ImportDataState) GetAllBatches(filePath string, tableNameTup sqlname.NameTuple) ([]*Batch, error) {
	return s.getBatches(filePath, tableNameTup, "CPD")
}

type FileImportState string

const (
	FILE_IMPORT_STATE_UNKNOWN FileImportState = "FILE_IMPORT_STATE_UNKNOWN"
	FILE_IMPORT_NOT_STARTED   FileImportState = "FILE_IMPORT_NOT_STARTED"
	FILE_IMPORT_IN_PROGRESS   FileImportState = "FILE_IMPORT_IN_PROGRESS"
	FILE_IMPORT_COMPLETED     FileImportState = "FILE_IMPORT_COMPLETED"
)

func (s *ImportDataState) GetFileImportState(filePath string, tableNameTup sqlname.NameTuple) (FileImportState, error) {
	batches, err := s.GetAllBatches(filePath, tableNameTup)
	if err != nil {
		return FILE_IMPORT_STATE_UNKNOWN, fmt.Errorf("error while getting all batches for %s: %w", tableNameTup, err)
	}
	if len(batches) == 0 {
		return FILE_IMPORT_NOT_STARTED, nil
	}
	batchGenerationCompleted := false
	interruptedCount, doneCount := 0, 0
	for _, batch := range batches {
		if batch.IsDone() {
			doneCount++
		} else if batch.IsInterrupted() {
			interruptedCount++
		}
		if batch.Number == LAST_SPLIT_NUM {
			batchGenerationCompleted = true
		}
	}
	if doneCount == len(batches) && batchGenerationCompleted {
		return FILE_IMPORT_COMPLETED, nil
	}
	if interruptedCount == 0 && doneCount == 0 {
		return FILE_IMPORT_NOT_STARTED, nil
	}
	return FILE_IMPORT_IN_PROGRESS, nil
}

func (s *ImportDataState) Recover(filePath string, tableNameTup sqlname.NameTuple) ([]*Batch, int64, int64, bool, error) {
	var pendingBatches []*Batch

	lastBatchNumber := int64(0)
	lastOffset := int64(0)
	fileFullySplit := false

	batches, err := s.GetAllBatches(filePath, tableNameTup)
	if err != nil {
		return nil, 0, 0, false, fmt.Errorf("error while getting all batches for %s: %w", tableNameTup, err)
	}
	for _, batch := range batches {
		/*
			offsets are 0-based, while numLines are 1-based
			offsetStart is the line in original datafile from where current split starts
			offsetEnd   is the line in original datafile from where next split starts
		*/
		if batch.Number == LAST_SPLIT_NUM {
			fileFullySplit = true
		}
		if batch.Number > lastBatchNumber {
			lastBatchNumber = batch.Number
		}
		if batch.OffsetEnd > lastOffset {
			lastOffset = batch.OffsetEnd
		}
		if !batch.IsDone() {
			pendingBatches = append(pendingBatches, batch)
		}
	}
	return pendingBatches, lastBatchNumber, lastOffset, fileFullySplit, nil
}

func (s *ImportDataState) Clean(filePath string, tableNameTup sqlname.NameTuple) error {
	log.Infof("Cleaning import data state for table %q.", tableNameTup)
	fileStateDir := s.getFileStateDir(filePath, tableNameTup)
	log.Infof("Removing %q.", fileStateDir)
	err := os.RemoveAll(fileStateDir)
	if err != nil {
		return fmt.Errorf("error while removing %q: %w", fileStateDir, err)
	}

	err = s.cleanFileImportStateFromDB(filePath, tableNameTup)
	if err != nil {
		return fmt.Errorf("error while cleaning file import state for %q: %w", tableNameTup, err)
	}
	return nil
}

func (s *ImportDataState) GetImportedRowCount(filePath string, tableNameTup sqlname.NameTuple) (int64, error) {
	batches, err := s.GetCompletedBatches(filePath, tableNameTup)
	if err != nil {
		return -1, fmt.Errorf("error while getting completed batches for %s: %w", tableNameTup, err)
	}
	result := int64(0)
	for _, batch := range batches {
		result += batch.RecordCount
	}
	return result, nil
}

func (s *ImportDataState) GetImportedByteCount(filePath string, tableNameTup sqlname.NameTuple) (int64, error) {
	batches, err := s.GetCompletedBatches(filePath, tableNameTup)
	if err != nil {
		return -1, fmt.Errorf("error while getting completed batches for %s: %w", tableNameTup, err)
	}
	result := int64(0)
	for _, batch := range batches {
		result += batch.ByteCount
	}
	return result, nil
}

// TODO:TABLENAME: revisit??
func (s *ImportDataState) DiscoverTableToFilesMapping() (map[string][]string, error) {
	tableNames, err := s.discoverTableNames()
	if err != nil {
		return nil, fmt.Errorf("error while discovering table names: %w", err)
	}
	result := make(map[string][]string)
	for _, tableName := range tableNames {
		fileNames, err := s.discoverTableFiles(tableName)
		if err != nil {
			return nil, fmt.Errorf("error while discovering file paths for table %q: %w", tableName, err)
		}
		result[tableName.ForKey()] = fileNames
	}
	return result, nil
}

func (s *ImportDataState) NewBatchWriter(filePath string, tableNameTup sqlname.NameTuple, batchNumber int64) *BatchWriter {
	return &BatchWriter{
		state:       s,
		filePath:    filePath,
		tableName:   tableNameTup,
		batchNumber: batchNumber,
	}
}

func (s *ImportDataState) getBatches(filePath string, tableNameTup sqlname.NameTuple, states string) ([]*Batch, error) {
	// result == nil: import not started.
	// empty result: import started but no batches created yet.
	result := []*Batch{}

	fileStateDir := s.getFileStateDir(filePath, tableNameTup)
	// Check if the fileStateDir exists.
	_, err := os.Stat(fileStateDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Infof("fileStateDir %q does not exist", fileStateDir)
			return nil, nil
		}
		return nil, fmt.Errorf("stat %q: %s", fileStateDir, err)
	}

	// Find regular files in the `fileStateDir` whose name starts with "batch::"
	files, err := os.ReadDir(fileStateDir)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %s", fileStateDir, err)
	}
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasPrefix(file.Name(), "batch::") {
			batchNum, offsetEnd, recordCount, byteCount, state, err := parseBatchFileName(file.Name())
			if err != nil {
				return nil, fmt.Errorf("parse batch file name %q: %w", file.Name(), err)
			}
			if !strings.Contains(states, state) {
				continue
			}
			batch := &Batch{
				SchemaName:   "",
				TableNameTup: tableNameTup,
				FilePath:     filepath.Join(fileStateDir, file.Name()),
				BaseFilePath: filePath,
				Number:       batchNum,
				OffsetStart:  offsetEnd - recordCount,
				OffsetEnd:    offsetEnd,
				ByteCount:    byteCount,
				RecordCount:  recordCount,
			}
			result = append(result, batch)
		}
	}
	return result, nil

}

func parseBatchFileName(fileName string) (batchNum, offsetEnd, recordCount, byteCount int64, state string, err error) {
	md := strings.Split(strings.Split(fileName, "::")[1], ".")
	if len(md) != 5 {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid batch file name %q", fileName)
	}
	batchNum, err = strconv.ParseInt(md[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid batchNumber %q in the file name %q", md[0], fileName)
	}
	offsetEnd, err = strconv.ParseInt(md[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid offsetEnd %q in the file name %q", md[1], fileName)
	}
	recordCount, err = strconv.ParseInt(md[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid recordCount %q in the file name %q", md[2], fileName)
	}
	byteCount, err = strconv.ParseInt(md[3], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid byteCount %q in the file name %q", md[3], fileName)
	}
	state = md[4]
	if !slices.Contains([]string{"C", "P", "D"}, state) {
		return 0, 0, 0, 0, "", fmt.Errorf("invalid state %q in the file name %q", md[4], fileName)
	}
	return batchNum, offsetEnd, recordCount, byteCount, state, nil
}

//============================================================================

func (s *ImportDataState) getTableStateDir(tableNameTup sqlname.NameTuple) string {
	return fmt.Sprintf("%s/table::%s", s.stateDir, tableNameTup.ForKey())
}

func (s *ImportDataState) getFileStateDir(filePath string, tableNameTup sqlname.NameTuple) string {
	// NOTE: filePath must be absolute.
	hash := computePathHash(filePath, s.exportDir)
	baseName := filepath.Base(filePath)
	return fmt.Sprintf("%s/file::%s::%s", s.getTableStateDir(tableNameTup), baseName, hash)
}

func computePathHash(filePath, exportDir string) string {
	// If filePath starts with exportDir, then this is a case of
	// import files output by the `export data` command. Stripping the exportDir
	// from the filePath makes the code independent from the exportDir.
	filePath = strings.TrimPrefix(filePath, exportDir)
	hash := sha1.New()
	hash.Write([]byte(filePath))
	return hex.EncodeToString(hash.Sum(nil))[0:8]
}

func (s *ImportDataState) discoverTableNames() ([]sqlname.NameTuple, error) {
	// Find directories in the `stateDir` whose name starts with "table::"
	dirEntries, err := os.ReadDir(s.stateDir)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %s", s.stateDir, err)
	}
	result := []sqlname.NameTuple{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() && strings.HasPrefix(dirEntry.Name(), "table::") {
			tableNameRaw := dirEntry.Name()[len("table::"):]
			tableName, err := namereg.NameReg.LookupTableName(tableNameRaw)
			if err != nil {
				return nil, fmt.Errorf("lookup table naame %s in name registry: %v", tableNameRaw, err)
			}
			result = append(result, tableName)
		}
	}
	return result, nil
}

func (s *ImportDataState) discoverTableFiles(tableNameTup sqlname.NameTuple) ([]string, error) {
	tableStateDir := s.getTableStateDir(tableNameTup)
	dirEntries, err := os.ReadDir(tableStateDir)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %s", tableStateDir, err)
	}
	result := []string{}
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() && strings.HasPrefix(dirEntry.Name(), "file::") {
			symLinkPath := filepath.Join(tableStateDir, dirEntry.Name(), "link")
			targetPath, err := os.Readlink(symLinkPath)
			if err != nil {
				return nil, fmt.Errorf("read link %q: %s", symLinkPath, err)
			}
			result = append(result, targetPath)
		}
	}
	return result, nil
}

func (s *ImportDataState) GetTotalNumOfEventsImportedByType(migrationUUID uuid.UUID) (int64, int64, int64, error) {
	query := fmt.Sprintf("SELECT SUM(num_inserts), SUM(num_updates), SUM(num_deletes) FROM %s where migration_uuid='%s'",
		EVENT_CHANNELS_METADATA_TABLE_NAME, migrationUUID)
	var numInserts, numUpdates, numDeletes int64
	err := tdb.QueryRow(query).Scan(&numInserts, &numUpdates, &numDeletes)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error in getting import stats from target db: %w", err)
	}
	return numInserts, numUpdates, numDeletes, nil
}

func (s *ImportDataState) InitLiveMigrationState(migrationUUID uuid.UUID, numChans int, startClean bool, tableNameTups []sqlname.NameTuple) error {
	if startClean {
		// TODO: common definition for these batch metadata name tuples
		evChanMetadataTbl := EVENT_CHANNELS_METADATA_TABLE_NAME
		if tconf.TargetDBType == ORACLE {
			evChanMetadataTbl = strings.ToUpper(evChanMetadataTbl)
		}
		parts := strings.Split(evChanMetadataTbl, ".")
		evChanMetadataTblName := sqlname.NewObjectName(tconf.TargetDBType, "public", parts[0], parts[1])
		evChanNt := sqlname.NameTuple{
			CurrentName: evChanMetadataTblName,
			SourceName:  nil,
			TargetName:  evChanMetadataTblName,
		}
		err := s.clearMigrationStateFromTable(evChanNt, migrationUUID)
		if err != nil {
			return fmt.Errorf("error clearing channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
		}

		evTblMetadataTbl := EVENTS_PER_TABLE_METADATA_TABLE_NAME
		if tconf.TargetDBType == ORACLE {
			evTblMetadataTbl = strings.ToUpper(evTblMetadataTbl)
		}
		parts = strings.Split(evTblMetadataTbl, ".")
		evTblMetadataTblName := sqlname.NewObjectName(tconf.TargetDBType, "public", parts[0], parts[1])
		evTblNt := sqlname.NameTuple{
			CurrentName: evTblMetadataTblName,
			SourceName:  nil,
			TargetName:  evTblMetadataTblName,
		}
		err = s.clearMigrationStateFromTable(evTblNt, migrationUUID)
		if err != nil {
			return fmt.Errorf("error clearing meta info for %s: %w", EVENTS_PER_TABLE_METADATA_TABLE_NAME, err)
		}
	}
	err := s.initChannelMetaInfo(migrationUUID, numChans)
	if err != nil {
		return fmt.Errorf("error initializing channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
	}

	err = s.initEventStatsByTableMetainfo(migrationUUID, tableNameTups, numChans)
	if err != nil {
		return fmt.Errorf("error initializing event stats by table meta info for %s: %w", EVENTS_PER_TABLE_METADATA_TABLE_NAME, err)
	}
	return nil
}

func (s *ImportDataState) clearMigrationStateFromTable(tableNameTup sqlname.NameTuple, migrationUUID uuid.UUID) error {
	stmt := fmt.Sprintf("DELETE FROM %s where migration_uuid='%s'", tableNameTup.ForUserQuery(), migrationUUID)
	rowsAffected, err := tdb.Exec(stmt)
	if err != nil {
		return fmt.Errorf("error executing stmt - %v: %w", stmt, err)
	}
	log.Infof("Query: %s ==> Rows affected: %d", stmt, rowsAffected)
	return nil
}

func (s *ImportDataState) initChannelMetaInfo(migrationUUID uuid.UUID, numChans int) error {
	// if there are >0 rows, then skip because already been inited.
	rowCount, err := s.getEventChannelsRowCount(migrationUUID)
	if err != nil {
		return fmt.Errorf("error getting channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
	}
	if rowCount > 0 {
		log.Info("event channels meta info already created. Skipping init.")
		return nil
	}
	err = tdb.WithTx(func(tx *sql.Tx) error {
		for c := 0; c < numChans; c++ {
			insertStmt := fmt.Sprintf("INSERT INTO %s VALUES ('%s', %d, -1, %d, %d, %d)", EVENT_CHANNELS_METADATA_TABLE_NAME, migrationUUID, c, 0, 0, 0)
			_, err := tx.Exec(insertStmt)
			if err != nil {
				return fmt.Errorf("error executing stmt - %v: %w", insertStmt, err)
			}
			log.Infof("created channels meta info: %s;", insertStmt)

			if err != nil {
				return fmt.Errorf("error initializing channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error initializing channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
	}
	return nil
}

func (s *ImportDataState) getEventChannelsRowCount(migrationUUID uuid.UUID) (int64, error) {
	rowsStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s where migration_uuid='%s'", EVENT_CHANNELS_METADATA_TABLE_NAME, migrationUUID)
	var rowCount int64
	err := tdb.QueryRow(rowsStmt).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("error executing stmt - %v: %w", rowsStmt, err)
	}
	return rowCount, nil
}

func (s *ImportDataState) initEventStatsByTableMetainfo(migrationUUID uuid.UUID, tableNameTups []sqlname.NameTuple, numChans int) error {
	tableRowCount := utils.NewStructMap[sqlname.NameTuple, int64]()
	for _, tableNameTup := range tableNameTups {
		rowCount, err := s.getLiveMigrationMetaInfoByTable(migrationUUID, tableNameTup)
		if err != nil {
			return fmt.Errorf("error getting channels meta info for %s: %w", EVENT_CHANNELS_METADATA_TABLE_NAME, err)
		}
		tableRowCount.Put(tableNameTup, rowCount)
	}

	return tdb.WithTx(func(tx *sql.Tx) error {
		for _, tableNameTup := range tableNameTups {
			rowCount, _ := tableRowCount.Get(tableNameTup)
			if rowCount > 0 {
				log.Info(fmt.Sprintf("event stats for %s already created. Skipping init.", tableNameTup))
			} else {
				for c := 0; c < numChans; c++ {
					insertStmt := fmt.Sprintf("INSERT INTO %s VALUES ('%s', '%s', %d, %d, %d, %d, %d)", EVENTS_PER_TABLE_METADATA_TABLE_NAME, migrationUUID, tableNameTup.ForKey(), c, 0, 0, 0, 0)
					_, err := tx.Exec(insertStmt)
					if err != nil {
						return fmt.Errorf("error executing stmt - %v: %w", insertStmt, err)
					}
					log.Infof("created table wise event meta info: %s;", insertStmt)
				}
			}
		}
		return nil
	})
}

func (s *ImportDataState) getLiveMigrationMetaInfoByTable(migrationUUID uuid.UUID, tableNameTup sqlname.NameTuple) (int64, error) {
	rowsStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s where migration_uuid='%s' AND table_name='%s'",
		EVENTS_PER_TABLE_METADATA_TABLE_NAME, migrationUUID, tableNameTup.ForKey())
	var rowCount int64
	err := tdb.QueryRow(rowsStmt).Scan(&rowCount)
	if err != nil {
		return 0, fmt.Errorf("error executing stmt - %v: %w", rowsStmt, err)
	}
	return rowCount, nil
}

func (s *ImportDataState) cleanFileImportStateFromDB(filePath string, tableNameTup sqlname.NameTuple) error {
	// Delete all entries from ${BATCH_METADATA_TABLE_NAME} for this table.
	sname, tname := tableNameTup.ForCatalogQuery()
	cmd := fmt.Sprintf(
		`DELETE FROM %s WHERE migration_uuid = '%s' AND data_file_name = '%s' AND schema_name = '%s' AND table_name = '%s'`,
		BATCH_METADATA_TABLE_NAME, migrationUUID, filePath, sname, tname)
	rowsAffected, err := tdb.Exec(cmd)
	if err != nil {
		return fmt.Errorf("remove %q related entries from %s: %w", tableNameTup, BATCH_METADATA_TABLE_NAME, err)
	}
	log.Infof("query: [%s] => rows affected %v", cmd, rowsAffected)
	return nil
}

func (s *ImportDataState) GetImportedSnapshotRowCountForTable(tableNameTup sqlname.NameTuple) (int64, error) {
	var snapshotRowCount int64
	sname, tname := tableNameTup.ForCatalogQuery()
	query := fmt.Sprintf(`SELECT COALESCE(SUM(rows_imported),0) FROM %s where migration_uuid='%s' AND schema_name='%s' AND table_name='%s'`,
		BATCH_METADATA_TABLE_NAME, migrationUUID, sname, tname)
	log.Infof("query to get total row count for snapshot import of table %s: %s", tableNameTup, query)
	err := tdb.QueryRow(query).Scan(&snapshotRowCount)
	if err != nil {
		log.Errorf("error in querying row_imported for snapshot import of table %s: %v", tableNameTup, err)
		return 0, fmt.Errorf("error in querying row_imported for snapshot import of table %s: %w", tableNameTup, err)
	}
	log.Infof("total row count for snapshot import of table %s: %d", tableNameTup, snapshotRowCount)
	return snapshotRowCount, nil
}

type EventChannelMetaInfo struct {
	ChanNo         int
	LastAppliedVsn int64
}

func (s *ImportDataState) GetEventChannelsMetaInfo(migrationUUID uuid.UUID) (map[int]EventChannelMetaInfo, error) {
	metainfo := map[int]EventChannelMetaInfo{}

	query := fmt.Sprintf("SELECT channel_no, last_applied_vsn FROM %s where migration_uuid='%s'", EVENT_CHANNELS_METADATA_TABLE_NAME, migrationUUID)
	rows, err := tdb.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query meta info for channels: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chanMetaInfo EventChannelMetaInfo
		err := rows.Scan(&(chanMetaInfo.ChanNo), &(chanMetaInfo.LastAppliedVsn))
		if err != nil {
			return nil, fmt.Errorf("error while scanning rows returned from DB: %w", err)
		}
		metainfo[chanMetaInfo.ChanNo] = chanMetaInfo
	}
	return metainfo, nil
}

func (s *ImportDataState) IsEventBatchAlreadyImported(batch *tgtdb.EventBatch, migrationUUID uuid.UUID) (bool, error) {
	query := fmt.Sprintf("SELECT last_applied_vsn FROM %s WHERE migration_uuid='%s' AND channel_no=%d",
		EVENT_CHANNELS_METADATA_TABLE_NAME, migrationUUID, batch.ChanNo)
	var lastAppliedVsnInChan int64
	err := tdb.QueryRow(query).Scan(&lastAppliedVsnInChan)
	if err != nil {
		return false, err
	}
	return lastAppliedVsnInChan >= batch.GetLastVsn(), nil
}

func (s *ImportDataState) GetImportedEventsStatsForTable(tableNameTup sqlname.NameTuple, migrationUUID uuid.UUID) (*tgtdb.EventCounter, error) {
	var eventCounter tgtdb.EventCounter
	query := fmt.Sprintf(`SELECT SUM(total_events), SUM(num_inserts), SUM(num_updates), SUM(num_deletes) FROM %s 
		WHERE table_name='%s' AND migration_uuid='%s'`, EVENTS_PER_TABLE_METADATA_TABLE_NAME, tableNameTup.ForKey(), migrationUUID)
	log.Infof("query to get import stats for table %s: %s", tableNameTup.ForKey(), query)
	err := tdb.QueryRow(query).Scan(&eventCounter.TotalEvents,
		&eventCounter.NumInserts, &eventCounter.NumUpdates, &eventCounter.NumDeletes)
	if err != nil {
		log.Errorf("error in getting import stats from target db: %v", err)
		return nil, fmt.Errorf("error in getting import stats from target db: %w", err)
	}
	log.Infof("import stats for table %s: %v", tableNameTup, eventCounter)
	return &eventCounter, nil
}

func (s *ImportDataState) GetImportedEventsStatsForTableList(tableNameTupList []sqlname.NameTuple, migrationUUID uuid.UUID) (*utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter], error) {
	tablesToEventCounter := utils.NewStructMap[sqlname.NameTuple, *tgtdb.EventCounter]()
	//in case import streaming is not started yet, metadata will not be initialized so initialising the tuples with empty counters
	for _, ntup := range tableNameTupList {
		dummyCounter := tgtdb.EventCounter{
			NumInserts: 0,
			NumUpdates: 0,
			NumDeletes: 0,
		}
		tablesToEventCounter.Put(ntup, &dummyCounter)
	}

	tableListQuery := strings.Join(lo.Map(tableNameTupList, func(tup sqlname.NameTuple, _ int) string {
		return tup.ForKey()
	}), "','") // format table1','table2','table3
	query := fmt.Sprintf(`SELECT table_name, SUM(total_events), SUM(num_inserts), SUM(num_updates), SUM(num_deletes) FROM %s 
		WHERE migration_uuid='%s' GROUP BY table_name HAVING table_name in ('%s')`, EVENTS_PER_TABLE_METADATA_TABLE_NAME, migrationUUID, tableListQuery)
	log.Infof("query to get import stats for tables '%s': %s", tableListQuery, query)
	rows, err := tdb.Query(query)
	if err != nil {
		log.Errorf("error in getting import stats from target db: %v", err)
		return nil, fmt.Errorf("error in getting import stats from target db: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var eventCounter tgtdb.EventCounter
		var tableName string
		err = rows.Scan(&tableName, &eventCounter.TotalEvents,
			&eventCounter.NumInserts, &eventCounter.NumUpdates, &eventCounter.NumDeletes)
		if err != nil {
			return nil, fmt.Errorf("get imported events stats for table %q: %w", tableName, err)
		}
		tableNameTup, err := namereg.NameReg.LookupTableName(tableName)
		if err != nil {
			return nil, fmt.Errorf("error in lookup in namreg: %v", err)
		}
		tablesToEventCounter.Put(tableNameTup, &eventCounter)
	}

	return tablesToEventCounter, nil
}

//============================================================================

type BatchWriter struct {
	state *ImportDataState

	filePath    string
	tableName   sqlname.NameTuple
	batchNumber int64

	NumRecordsWritten      int64
	flagFirstRecordWritten bool

	outFile *os.File
	w       *bufio.Writer
}

func (bw *BatchWriter) Init() error {
	fileStateDir := bw.state.getFileStateDir(bw.filePath, bw.tableName)
	currTmpFileName := fmt.Sprintf("%s/tmp::%v", fileStateDir, bw.batchNumber)
	log.Infof("current temp file: %s", currTmpFileName)
	outFile, err := os.Create(currTmpFileName)
	if err != nil {
		return fmt.Errorf("create file %q: %s", currTmpFileName, err)
	}
	bw.outFile = outFile
	bw.w = bufio.NewWriterSize(outFile, 4*MB)
	return nil
}

func (bw *BatchWriter) WriteHeader(header string) error {
	_, err := bw.w.WriteString(header + "\n")
	if err != nil {
		return fmt.Errorf("write header to %q: %s", bw.outFile.Name(), err)
	}
	return nil
}

func (bw *BatchWriter) WriteRecord(record string) error {
	if record == "" {
		return nil
	}
	var err error
	if bw.flagFirstRecordWritten {
		_, err = bw.w.WriteString("\n")
		if err != nil {
			return fmt.Errorf("write to %q: %s", bw.outFile.Name(), err)
		}
	}
	_, err = bw.w.WriteString(record)
	if err != nil {
		return fmt.Errorf("write record to %q: %s", bw.outFile.Name(), err)
	}
	bw.NumRecordsWritten++
	bw.flagFirstRecordWritten = true
	return nil
}

func (bw *BatchWriter) Done(isLastBatch bool, offsetEnd int64, byteCount int64) (*Batch, error) {
	err := bw.w.Flush()
	if err != nil {
		return nil, fmt.Errorf("flush %q: %s", bw.outFile.Name(), err)
	}
	tmpFileName := bw.outFile.Name()
	err = bw.outFile.Close()
	if err != nil {
		return nil, fmt.Errorf("close %q: %s", bw.outFile.Name(), err)
	}

	batchNumber := bw.batchNumber
	if isLastBatch {
		batchNumber = LAST_SPLIT_NUM
	}
	fileStateDir := bw.state.getFileStateDir(bw.filePath, bw.tableName)
	batchFilePath := fmt.Sprintf("%s/batch::%d.%d.%d.%d.C",
		fileStateDir, batchNumber, offsetEnd, bw.NumRecordsWritten, byteCount)
	log.Infof("Renaming %q to %q", tmpFileName, batchFilePath)
	err = os.Rename(tmpFileName, batchFilePath)
	if err != nil {
		return nil, fmt.Errorf("rename %q to %q: %s", tmpFileName, batchFilePath, err)
	}
	batch := &Batch{
		SchemaName:   "",
		TableNameTup: bw.tableName,
		FilePath:     batchFilePath,
		BaseFilePath: bw.filePath,
		Number:       batchNumber,
		OffsetStart:  offsetEnd - bw.NumRecordsWritten,
		OffsetEnd:    offsetEnd,
		RecordCount:  bw.NumRecordsWritten,
		ByteCount:    byteCount,
	}
	return batch, nil
}

//============================================================================

type Batch struct {
	Number              int64
	TableNameTup        sqlname.NameTuple
	SchemaName          string
	FilePath            string // Path of the batch file.
	BaseFilePath        string // Path of the original data file.
	OffsetStart         int64
	OffsetEnd           int64
	RecordCount         int64
	ByteCount           int64
	TmpConnectionString string
	Interrupted         bool
}

func (batch *Batch) Open() (*os.File, error) {
	return os.Open(batch.FilePath)
}

func (batch *Batch) Delete() error {
	err := os.RemoveAll(batch.FilePath)
	if err != nil {
		return fmt.Errorf("remove %q: %s", batch.FilePath, err)
	}
	log.Infof("Deleted %q", batch.FilePath)
	batch.FilePath = ""
	return nil
}

func (batch *Batch) IsNotStarted() bool {
	return strings.HasSuffix(batch.FilePath, ".C")
}

func (batch *Batch) IsInterrupted() bool {
	return strings.HasSuffix(batch.FilePath, ".P")
}

func (batch *Batch) IsDone() bool {
	return strings.HasSuffix(batch.FilePath, ".D")
}

func (batch *Batch) MarkPending() error {
	// Rename the file to .P
	inProgressFilePath := batch.getInProgressFilePath()
	log.Infof("Renaming file from %q to %q", batch.FilePath, inProgressFilePath)
	err := os.Rename(batch.FilePath, inProgressFilePath)
	if err != nil {
		return fmt.Errorf("rename %q to %q: %w", batch.FilePath, inProgressFilePath, err)
	}
	batch.FilePath = inProgressFilePath
	return nil
}

func (batch *Batch) MarkDone() error {
	inProgressFilePath := batch.getInProgressFilePath()
	doneFilePath := batch.getDoneFilePath()
	log.Infof("Renaming %q => %q", inProgressFilePath, doneFilePath)
	err := os.Rename(inProgressFilePath, doneFilePath)
	if err != nil {
		return fmt.Errorf("rename %q => %q: %w", inProgressFilePath, doneFilePath, err)
	}

	if truncateSplits {
		err = os.Truncate(doneFilePath, 0)
		if err != nil {
			log.Warnf("truncate file %q: %s", doneFilePath, err)
		}
	}
	batch.FilePath = doneFilePath
	return nil
}

func (batch *Batch) GetQueryIsBatchAlreadyImported() string {
	schemaName, tableName := batch.TableNameTup.ForCatalogQuery()
	query := fmt.Sprintf(
		"SELECT rows_imported FROM %s "+
			"WHERE migration_uuid = '%s' AND data_file_name = '%s' AND batch_number = %d AND schema_name = '%s' AND table_name = '%s'",
		BATCH_METADATA_TABLE_NAME, migrationUUID, batch.BaseFilePath, batch.Number, schemaName, tableName)

	return query
}

func (batch *Batch) GetQueryToRecordEntryInDB(rowsAffected int64) string {
	// Record an entry in ${BATCH_METADATA_TABLE_NAME}, that the split is imported.
	schemaName, tableName := batch.TableNameTup.ForCatalogQuery()
	cmd := fmt.Sprintf(
		`INSERT INTO %s (migration_uuid, data_file_name, batch_number, schema_name, table_name, rows_imported)
			VALUES ('%s', '%s', %d, '%s', '%s', %v)`,
		BATCH_METADATA_TABLE_NAME, migrationUUID, batch.BaseFilePath, batch.Number, schemaName, tableName, rowsAffected)

	return cmd
}

func (batch *Batch) GetFilePath() string {
	return batch.FilePath
}

func (batch *Batch) GetTableName() sqlname.NameTuple {
	return batch.TableNameTup
}

func (batch *Batch) getInProgressFilePath() string {
	return batch.FilePath[0:len(batch.FilePath)-1] + "P" // *.C -> *.P
}

func (batch *Batch) getDoneFilePath() string {
	return batch.FilePath[0:len(batch.FilePath)-1] + "D" // *.P -> *.D
}
