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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/davecgh/go-spew/spew"
	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datastore"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var metaInfoDirName = META_INFO_DIR_NAME
var batchSize = int64(0)
var batchImportPool *pool.Pool
var tablesProgressMetadata map[string]*utils.TableProgressMetadata
var importerRole string
var identityColumnsMetaDBKey string

// stores the data files description in a struct
var dataFileDescriptor *datafile.Descriptor
var truncateSplits utils.BoolStr                                             // to truncate *.D splits after import
var TableToColumnNames = utils.NewStructMap[sqlname.NameTuple, []string]()   // map of table name to columnNames
var TableToIdentityColumnNames *utils.StructMap[sqlname.NameTuple, []string] // map of table name to generated always as identity column's names
var valueConverter dbzm.ValueConverter

var TableNameToSchema *utils.StructMap[sqlname.NameTuple, map[string]map[string]string]
var conflictDetectionCache *ConflictDetectionCache
var targetDBDetails *callhome.TargetDBDetails

var importDataCmd = &cobra.Command{
	Use: "data",
	Short: "Import data from compatible source database to target database.\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/reference/data-migration/import-data/",
	Long: `Import the data exported from the source database into the target database. Also import data(snapshot + changes from target) into source-replica/source in case of live migration with fall-back/fall-forward worflows.`,
	Args: cobra.NoArgs,
	PreRun: func(cmd *cobra.Command, args []string) {
		if tconf.TargetDBType == "" {
			tconf.TargetDBType = YUGABYTEDB
		}
		if importerRole == "" {
			importerRole = TARGET_DB_IMPORTER_ROLE
		}
		sourceDBType = GetSourceDBTypeFromMSR()
		err := validateImportFlags(cmd, importerRole)
		if err != nil {
			utils.ErrExit("Error: %s", err.Error())
		}
	},
	Run: importDataCommandFn,
}

var importDataToCmd = &cobra.Command{
	Use:   "to",
	Short: "Import data into various databases",
	Long:  `Import data into various databases`,
}

var importDataToTargetCmd = &cobra.Command{
	Use:   "target",
	Short: importDataCmd.Short,
	Long:  importDataCmd.Long,
	Args:  importDataCmd.Args,

	PreRun: importDataCmd.PreRun,

	Run: importDataCmd.Run,
}

func importDataCommandFn(cmd *cobra.Command, args []string) {
	ExitIfAlreadyCutover(importerRole)
	reportProgressInBytes = false
	tconf.ImportMode = true
	checkExportDataDoneFlag()
	sourceDBType = GetSourceDBTypeFromMSR()
	sqlname.SourceDBType = sourceDBType

	if tconf.TargetDBType == YUGABYTEDB {
		tconf.Schema = strings.ToLower(tconf.Schema)
	} else if tconf.TargetDBType == ORACLE && !utils.IsQuotedString(tconf.Schema) {
		tconf.Schema = strings.ToUpper(tconf.Schema)
	}
	tdb = tgtdb.NewTargetDB(&tconf)
	err := tdb.Init()
	if err != nil {
		utils.ErrExit("Failed to initialize the target DB: %s", err)
	}
	targetDBDetails = tdb.GetCallhomeTargetDBInfo()

	err = InitNameRegistry(exportDir, importerRole, nil, nil, &tconf, tdb, bool(startClean))
	if err != nil {
		utils.ErrExit("initialize name registry: %v", err)
	}

	dataStore = datastore.NewDataStore(filepath.Join(exportDir, "data"))
	dataFileDescriptor = datafile.OpenDescriptor(exportDir)
	// TODO: handle case-sensitive in table names with oracle ff-db
	// quoteTableNameIfRequired()
	importFileTasks := discoverFilesToImport()
	record, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("Failed to get migration status record: %s", err)
	}
	if importerRole == TARGET_DB_IMPORTER_ROLE {

		importType = record.ExportType
		identityColumnsMetaDBKey = metadb.TARGET_DB_IDENTITY_COLUMNS_KEY
	}

	if importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE {
		if record.FallbackEnabled {
			utils.ErrExit("cannot import data to source-replica. Fall-back workflow is already enabled.")
		}
		updateFallForwardEnabledInMetaDB()
		identityColumnsMetaDBKey = metadb.FF_DB_IDENTITY_COLUMNS_KEY
	}

	if changeStreamingIsEnabled(importType) && (tconf.TableList != "" || tconf.ExcludeTableList != "") {
		utils.ErrExit("--table-list and --exclude-table-list are not supported for live migration. Re-run the command without these flags.")
	} else {
		importFileTasks = applyTableListFilter(importFileTasks)
	}

	importData(importFileTasks)
	tdb.Finalize()
	if changeStreamingIsEnabled(importType) {
		startExportDataFromTargetIfRequired()
	}
}

func startExportDataFromTargetIfRequired() {
	if importerRole != TARGET_DB_IMPORTER_ROLE {
		return
	}
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("could not fetch MigrationStatusRecord: %w", err)
	}
	if !msr.FallForwardEnabled && !msr.FallbackEnabled {
		utils.PrintAndLog("No fall-forward/back enabled. Exiting.")
		return
	}
	tableListExportedFromSource := msr.TableListExportedFromSource
	importTableList, err := getImportTableList(tableListExportedFromSource)
	if err != nil {
		utils.ErrExit("failed to generate table list : %v", err)
	}
	importTableNames := lo.Map(importTableList, func(tableName sqlname.NameTuple, _ int) string {
		return tableName.ForUserQuery()
	})

	lockFile.Unlock() // unlock export dir from import data cmd before switching current process to ff/fb sync cmd

	if tconf.SSLMode == "prefer" || tconf.SSLMode == "allow" {
		utils.PrintAndLog(color.RedString("Warning: SSL mode '%s' is not supported for 'export data from target' yet. Downgrading it to 'disable'.\nIf you don't want these settings you can restart the 'export data from target' with a different value for --target-ssl-mode and --target-ssl-root-cert flag.", source.SSLMode))
		tconf.SSLMode = "disable"
	}
	cmd := []string{"yb-voyager", "export", "data", "from", "target",
		"--export-dir", exportDir,
		"--table-list", strings.Join(importTableNames, ","),
		fmt.Sprintf("--transaction-ordering=%t", transactionOrdering),
		fmt.Sprintf("--send-diagnostics=%t", callhome.SendDiagnostics),
		"--target-ssl-mode", tconf.SSLMode,
	}
	if tconf.SSLRootCert != "" {
		cmd = append(cmd, "--target-ssl-root-cert", tconf.SSLRootCert)
	}
	if utils.DoNotPrompt {
		cmd = append(cmd, "--yes")
	}
	if disablePb {
		cmd = append(cmd, "--disable-pb=true")
	}
	cmdStr := "TARGET_DB_PASSWORD=*** " + strings.Join(cmd, " ")

	utils.PrintAndLog("Starting export data from target with command:\n %s", color.GreenString(cmdStr))
	binary, lookErr := exec.LookPath(os.Args[0])
	if lookErr != nil {
		utils.ErrExit("could not find yb-voyager - %w", lookErr)
	}
	env := os.Environ()
	env = slices.Insert(env, 0, "TARGET_DB_PASSWORD="+tconf.Password)

	execErr := syscall.Exec(binary, cmd, env)
	if execErr != nil {
		utils.ErrExit("failed to run yb-voyager export data from target - %w\n Please re-run with command :\n%s", execErr, cmdStr)
	}
}

type ImportFileTask struct {
	ID           int
	FilePath     string
	TableNameTup sqlname.NameTuple
	RowCount     int64
	FileSize     int64
}

// func quoteTableNameIfRequired() {
// 	if tconf.TargetDBType != ORACLE {
// 		return
// 	}
// 	for _, fileEntry := range dataFileDescriptor.DataFileList {
// 		if sqlname.IsQuoted(fileEntry.TableName) {
// 			continue
// 		}
// 		if sqlname.IsReservedKeywordOracle(fileEntry.TableName) ||
// 			(sqlname.IsCaseSensitive(fileEntry.TableName, ORACLE)) {
// 			newTableName := fmt.Sprintf(`"%s"`, fileEntry.TableName)
// 			if dataFileDescriptor.TableNameToExportedColumns != nil {
// 				dataFileDescriptor.TableNameToExportedColumns[newTableName] = dataFileDescriptor.TableNameToExportedColumns[fileEntry.TableName]
// 				delete(dataFileDescriptor.TableNameToExportedColumns, fileEntry.TableName)
// 			}
// 			fileEntry.TableName = newTableName
// 		}
// 	}
// }

func discoverFilesToImport() []*ImportFileTask {
	result := []*ImportFileTask{}
	if dataFileDescriptor.DataFileList == nil {
		utils.ErrExit("It looks like the data is exported using older version of Voyager. Please use matching version to import the data.")
	}

	for i, fileEntry := range dataFileDescriptor.DataFileList {
		if fileEntry.RowCount == 0 {
			// In case of PG Live migration  pg_dump and dbzm both are used and we don't skip empty tables
			// but pb hangs for empty so skipping empty tables in snapshot import
			continue
		}
		tableName, err := namereg.NameReg.LookupTableName(fileEntry.TableName)
		if err != nil {
			utils.ErrExit("lookup table name from name registry: %v", err)
		}
		task := &ImportFileTask{
			ID:           i,
			FilePath:     fileEntry.FilePath,
			TableNameTup: tableName,
			RowCount:     fileEntry.RowCount,
			FileSize:     fileEntry.FileSize,
		}
		result = append(result, task)
	}
	return result
}

func applyTableListFilter(importFileTasks []*ImportFileTask) []*ImportFileTask {
	result := []*ImportFileTask{}

	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("could not fetch migration status record: %w", err)
	}
	source = *msr.SourceDBConf
	_, noDefaultSchema := getDefaultSourceSchemaName()

	allTables := lo.Uniq(lo.Map(importFileTasks, func(task *ImportFileTask, _ int) sqlname.NameTuple {
		return task.TableNameTup
	}))
	slices.SortFunc(allTables, func(a, b sqlname.NameTuple) bool {
		return a.ForKey() < b.ForKey()
	})
	log.Infof("allTables: %v", allTables)

	findPatternMatchingTables := func(pattern string) []sqlname.NameTuple {
		result := lo.Filter(allTables, func(tableNameTup sqlname.NameTuple, _ int) bool {
			matched, err := tableNameTup.MatchesPattern(pattern)
			if err != nil {
				utils.ErrExit("Invalid table name pattern %q: %s", pattern, err)
			}
			return matched
		})
		return result
	}

	extractTableList := func(flagTableList, listName string) ([]sqlname.NameTuple, []string) {
		tableList := utils.CsvStringToSlice(flagTableList)
		var result []sqlname.NameTuple
		var unqualifiedTables []string
		var unknownTables []string
		for _, table := range tableList {
			if noDefaultSchema && len(strings.Split(table, ".")) == 1 {
				unqualifiedTables = append(unqualifiedTables, table)
				continue
			}

			matchingTables := findPatternMatchingTables(table)
			if len(matchingTables) == 0 {
				unknownTables = append(unknownTables, table) //so that unknown check can be done later
			} else {
				result = append(result, matchingTables...)
			}
		}
		if len(unqualifiedTables) > 0 {
			utils.ErrExit("Qualify following table names %v in the %s list with schema-name.", unqualifiedTables, listName)
		}
		log.Infof("%s tableList: %v", listName, result)
		return result, unknownTables
	}

	includeList, unknownInclude := extractTableList(tconf.TableList, "include")
	excludeList, unknownExclude := extractTableList(tconf.ExcludeTableList, "exclude")
	allUnknown := append(unknownInclude, unknownExclude...)
	if len(allUnknown) > 0 {
		utils.PrintAndLog("Unknown table names in the table-list: %v", allUnknown)
		utils.PrintAndLog("Valid table names are: %v", lo.Map(allTables, func(t sqlname.NameTuple, _ int) string {
			return t.ForOutput()
		}))
		utils.ErrExit("Please fix the table names in table-list and retry.")
	}

	for _, task := range importFileTasks {
		if len(includeList) > 0 && !slices.Contains(includeList, task.TableNameTup) {
			log.Infof("Skipping table %q (fileName: %s) as it is not in the include list", task.TableNameTup, task.FilePath)
			continue
		}
		if len(excludeList) > 0 && slices.Contains(excludeList, task.TableNameTup) {
			log.Infof("Skipping table %q (fileName: %s) as it is in the exclude list", task.TableNameTup, task.FilePath)
			continue
		}
		result = append(result, task)
	}
	return result
}

func updateTargetConfInMigrationStatus() {
	err := metaDB.UpdateMigrationStatusRecord(func(record *metadb.MigrationStatusRecord) {
		switch importerRole {
		case TARGET_DB_IMPORTER_ROLE, IMPORT_FILE_ROLE:
			record.TargetDBConf = tconf.Clone()
			record.TargetDBConf.Password = ""
			record.TargetDBConf.Uri = ""
		case SOURCE_REPLICA_DB_IMPORTER_ROLE:
			record.SourceReplicaDBConf = tconf.Clone()
			record.SourceReplicaDBConf.Password = ""
			record.SourceReplicaDBConf.Uri = ""
		case SOURCE_DB_IMPORTER_ROLE:
			record.SourceDBAsTargetConf = tconf.Clone()
			record.SourceDBAsTargetConf.Password = ""
			record.SourceDBAsTargetConf.Uri = ""
		default:
			panic(fmt.Sprintf("unsupported importer role: %s", importerRole))
		}
	})
	if err != nil {
		utils.ErrExit("Failed to update target conf in migration status record: %s", err)
	}
}

func importData(importFileTasks []*ImportFileTask) {
	err := retrieveMigrationUUID()
	if err != nil {
		utils.ErrExit("failed to get migration UUID: %w", err)
	}

	if importerRole == TARGET_DB_IMPORTER_ROLE {
		importDataStartEvent := createSnapshotImportStartedEvent()
		controlPlane.SnapshotImportStarted(&importDataStartEvent)
	}
	updateTargetConfInMigrationStatus()
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("Failed to get migration status record: %s", err)
	}

	if msr.SnapshotMechanism == "debezium" {
		valueConverter, err = dbzm.NewValueConverter(exportDir, tdb, tconf, importerRole, msr.SourceDBConf.DBType)
	} else {
		valueConverter, err = dbzm.NewNoOpValueConverter()
	}
	if err != nil {
		utils.ErrExit("create value converter: %s", err)
	}

	TableNameToSchema, err = valueConverter.GetTableNameToSchema()
	if err != nil {
		utils.ErrExit("getting table name to schema: %s", err)
	}

	err = tdb.InitConnPool()
	if err != nil {
		utils.ErrExit("Failed to initialize the target DB connection pool: %s", err)
	}
	utils.PrintAndLog("Using %d parallel jobs.", tconf.Parallelism)

	targetDBVersion := tdb.GetVersion()

	fmt.Printf("%s version: %s\n", tconf.TargetDBType, targetDBVersion)

	err = tdb.CreateVoyagerSchema()
	if err != nil {
		utils.ErrExit("Failed to create voyager metadata schema on target DB: %s", err)
	}

	utils.PrintAndLog("\nimport of data in %q database started", tconf.DBName)
	var pendingTasks, completedTasks []*ImportFileTask
	state := NewImportDataState(exportDir)
	if startClean {
		cleanImportState(state, importFileTasks)
		pendingTasks = importFileTasks
	} else {
		pendingTasks, completedTasks, err = classifyTasks(state, importFileTasks)
		if err != nil {
			utils.ErrExit("Failed to classify tasks: %s", err)
		}
	}

	//TODO: BUG: we are applying table-list filter on importFileTasks, but here we are considering all tables as per
	// export-data table-list. Should be fine because we are only disabling and re-enabling, but this is still not ideal.
	sourceTableList := msr.TableListExportedFromSource
	if msr.SourceDBConf != nil {
		source = *msr.SourceDBConf
	}
	importTableList, err := getImportTableList(sourceTableList)
	if err != nil {
		utils.ErrExit("Error generating table list to import: %v", err)
	}

	disableGeneratedAlwaysAsIdentityColumns(importTableList)
	// restore value for IDENTITY BY DEFAULT columns once IDENTITY ALWAYS columns are enabled back
	defer restoreGeneratedByDefaultAsIdentityColumns(importTableList)
	defer enableGeneratedAlwaysAsIdentityColumns()

	// Import snapshots
	if importerRole != SOURCE_DB_IMPORTER_ROLE {
		utils.PrintAndLog("Already imported tables: %v", importFileTasksToTableNames(completedTasks))
		if len(pendingTasks) == 0 {
			utils.PrintAndLog("All the tables are already imported, nothing left to import\n")
		} else {
			utils.PrintAndLog("Tables to import: %v", importFileTasksToTableNames(pendingTasks))
			prepareTableToColumns(pendingTasks) //prepare the tableToColumns map
			poolSize := tconf.Parallelism * 2
			progressReporter := NewImportDataProgressReporter(bool(disablePb))

			if importerRole == TARGET_DB_IMPORTER_ROLE {
				importDataAllTableMetrics := createInitialImportDataTableMetrics(pendingTasks)
				controlPlane.UpdateImportedRowCount(importDataAllTableMetrics)
			}

			for _, task := range pendingTasks {
				// The code can produce `poolSize` number of batches at a time. But, it can consume only
				// `parallelism` number of batches at a time.
				batchImportPool = pool.New().WithMaxGoroutines(poolSize)

				totalProgressAmount := getTotalProgressAmount(task)
				progressReporter.ImportFileStarted(task, totalProgressAmount)
				importedProgressAmount := getImportedProgressAmount(task, state)
				progressReporter.AddProgressAmount(task, importedProgressAmount)

				var currentProgress int64
				updateProgressFn := func(progressAmount int64) {
					currentProgress += progressAmount
					progressReporter.AddProgressAmount(task, progressAmount)

					if importerRole == TARGET_DB_IMPORTER_ROLE && totalProgressAmount > currentProgress {
						importDataTableMetrics := createImportDataTableMetrics(task.TableNameTup.ForKey(),
							currentProgress, totalProgressAmount, ROW_UPDATE_STATUS_IN_PROGRESS)
						// The metrics are sent after evry 5 secs in implementation of UpdateImportedRowCount
						controlPlane.UpdateImportedRowCount(
							[]*cp.UpdateImportedRowCountEvent{&importDataTableMetrics})
					}
				}

				importFile(state, task, updateProgressFn)
				batchImportPool.Wait() // Wait for the file import to finish.

				if importerRole == TARGET_DB_IMPORTER_ROLE {
					importDataTableMetrics := createImportDataTableMetrics(task.TableNameTup.ForKey(),
						currentProgress, totalProgressAmount, ROW_UPDATE_STATUS_COMPLETED)
					controlPlane.UpdateImportedRowCount(
						[]*cp.UpdateImportedRowCountEvent{&importDataTableMetrics})
				}

				progressReporter.FileImportDone(task) // Remove the progress-bar for the file.\
			}
			time.Sleep(time.Second * 2)
		}
		utils.PrintAndLog("snapshot data import complete\n\n")
	}

	if !dbzm.IsDebeziumForDataExport(exportDir) {
		errImport := executePostSnapshotImportSqls()
		if errImport != nil {
			utils.ErrExit("Error in importing post-snapshot-import sql: %v", err)
		}
		displayImportedRowCountSnapshot(state, importFileTasks)
	} else {
		if changeStreamingIsEnabled(importType) {
			if importerRole != SOURCE_DB_IMPORTER_ROLE {
				displayImportedRowCountSnapshot(state, importFileTasks)
			}

			color.Blue("streaming changes to %s...", tconf.TargetDBType)

			if err != nil {
				utils.ErrExit("failed to get table unique key columns map: %s", err)
			}
			valueConverter, err = dbzm.NewValueConverter(exportDir, tdb, tconf, importerRole, source.DBType)
			if err != nil {
				utils.ErrExit("Failed to create value converter: %s", err)
			}
			err = streamChanges(state, importTableList)
			if err != nil {
				utils.ErrExit("Failed to stream changes to %s: %s", tconf.TargetDBType, err)
			}

			status, err := dbzm.ReadExportStatus(filepath.Join(exportDir, "data", "export_status.json"))
			if err != nil {
				utils.ErrExit("failed to read export status for restore sequences: %s", err)
			}
			// in case of live migration sequences are restored after cutover
			err = tdb.RestoreSequences(status.Sequences)
			if err != nil {
				utils.ErrExit("failed to restore sequences: %s", err)
			}

			utils.PrintAndLog("Completed streaming all relevant changes to %s", tconf.TargetDBType)
			err = markCutoverProcessed(importerRole)
			if err != nil {
				utils.ErrExit("failed to mark cutover as processed: %s", err)
			}
			utils.PrintAndLog("\nRun the following command to get the current report of the migration:\n" +
				color.CyanString("yb-voyager get data-migration-report --export-dir %q", exportDir))
		} else {
			status, err := dbzm.ReadExportStatus(filepath.Join(exportDir, "data", "export_status.json"))
			if err != nil {
				utils.ErrExit("failed to read export status for restore sequences: %s", err)
			}
			err = tdb.RestoreSequences(status.Sequences)
			if err != nil {
				utils.ErrExit("failed to restore sequences: %s", err)
			}
			displayImportedRowCountSnapshot(state, importFileTasks)
		}
	}

	fmt.Printf("\nImport data complete.\n")

	if importerRole == TARGET_DB_IMPORTER_ROLE {
		importDataCompletedEvent := createSnapshotImportCompletedEvent()
		controlPlane.SnapshotImportCompleted(&importDataCompletedEvent)
		packAndSendImportDataPayload(COMPLETE) // TODO: later for other import data commands
	}

}

func packAndSendImportDataPayload(status string) {

	if !callhome.SendDiagnostics {
		return
	}
	//basic payload details
	payload := createCallhomePayload()
	switch importType {
	case SNAPSHOT_ONLY:
		payload.MigrationType = OFFLINE
	case SNAPSHOT_AND_CHANGES:
		payload.MigrationType = LIVE_MIGRATION //TODO: add FF/FB details
	}
	payload.TargetDBDetails = callhome.MarshalledJsonString(targetDBDetails)
	payload.MigrationPhase = IMPORT_DATA_PHASE
	importDataPayload := callhome.ImportDataPhasePayload{
		ParallelJobs: int64(tconf.Parallelism),
		StartClean:   bool(startClean),
	}

	//Getting the imported snapshot details
	importRowsMap, err := getImportedSnapshotRowsMap("target")
	if err != nil {
		log.Errorf("callhome: error in getting the import data: %v", err)
	} else {
		importRowsMap.IterKV(func(key sqlname.NameTuple, value int64) (bool, error) {
			importDataPayload.TotalRows += value
			if value > importDataPayload.LargestTableRows {
				importDataPayload.LargestTableRows = value
			}
			return true, nil
		})
	}

	payload.PhasePayload = callhome.MarshalledJsonString(importDataPayload)
	payload.Status = status

	err = callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}

func disableGeneratedAlwaysAsIdentityColumns(tables []sqlname.NameTuple) {
	found, err := metaDB.GetJsonObject(nil, identityColumnsMetaDBKey, &TableToIdentityColumnNames)
	if err != nil {
		utils.ErrExit("failed to get identity columns from meta db: %s", err)
	}
	if !found {
		TableToIdentityColumnNames = getIdentityColumnsForTables(tables, "ALWAYS")
		// saving in metadb for handling restarts
		metaDB.InsertJsonObject(nil, identityColumnsMetaDBKey, TableToIdentityColumnNames)
	}

	err = tdb.DisableGeneratedAlwaysAsIdentityColumns(TableToIdentityColumnNames)
	if err != nil {
		utils.ErrExit("failed to disable generated always as identity columns: %s", err)
	}
}

func enableGeneratedAlwaysAsIdentityColumns() {
	err := tdb.EnableGeneratedAlwaysAsIdentityColumns(TableToIdentityColumnNames)
	if err != nil {
		utils.ErrExit("failed to enable generated always as identity columns: %s", err)
	}
}

func restoreGeneratedByDefaultAsIdentityColumns(tables []sqlname.NameTuple) {
	log.Infof("restoring generated by default as identity columns for tables: %v", tables)
	tablesToIdentityColumnNames := getIdentityColumnsForTables(tables, "BY DEFAULT")
	err := tdb.EnableGeneratedByDefaultAsIdentityColumns(tablesToIdentityColumnNames)
	if err != nil {
		utils.ErrExit("failed to enable generated by default as identity columns: %s", err)
	}
}

func getIdentityColumnsForTables(tables []sqlname.NameTuple, identityType string) *utils.StructMap[sqlname.NameTuple, []string] {
	var result = utils.NewStructMap[sqlname.NameTuple, []string]()
	log.Infof("getting identity(%s) columns for tables: %v", identityType, tables)
	for _, table := range tables {
		identityColumns, err := tdb.GetIdentityColumnNamesForTable(table, identityType)
		if err != nil {
			utils.ErrExit("error in getting identity(%s) columns for table %s: %w", identityType, table, err)
		}
		if len(identityColumns) > 0 {
			log.Infof("identity(%s) columns for table %s: %v", identityType, table, identityColumns)
			result.Put(table, identityColumns)
		}
	}
	return result
}

func getTotalProgressAmount(task *ImportFileTask) int64 {
	if reportProgressInBytes {
		return task.FileSize
	} else {
		return task.RowCount
	}
}

func getImportedProgressAmount(task *ImportFileTask, state *ImportDataState) int64 {
	if reportProgressInBytes {
		byteCount, err := state.GetImportedByteCount(task.FilePath, task.TableNameTup)
		if err != nil {
			utils.ErrExit("Failed to get imported byte count for table %s: %s", task.TableNameTup, err)
		}
		return byteCount
	} else {
		rowCount, err := state.GetImportedRowCount(task.FilePath, task.TableNameTup)
		if err != nil {
			utils.ErrExit("Failed to get imported row count for table %s: %s", task.TableNameTup, err)
		}
		return rowCount
	}
}

func importFileTasksToTableNames(tasks []*ImportFileTask) []string {
	tableNames := []string{}
	for _, t := range tasks {
		tableNames = append(tableNames, t.TableNameTup.ForKey())
	}
	return lo.Uniq(tableNames)
}

func importFileTasksToTableNameTuples(tasks []*ImportFileTask) []sqlname.NameTuple {
	tableNames := []sqlname.NameTuple{}
	for _, t := range tasks {
		tableNames = append(tableNames, t.TableNameTup)
	}
	return lo.UniqBy(tableNames, func(t sqlname.NameTuple) string {
		return t.ForKey()
	})
}

func classifyTasks(state *ImportDataState, tasks []*ImportFileTask) (pendingTasks, completedTasks []*ImportFileTask, err error) {
	inProgressTasks := []*ImportFileTask{}
	notStartedTasks := []*ImportFileTask{}
	for _, task := range tasks {
		fileImportState, err := state.GetFileImportState(task.FilePath, task.TableNameTup)
		if err != nil {
			return nil, nil, fmt.Errorf("get table import state: %w", err)
		}
		switch fileImportState {
		case FILE_IMPORT_COMPLETED:
			completedTasks = append(completedTasks, task)
		case FILE_IMPORT_IN_PROGRESS:
			inProgressTasks = append(inProgressTasks, task)
		case FILE_IMPORT_NOT_STARTED:
			notStartedTasks = append(notStartedTasks, task)
		default:
			return nil, nil, fmt.Errorf("invalid table import state: %s", fileImportState)
		}
	}
	// Start with in-progress tasks, followed by not-started tasks.
	return append(inProgressTasks, notStartedTasks...), completedTasks, nil
}

func cleanImportState(state *ImportDataState, tasks []*ImportFileTask) {
	tableNames := importFileTasksToTableNameTuples(tasks)
	nonEmptyNts := tdb.GetNonEmptyTables(tableNames)
	if len(nonEmptyNts) > 0 {
		nonEmptyTableNames := lo.Map(nonEmptyNts, func(nt sqlname.NameTuple, _ int) string {
			return nt.ForOutput()
		})
		utils.PrintAndLog("Following tables are not empty. "+
			"TRUNCATE them before importing data with --start-clean.\n%s",
			strings.Join(nonEmptyTableNames, ", "))
		yes := utils.AskPrompt("Do you want to continue without truncating these tables?")
		if !yes {
			utils.ErrExit("Aborting import.")
		}
	}

	for _, task := range tasks {
		err := state.Clean(task.FilePath, task.TableNameTup)
		if err != nil {
			utils.ErrExit("failed to clean import data state for table %q: %s", task.TableNameTup, err)
		}
	}

	sqlldrDir := filepath.Join(exportDir, "sqlldr")
	if utils.FileOrFolderExists(sqlldrDir) {
		err := os.RemoveAll(sqlldrDir)
		if err != nil {
			utils.ErrExit("failed to remove sqlldr directory %q: %s", sqlldrDir, err)
		}
	}

	if changeStreamingIsEnabled(importType) {
		// clearing state from metaDB based on importerRole
		err := metaDB.ResetQueueSegmentMeta(importerRole)
		if err != nil {
			utils.ErrExit("failed to reset queue segment meta: %s", err)
		}
		err = metaDB.DeleteJsonObject(identityColumnsMetaDBKey)
		if err != nil {
			utils.ErrExit("failed to reset identity columns meta: %s", err)
		}
	}
}

func getImportBatchArgsProto(tableNameTup sqlname.NameTuple, filePath string) *tgtdb.ImportBatchArgs {
	columns, _ := TableToColumnNames.Get(tableNameTup)
	columns, err := tdb.QuoteAttributeNames(tableNameTup, columns)
	if err != nil {
		utils.ErrExit("if required quote column names: %s", err)
	}
	// If `columns` is unset at this point, no attribute list is passed in the COPY command.
	fileFormat := dataFileDescriptor.FileFormat
	if fileFormat == datafile.SQL {
		fileFormat = datafile.TEXT
	}
	importBatchArgsProto := &tgtdb.ImportBatchArgs{
		TableNameTup: tableNameTup,
		Columns:      columns,
		FileFormat:   fileFormat,
		Delimiter:    dataFileDescriptor.Delimiter,
		HasHeader:    dataFileDescriptor.HasHeader && fileFormat == datafile.CSV,
		QuoteChar:    dataFileDescriptor.QuoteChar,
		EscapeChar:   dataFileDescriptor.EscapeChar,
		NullString:   dataFileDescriptor.NullString,
	}
	log.Infof("ImportBatchArgs: %v", spew.Sdump(importBatchArgsProto))
	return importBatchArgsProto
}

func importFile(state *ImportDataState, task *ImportFileTask, updateProgressFn func(int64)) {

	origDataFile := task.FilePath
	importBatchArgsProto := getImportBatchArgsProto(task.TableNameTup, task.FilePath)
	log.Infof("Start splitting table %q: data-file: %q", task.TableNameTup, origDataFile)

	err := state.PrepareForFileImport(task.FilePath, task.TableNameTup)
	if err != nil {
		utils.ErrExit("preparing for file import: %s", err)
	}
	log.Infof("Collect all interrupted/remaining splits.")
	pendingBatches, lastBatchNumber, lastOffset, fileFullySplit, err := state.Recover(task.FilePath, task.TableNameTup)
	if err != nil {
		utils.ErrExit("recovering state for table %q: %s", task.TableNameTup, err)
	}
	for _, batch := range pendingBatches {
		submitBatch(batch, updateProgressFn, importBatchArgsProto)
	}
	if !fileFullySplit {
		splitFilesForTable(state, origDataFile, task.TableNameTup, lastBatchNumber, lastOffset, updateProgressFn, importBatchArgsProto)
	}
}

func splitFilesForTable(state *ImportDataState, filePath string, t sqlname.NameTuple,
	lastBatchNumber int64, lastOffset int64, updateProgressFn func(int64), importBatchArgsProto *tgtdb.ImportBatchArgs) {
	log.Infof("Split data file %q: tableName=%q, largestSplit=%v, largestOffset=%v", filePath, t, lastBatchNumber, lastOffset)
	batchNum := lastBatchNumber + 1
	numLinesTaken := lastOffset

	reader, err := dataStore.Open(filePath)
	if err != nil {
		utils.ErrExit("preparing reader for split generation on file %q: %v", filePath, err)
	}

	dataFile, err := datafile.NewDataFile(filePath, reader, dataFileDescriptor)
	if err != nil {
		utils.ErrExit("open datafile %q: %v", filePath, err)
	}
	defer dataFile.Close()

	log.Infof("Skipping %d lines from %q", lastOffset, filePath)
	err = dataFile.SkipLines(lastOffset)
	if err != nil {
		utils.ErrExit("skipping line for offset=%d: %v", lastOffset, err)
	}

	var readLineErr error = nil
	var line string
	var batchWriter *BatchWriter
	header := ""
	if dataFileDescriptor.HasHeader {
		header = dataFile.GetHeader()
	}
	for readLineErr == nil {

		if batchWriter == nil {
			batchWriter = state.NewBatchWriter(filePath, t, batchNum)
			err := batchWriter.Init()
			if err != nil {
				utils.ErrExit("initializing batch writer for table %q: %s", t, err)
			}
			if header != "" && dataFileDescriptor.FileFormat == datafile.CSV {
				err = batchWriter.WriteHeader(header)
				if err != nil {
					utils.ErrExit("writing header for table %q: %s", t, err)
				}
			}
		}

		line, readLineErr = dataFile.NextLine()
		if readLineErr == nil || (readLineErr == io.EOF && line != "") {
			// handling possible case: last dataline(i.e. EOF) but no newline char at the end
			numLinesTaken += 1
		}
		if line != "" {
			// can't use importBatchArgsProto.Columns as to use case insenstiive column names
			columnNames, _ := TableToColumnNames.Get(t)
			line, err = valueConverter.ConvertRow(t, columnNames, line)
			if err != nil {
				utils.ErrExit("transforming line number=%d for table %q in file %s: %s", batchWriter.NumRecordsWritten+1, t, filePath, err)
			}
		}
		err = batchWriter.WriteRecord(line)
		if err != nil {
			utils.ErrExit("Write to batch %d: %s", batchNum, err)
		}
		if batchWriter.NumRecordsWritten == batchSize ||
			dataFile.GetBytesRead() >= tdb.MaxBatchSizeInBytes() ||
			readLineErr != nil {

			isLastBatch := false
			if readLineErr == io.EOF {
				isLastBatch = true
			} else if readLineErr != nil {
				utils.ErrExit("read line from data file %q: %s", filePath, readLineErr)
			}

			offsetEnd := numLinesTaken
			batch, err := batchWriter.Done(isLastBatch, offsetEnd, dataFile.GetBytesRead())
			if err != nil {
				utils.ErrExit("finalizing batch %d: %s", batchNum, err)
			}
			batchWriter = nil
			dataFile.ResetBytesRead()
			submitBatch(batch, updateProgressFn, importBatchArgsProto)

			if !isLastBatch {
				batchNum += 1
			}
		}
	}
	log.Infof("splitFilesForTable: done splitting data file %q for table %q", filePath, t)
}

func executePostSnapshotImportSqls() error {
	sequenceFilePath := filepath.Join(exportDir, "data", "postdata.sql")
	if utils.FileOrFolderExists(sequenceFilePath) {
		fmt.Printf("setting resume value for sequences %10s\n", "")
		err := executeSqlFile(sequenceFilePath, "SEQUENCE", func(_, _ string) bool { return false })
		if err != nil {
			return err
		}
	}
	return nil
}

func submitBatch(batch *Batch, updateProgressFn func(int64), importBatchArgsProto *tgtdb.ImportBatchArgs) {
	batchImportPool.Go(func() {
		// There are `poolSize` number of competing go-routines trying to invoke COPY.
		// But the `connPool` will allow only `parallelism` number of connections to be
		// used at a time. Thus limiting the number of concurrent COPYs to `parallelism`.
		importBatch(batch, importBatchArgsProto)
		if reportProgressInBytes {
			updateProgressFn(batch.ByteCount)
		} else {
			updateProgressFn(batch.RecordCount)
		}
	})
	log.Infof("Queued batch: %s", spew.Sdump(batch))
}

func importBatch(batch *Batch, importBatchArgsProto *tgtdb.ImportBatchArgs) {
	err := batch.MarkPending()
	if err != nil {
		utils.ErrExit("marking batch %d as pending: %s", batch.Number, err)
	}
	log.Infof("Importing %q", batch.FilePath)

	importBatchArgs := *importBatchArgsProto
	importBatchArgs.FilePath = batch.FilePath
	importBatchArgs.RowsPerTransaction = batch.OffsetEnd - batch.OffsetStart

	var rowsAffected int64
	sleepIntervalSec := 0
	for attempt := 0; attempt < COPY_MAX_RETRY_COUNT; attempt++ {
		tableSchema, _ := TableNameToSchema.Get(batch.TableNameTup)
		rowsAffected, err = tdb.ImportBatch(batch, &importBatchArgs, exportDir, tableSchema)
		if err == nil || tdb.IsNonRetryableCopyError(err) {
			break
		}
		log.Warnf("COPY FROM file %q: %s", batch.FilePath, err)
		sleepIntervalSec += 10
		if sleepIntervalSec > MAX_SLEEP_SECOND {
			sleepIntervalSec = MAX_SLEEP_SECOND
		}
		log.Infof("sleep for %d seconds before retrying the file %s (attempt %d)",
			sleepIntervalSec, batch.FilePath, attempt)
		time.Sleep(time.Duration(sleepIntervalSec) * time.Second)
	}
	log.Infof("%q => %d rows affected", batch.FilePath, rowsAffected)
	if err != nil {
		utils.ErrExit("import %q into %s: %s", batch.FilePath, batch.TableNameTup, err)
	}
	err = batch.MarkDone()
	if err != nil {
		utils.ErrExit("marking batch %q as done: %s", batch.FilePath, err)
	}
}

func newTargetConn() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), tconf.GetConnectionUri())
	if err != nil {
		utils.WaitChannel <- 1
		<-utils.WaitChannel
		utils.ErrExit("connect to target db: %s", err)
	}

	setTargetSchema(conn)

	if sourceDBType == ORACLE && enableOrafce {
		setOrafceSearchPath(conn)
	}

	return conn
}

// TODO: Eventually get rid of this function in favour of TargetYugabyteDB.setTargetSchema().
func setTargetSchema(conn *pgx.Conn) {
	if sourceDBType == POSTGRESQL || tconf.Schema == YUGABYTEDB_DEFAULT_SCHEMA {
		// For PG, schema name is already included in the object name.
		// No need to set schema if importing in the default schema.
		return
	}
	checkSchemaExistsQuery := fmt.Sprintf("SELECT count(schema_name) FROM information_schema.schemata WHERE schema_name = '%s'", tconf.Schema)
	var cntSchemaName int

	if err := conn.QueryRow(context.Background(), checkSchemaExistsQuery).Scan(&cntSchemaName); err != nil {
		utils.ErrExit("run query %q on target %q to check schema exists: %s", checkSchemaExistsQuery, tconf.Host, err)
	} else if cntSchemaName == 0 {
		utils.ErrExit("schema '%s' does not exist in target", tconf.Schema)
	}

	setSchemaQuery := fmt.Sprintf("SET SCHEMA '%s'", tconf.Schema)
	_, err := conn.Exec(context.Background(), setSchemaQuery)
	if err != nil {
		utils.ErrExit("run query %q on target %q: %s", setSchemaQuery, tconf.Host, err)
	}
}

func dropIdx(conn *pgx.Conn, idxName string) error {
	dropIdxQuery := fmt.Sprintf("DROP INDEX IF EXISTS %s", idxName)
	log.Infof("Dropping index: %q", dropIdxQuery)
	_, err := conn.Exec(context.Background(), dropIdxQuery)
	if err != nil {
		return fmt.Errorf("failed to drop index %q: %w", idxName, err)
	}
	return nil
}

func setOrafceSearchPath(conn *pgx.Conn) {
	// append oracle schema in the search_path for orafce
	updateSearchPath := `SELECT set_config('search_path', current_setting('search_path') || ', oracle', false)`
	_, err := conn.Exec(context.Background(), updateSearchPath)
	if err != nil {
		utils.ErrExit("unable to update search_path for orafce extension: %v", err)
	}
}

func getIndexName(sqlQuery string, indexName string) (string, error) {
	// Return the index name itself if it is aleady qualified with schema name
	if len(strings.Split(indexName, ".")) == 2 {
		return indexName, nil
	}

	parts := strings.FieldsFunc(sqlQuery, func(c rune) bool { return unicode.IsSpace(c) || c == '(' || c == ')' })
	for index, part := range parts {
		if strings.EqualFold(part, "ON") {
			tableName := parts[index+1]
			schemaName := getTargetSchemaName(tableName)
			return fmt.Sprintf("%s.%s", schemaName, indexName), nil
		}
	}
	return "", fmt.Errorf("could not find `ON` keyword in the CREATE INDEX statement")
}

// TODO: need automation tests for this, covering cases like schema(public vs non-public) or case sensitive names
func beforeIndexCreation(sqlInfo sqlInfo, conn **pgx.Conn, objType string) error {
	if !strings.Contains(strings.ToUpper(sqlInfo.stmt), "CREATE INDEX") {
		return nil
	}

	fullyQualifiedObjName, err := getIndexName(sqlInfo.stmt, sqlInfo.objName)
	if err != nil {
		return fmt.Errorf("extract qualified index name from DDL [%v]: %w", sqlInfo.stmt, err)
	}
	if invalidTargetIndexesCache == nil {
		invalidTargetIndexesCache, err = getInvalidIndexes(conn)
		if err != nil {
			return fmt.Errorf("failed to fetch invalid indexes: %w", err)
		}
	}

	// check index valid or not
	if invalidTargetIndexesCache[fullyQualifiedObjName] {
		log.Infof("index %q already exists but in invalid state, dropping it", fullyQualifiedObjName)
		err = dropIdx(*conn, fullyQualifiedObjName)
		if err != nil {
			return fmt.Errorf("drop invalid index %q: %w", fullyQualifiedObjName, err)
		}
	}

	// print the index name as index creation takes time and user can see the progress
	color.Yellow("creating index %s ...", fullyQualifiedObjName)
	return nil
}

func getInvalidIndexes(conn **pgx.Conn) (map[string]bool, error) {
	var result = make(map[string]bool)
	// NOTE: this shouldn't fetch any predefined indexes of pg_catalog schema (assuming they can't be invalid) or indexes of other successful migrations
	query := "SELECT indexrelid::regclass FROM pg_index WHERE indisvalid = false"

	rows, err := (*conn).Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("querying invalid indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fullyQualifiedIndexName string
		err := rows.Scan(&fullyQualifiedIndexName)
		if err != nil {
			return nil, fmt.Errorf("scanning row for invalid index name: %w", err)
		}
		// if schema is not provided by catalog table, then it is public schema
		if !strings.Contains(fullyQualifiedIndexName, ".") {
			fullyQualifiedIndexName = fmt.Sprintf("public.%s", fullyQualifiedIndexName)
		}
		result[fullyQualifiedIndexName] = true
	}
	return result, nil
}

// TODO: This function is a duplicate of the one in tgtdb/yb.go. Consolidate the two.
func getTargetSchemaName(tableName string) string {
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		return parts[0]
	}
	if tconf.TargetDBType == POSTGRESQL {
		defaultSchema, noDefaultSchema := GetDefaultPGSchema(tconf.Schema, ",")
		if noDefaultSchema {
			utils.ErrExit("no default schema for table %q ", tableName)
		}
		return defaultSchema
	}
	return tconf.Schema // default set to "public"
}

func prepareTableToColumns(tasks []*ImportFileTask) {
	for _, task := range tasks {
		var columns []string
		dfdTableToExportedColumns := getDfdTableNameToExportedColumns(dataFileDescriptor)
		if dfdTableToExportedColumns != nil {
			columns, _ = dfdTableToExportedColumns.Get(task.TableNameTup)
		} else if dataFileDescriptor.HasHeader {
			// File is either exported from debezium OR this is `import data file` case.
			reader, err := dataStore.Open(task.FilePath)
			if err != nil {
				utils.ErrExit("datastore.Open %q: %v", task.FilePath, err)
			}
			df, err := datafile.NewDataFile(task.FilePath, reader, dataFileDescriptor)
			if err != nil {
				utils.ErrExit("opening datafile %q: %v", task.FilePath, err)
			}
			header := df.GetHeader()
			columns = strings.Split(header, dataFileDescriptor.Delimiter)
			log.Infof("read header from file %q: %s", task.FilePath, header)
			log.Infof("header row split using delimiter %q: %v\n", dataFileDescriptor.Delimiter, columns)
			df.Close()
		}
		TableToColumnNames.Put(task.TableNameTup, columns)
	}
}

func getDfdTableNameToExportedColumns(dataFileDescriptor *datafile.Descriptor) *utils.StructMap[sqlname.NameTuple, []string] {
	if dataFileDescriptor.TableNameToExportedColumns == nil {
		return nil
	}

	result := utils.NewStructMap[sqlname.NameTuple, []string]()
	for tableNameRaw, columnList := range dataFileDescriptor.TableNameToExportedColumns {
		nt, err := namereg.NameReg.LookupTableName(tableNameRaw)
		if err != nil {
			utils.ErrExit("lookup table [%s] in name registry: %v", tableNameRaw, err)
		}
		result.Put(nt, columnList)
	}
	return result
}

func checkExportDataDoneFlag() {
	metaInfoDir := filepath.Join(exportDir, metaInfoDirName)
	_, err := os.Stat(metaInfoDir)
	if err != nil {
		utils.ErrExit("metainfo dir is missing. Exiting.")
	}

	if dataIsExported() {
		return
	}

	utils.PrintAndLog("Waiting for snapshot data export to complete...")
	for !dataIsExported() {
		time.Sleep(time.Second * 2)
	}
	utils.PrintAndLog("Snapshot data export is complete.")
}

func init() {
	importCmd.AddCommand(importDataCmd)
	importDataCmd.AddCommand(importDataToCmd)
	importDataToCmd.AddCommand(importDataToTargetCmd)
	registerFlagsForTarget(importDataCmd)
	registerFlagsForTarget(importDataToTargetCmd)
	registerCommonGlobalFlags(importDataCmd)
	registerCommonGlobalFlags(importDataToTargetCmd)
	registerCommonImportFlags(importDataCmd)
	registerCommonImportFlags(importDataToTargetCmd)
	importDataCmd.Flags().MarkHidden("continue-on-error")
	importDataToTargetCmd.Flags().MarkHidden("continue-on-error")
	registerTargetDBConnFlags(importDataCmd)
	registerTargetDBConnFlags(importDataToTargetCmd)
	registerImportDataCommonFlags(importDataCmd)
	registerImportDataCommonFlags(importDataToTargetCmd)
	registerImportDataFlags(importDataCmd)
	registerImportDataFlags(importDataToTargetCmd)
}

func createSnapshotImportStartedEvent() cp.SnapshotImportStartedEvent {
	result := cp.SnapshotImportStartedEvent{}
	initBaseTargetEvent(&result.BaseEvent, "IMPORT DATA")
	return result
}

func createSnapshotImportCompletedEvent() cp.SnapshotImportCompletedEvent {

	result := cp.SnapshotImportCompletedEvent{}
	initBaseTargetEvent(&result.BaseEvent, "IMPORT DATA")
	return result
}

func createInitialImportDataTableMetrics(tasks []*ImportFileTask) []*cp.UpdateImportedRowCountEvent {

	result := []*cp.UpdateImportedRowCountEvent{}

	for _, task := range tasks {

		var schemaName, tableName string
		schemaName, tableName = cp.SplitTableNameForPG(task.TableNameTup.ForKey())
		tableMetrics := cp.UpdateImportedRowCountEvent{
			BaseUpdateRowCountEvent: cp.BaseUpdateRowCountEvent{
				BaseEvent: cp.BaseEvent{
					EventType:     "IMPORT DATA",
					MigrationUUID: migrationUUID,
					SchemaNames:   []string{schemaName},
				},
				TableName:         tableName,
				Status:            cp.EXPORT_OR_IMPORT_DATA_STATUS_INT_TO_STR[ROW_UPDATE_STATUS_NOT_STARTED],
				TotalRowCount:     getTotalProgressAmount(task),
				CompletedRowCount: 0,
			},
		}
		result = append(result, &tableMetrics)
	}

	return result
}

func createImportDataTableMetrics(tableName string, countLiveRows int64, countTotalRows int64,
	status int) cp.UpdateImportedRowCountEvent {

	var schemaName, tableName2 string
	if strings.Count(tableName, ".") == 1 {
		schemaName, tableName2 = cp.SplitTableNameForPG(tableName)
	} else {
		schemaName, tableName2 = tconf.Schema, tableName
	}
	result := cp.UpdateImportedRowCountEvent{
		BaseUpdateRowCountEvent: cp.BaseUpdateRowCountEvent{
			BaseEvent: cp.BaseEvent{
				EventType:     "IMPORT DATA",
				MigrationUUID: migrationUUID,
				SchemaNames:   []string{schemaName},
			},
			TableName:         tableName2,
			Status:            cp.EXPORT_OR_IMPORT_DATA_STATUS_INT_TO_STR[status],
			TotalRowCount:     countTotalRows,
			CompletedRowCount: countLiveRows,
		},
	}

	return result
}
