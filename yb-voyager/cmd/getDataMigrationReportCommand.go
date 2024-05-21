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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/gosuri/uitable"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/jsonfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var targetDbPassword string
var sourceReplicaDbPassword string
var sourceDbPassword string
var nameRegistryForSourceReplicaRole *namereg.NameRegistry

var getDataMigrationReportCmd = &cobra.Command{
	Use:   "data-migration-report",
	Short: "Print the consolidated report of migration of data.",
	Long:  `Print the consolidated report of migration of data among different DBs (source / target / source-replica) when export-type 'snapshot-and-changes' is enabled.`,
	PreRun: func(cmd *cobra.Command, args []string) {
		validateReportOutputFormat(migrationReportFormats, reportOrStatusCmdOutputFormat)
	},
	Run: func(cmd *cobra.Command, args []string) {
		migrationStatus, err := metaDB.GetMigrationStatusRecord()
		if err != nil {
			utils.ErrExit("error while getting migration status: %w\n", err)
		}
		streamChanges, err := checkStreamingMode()
		if err != nil {
			utils.ErrExit("error while checking streaming mode: %w\n", err)
		}
		migrationUUID, err = uuid.Parse(migrationStatus.MigrationUUID)
		if err != nil {
			utils.ErrExit("error while parsing migration UUID: %w\n", err)
		}
		if streamChanges {
			if migrationStatus.TargetDBConf != nil {
				getTargetPassword(cmd)
				migrationStatus.TargetDBConf.Password = tconf.Password
			}
			if migrationStatus.FallForwardEnabled {
				getSourceReplicaDBPassword(cmd)
				migrationStatus.SourceReplicaDBConf.Password = tconf.Password
			}
			if migrationStatus.FallbackEnabled {
				getSourceDBPassword(cmd)
				migrationStatus.SourceDBAsTargetConf.Password = tconf.Password
			}
			err = InitNameRegistry(exportDir, "", nil, nil, nil, nil, false)
			if err != nil {
				utils.ErrExit("initializing name registry: %v", err)
			}
			color.Yellow("Generating data migration report for migration UUID: %s...\n", migrationStatus.MigrationUUID)
			getDataMigrationReportCmdFn(migrationStatus)
		} else {
			utils.ErrExit("Error: Data migration report is only applicable when export-type is 'snapshot-and-changes'(live migration)\nPlease run export data status/import data status commands.")
		}
	},
}

type rowData struct {
	TableName            string `json:"table_name"`
	DBType               string `json:"db_type"`
	ExportedSnapshotRows int64  `json:"exported_snapshot_rows"`
	ImportedSnapshotRows int64  `json:"imported_snapshot_rows"`
	ImportedInserts      int64  `json:"imported_inserts"`
	ImportedUpdates      int64  `json:"imported_updates"`
	ImportedDeletes      int64  `json:"imported_deletes"`
	ExportedInserts      int64  `json:"exported_inserts"`
	ExportedUpdates      int64  `json:"exported_updates"`
	ExportedDeletes      int64  `json:"exported_deletes"`
	FinalRowCount        int64  `json:"final_row_count"`
}

var reportData []*rowData

var fBEnabled, fFEnabled bool
var firstHeader = []string{"TABLE", "DB_TYPE", "EXPORTED", "IMPORTED", "EXPORTED", "EXPORTED", "EXPORTED", "IMPORTED", "IMPORTED", "IMPORTED", "FINAL_ROW_COUNT"}
var secondHeader = []string{"", "", "SNAPSHOT_ROWS", "SNAPSHOT_ROWS", "INSERTS", "UPDATES", "DELETES", "INSERTS", "UPDATES", "DELETES", ""}

func getDataMigrationReportCmdFn(msr *metadb.MigrationStatusRecord) {
	fBEnabled = msr.FallbackEnabled
	fFEnabled = msr.FallForwardEnabled
	tableList := msr.TableListExportedFromSource
	tableNameTups, err := getImportTableList(tableList)
	if err != nil {
		utils.ErrExit("getting name tuples from table list: %v", err)
	}
	params := namereg.NameRegistryParams{
		FilePath: fmt.Sprintf("%s/metainfo/name_registry.json", exportDir),
		Role:     SOURCE_REPLICA_DB_IMPORTER_ROLE,
		SDB:      nil,
		YBDB:     nil,
	}
	nameRegistryForSourceReplicaRole = namereg.NewNameRegistry(params)
	err = nameRegistryForSourceReplicaRole.Init()
	if err != nil {
		utils.ErrExit("initializing name registry for source replica: %v", err)
	}
	uitbl := uitable.New()
	uitbl.MaxColWidth = 50
	uitbl.Wrap = true
	uitbl.Separator = " | "

	maxTablesInOnePage := 10

	addHeader(uitbl, firstHeader...)
	addHeader(uitbl, secondHeader...)
	exportStatusFilePath := filepath.Join(exportDir, "data", "export_status.json")
	dbzmStatus, err := dbzm.ReadExportStatus(exportStatusFilePath)
	if err != nil {
		utils.ErrExit("Failed to read export status file %s: %v", exportStatusFilePath, err)
	}
	dbzmNameTupToRowCount := utils.NewStructMap[sqlname.NameTuple, int64]()

	exportSnapshotStatusFilePath := filepath.Join(exportDir, "metainfo", "export_snapshot_status.json")
	exportSnapshotStatusFile = jsonfile.NewJsonFile[ExportSnapshotStatus](exportSnapshotStatusFilePath)
	var exportSnapshotStatus *ExportSnapshotStatus

	sqlname.SourceDBType = source.DBType
	var exportedPGSnapshotRowsMap *utils.StructMap[sqlname.NameTuple, int64]

	source = *msr.SourceDBConf
	if source.DBType == POSTGRESQL {
		exportSnapshotStatus, err = exportSnapshotStatusFile.Read()
		if err != nil {
			utils.ErrExit("Failed to read export status file %s: %v", exportSnapshotStatusFilePath, err)
		}
		exportedPGSnapshotRowsMap, _, err = getExportedSnapshotRowsMap(exportSnapshotStatus)
		if err != nil {
			utils.ErrExit("error while getting exported snapshot rows: %w\n", err)
		}
	} else {
		for _, tableExportStatus := range dbzmStatus.Tables {
			tableName := fmt.Sprintf("%s.%s", tableExportStatus.SchemaName, tableExportStatus.TableName)
			nt, err := namereg.NameReg.LookupTableName(tableName)
			if err != nil {
				utils.ErrExit("lookup %s in name registry: %v", tableName, err)
			}
			dbzmNameTupToRowCount.Put(nt, tableExportStatus.ExportedRowCountSnapshot)
		}
	}

	var sourceExportedEventsMap *utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter]
	var targetExportedEventsMap *utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter]
	sourceExportedEventsMap, err = metaDB.GetExportedEventsStatsForExporterRole(SOURCE_DB_EXPORTER_ROLE)
	if err != nil {
		utils.ErrExit("getting exported events from source stats: %v", err)
	}
	if fFEnabled {
		targetExportedEventsMap, err = metaDB.GetExportedEventsStatsForExporterRole(TARGET_DB_EXPORTER_FF_ROLE)
		if err != nil {
			utils.ErrExit("getting exported events from target stats: %v", err)
		}
	}
	if fBEnabled {
		targetExportedEventsMap, err = metaDB.GetExportedEventsStatsForExporterRole(TARGET_DB_EXPORTER_FB_ROLE)
		if err != nil {
			utils.ErrExit("getting exported events from target stats: %v", err)
		}
	}

	var targetImportedSnapshotRowsMap *utils.StructMap[sqlname.NameTuple, int64]
	if msr.TargetDBConf != nil {
		targetImportedSnapshotRowsMap, err = getImportedSnapshotRowsMap("target", tableNameTups)
		if err != nil {
			utils.ErrExit("error while getting imported snapshot rows for target DB: %w\n", err)
		}
	}

	var replicaImportedSnapshotRowsMap *utils.StructMap[sqlname.NameTuple, int64]
	if fFEnabled {
		oldNameReg := namereg.NameReg
		namereg.NameReg = *nameRegistryForSourceReplicaRole
		replicaImportedSnapshotRowsMap, err = getImportedSnapshotRowsMap("source-replica", tableNameTups)
		if err != nil {
			utils.ErrExit("error while getting imported snapshot rows for source-replica DB: %w\n", err)
		}
		namereg.NameReg = oldNameReg
	}

	for i, nameTup := range tableNameTups {
		uitbl.AddRow() // blank row

		row := rowData{}
		updateExportedSnapshotRowsInTheRow(msr, &row, nameTup, dbzmNameTupToRowCount, exportedPGSnapshotRowsMap)
		row.ImportedSnapshotRows = 0
		row.TableName = nameTup.ForKey()
		row.DBType = "source"
		err := updateExportedEventsCountsInTheRow(&row, nameTup, sourceExportedEventsMap, targetExportedEventsMap) //source OUT counts
		if err != nil {
			utils.ErrExit("error while getting exported events counts for source DB: %w\n", err)
		}
		if fBEnabled {
			err = updateImportedEventsCountsInTheRow(source.DBType, &row, nameTup, msr.SourceDBAsTargetConf, nil) //fall back IN counts
			if err != nil {
				utils.ErrExit("error while getting imported events for source DB in case of fall-back: %w\n", err)
			}
		}
		addRowInTheTable(uitbl, row, nameTup)
		row = rowData{}
		row.TableName = ""
		row.DBType = "target"
		row.ExportedSnapshotRows = 0
		if msr.TargetDBConf != nil { // In case import is not started yet, target DB conf will be nil
			err = updateImportedEventsCountsInTheRow(source.DBType, &row, nameTup, msr.TargetDBConf, targetImportedSnapshotRowsMap) //target IN counts
			if err != nil {
				utils.ErrExit("error while getting imported events for target DB: %w\n", err)
			}
		}
		if fFEnabled || fBEnabled {
			err = updateExportedEventsCountsInTheRow(&row, nameTup, sourceExportedEventsMap, targetExportedEventsMap) // target OUT counts
			if err != nil {
				utils.ErrExit("error while getting exported events for target DB: %w\n", err)
			}
		}
		addRowInTheTable(uitbl, row, nameTup)
		if fFEnabled {
			row = rowData{}
			row.TableName = ""
			row.DBType = "source-replica"
			row.ExportedSnapshotRows = 0
			err = updateImportedEventsCountsInTheRow(source.DBType, &row, nameTup, msr.SourceReplicaDBConf, replicaImportedSnapshotRowsMap) //fall forward IN counts
			if err != nil {
				utils.ErrExit("error while getting imported events for DB %s: %w\n", row.DBType, err)
			}
			addRowInTheTable(uitbl, row, nameTup)
		}

		if i%maxTablesInOnePage == 0 && i != 0 && reportOrStatusCmdOutputFormat == "table" {
			//multiple table in case of large set of tables
			fmt.Print("\n")
			fmt.Println(uitbl)
			fmt.Print("\n")
			uitbl = uitable.New()
			uitbl.MaxColWidth = 50
			uitbl.Separator = " | "
			addHeader(uitbl, firstHeader...)
			addHeader(uitbl, secondHeader...)
		}
	}
	if reportOrStatusCmdOutputFormat == "json" {
		// Print the report in json format.
		reportFilePath := filepath.Join(exportDir, "reports", "data-migration-report.json")
		reportFile := jsonfile.NewJsonFile[[]*rowData](reportFilePath)
		err := reportFile.Create(&reportData)
		if err != nil {
			utils.ErrExit("creating into json file %s: %v", reportFilePath, err)
		}
		fmt.Print(color.GreenString("Data migration report is written to %s\n", reportFilePath))
		return
	}
	if uitbl.Rows != nil {
		fmt.Print("\n")
		fmt.Println(uitbl)
		fmt.Print("\n")
	}

}

func addRowInTheTable(uitbl *uitable.Table, row rowData, nameTup sqlname.NameTuple) {
	if reportOrStatusCmdOutputFormat == "json" {
		row.TableName = nameTup.ForKey()
		row.FinalRowCount = getFinalRowCount(row)
		reportData = append(reportData, &row)
		return
	}
	uitbl.AddRow(row.TableName, row.DBType, row.ExportedSnapshotRows, row.ImportedSnapshotRows, row.ExportedInserts, row.ExportedUpdates, row.ExportedDeletes, row.ImportedInserts, row.ImportedUpdates, row.ImportedDeletes, getFinalRowCount(row))
}

func updateExportedSnapshotRowsInTheRow(msr *metadb.MigrationStatusRecord, row *rowData, nameTup sqlname.NameTuple, dbzmSnapshotRowCount *utils.StructMap[sqlname.NameTuple, int64], exportedSnapshotPGRowsMap *utils.StructMap[sqlname.NameTuple, int64]) error {
	// TODO: read only from one place(data file descriptor). Right now, data file descriptor does not store schema names.
	if msr.SnapshotMechanism == "debezium" {
		row.ExportedSnapshotRows, _ = dbzmSnapshotRowCount.Get(nameTup)
	} else {
		row.ExportedSnapshotRows, _ = exportedSnapshotPGRowsMap.Get(nameTup)
	}
	return nil
}

func updateImportedEventsCountsInTheRow(sourceDBType string, row *rowData, tableNameTup sqlname.NameTuple, targetConf *tgtdb.TargetConf, snapshotImportedRowsMap *utils.StructMap[sqlname.NameTuple, int64]) error {
	switch row.DBType {
	case "target":
		importerRole = TARGET_DB_IMPORTER_ROLE
	case "source-replica":
		importerRole = SOURCE_REPLICA_DB_IMPORTER_ROLE
	case "source":
		importerRole = SOURCE_DB_IMPORTER_ROLE
	}
	if importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE {
		var err error
		tblName := tableNameTup.ForKey()
		tableNameTup, err = nameRegistryForSourceReplicaRole.LookupTableName(tblName)
		if err != nil {
			return fmt.Errorf("lookup %s in source replica name registry: %v", tblName, err)
		}
	}

	//reinitialise targetDB
	tconf = *targetConf
	tdb = tgtdb.NewTargetDB(&tconf)
	err := tdb.Init()
	if err != nil {
		return fmt.Errorf("failed to initialize the target DB: %w", err)
	}
	defer tdb.Finalize()
	state := NewImportDataState(exportDir)

	if importerRole != SOURCE_DB_IMPORTER_ROLE {
		row.ImportedSnapshotRows, _ = snapshotImportedRowsMap.Get(tableNameTup)
	}

	eventCounter, err := state.GetImportedEventsStatsForTable(tableNameTup, migrationUUID)
	if err != nil {
		if !strings.Contains(err.Error(), "cannot assign NULL to *int64") &&
			!strings.Contains(err.Error(), "converting NULL to int64") { //TODO: handle better in GetImportedEventsStatsForTable() itself later
			return fmt.Errorf("get imported events stats for table %q for DB type %s: %w", tableNameTup, row.DBType, err)
		} else {
			//in case import streaming is not started yet, metadata will not be initialized
			log.Warnf("stream ingestion is not started yet for table %q for DB type %s", tableNameTup, row.DBType)
			eventCounter = &tgtdb.EventCounter{
				NumInserts: 0,
				NumUpdates: 0,
				NumDeletes: 0,
			}
		}
	}
	row.ImportedInserts = eventCounter.NumInserts
	row.ImportedUpdates = eventCounter.NumUpdates
	row.ImportedDeletes = eventCounter.NumDeletes
	return nil
}

func updateExportedEventsCountsInTheRow(row *rowData, tableNameTup sqlname.NameTuple, sourceExportedEventsMap *utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter], targetExportedEventsMap *utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter]) error {
	var exportedEventsMap *utils.StructMap[sqlname.NameTuple, *tgtdb.EventCounter]
	switch row.DBType {
	case "source":
		exportedEventsMap = sourceExportedEventsMap
	case "target":
		exportedEventsMap = targetExportedEventsMap
	}

	eventCounter, _ := exportedEventsMap.Get(tableNameTup)
	if eventCounter != nil {
		row.ExportedInserts = eventCounter.NumInserts
		row.ExportedUpdates = eventCounter.NumUpdates
		row.ExportedDeletes = eventCounter.NumDeletes
	}

	return nil
}

func getFinalRowCount(row rowData) int64 {
	if row.DBType == "source" {
		return row.ExportedSnapshotRows + row.ExportedInserts + row.ImportedInserts - row.ExportedDeletes - row.ImportedDeletes
	}
	return row.ImportedSnapshotRows + row.ImportedInserts + row.ExportedInserts - row.ImportedDeletes - row.ExportedDeletes
}

func init() {
	getCommand.AddCommand(getDataMigrationReportCmd)
	registerExportDirFlag(getDataMigrationReportCmd)
	getDataMigrationReportCmd.Flags().StringVar(&reportOrStatusCmdOutputFormat, "output-format", "table",
		"format in which report will be generated: (table, json)")
	getDataMigrationReportCmd.Flags().MarkHidden("output-format") //confirm this if should be hidden or not

	getDataMigrationReportCmd.Flags().StringVar(&sourceReplicaDbPassword, "source-replica-db-password", "",
		"password with which to connect to the target Source-Replica DB server. Alternatively, you can also specify the password by setting the environment variable SOURCE_REPLICA_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes.")

	getDataMigrationReportCmd.Flags().StringVar(&sourceDbPassword, "source-db-password", "",
		"password with which to connect to the target source DB server. Alternatively, you can also specify the password by setting the environment variable SOURCE_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes")

	getDataMigrationReportCmd.Flags().StringVar(&targetDbPassword, "target-db-password", "",
		"password with which to connect to the target YugabyteDB server. Alternatively, you can also specify the password by setting the environment variable TARGET_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes.")
}
