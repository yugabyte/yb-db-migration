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
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/jsonfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

const exportDataStatusMsg = "Export Data Status for SourceDB\n"

var exportDataStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Print status of an ongoing/completed data export.",

	PreRun: func(cmd *cobra.Command, args []string) {
		validateReportOutputFormat(migrationReportFormats, reportOrStatusCmdOutputFormat)
	},

	Run: func(cmd *cobra.Command, args []string) {
		streamChanges, err := checkStreamingMode()
		if err != nil {
			utils.ErrExit("error while checking streaming mode: %w\n", err)
		}
		if streamChanges {
			utils.ErrExit("\nNote: Run the following command to get the current report of live migration:\n" +
				color.CyanString("yb-voyager get data-migration-report --export-dir %q\n", exportDir))
		}
		err = InitNameRegistry(exportDir, SOURCE_DB_EXPORTER_ROLE, nil, nil, nil, nil, false)
		if err != nil {
			utils.ErrExit("initializing name registry: %v", err)
		}
		useDebezium = dbzm.IsDebeziumForDataExport(exportDir)
		var rows []*exportTableMigStatusOutputRow
		if useDebezium {
			rows, err = runExportDataStatusCmdDbzm(streamChanges)
		} else {
			rows, err = runExportDataStatusCmd()
		}
		if err != nil {
			utils.ErrExit("error: %s\n", err)
		}
		if reportOrStatusCmdOutputFormat == "json" {
			// Print the report in json format.
			reportFilePath := filepath.Join(exportDir, "reports", "export-data-status-report.json")
			reportFile := jsonfile.NewJsonFile[[]*exportTableMigStatusOutputRow](reportFilePath)
			err := reportFile.Create(&rows)
			if err != nil {
				utils.ErrExit("creating into json file %s: %v", reportFilePath, err)
			}
			fmt.Print(color.GreenString("Export data status report is written to %s\n", reportFilePath))
			return
		}
		displayExportDataStatus(rows)
	},
}

var migrationReportFormats = []string{"table", "json"}

func init() {
	exportDataCmd.AddCommand(exportDataStatusCmd)
	exportDataStatusCmd.Flags().StringVar(&reportOrStatusCmdOutputFormat, "output-format", "table",
		"format in which report will be generated: (table, json)")
	exportDataStatusCmd.Flags().MarkHidden("output-format") //confirm this if should be hidden or not
}

type exportTableMigStatusOutputRow struct {
	TableName     string `json:"table_name"`
	Status        string `json:"status"`
	ExportedCount int64  `json:"exported_count"`
}

var InProgressTableSno int

// Note that the `export data status` is running in a separate process. It won't have access to the in-memory state
// held in the main `export data` process.
func runExportDataStatusCmdDbzm(streamChanges bool) ([]*exportTableMigStatusOutputRow, error) {
	exportStatusFilePath := filepath.Join(exportDir, "data", "export_status.json")
	status, err := dbzm.ReadExportStatus(exportStatusFilePath)
	if err != nil {
		utils.ErrExit("Failed to read export status file %s: %v", exportStatusFilePath, err)
	}
	if status == nil {
		return nil, nil
	}
	InProgressTableSno = status.InProgressTableSno()
	var rows []*exportTableMigStatusOutputRow
	var row *exportTableMigStatusOutputRow

	for _, tableStatus := range status.Tables {
		row = getSnapshotExportStatusRow(&tableStatus)
		rows = append(rows, row)
	}
	return rows, nil
}

func getSnapshotExportStatusRow(tableStatus *dbzm.TableExportStatus) *exportTableMigStatusOutputRow {
	nt, err := namereg.NameReg.LookupTableName(fmt.Sprintf("%s.%s", tableStatus.SchemaName, tableStatus.TableName))
	if err != nil {
		utils.ErrExit("lookup %s in name registry: %v", tableStatus.TableName, err)
	}
	row := &exportTableMigStatusOutputRow{
		TableName:     nt.ForMinOutput(),
		Status:        "DONE",
		ExportedCount: tableStatus.ExportedRowCountSnapshot,
	}
	isSnapshot, err := dbzm.IsLiveMigrationInSnapshotMode(exportDir)
	if err != nil {
		utils.ErrExit("failed to read the status of live migration: %v", err)
	}
	if tableStatus.Sno == InProgressTableSno && isSnapshot {
		row.Status = "EXPORTING"
	}
	return row
}

func runExportDataStatusCmd() ([]*exportTableMigStatusOutputRow, error) {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return nil, fmt.Errorf("error while getting migration status record: %v", err)
	}
	tableList := msr.TableListExportedFromSource
	source = *msr.SourceDBConf
	sqlname.SourceDBType = source.DBType
	var outputRows []*exportTableMigStatusOutputRow
	exportSnapshotStatusFilePath := filepath.Join(exportDir, "metainfo", "export_snapshot_status.json")
	exportSnapshotStatusFile = jsonfile.NewJsonFile[ExportSnapshotStatus](exportSnapshotStatusFilePath)
	exportStatusSnapshot, err := exportSnapshotStatusFile.Read()
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		utils.ErrExit("Failed to read export status file %s: %v", exportSnapshotStatusFilePath, err)
	}

	exportedSnapshotRow, exportedSnapshotStatus, err := getExportedSnapshotRowsMap(exportStatusSnapshot)
	if err != nil {
		return nil, fmt.Errorf("error while getting exported snapshot rows map: %v", err)
	}

	leafPartitions := getLeafPartitionsFromRootTable()

	for _, tableName := range tableList {
		finalFullTableName, err := namereg.NameReg.LookupTableName(tableName)
		if err != nil {
			return nil, fmt.Errorf("lookup %s in name registry: %v", tableName, err)
		}
		displayTableName := finalFullTableName.ForMinOutput()
		partitions := leafPartitions[finalFullTableName.ForOutput()]
		if source.DBType == POSTGRESQL && partitions != nil {
			partitions := strings.Join(partitions, ", ")
			displayTableName = fmt.Sprintf("%s (%s)", displayTableName, partitions)
		}
		snapshotStatus, _ := exportedSnapshotStatus.Get(finalFullTableName)
		finalStatus := snapshotStatus[0]
		if len(snapshotStatus) > 1 { // status for root partition wrt leaf partitions
			exportingLeaf := 0
			doneLeaf := 0
			not_started := 0
			for _, status := range snapshotStatus {
				if status == "EXPORTING" {
					exportingLeaf++
				} else if status == "DONE" {
					doneLeaf++
				} else {
					not_started++
				}
			}
			if exportingLeaf > 0 {
				finalStatus = "EXPORTING"
			} else if doneLeaf == len(snapshotStatus) {
				finalStatus = "DONE"
			} else if not_started == len(snapshotStatus) {
				finalStatus = "NOT_STARTED"
			}
		}
		exportedCount, _ := exportedSnapshotRow.Get(finalFullTableName)
		row := &exportTableMigStatusOutputRow{
			TableName:     displayTableName,
			Status:        finalStatus,
			ExportedCount: exportedCount,
		}
		outputRows = append(outputRows, row)
	}

	return outputRows, nil
}

func displayExportDataStatus(rows []*exportTableMigStatusOutputRow) {
	color.Cyan(exportDataStatusMsg)
	table := uitable.New()
	addHeader(table, "TABLE", "STATUS", "EXPORTED ROWS")

	// First sort by status and then by table-name.
	sort.Slice(rows, func(i, j int) bool {
		ordStates := map[string]int{"EXPORTING": 1, "DONE": 2, "NOT_STARTED": 3, "STREAMING": 4}
		row1 := rows[i]
		row2 := rows[j]
		if row1.Status == row2.Status {
			return strings.Compare(row1.TableName, row2.TableName) < 0
		} else {
			return ordStates[row1.Status] < ordStates[row2.Status]
		}
	})
	for _, row := range rows {
		table.AddRow(row.TableName, row.Status, row.ExportedCount)
	}
	if len(rows) > 0 {
		fmt.Print("\n")
		fmt.Println(table)
		fmt.Print("\n")
	}
}
