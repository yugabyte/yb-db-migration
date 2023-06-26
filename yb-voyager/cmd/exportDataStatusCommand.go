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
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var exportDataStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Print status of an ongoing/completed data export.",

	Run: func(cmd *cobra.Command, args []string) {
		validateExportDirFlag()
		var err error
		useDebezium = usingDebeziumForDataExport()
		if useDebezium {
			err = runExportDataStatusCmdDbzm()
		} else {
			err = runExportDataStatusCmd()
		}
		if err != nil {
			utils.ErrExit("error: %s\n", err)
		}
	},
}

func init() {
	exportDataCmd.AddCommand(exportDataStatusCmd)
}

type exportTableMigStatusOutputRow struct {
	tableName     string
	status        string
	exportedCount int64
}

// Note that the `export data status` is running in a separate process. It won't have access to the in-memory state
// held in the main `export data` process.
func runExportDataStatusCmdDbzm() error {
	exportStatusFilePath := filepath.Join(exportDir, "data", "export_status.json")
	status, err := dbzm.ReadExportStatus(exportStatusFilePath)
	if err != nil {
		utils.ErrExit("Failed to read export status file %s: %v", exportStatusFilePath, err)
	}
	InProgressTableSno := status.InProgressTableSno()
	var rows []*exportTableMigStatusOutputRow
	for _, table := range status.Tables {
		row := &exportTableMigStatusOutputRow{
			tableName:     table.TableName,
			status:        "DONE",
			exportedCount: table.ExportedRowCountSnapshot,
		}
		if table.Sno == InProgressTableSno && dbzm.IsLiveMigrationInSnapshotMode(exportDir) {
			row.status = "EXPORTING"
		}
		rows = append(rows, row)
	}
	displayExportDataStatus(rows)
	return nil
}

func runExportDataStatusCmd() error {
	tableMap := make(map[string]string)
	dataDir := filepath.Join(exportDir, "data")
	dbTypeFlag := ExtractMetaInfo(exportDir).SourceDBType
	source.DBType = dbTypeFlag
	if dbTypeFlag == "postgresql" {
		tableMap = getMappingForTableNameVsTableFileName(dataDir)
	} else if dbTypeFlag == "mysql" || dbTypeFlag == "oracle" {
		files, err := filepath.Glob(filepath.Join(dataDir, "*_data.sql"))
		if err != nil {
			return fmt.Errorf("error while checking data directory for export data status: %v", err)
		}
		var fileName string
		for _, file := range files {
			fileName = filepath.Base(file)
			//Sample file name: [tmp_]YB_VOYAGER_TEST_data.sql
			if strings.HasPrefix(fileName, "tmp_") {
				tableMap[fileName[4:]] = fileName
			} else {
				tableMap[fileName] = "tmp_" + fileName
			}
		}
	} else {
		return fmt.Errorf("unable to identify source-db-type")
	}

	var outputRows []*exportTableMigStatusOutputRow
	var finalFullTableName string
	for tableName := range tableMap {
		//"_" is treated as a wildcard character in regex query for Glob
		if tableName == "tmp_postdata.sql" || tableName == "tmp_data.sql" {
			continue
		}
		if strings.HasPrefix(tableName, "public.") {
			finalFullTableName = tableName[7:]
		} else {
			finalFullTableName = tableName
		}

		var status string
		//postgresql map returns table names, oracle/mysql map contains file names
		if (source.DBType == POSTGRESQL && utils.FileOrFolderExists(filepath.Join(dataDir, finalFullTableName)+"_data.sql")) || utils.FileOrFolderExists(filepath.Join(dataDir, finalFullTableName)) {
			status = "DONE"
		} else if utils.FileOrFolderExists(filepath.Join(dataDir, tableMap[tableName])) {
			status = "EXPORTING"
		} else {
			status = "NOT_STARTED"
		}
		if source.DBType == ORACLE || source.DBType == MYSQL {
			finalFullTableName = tableName[:len(tableName)-len("_data.sql")]
		}
		row := &exportTableMigStatusOutputRow{
			tableName: finalFullTableName,
			status:    status,
		}
		outputRows = append(outputRows, row)
	}

	displayExportDataStatus(outputRows)

	return nil
}

func displayExportDataStatus(rows []*exportTableMigStatusOutputRow) {
	table := uitable.New()
	headerfmt := color.New(color.FgGreen, color.Underline).SprintFunc()

	if useDebezium {
		table.AddRow(headerfmt("TABLE"), headerfmt("STATUS"), headerfmt("EXPORTED ROWS"))
	} else {
		table.AddRow(headerfmt("TABLE"), headerfmt("STATUS"))
	}

	// First sort by status and then by table-name.
	sort.Slice(rows, func(i, j int) bool {
		ordStates := map[string]int{"EXPORTING": 1, "DONE": 2, "NOT_STARTED": 3}
		row1 := rows[i]
		row2 := rows[j]
		if row1.status == row2.status {
			return strings.Compare(row1.tableName, row2.tableName) < 0
		} else {
			return ordStates[row1.status] < ordStates[row2.status]
		}
	})
	for _, row := range rows {
		if useDebezium {
			table.AddRow(row.tableName, row.status, row.exportedCount)
		} else {
			table.AddRow(row.tableName, row.status)
		}
	}
	if len(rows) > 0 {
		fmt.Print("\n")
		fmt.Println(table)
		fmt.Print("\n")
	}
}

func usingDebeziumForDataExport() bool {
	exportStatusFilePath := filepath.Join(exportDir, "data", "export_status.json") //checking if this file exists to determine if debezium is being used
	_, err := os.Stat(exportStatusFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		utils.ErrExit("checking if debezium is being used as exporting tool: %s\n", err)
	}
	return true
}
