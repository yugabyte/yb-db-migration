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
package dbzm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/samber/lo"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

const (
	MODE_SNAPSHOT  = "SNAPSHOT"
	MODE_STREAMING = "STREAMING"
)

type TableExportStatus struct {
	Sno                      int    `json:"sno"`
	DatabaseName             string `json:"database_name"`
	SchemaName               string `json:"schema_name"`
	TableName                string `json:"table_name"`
	FileName                 string `json:"file_name"`
	ExportedRowCountSnapshot int64  `json:"exported_row_count_snapshot"`
}

type ExportStatus struct {
	Mode      string              `json:"mode"`
	Tables    []TableExportStatus `json:"tables"`
	Sequences map[string]int64    `json:"sequences"`
}

func (status *ExportStatus) SnapshotExportIsComplete() bool {
	// When streaming mode is active, we assume that the snapshot export is complete.
	return status.Mode == MODE_STREAMING
}

func ReadExportStatus(statusFilePath string) (*ExportStatus, error) {
	file, err := os.Open(statusFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open file %s: %v", statusFilePath, err)
	}
	defer file.Close()

	var status ExportStatus
	err = json.NewDecoder(file).Decode(&status)
	if err != nil {
		return nil, fmt.Errorf("failed to decode export status file %s: %v", statusFilePath, err)
	}
	return &status, nil
}

func IsLiveMigrationInSnapshotMode(exportDir string) (bool, error) {
	statusFilePath := filepath.Join(exportDir, "data", "export_status.json")
	status, err := ReadExportStatus(statusFilePath)
	if err != nil {
		return false, fmt.Errorf("failed to read export status file %s: %v", statusFilePath, err)
	}
	return (status != nil && status.Mode == MODE_SNAPSHOT), nil
}

func IsMigrationInStreamingMode(exportDir string) bool {
	statusFilePath := filepath.Join(exportDir, "data", "export_status.json")
	status, err := ReadExportStatus(statusFilePath)
	if err != nil {
		utils.ErrExit("Failed to read export status file %s: %v", statusFilePath, err)
	}
	return status != nil && status.Mode == MODE_STREAMING
}

// get table with largest sno
func (status *ExportStatus) GetTableWithLargestSno() *TableExportStatus {
	var table *TableExportStatus
	for i := range status.Tables {
		if table == nil || status.Tables[i].Sno > table.Sno {
			table = &status.Tables[i]
		}
	}
	return table
}

func (status *ExportStatus) InProgressTableSno() int {
	if status.SnapshotExportIsComplete() || status.GetTableWithLargestSno() == nil {
		return -1
	}

	return status.GetTableWithLargestSno().Sno
}

func (status *ExportStatus) GetQualifiedTableName(tableSno int) string {
	var schemaOrDb, tableName string
	for i := range status.Tables {
		if status.Tables[i].Sno == tableSno {
			schemaOrDb = status.Tables[i].SchemaName
			if schemaOrDb == "" {
				schemaOrDb = status.Tables[i].DatabaseName
			}
			tableName = status.Tables[i].TableName
			break
		}
	}

	return fmt.Sprintf("%s.%s", schemaOrDb, tableName)
}

func (status *ExportStatus) GetTableExportedRowCount(tableSno int) int64 {
	for i := range status.Tables {
		if status.Tables[i].Sno == tableSno {
			return status.Tables[i].ExportedRowCountSnapshot
		}
	}
	panic("table sno not found in export status")
}

func (status *ExportStatus) GetTableExportStatus(tableName, schemaName string) *TableExportStatus {
	result, found := lo.Find(status.Tables, func(tableStatus TableExportStatus) bool {
		return tableStatus.TableName == tableName && tableStatus.SchemaName == schemaName
	})
	if found {
		return &result
	}
	return nil
}
