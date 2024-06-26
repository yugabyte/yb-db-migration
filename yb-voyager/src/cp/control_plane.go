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
package cp

import (
	"strings"

	"github.com/google/uuid"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var EXPORT_OR_IMPORT_DATA_STATUS_INT_TO_STR = map[int]string{
	0: "NOT STARTED",
	1: "IN PROGRESS",
	2: "DONE",
	3: "COMPLETED",
}

type ControlPlane interface {
	Init() error
	Finalize()

	MigrationAssessmentStarted(*MigrationAssessmentStartedEvent)
	MigrationAssessmentCompleted(*MigrationAssessmentCompletedEvent)

	ExportSchemaStarted(*ExportSchemaStartedEvent)
	ExportSchemaCompleted(*ExportSchemaCompletedEvent) // Only success is reported.

	SchemaAnalysisStarted(*SchemaAnalysisStartedEvent)
	SchemaAnalysisIterationCompleted(*SchemaAnalysisIterationCompletedEvent)

	SnapshotExportStarted(*SnapshotExportStartedEvent)
	UpdateExportedRowCount([]*UpdateExportedRowCountEvent)
	SnapshotExportCompleted(*SnapshotExportCompletedEvent)

	ImportSchemaStarted(*ImportSchemaStartedEvent)
	ImportSchemaCompleted(*ImportSchemaCompletedEvent)

	SnapshotImportStarted(*SnapshotImportStartedEvent)
	UpdateImportedRowCount([]*UpdateImportedRowCountEvent)
	SnapshotImportCompleted(*SnapshotImportCompletedEvent)

	MigrationEnded(*MigrationEndedEvent)
}

func GetSchemaList(schema string) []string {
	return strings.Split(schema, "|")
}

func SplitTableNameForPG(tableName string) (string, string) {
	splitTableName := strings.Split(tableName, ".")
	return splitTableName[0], splitTableName[1]
}

type BaseEvent struct {
	EventType     string
	MigrationUUID uuid.UUID
	DBType        string
	DatabaseName  string
	SchemaNames   []string
}

type BaseUpdateRowCountEvent struct {
	BaseEvent
	TableName         string
	Status            string
	TotalRowCount     int64
	CompletedRowCount int64
}

type ExportSchemaStartedEvent struct {
	BaseEvent
}

type ExportSchemaCompletedEvent struct {
	BaseEvent
}

type MigrationAssessmentStartedEvent struct {
	BaseEvent
}

type MigrationAssessmentCompletedEvent struct {
	BaseEvent
	Report string
}

type SchemaAnalysisStartedEvent struct {
	BaseEvent
}

type SchemaAnalysisIterationCompletedEvent struct {
	BaseEvent
	AnalysisReport utils.SchemaReport
}

type SnapshotExportStartedEvent struct {
	BaseEvent
}

type UpdateExportedRowCountEvent struct {
	BaseUpdateRowCountEvent
}

type SnapshotExportCompletedEvent struct {
	BaseEvent
}

type ImportSchemaStartedEvent struct {
	BaseEvent
}

type ImportSchemaCompletedEvent struct {
	BaseEvent
}

type SnapshotImportStartedEvent struct {
	BaseEvent
}

type UpdateImportedRowCountEvent struct {
	BaseUpdateRowCountEvent
}

type SnapshotImportCompletedEvent struct {
	BaseEvent
}

type MigrationEndedEvent struct {
	BaseEvent
}
