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
package callhome

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

// call-home json formats
var (
	SendDiagnostics utils.BoolStr
)

var (
	CALL_HOME_SERVICE_HOST = "34.105.39.253"
	CALL_HOME_SERVICE_PORT = 80
)

/*
Call-home diagnostics table structure -
CREATE TABLE diagnostics (

	migration_uuid UUID,
	phase_start_time TIMESTAMP WITH TIME ZONE,
	collected_at TIMESTAMP WITH TIME ZONE,
	source_db_details JSONB,
	target_db_details JSONB,
	yb_voyager_version TEXT,
	migration_phase TEXT,
	phase_payload JSONB,
	migration_type TEXT,
	time_taken_sec int,
	status TEXT,
	host_ip character varying 255 -- set by the callhome service
	PRIMARY KEY (migration_uuid, migration_phase, collected_at)

);
*/
type Payload struct {
	MigrationUUID    uuid.UUID `json:"migration_uuid"`
	PhaseStartTime   string    `json:"phase_start_time"`
	CollectedAt      string    `json:"collected_at"`
	SourceDBDetails  string    `json:"source_db_details"`
	TargetDBDetails  string    `json:"target_db_details"`
	YBVoyagerVersion string    `json:"yb_voyager_version"`
	MigrationPhase   string    `json:"migration_phase"`
	PhasePayload     string    `json:"phase_payload"`
	MigrationType    string    `json:"migration_type"`
	TimeTakenSec     int       `json:"time_taken_sec"`
	Status           string    `json:"status"`
}

type SourceDBDetails struct {
	Host      string `json:"host"`
	DBType    string `json:"db_type"`
	DBVersion string `json:"db_version"`
	DBSize    int64  `json:"total_db_size_bytes"` //bytes
	Role      string `json:"role,omitempty"`      //for differentiating replica details
}

type TargetDBDetails struct {
	Host      string `json:"host"`
	DBVersion string `json:"db_version"`
	NodeCount int    `json:"node_count"`
	Cores     int    `json:"total_cores"`
}

type AssessMigrationPhasePayload struct {
	UnsupportedFeatures  string `json:"unsupported_features"`
	UnsupportedDataTypes string `json:"unsupported_datatypes"`
	Error                string `json:"error,omitempty"`
	TableSizingStats     string `json:"table_sizing_stats"`
	IndexSizingStats     string `json:"index_sizing_stats"`
	SchemaSummary        string `json:"schema_summary"`
	SourceConnectivity   bool   `json:"source_connectivity"`
	CommandLineArgs      string `json:"command_line_args"`
}

type ObjectSizingStats struct {
	SchemaName      string `json:"schema_name"`
	ObjectName      string `json:"object_name"`
	ReadsPerSecond  int64  `json:"reads_per_second"`
	WritesPerSecond int64  `json:"writes_per_second"`
	SizeInBytes     int64  `json:"size_in_bytes"`
}

type ExportSchemaPhasePayload struct {
	StartClean             bool   `json:"start_clean"`
	AppliedRecommendations bool   `json:"applied_recommendations"`
	CommandLineArgs        string `json:"command_line_args"`
}

type AnalyzePhasePayload struct {
	Issues          string `json:"issues"`
	DatabaseObjects string `json:"database_objects"`
}
type ExportDataPhasePayload struct {
	ParallelJobs            int64  `json:"parallel_jobs"`
	TotalRows               int64  `json:"total_rows_exported"`
	LargestTableRows        int64  `json:"largest_table_rows_exported"`
	StartClean              bool   `json:"start_clean"`
	ExportSnapshotMechanism string `json:"export_snapshot_mechanism,omitempty"`
	//TODO: see if these three can be changed to not use omitempty to put the data for 0 rate or total events
	Phase               string `json:"phase,omitempty"`
	TotalExportedEvents int64  `json:"total_exported_events,omitempty"`
	EventsExportRate    int64  `json:"events_export_rate_3m,omitempty"`
	LiveWorkflowType    string `json:"live_workflow_type,omitempty"`
	CommandLineArgs     string `json:"command_line_args,omitempty"`
}

type ImportSchemaPhasePayload struct {
	ContinueOnError    bool     `json:"continue_on_error"`
	Errors             []string `json:"errors"`
	PostSnapshotImport bool     `json:"post_snapshot_import"`
	StartClean         bool     `json:"start_clean"`
	CommandLineArgs    string   `json:"command_line_args"`
}

type ImportDataPhasePayload struct {
	ParallelJobs     int64 `json:"parallel_jobs"`
	TotalRows        int64 `json:"total_rows_imported"`
	LargestTableRows int64 `json:"largest_table_rows_imported"`
	StartClean       bool  `json:"start_clean"`
	//TODO: see if these three can be changed to not use omitempty to put the data for 0 rate or total events
	Phase               string `json:"phase,omitempty"`
	TotalImportedEvents int64  `json:"total_imported_events,omitempty"`
	EventsImportRate    int64  `json:"events_import_rate_3m,omitempty"`
	LiveWorkflowType    string `json:"live_workflow_type,omitempty"`
	CommandLineArgs     string `json:"command_line_args"`
}

type ImportDataFilePhasePayload struct {
	ParallelJobs       int64  `json:"parallel_jobs"`
	TotalSize          int64  `json:"total_size_imported"`
	LargestTableSize   int64  `json:"largest_table_size_imported"`
	FileStorageType    string `json:"file_storage_type"`
	StartClean         bool   `json:"start_clean"`
	DataFileParameters string `json:"data_file_parameters"`
}

type DataFileParameters struct {
	FileFormat string `json:"FileFormat"`
	Delimiter  string `json:"Delimiter"`
	HasHeader  bool   `json:"HasHeader"`
	QuoteChar  string `json:"QuoteChar,omitempty"`
	EscapeChar string `json:"EscapeChar,omitempty"`
	NullString string `json:"NullString,omitempty"`
}

type EndMigrationPhasePayload struct {
	CommandLineArgs string `json:"command_line_args"`
}

var DoNotStoreFlags = []string{
	"source-db-password",
	"target-db-password",
	"source-replica-db-password",
	"export-dir",
}

func MarshalledJsonString[T any](value T) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		log.Errorf("callhome: error in parsing %v: %v", reflect.TypeOf(value).Name(), err)
		return ""
	}
	return string(bytes)
}

// [For development] Read ENV VARS for value of SendDiagnostics
func ReadEnvSendDiagnostics() {
	rawSendDiag := os.Getenv("YB_VOYAGER_SEND_DIAGNOSTICS")
	for _, val := range []string{"0", "no", "false"} {
		if rawSendDiag == val {
			SendDiagnostics = false
		}
	}
}

func readCallHomeServiceEnv() {
	host := os.Getenv("LOCAL_CALL_HOME_SERVICE_HOST")
	port := os.Getenv("LOCAL_CALL_HOME_SERVICE_PORT")
	CALL_HOME_SERVICE_HOST = lo.Ternary(host != "", host, CALL_HOME_SERVICE_HOST)

	if port != "" {
		portNum, err := strconv.Atoi(port)
		if err != nil {
			utils.ErrExit("call-home port is not in valid format %s: %s", port, err)
		}
		CALL_HOME_SERVICE_PORT = portNum
	}
}

// Send http request to flask servers after saving locally
func SendPayload(payload *Payload) error {

	if !SendDiagnostics {
		return nil
	}

	//for local call-home setup
	readCallHomeServiceEnv()

	postBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error while creating http request for diagnostics: %v", err)
	}
	requestBody := bytes.NewBuffer(postBody)

	log.Infof("Payload being sent for diagnostic usage: %s\n", string(postBody))
	callhomeURL := fmt.Sprintf("http://%s:%d/", CALL_HOME_SERVICE_HOST, CALL_HOME_SERVICE_PORT)
	resp, err := http.Post(callhomeURL, "application/json", requestBody)

	if err != nil {
		return fmt.Errorf("error while sending diagnostic data: %v", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error while reading HTTP response from call-home server: %v", err)
	}
	log.Infof("HTTP response after sending diagnostic.json: %s\n", string(body))

	return nil
}
