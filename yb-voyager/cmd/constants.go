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

const (
	KB                              = 1024
	MB                              = 1024 * 1024
	META_INFO_DIR_NAME              = "metainfo"
	NEWLINE                         = '\n'
	ORACLE_DEFAULT_PORT             = 1521
	MYSQL_DEFAULT_PORT              = 3306
	POSTGRES_DEFAULT_PORT           = 5432
	YUGABYTEDB_YSQL_DEFAULT_PORT    = 5433
	YUGABYTEDB_DEFAULT_DATABASE     = "yugabyte"
	YUGABYTEDB_DEFAULT_SCHEMA       = "public"
	ORACLE                          = "oracle"
	MYSQL                           = "mysql"
	POSTGRESQL                      = "postgresql"
	YUGABYTEDB                      = "yugabytedb"
	LAST_SPLIT_NUM                  = 0
	SPLIT_INFO_PATTERN              = "[0-9]*.[0-9]*.[0-9]*.[0-9]*"
	LAST_SPLIT_PATTERN              = "0.[0-9]*.[0-9]*.[0-9]*"
	COPY_MAX_RETRY_COUNT            = 10
	MAX_SLEEP_SECOND                = 60
	DEFAULT_BATCH_SIZE_ORACLE       = 10000000
	DEFAULT_BATCH_SIZE_YUGABYTEDB   = 20000
	DEFAULT_BATCH_SIZE_POSTGRESQL   = 100000
	INDEX_RETRY_COUNT               = 5
	DDL_MAX_RETRY_COUNT             = 5
	SCHEMA_VERSION_MISMATCH_ERR     = "Query error: schema version mismatch for table"
	SNAPSHOT_ONLY                   = "snapshot-only"
	SNAPSHOT_AND_CHANGES            = "snapshot-and-changes"
	CHANGES_ONLY                    = "changes-only"
	TARGET_DB                       = "target"
	FF_DB                           = "ff"
	SOURCE_REPLICA_DB_IMPORTER_ROLE = "source_replica_db_importer"
	SOURCE_DB_IMPORTER_ROLE         = "source_db_importer"
	TARGET_DB_IMPORTER_ROLE         = "target_db_importer"
	SOURCE_DB_EXPORTER_ROLE         = "source_db_exporter"
	TARGET_DB_EXPORTER_FF_ROLE      = "target_db_exporter_ff"
	TARGET_DB_EXPORTER_FB_ROLE      = "target_db_exporter_fb"
	IMPORT_FILE_ROLE                = "import_file"
	ROW_UPDATE_STATUS_NOT_STARTED   = 0
	ROW_UPDATE_STATUS_IN_PROGRESS   = 1
	ROW_UPDATE_STATUS_COMPLETED     = 3
	COLOCATION_CLAUSE               = "colocation"
	//phase names used in call-home payload
	ANALYZE_PHASE          = "analyze-schema"
	EXPORT_SCHEMA_PHASE    = "export-schema"
	EXPORT_DATA_PHASE      = "export-data"
	IMPORT_SCHEMA_PHASE    = "import-schema"
	IMPORT_DATA_PHASE      = "import-data"
	END_MIGRATION_PHASE    = "end-migration"
	ASSESS_MIGRATION_PHASE = "assess-migration"
	//...more phases
	OFFLINE              = "offline"
	LIVE_MIGRATION       = "live migration"
	ERROR                = "ERROR"
	EXIT                 = "EXIT"
	COMPLETE             = "COMPLETE"
	COMPLETE_WITH_ERRORS = "COMPLETE-WITH-ERRORS"
)

var supportedSourceDBTypes = []string{ORACLE, MYSQL, POSTGRESQL, YUGABYTEDB}
var validExportTypes = []string{SNAPSHOT_ONLY, CHANGES_ONLY, SNAPSHOT_AND_CHANGES}

var validSSLModes = map[string][]string{
	"mysql":      {"disable", "prefer", "require", "verify-ca", "verify-full"},
	"postgresql": {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"},
	"yugabytedb": {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"},
}

var EVENT_BATCH_MAX_RETRY_COUNT = 50
