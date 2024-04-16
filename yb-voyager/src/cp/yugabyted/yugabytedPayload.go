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
package yugabyted

import "github.com/google/uuid"

type MigrationEvent struct {
	MigrationUUID       uuid.UUID `json:"migration_uuid"`
	MigrationPhase      int       `json:"migration_phase"`
	InvocationSequence  int       `json:"invocation_sequence"`
	MigrationDirectory  string    `json:"migration_dir"`
	DatabaseName        string    `json:"database_name"`
	SchemaName          string    `json:"schema_name"`
	Payload             string    `json:"payload"`
	DBType              string    `json:"db_type"`
	Status              string    `json:"status"`
	InvocationTimestamp string    `json:"invocation_timestamp"`
}

var MIGRATION_PHASE_MAP = map[string]int{
	"ASSESS MIGRATION": 1,
	"EXPORT SCHEMA":    2,
	"ANALYZE SCHEMA":   3,
	"EXPORT DATA":      4,
	"IMPORT SCHEMA":    5,
	"IMPORT DATA":      6,
}
