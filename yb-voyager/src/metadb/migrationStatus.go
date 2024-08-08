package metadb

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

type MigrationStatusRecord struct {
	MigrationUUID                                   string            `json:"MigrationUUID"`
	VoyagerVersion                                  string            `json:"VoyagerVersion"`
	ExportType                                      string            `json:"ExportType"`
	ArchivingEnabled                                bool              `json:"ArchivingEnabled"`
	FallForwardEnabled                              bool              `json:"FallForwardEnabled"`
	FallbackEnabled                                 bool              `json:"FallbackEnabled"`
	TargetDBConf                                    *tgtdb.TargetConf `json:"TargetDBConf"`
	SourceReplicaDBConf                             *tgtdb.TargetConf `json:"SourceReplicaDBConf"`
	SourceDBAsTargetConf                            *tgtdb.TargetConf `json:"SourceDBAsTargetConf"`
	TableListExportedFromSource                     []string          `json:"TableListExportedFromSource"`
	SourceDBConf                                    *srcdb.Source     `json:"SourceDBConf"`
	CutoverToTargetRequested                        bool              `json:"CutoverToTargetRequested"`
	CutoverProcessedBySourceExporter                bool              `json:"CutoverProcessedBySourceExporter"`
	CutoverProcessedByTargetImporter                bool              `json:"CutoverProcessedByTargetImporter"`
	ExportFromTargetFallForwardStarted              bool              `json:"ExportFromTargetFallForwardStarted"`
	CutoverToSourceReplicaRequested                 bool              `json:"CutoverToSourceReplicaRequested"`
	CutoverToSourceReplicaProcessedByTargetExporter bool              `json:"CutoverToSourceReplicaProcessedByTargetExporter"`
	CutoverToSourceReplicaProcessedBySRImporter     bool              `json:"CutoverToSourceReplicaProcessedBySRImporter"`
	ExportFromTargetFallBackStarted                 bool              `json:"ExportFromTargetFallBackStarted"`
	CutoverToSourceRequested                        bool              `json:"CutoverToSourceRequested"`
	CutoverToSourceProcessedByTargetExporter        bool              `json:"CutoverToSourceProcessedByTargetExporter"`
	CutoverToSourceProcessedBySourceImporter        bool              `json:"CutoverToSourceProcessedBySourceImporter"`
	ExportSchemaDone                                bool              `json:"ExportSchemaDone"`
	ExportDataDone                                  bool              `json:"ExportDataDone"`
	YBCDCStreamID                                   string            `json:"YBCDCStreamID"`
	EndMigrationRequested                           bool              `json:"EndMigrationRequested"`
	PGReplicationSlotName                           string            `json:"PGReplicationSlotName"` // of the format voyager_<migrationUUID> (with replace "-" -> "_")
	PGPublicationName                               string            `json:"PGPublicationName"`     // of the format voyager_<migrationUUID> (with replace "-" -> "_")
	SnapshotMechanism                               string            `json:"SnapshotMechanism"`     // one of (debezium, pg_dump, ora2pg)
	SourceRenameTablesMap                           map[string]string `json:"SourceRenameTablesMap"` // map of source table.Qualified.Unquoted -> table.Qualified.Unquoted for renaming the leaf partitions to root table in case of PG migration
	TargetRenameTablesMap                           map[string]string `json:"TargetRenameTablesMap"` // map of target table.Qualified.Unquoted -> table.Qualified.Unquoted for renaming the leaf partitions to root table in case of PG migration
	IsExportTableListSet                            bool              `json:"IsExportTableListSet"`
	MigrationAssessmentDone                         bool              `json:"MigrationAssessmentDone"`
}

const MIGRATION_STATUS_KEY = "migration_status"

func (m *MetaDB) UpdateMigrationStatusRecord(updateFn func(*MigrationStatusRecord)) error {
	return UpdateJsonObjectInMetaDB(m, MIGRATION_STATUS_KEY, updateFn)
}

func (m *MetaDB) GetMigrationStatusRecord() (*MigrationStatusRecord, error) {
	record := new(MigrationStatusRecord)
	found, err := m.GetJsonObject(nil, MIGRATION_STATUS_KEY, record)
	if err != nil {
		return nil, fmt.Errorf("error while getting migration status record from meta db: %w", err)
	}
	if !found {
		return nil, nil
	}
	return record, nil
}

func (m *MetaDB) InitMigrationStatusRecord() error {
	return m.UpdateMigrationStatusRecord(func(record *MigrationStatusRecord) {
		if record != nil && record.MigrationUUID != "" {
			return // already initialized
		}
		if record.VoyagerVersion == "" {
			record.VoyagerVersion = utils.YB_VOYAGER_VERSION
		}

		record.MigrationUUID = uuid.New().String()
		record.ExportType = utils.SNAPSHOT_ONLY
	})
}

func (msr *MigrationStatusRecord) IsSnapshotExportedViaDebezium() bool {
	return msr.SnapshotMechanism == "debezium"
}
